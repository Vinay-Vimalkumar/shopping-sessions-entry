//! Shopping Sessions pipeline: exact implementation of rules.md.
//! Multi-threaded: parse -> partitioned dedup -> per-person grouping -> sessions/attribution -> Parquet.

mod pq;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use xxhash_rust::xxh3::{xxh3_128, xxh3_64};

const DAY_MS: i64 = 86_400_000;
const GAP_MS: u32 = 1_800_000;
const WEEK_MS: i64 = 604_800_000;
const LATE_MS: i64 = 86_400_000;


// ---------------------------------------------------------------- allocator
// Big allocations come straight from mmap with transparent huge pages requested: fewer page
// faults on fresh memory and much cheaper teardown.
#[cfg(target_os = "linux")]
mod halloc {
    use std::alloc::{GlobalAlloc, Layout, System};
    pub struct HugeAlloc;
    const BIG: usize = 1 << 20;
    const HP: usize = 2 << 20;
    #[inline]
    fn round(n: usize) -> usize {
        (n + HP - 1) & !(HP - 1)
    }
    #[inline]
    fn big(size: usize, align: usize) -> bool {
        size >= BIG && align <= 4096
    }
    unsafe impl GlobalAlloc for HugeAlloc {
        unsafe fn alloc(&self, l: Layout) -> *mut u8 {
            if big(l.size(), l.align()) {
                let sz = round(l.size());
                let p = libc::mmap(
                    std::ptr::null_mut(),
                    sz,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                    -1,
                    0,
                );
                if p == libc::MAP_FAILED {
                    return std::ptr::null_mut();
                }
                libc::madvise(p, sz, libc::MADV_HUGEPAGE);
                p as *mut u8
            } else {
                System.alloc(l)
            }
        }
        unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
            if big(l.size(), l.align()) {
                self.alloc(l)
            } else {
                System.alloc_zeroed(l)
            }
        }
        unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
            if big(l.size(), l.align()) {
                libc::munmap(p as *mut libc::c_void, round(l.size()));
            } else {
                System.dealloc(p, l)
            }
        }
        unsafe fn realloc(&self, p: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
            let ob = big(l.size(), l.align());
            let nb = big(new_size, l.align());
            if ob && nb {
                let q = libc::mremap(p as *mut libc::c_void, round(l.size()), round(new_size), libc::MREMAP_MAYMOVE);
                if q == libc::MAP_FAILED {
                    return std::ptr::null_mut();
                }
                libc::madvise(q, round(new_size), libc::MADV_HUGEPAGE);
                q as *mut u8
            } else if !ob && !nb {
                System.realloc(p, l, new_size)
            } else {
                let nl = Layout::from_size_align_unchecked(new_size, l.align());
                let q = self.alloc(nl);
                if !q.is_null() {
                    std::ptr::copy_nonoverlapping(p, q, l.size().min(new_size));
                    self.dealloc(p, l);
                }
                q
            }
        }
    }
}
#[cfg(target_os = "linux")]
#[global_allocator]
static GA: halloc::HugeAlloc = halloc::HugeAlloc;

/// Records staged per partition before a write-combined flush.
const WK: usize = 8;

/// Per-thread partitioned buffer: one big allocation, fixed capacity per partition, overflow Vecs.
/// Pushes are staged WK at a time in a small cache-resident buffer and flushed as whole cache
/// lines (non-temporal stores on Linux/AVX2 builds), so scattering does not stall on misses.
struct PartBuf<T: Copy> {
    mem: Vec<T>,
    cap: usize,
    lens: Vec<u32>,
    over: Vec<Vec<T>>,
    stage: Vec<T>,
    slen: Vec<u32>,
    nt: bool,
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn nt_copy(src: *const u8, dst: *mut u8, bytes: usize) {
    use std::arch::x86_64::*;
    let mut i = 0;
    while i < bytes {
        let v = _mm256_loadu_si256(src.add(i) as *const __m256i);
        _mm256_stream_si256(dst.add(i) as *mut __m256i, v);
        i += 32;
    }
}

impl<T: Copy> PartBuf<T> {
    fn new(nparts: usize, cap: usize) -> Self {
        let cap = (cap + WK - 1) / WK * WK;
        let mem: Vec<T> = Vec::with_capacity(nparts * cap);
        let nt = cfg!(all(target_arch = "x86_64", target_feature = "avx2"))
            && (mem.as_ptr() as usize) % 32 == 0
            && (std::mem::size_of::<T>() * WK) % 32 == 0;
        PartBuf {
            mem,
            cap,
            lens: vec![0; nparts],
            over: (0..nparts).map(|_| Vec::new()).collect(),
            stage: Vec::with_capacity(nparts * WK),
            slen: vec![0; nparts],
            nt,
        }
    }
    #[inline(always)]
    fn push(&mut self, p: usize, v: T) {
        unsafe {
            let n = *self.slen.get_unchecked(p) as usize;
            self.stage.as_mut_ptr().add(p * WK + n).write(v);
            if n + 1 < WK {
                *self.slen.get_unchecked_mut(p) = (n + 1) as u32;
            } else {
                *self.slen.get_unchecked_mut(p) = 0;
                self.flush_full(p);
            }
        }
    }
    #[inline(always)]
    unsafe fn flush_full(&mut self, p: usize) {
        let l = *self.lens.get_unchecked(p) as usize;
        let src = self.stage.as_ptr().add(p * WK);
        if l + WK <= self.cap {
            let dst = self.mem.as_mut_ptr().add(p * self.cap + l);
            #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
            {
                if self.nt {
                    nt_copy(src as *const u8, dst as *mut u8, WK * std::mem::size_of::<T>());
                } else {
                    std::ptr::copy_nonoverlapping(src, dst, WK);
                }
            }
            #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
            std::ptr::copy_nonoverlapping(src, dst, WK);
            *self.lens.get_unchecked_mut(p) = (l + WK) as u32;
        } else {
            self.over.get_unchecked_mut(p).extend_from_slice(std::slice::from_raw_parts(src, WK));
        }
    }
    /// Flush staged records; must be called before reading.
    fn finish(&mut self) {
        unsafe {
            for p in 0..self.lens.len() {
                let n = self.slen[p] as usize;
                if n == 0 {
                    continue;
                }
                let l = self.lens[p] as usize;
                let src = self.stage.as_ptr().add(p * WK);
                if l + n <= self.cap {
                    std::ptr::copy_nonoverlapping(src, self.mem.as_mut_ptr().add(p * self.cap + l), n);
                    self.lens[p] = (l + n) as u32;
                } else {
                    self.over[p].extend_from_slice(std::slice::from_raw_parts(src, n));
                }
                self.slen[p] = 0;
            }
            #[cfg(target_arch = "x86_64")]
            std::arch::x86_64::_mm_sfence();
        }
        self.stage = Vec::new();
    }
    #[inline(always)]
    fn main(&self, p: usize) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.mem.as_ptr().add(p * self.cap), self.lens[p] as usize) }
    }
    #[inline(always)]
    fn over(&self, p: usize) -> &[T] {
        &self.over[p]
    }
    fn len(&self, p: usize) -> usize {
        self.lens[p] as usize + self.over[p].len()
    }
}

/// Release big structures on several threads at once (process teardown is otherwise serial).
fn par_drop<T: Send>(items: Vec<T>) {
    std::thread::scope(|s| {
        for x in items {
            s.spawn(move || drop(x));
        }
    });
}

// ---------------------------------------------------------------- logging
static T0: OnceLock<Instant> = OnceLock::new();
static LOG_ON: OnceLock<bool> = OnceLock::new();
/// With SS_LOG on Linux: log resident and huge-page anonymous memory.
fn log_mem(tag: &str) {
    #[cfg(target_os = "linux")]
    {
        if *LOG_ON.get_or_init(|| std::env::var_os("SS_LOG").is_some()) {
            let sm = std::fs::read_to_string("/proc/self/smaps_rollup").unwrap_or_default();
            let pick = |k: &str| sm.lines().find(|l| l.starts_with(k)).map(|l| l.split_whitespace().nth(1).unwrap_or("0").to_string()).unwrap_or_default();
            log(&format!("mem {tag}: rss {} kB, anon {} kB, anon-huge {} kB", pick("Rss:"), pick("Anonymous:"), pick("AnonHugePages:")));
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = tag;
}

fn log(msg: &str) {
    if *LOG_ON.get_or_init(|| std::env::var_os("SS_LOG").is_some()) {
        let t = T0.get().unwrap().elapsed().as_secs_f64() * 1000.0;
        eprintln!("[{t:8.1} ms] {msg}");
    }
}

// ---------------------------------------------------------------- string refs
// A string inside a leaked mmap, packed into 64 bits: 47-bit pointer, 17-bit length.
// Lengths >= LONG go through a registry.
const LONG: u64 = (1 << 17) - 1;
static LONG_STRS: Mutex<Vec<&'static [u8]>> = Mutex::new(Vec::new());

#[inline]
fn sref(s: &'static [u8]) -> u64 {
    let len = s.len() as u64;
    if len < LONG {
        (s.as_ptr() as u64) | (len << 47)
    } else {
        let mut g = LONG_STRS.lock().unwrap();
        g.push(s);
        ((g.len() - 1) as u64) | (LONG << 47)
    }
}

#[inline]
fn sget(r: u64) -> &'static [u8] {
    let len = r >> 47;
    let p = r & ((1u64 << 47) - 1);
    if len < LONG {
        unsafe { std::slice::from_raw_parts(p as *const u8, len as usize) }
    } else {
        LONG_STRS.lock().unwrap()[p as usize]
    }
}

// ---------------------------------------------------------------- CSV
/// Split a line into exactly N fields per the rules.md dialect. Returns false if malformed.
#[inline(always)]
fn split_fields<const N: usize>(line: &'static [u8], out: &mut [&'static [u8]; N]) -> bool {
    let mut k = 0usize;
    let mut s = 0usize;
    let mut quotes = false;
    let n = line.len();
    let mut i = 0usize;
    while i < n {
        let c = line[i];
        if c == b',' {
            if k + 1 >= N {
                return false;
            }
            out[k] = &line[s..i];
            k += 1;
            s = i + 1;
        } else if c == b'"' {
            quotes = true;
        }
        i += 1;
    }
    if k != N - 1 {
        return false;
    }
    out[N - 1] = &line[s..n];
    if quotes {
        for f in out.iter_mut() {
            let v: &'static [u8] = f;
            if !v.is_empty() && v[0] == b'"' {
                if v.len() < 2 || v[v.len() - 1] != b'"' {
                    return false;
                }
                let inner = &v[1..v.len() - 1];
                if memchr::memchr(b'"', inner).is_some() {
                    return false;
                }
                *f = inner;
            } else if memchr::memchr(b'"', v).is_some() {
                return false;
            }
        }
    }
    true
}

// ---------------------------------------------------------------- SIMD line scanner
/// Masks of '\n', ',' and '"' for 64 bytes at p (all 64 bytes must be readable).
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let nl = _mm256_set1_epi8(b'\n' as i8);
    let cm = _mm256_set1_epi8(b',' as i8);
    let qt = _mm256_set1_epi8(b'"' as i8);
    let m = |x: __m256i, y: __m256i| -> u64 {
        let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(x, y)) as u32 as u64;
        lo
    };
    let n = m(a, nl) | (m(b, nl) << 32);
    let c = m(a, cm) | (m(b, cm) << 32);
    let q = m(a, qt) | (m(b, qt) << 32);
    (n, c, q)
}

/// Line callback: (line_start, line_end, comma offsets relative to line_start, comma count, quote count).
/// Calls f for every line that starts in [start, end) of data. Lines are split on '\n' only; a final
/// line without '\n' is a line.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,popcnt,bmi1")]
unsafe fn scan_lines<F: FnMut(usize, usize, &[u32; 8], usize, u32)>(data: &[u8], start: usize, end: usize, f: &mut F) {
    let len = data.len();
    let mut pos = start;
    if pos > 0 && *data.get_unchecked(pos - 1) != b'\n' {
        match memchr::memchr(b'\n', &data[pos..]) {
            Some(i) => pos += i + 1,
            None => return,
        }
    }
    if pos >= end || pos >= len {
        return;
    }
    let mut line_start = pos;
    let mut seps = [0u32; 8];
    let mut nsep = 0usize;
    let mut qcount = 0u32;
    let mut base = pos;
    let mut pad: [u8; 64];
    loop {
        let (mn, mc, mq) = if base + 64 <= len {
            masks64(data.as_ptr().add(base))
        } else {
            let rem = len - base;
            pad = [0u8; 64];
            pad[..rem].copy_from_slice(&data[base..]);
            masks64(pad.as_ptr())
        };
        let mut sep = mn | mc;
        let mut consumed = 0u32; // bits below belong to finished lines
        while sep != 0 {
            let b = sep.trailing_zeros();
            sep &= sep - 1;
            if (mn >> b) & 1 != 0 {
                let below = if b == 0 { 0 } else { u64::MAX >> (64 - b) };
                let from = if consumed == 0 { u64::MAX } else { !(u64::MAX >> (64 - consumed)) };
                let lq = qcount + (mq & below & from).count_ones();
                let le = base + b as usize;
                f(line_start, le, &seps, nsep, lq);
                line_start = le + 1;
                nsep = 0;
                qcount = 0;
                consumed = b + 1;
                if line_start >= end || line_start >= len {
                    return;
                }
            } else {
                if nsep < 8 {
                    *seps.get_unchecked_mut(nsep) = (base + b as usize - line_start) as u32;
                }
                nsep += 1;
            }
        }
        let from = if consumed == 0 { u64::MAX } else if consumed >= 64 { 0 } else { !(u64::MAX >> (64 - consumed)) };
        qcount += (mq & from).count_ones();
        base += 64;
        if base >= len {
            // last line without trailing newline
            if line_start < len {
                f(line_start, len, &seps, nsep, qcount);
            }
            return;
        }
    }
}

/// Line-at-a-time kernel: for each line starting in [start, end), find its newline, commas and
/// quotes from 128-byte bitmask windows, resolve exactly N fields (quotes stripped, branch-free
/// validation from the quote mask) and call f. Malformed lines are skipped. Lines of 128+ bytes
/// take the exact scalar path.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,popcnt")]
unsafe fn scan_fields<const N: usize, F: FnMut(&[&'static [u8]; N])>(
    data: &'static [u8],
    start: usize,
    end: usize,
    f: &mut F,
) {
    let len = data.len();
    let mut s = start;
    if s > 0 && *data.get_unchecked(s - 1) != b'\n' {
        match memchr::memchr(b'\n', &data[s..]) {
            Some(i) => s += i + 1,
            None => return,
        }
    }
    let mut pad: [u8; 128];
    let pf = PF_DIST.load(Ordering::Relaxed);
    while s < end && s < len {
        let avail = len - s;
        if pf != 0 && avail > pf + 64 {
            prefetch(data.as_ptr().add(s + pf));
        }
        let p: *const u8 = if avail >= 128 {
            data.as_ptr().add(s)
        } else {
            pad = [0u8; 128];
            pad[..avail].copy_from_slice(&data[s..]);
            pad.as_ptr()
        };
        let (n0, c0, q0) = masks64(p);
        let e: usize;
        let cm: u128;
        let qm: u128;
        if n0 != 0 {
            let ee = n0.trailing_zeros();
            let below = (1u64 << ee) - 1;
            e = ee as usize;
            cm = (c0 & below) as u128;
            qm = (q0 & below) as u128;
        } else {
            let (n1, c1, q1) = masks64(p.add(64));
            if n1 != 0 {
                let ee = n1.trailing_zeros();
                let below = (1u64 << ee) - 1;
                e = 64 + ee as usize;
                cm = (c0 as u128) | (((c1 & below) as u128) << 64);
                qm = (q0 as u128) | (((q1 & below) as u128) << 64);
            } else if avail < 128 {
                // final line of the file, no trailing newline
                e = avail;
                cm = (c0 as u128) | ((c1 as u128) << 64);
                qm = (q0 as u128) | ((q1 as u128) << 64);
            } else {
                // long line: exact scalar path
                let le = match memchr::memchr(b'\n', &data[s..]) {
                    Some(i) => s + i,
                    None => len,
                };
                let mut fl: [&'static [u8]; N] = [&[]; N];
                if split_fields(&data[s..le], &mut fl) {
                    f(&fl);
                }
                s = le + 1;
                continue;
            }
        }
        if cm.count_ones() as usize == N - 1 {
            let base = data.as_ptr().add(s);
            let mut fl: [&'static [u8]; N] = [&[]; N];
            let mut bits = cm;
            let mut st = 0usize;
            let q = qm.count_ones();
            if q == 0 {
                for k in 0..N {
                    let en = if k + 1 < N {
                        let c = bits.trailing_zeros() as usize;
                        bits &= bits - 1;
                        c
                    } else {
                        e
                    };
                    *fl.get_unchecked_mut(k) = std::slice::from_raw_parts(base.add(st), en - st);
                    st = en + 1;
                }
                f(&fl);
            } else {
                let mut expect = 0u32;
                let mut bad = 0u32;
                for k in 0..N {
                    let en = if k + 1 < N {
                        let c = bits.trailing_zeros() as usize;
                        bits &= bits - 1;
                        c
                    } else {
                        e
                    };
                    let n = en - st;
                    let qd = ((qm >> st) & 1) as u32;
                    let ql = ((qm >> (en.wrapping_sub(1) & 127)) & 1) as u32;
                    bad |= qd & (((n < 2) as u32) | (ql ^ 1));
                    expect += 2 * qd;
                    let o = qd as usize;
                    *fl.get_unchecked_mut(k) = std::slice::from_raw_parts(base.add(st + o), n.saturating_sub(2 * o));
                    st = en + 1;
                }
                if bad == 0 && expect == q {
                    f(&fl);
                }
            }
        }
        s += e + 1;
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt")]
unsafe fn scan_fields_px<const N: usize, F: FnMut(&[&'static [u8]; N])>(
    data: &'static [u8],
    start: usize,
    end: usize,
    f: &mut F,
) {
    let len = data.len();
    let mut s = start;
    if s > 0 && *data.get_unchecked(s - 1) != b'\n' {
        match memchr::memchr(b'\n', &data[s..]) {
            Some(i) => s += i + 1,
            None => return,
        }
    }
    let mut pad: [u8; 128];
    let pf = PF_DIST.load(Ordering::Relaxed);
    while s < end && s < len {
        let avail = len - s;
        if pf != 0 && avail > pf + 64 {
            prefetch(data.as_ptr().add(s + pf));
        }
        let p: *const u8 = if avail >= 128 {
            data.as_ptr().add(s)
        } else {
            pad = [0u8; 128];
            pad[..avail].copy_from_slice(&data[s..]);
            pad.as_ptr()
        };
        let (n0, c0, q0) = masks64(p);
        let e: usize;
        let cm: u128;
        let qm: u128;
        if n0 != 0 {
            let ee = n0.trailing_zeros();
            let below = (1u64 << ee) - 1;
            e = ee as usize;
            cm = (c0 & below) as u128;
            qm = (q0 & below) as u128;
        } else {
            let (n1, c1, q1) = masks64(p.add(64));
            if n1 != 0 {
                let ee = n1.trailing_zeros();
                let below = (1u64 << ee) - 1;
                e = 64 + ee as usize;
                cm = (c0 as u128) | (((c1 & below) as u128) << 64);
                qm = (q0 as u128) | (((q1 & below) as u128) << 64);
            } else if avail < 128 {
                // final line of the file, no trailing newline
                e = avail;
                cm = (c0 as u128) | ((c1 as u128) << 64);
                qm = (q0 as u128) | ((q1 as u128) << 64);
            } else {
                // long line: exact scalar path
                let le = match memchr::memchr(b'\n', &data[s..]) {
                    Some(i) => s + i,
                    None => len,
                };
                let mut fl: [&'static [u8]; N] = [&[]; N];
                if split_fields(&data[s..le], &mut fl) {
                    f(&fl);
                }
                s = le + 1;
                continue;
            }
        }
        let clo = cm as u64;
        let chi = (cm >> 64) as u64;
        let plo = clo.count_ones();
        if (plo + chi.count_ones()) as usize == N - 1 && (clo & 1) == 0 && e > 0 {
            use std::arch::x86_64::{_pdep_u64, _pext_u64};
            let base = data.as_ptr().add(s);
            let mut pos = [0usize; 8];
            for k in 0..N - 1 {
                let kk = k as u32;
                let c = if kk < plo {
                    _pdep_u64(1u64 << kk, clo).trailing_zeros() as usize
                } else {
                    64 + _pdep_u64(1u64 << (kk - plo), chi).trailing_zeros() as usize
                };
                *pos.get_unchecked_mut(k) = c;
            }
            *pos.get_unchecked_mut(N - 1) = e;
            let mut fl: [&'static [u8]; N] = [&[]; N];
            if qm == 0 {
                let mut st = 0usize;
                for k in 0..N {
                    let en = *pos.get_unchecked(k);
                    *fl.get_unchecked_mut(k) = std::slice::from_raw_parts(base.add(st), en - st);
                    st = en + 1;
                }
                f(&fl);
            } else {
                let starts: u128 = (cm << 1) | 1;
                let ends: u128 = (cm >> 1) | (1u128 << (e - 1));
                let qs = qm & starts;
                let qe = qm & ends;
                let pext128 = |x: u128, m: u128| -> u64 {
                    let ml = m as u64;
                    _pext_u64(x as u64, ml) | (_pext_u64((x >> 64) as u64, (m >> 64) as u64) << ml.count_ones())
                };
                if qm == (qs | qe) && (qs & qe) == 0 {
                    let fq = pext128(qs, starts);
                    if fq == pext128(qe, ends) {
                        let mut st = 0usize;
                        for k in 0..N {
                            let en = *pos.get_unchecked(k);
                            let o = ((fq >> k) & 1) as usize;
                            *fl.get_unchecked_mut(k) = std::slice::from_raw_parts(base.add(st + o), en - st - 2 * o);
                            st = en + 1;
                        }
                        f(&fl);
                    }
                }
            }
        }
        s += e + 1;
    }
}

/// Resolve N fields of a scanned line. Returns false if malformed.
#[inline(always)]
fn fields_from_scan<const N: usize>(
    data: &'static [u8],
    ls: usize,
    le: usize,
    seps: &[u32; 8],
    nsep: usize,
    q: u32,
    out: &mut [&'static [u8]; N],
) -> bool {
    if nsep != N - 1 {
        return false;
    }
    let mut s = ls;
    for k in 0..N {
        let e = if k + 1 < N { ls + seps[k] as usize } else { le };
        out[k] = unsafe { data.get_unchecked(s..e) };
        s = e + 1;
    }
    if q != 0 {
        let mut expect = 0u32;
        for fl in out.iter_mut() {
            let v: &'static [u8] = fl;
            if !v.is_empty() && v[0] == b'"' {
                if v.len() < 2 || v[v.len() - 1] != b'"' {
                    return false;
                }
                expect += 2;
                *fl = &v[1..v.len() - 1];
            }
        }
        if expect != q {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------- timestamps
#[inline(always)]
fn d2(b: &[u8], i: usize) -> Option<u32> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a < 10 && c < 10 {
        Some(a as u32 * 10 + c as u32)
    } else {
        None
    }
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = ((m + 9) % 12) as i64;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline(always)]
fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// "YYYY-MM-DDTHH:MM:SS" (first 19 bytes) -> seconds since epoch
#[inline(always)]
fn parse_dt(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = (d2(b, 0)? * 100 + d2(b, 2)?) as i64;
    let mo = d2(b, 5)?;
    let d = d2(b, 8)?;
    let h = d2(b, 11)?;
    let mi = d2(b, 14)?;
    let s = d2(b, 17)?;
    if mo < 1 || mo > 12 || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    Some(days_from_civil(y, mo, d) * 86400 + (h * 3600 + mi * 60 + s) as i64)
}

/// Event/click ts -> epoch ms, None if bad form.
#[inline(always)]
fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 0 {
        return None;
    }
    if n == 20 && b[19] == b'Z' {
        return Some(parse_dt(b)? * 1000);
    }
    if b[0].is_ascii_digit() {
        // F3 if all digits
        let mut v: i64 = 0;
        let mut all = true;
        let mut overflow = false;
        for &c in b {
            let d = c.wrapping_sub(b'0');
            if d >= 10 {
                all = false;
                break;
            }
            if !overflow {
                match v.checked_mul(10).and_then(|x| x.checked_add(d as i64)) {
                    Some(x) => v = x,
                    None => overflow = true,
                }
            }
        }
        if all {
            return if overflow { None } else { Some(v) };
        }
    }
    if n == 25 {
        let sign = b[19];
        if (sign != b'+' && sign != b'-') || b[22] != b':' {
            return None;
        }
        let oh = d2(b, 20)?;
        let om = d2(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 3600 + om * 60) as i64;
        let local = parse_dt(b)?;
        let utc = if sign == b'+' { local - off } else { local + off };
        return Some(utc * 1000);
    }
    None
}

// ---- fast timestamp paths (exact; anything unusual falls back to the slow parsers above)
#[inline(always)]
fn ld8(b: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(b[i..i + 8].try_into().unwrap())
}

/// "HH:MM:SS" at b[i..i+8] -> seconds of day (validated)
#[inline(always)]
fn hms(b: &[u8], i: usize) -> Option<i64> {
    let w = ld8(b, i);
    const HM: u64 = 0xF0F0_00F0_F000_F0F0;
    const EX: u64 = 0x3030_0030_3000_3030;
    const SEPM: u64 = 0x0000_FF00_00FF_0000;
    const SEPV: u64 = 0x0000_3A00_003A_0000;
    if (w & SEPM) != SEPV || (w & HM) != EX || ((w.wrapping_add(0x0606_0006_0600_0606)) & HM) != EX {
        return None;
    }
    let d = w - EX;
    let h = (d & 0xFF) * 10 + ((d >> 8) & 0xFF);
    let m = ((d >> 24) & 0xFF) * 10 + ((d >> 32) & 0xFF);
    let s = ((d >> 48) & 0xFF) * 10 + ((d >> 56) & 0xFF);
    if h > 23 || m > 59 || s > 59 {
        return None;
    }
    Some((h * 3600 + m * 60 + s) as i64)
}

/// 8 ASCII digits (first digit most significant) -> value
#[inline(always)]
fn digits8(w: u64) -> Option<u64> {
    const H: u64 = 0xF0F0_F0F0_F0F0_F0F0;
    const Z: u64 = 0x3030_3030_3030_3030;
    if (w & H) != Z || ((w.wrapping_add(0x0606_0606_0606_0606)) & H) != Z {
        return None;
    }
    let mut v = w - Z;
    v = v.wrapping_mul(10).wrapping_add(v >> 8);
    v = ((v & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000 << 32))
        .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000 << 32))))
        >> 32;
    Some(v)
}

/// Months around the window: "YYYY-MM-" prefix (LE u64) -> (epoch day of the 1st, days in month)
struct DateTab {
    pre: [u64; 8],
    day0: [i64; 8],
    dim: [u32; 8],
    n: usize,
}

impl DateTab {
    fn new(lo_ms: i64, hi_ms: i64) -> Self {
        let mut t = DateTab { pre: [0; 8], day0: [0; 8], dim: [0; 8], n: 0 };
        let civil = |days: i64| -> (i64, u32, u32) {
            // inverse of days_from_civil
            let z = days + 719468;
            let era = if z >= 0 { z } else { z - 146096 } / 146097;
            let doe = z - era * 146097;
            let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
            let y = yoe + era * 400;
            let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
            let mp = (5 * doy + 2) / 153;
            let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
            let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
            (if m <= 2 { y + 1 } else { y }, m, d)
        };
        let mut day = lo_ms.div_euclid(DAY_MS);
        let last = hi_ms.div_euclid(DAY_MS);
        while day <= last && t.n < 8 {
            let (y, m, _) = civil(day);
            if (0..=9999).contains(&y) {
                let s = format!("{:04}-{:02}-", y, m);
                let p = u64::from_le_bytes(s.as_bytes()[..8].try_into().unwrap());
                if !t.pre[..t.n].contains(&p) {
                    t.pre[t.n] = p;
                    t.day0[t.n] = days_from_civil(y, m, 1);
                    t.dim[t.n] = days_in_month(y, m);
                    t.n += 1;
                }
            }
            day += 1;
        }
        t
    }
    /// Epoch day of "YYYY-MM-DD" at b[0..10]: Some(Some(day)) valid, Some(None) invalid, None = not in table.
    #[inline(always)]
    fn day(&self, b: &[u8]) -> Option<Option<i64>> {
        let p = ld8(b, 0);
        for k in 0..self.n {
            if self.pre[k] == p {
                let dd = match d2(b, 8) {
                    Some(v) => v,
                    None => return Some(None),
                };
                if dd < 1 || dd > self.dim[k] {
                    return Some(None);
                }
                return Some(Some(self.day0[k] + dd as i64 - 1));
            }
        }
        None
    }
}

/// F1/F2/F3 -> epoch ms, exact.
#[inline(always)]
fn parse_ts_fast(b: &[u8], tab: &DateTab) -> Option<i64> {
    let n = b.len();
    if n == 20 {
        if b[19] == b'Z' && b[10] == b'T' {
            return match tab.day(b) {
                Some(Some(day)) => Some((day * 86400 + hms(b, 11)?) * 1000),
                Some(None) => None,
                None => parse_ts(b),
            };
        }
        return parse_ts(b);
    }
    if n == 13 {
        if let (Some(a), Some(c)) = (digits8(ld8(b, 0)), digits8(ld8(b, 5))) {
            return Some((a * 100_000 + c % 100_000) as i64);
        }
        return parse_ts(b);
    }
    if n == 25 && b[10] == b'T' && b[22] == b':' {
        let sign = b[19];
        if sign == b'+' || sign == b'-' {
            return match tab.day(b) {
                Some(Some(day)) => {
                    let oh = d2(b, 20)?;
                    let om = d2(b, 23)?;
                    if oh > 23 || om > 59 {
                        return None;
                    }
                    let off = (oh * 3600 + om * 60) as i64;
                    let local = day * 86400 + hms(b, 11)?;
                    Some((if sign == b'+' { local - off } else { local + off }) * 1000)
                }
                Some(None) => None,
                None => parse_ts(b),
            };
        }
    }
    parse_ts(b)
}

/// ingest_ts (strict F1) -> epoch seconds, exact.
#[inline(always)]
fn parse_f1_fast(b: &[u8], tab: &DateTab) -> Option<i64> {
    if b.len() != 20 || b[19] != b'Z' || b[10] != b'T' {
        return None;
    }
    match tab.day(b) {
        Some(Some(day)) => Some(day * 86400 + hms(b, 11)?),
        Some(None) => None,
        None => parse_dt(b),
    }
}


#[inline(always)]
fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    if v == 0 {
        None
    } else {
        Some(v as u32)
    }
}

// ---------------------------------------------------------------- hash maps
#[inline(always)]
fn nz(h: u64) -> u64 {
    if h == 0 {
        1
    } else {
        h
    }
}

#[inline(always)]
fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(p as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
}

/// Open addressing u64 (nonzero) -> u32, interleaved slots (one cache line per probe).
#[derive(Clone, Copy)]
#[repr(C)]
struct Slot {
    key: u64,
    val: u32,
    _pad: u32,
}

struct U64Map {
    slots: Vec<Slot>,
    mask: usize,
    len: usize,
}

impl U64Map {
    fn with_capacity(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        U64Map { slots: vec![Slot { key: 0, val: 0, _pad: 0 }; cap], mask: cap - 1, len: 0 }
    }
    #[inline(always)]
    fn slot_ptr(&self, k: u64) -> *const Slot {
        unsafe { self.slots.as_ptr().add((k as usize) & self.mask) }
    }
    #[inline(always)]
    fn get(&self, k: u64) -> Option<u32> {
        let mut i = (k as usize) & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            if s.key == k {
                return Some(s.val);
            }
            if s.key == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    /// returns (value, inserted)
    #[inline(always)]
    fn get_or_insert(&mut self, k: u64, v: u32) -> (u32, bool) {
        if (self.len + 1) * 2 > self.slots.len() {
            self.grow();
        }
        let mut i = (k as usize) & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked_mut(i) };
            if s.key == k {
                return (s.val, false);
            }
            if s.key == 0 {
                s.key = k;
                s.val = v;
                self.len += 1;
                return (v, true);
            }
            i = (i + 1) & self.mask;
        }
    }
    fn grow(&mut self) {
        let old = std::mem::take(&mut self.slots);
        let cap = old.len() * 2;
        self.slots = vec![Slot { key: 0, val: 0, _pad: 0 }; cap];
        self.mask = cap - 1;
        for s in old {
            if s.key != 0 {
                let mut i = (s.key as usize) & self.mask;
                while self.slots[i].key != 0 {
                    i = (i + 1) & self.mask;
                }
                self.slots[i] = s;
            }
        }
    }
}

/// Exact string -> u32 map (verifies bytes).
struct StrMap {
    hashes: Vec<u64>,
    keys: Vec<&'static [u8]>,
    vals: Vec<u32>,
    mask: usize,
}

impl StrMap {
    fn new(items: &[(&'static [u8], u32)]) -> Self {
        let cap = (items.len() * 2).next_power_of_two().max(16);
        let mut m = StrMap { hashes: vec![0; cap], keys: vec![&[][..]; cap], vals: vec![0; cap], mask: cap - 1 };
        for &(k, v) in items {
            let h = nz(xxh3_64(k));
            let mut i = (h as usize) & m.mask;
            loop {
                if m.hashes[i] == 0 {
                    m.hashes[i] = h;
                    m.keys[i] = k;
                    m.vals[i] = v;
                    break;
                }
                if m.hashes[i] == h && m.keys[i] == k {
                    m.vals[i] = v; // last wins
                    break;
                }
                i = (i + 1) & m.mask;
            }
        }
        m
    }
    #[inline(always)]
    fn get(&self, k: &[u8]) -> Option<u32> {
        let h = nz(xxh3_64(k));
        let mut i = (h as usize) & self.mask;
        loop {
            let hh = self.hashes[i];
            if hh == 0 {
                return None;
            }
            if hh == h && self.keys[i] == k {
                return Some(self.vals[i]);
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ---------------------------------------------------------------- input files
static MAPS: Mutex<Vec<(usize, usize)>> = Mutex::new(Vec::new());
static PROF: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static PF_DIST: AtomicUsize = AtomicUsize::new(0);
// debug accumulators (ns, summed over threads)
static TACC: [std::sync::atomic::AtomicU64; 8] = [const { std::sync::atomic::AtomicU64::new(0) }; 8];
#[inline(always)]
fn tadd(i: usize, t: Instant) {
    TACC[i].fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
}


/// Unmap inputs and release memory on all threads, then leave without the serial teardown.
fn fast_exit(nthreads: usize) -> ! {
    #[cfg(target_os = "linux")]
    {
        let maps = std::mem::take(&mut *MAPS.lock().unwrap());
        let next = AtomicUsize::new(0);
        std::thread::scope(|s| {
            for _ in 0..nthreads {
                s.spawn(|| loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= maps.len() {
                        break;
                    }
                    unsafe {
                        libc::munmap(maps[i].0 as *mut libc::c_void, maps[i].1);
                    }
                });
            }
        });
        unsafe { libc::_exit(0) }
    }
    #[allow(unreachable_code)]
    {
        let _ = nthreads;
        std::process::exit(0)
    }
}
fn map_file(p: &Path) -> &'static [u8] {
    let f = match fs::File::open(p) {
        Ok(f) => f,
        Err(_) => return &[],
    };
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        return &[];
    }
    match unsafe { memmap2::Mmap::map(&f) } {
        Ok(m) => {
            let m: &'static memmap2::Mmap = Box::leak(Box::new(m));
            MAPS.lock().unwrap().push((m.as_ptr() as usize, m.len()));
            &m[..]
        }
        Err(_) => {
            let v = fs::read(p).unwrap_or_default();
            Box::leak(v.into_boxed_slice())
        }
    }
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            let name = e.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".csv") && p.is_file() {
                v.push(p);
            }
        }
    }
    v.sort();
    v
}

/// Body of a CSV (after the header line).
fn body(data: &'static [u8]) -> &'static [u8] {
    match memchr::memchr(b'\n', data) {
        Some(i) => &data[i + 1..],
        None => &[],
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Event,
    Click,
    Link,
}

#[derive(Clone, Copy)]
struct Chunk {
    kind: Kind,
    data: &'static [u8], // file body
    start: usize,
    end: usize,
}

fn make_chunks(kind: Kind, files: &[&'static [u8]], target: usize, out: &mut Vec<Chunk>) {
    for &f in files {
        let b = body(f);
        if b.is_empty() {
            continue;
        }
        let n = b.len();
        let parts = (n + target - 1) / target;
        for i in 0..parts {
            out.push(Chunk { kind, data: b, start: i * n / parts, end: (i + 1) * n / parts });
        }
    }
}

/// Iterate lines that START in [start, end) of data.
#[inline(always)]
fn for_lines(data: &'static [u8], start: usize, end: usize, mut f: impl FnMut(&'static [u8])) {
    let mut pos = start;
    if pos > 0 && data[pos - 1] != b'\n' {
        match memchr::memchr(b'\n', &data[pos..]) {
            Some(i) => pos += i + 1,
            None => return,
        }
    }
    while pos < end {
        let nl = match memchr::memchr(b'\n', &data[pos..]) {
            Some(i) => pos + i,
            None => data.len(),
        };
        f(&data[pos..nl]);
        pos = nl + 1;
    }
}

// ---------------------------------------------------------------- records
/// Event copy, partitioned by event_id hash.
#[derive(Clone, Copy)]
#[repr(C)]
struct EvA {
    key: u64,  // event_id fingerprint (low 64 bits of xxh3_128; partition uses high bits)
    user: u32, // dense user index
    ts: u32,   // ms since base (ws - 7d); u32::MAX = invalid copy
    ingest: u32,
    meta: u32, // price (24 bits) | cat << 24 ; price 0 = not a purchase
}

/// Surviving event, partitioned by person range.
#[derive(Clone, Copy)]
#[repr(C)]
struct EvB {
    person: u32,
    ts: u32,
    meta: u32,
}

/// Valid click (before person mapping).
#[derive(Clone, Copy)]
#[repr(C)]
struct ClA {
    cid: u64, // sref
    user: u32,
    ts: u32,
    camp: u32,
    _pad: u32,
}

/// Valid click, partitioned by person range.
#[derive(Clone, Copy)]
#[repr(C)]
struct ClB {
    cid: u64,
    person: u32,
    ts: u32,
    camp: u32,
    _pad: u32,
}

struct Ctx {
    base: i64,  // ws - 7d (ms)
    ev_lo: u32, // ws - base
    ev_hi: u32, // we - base
    cl_hi: u32, // we - base
    ingest_base: i64, // seconds
    products: StrMap,
    campaigns: StrMap,
    unknown_cat: u32,
    dates: DateTab,
    pshift: u32,
}

#[inline(always)]
fn part_of(h: u64, shift: u32) -> usize {
    (h >> shift) as usize
}

const RING: usize = 16;

/// Per-thread parse state: output buffers, user-index allocator, and small pipelines that
/// prefetch a row's user-table slot and resolve it RING rows later.
struct PState {
    ev: PartBuf<EvA>,
    cl: PartBuf<ClA>, // single partition; write-combined like the event scatter
    alloc: IdxAlloc,
    pe: [(EvA, u64, u64, u32); RING],
    pe_pos: usize,
    pc: [(ClA, u64, u64); RING],
    pc_pos: usize,
}

impl PState {
    fn new(npart: usize, per_part: usize, cl_cap: usize) -> Self {
        let ze = EvA { key: 0, user: 0, ts: 0, ingest: 0, meta: 0 };
        let zc = ClA { cid: 0, user: 0, ts: 0, camp: 0, _pad: 0 };
        PState {
            ev: PartBuf::new(npart, per_part),
            cl: PartBuf::new(1, cl_cap),
            alloc: IdxAlloc { next: 0, end: 0 },
            pe: [(ze, 0, 0, 0); RING],
            pe_pos: 0,
            pc: [(zc, 0, 0); RING],
            pc_pos: 0,
        }
    }
    #[inline(always)]
    fn ev_pending(&mut self, ut: &UTab, rec: EvA, uid_h: u64, uref: u64, part: u32) {
        prefetch(ut.slot_ptr(uid_h));
        let i = self.pe_pos & (RING - 1);
        if self.pe_pos >= RING {
            let (mut r, h, s, p) = self.pe[i];
            r.user = ut.get_or_insert(h, s, &mut self.alloc);
            self.ev.push(p as usize, r);
        }
        self.pe[i] = (rec, uid_h, uref, part);
        self.pe_pos += 1;
    }
    #[inline(always)]
    fn cl_pending(&mut self, ut: &UTab, rec: ClA, uid_h: u64, uref: u64) {
        prefetch(ut.slot_ptr(uid_h));
        let i = self.pc_pos & (RING - 1);
        if self.pc_pos >= RING {
            let (mut r, h, s) = self.pc[i];
            r.user = ut.get_or_insert(h, s, &mut self.alloc);
            self.cl.push(0, r);
        }
        self.pc[i] = (rec, uid_h, uref);
        self.pc_pos += 1;
    }
    fn flush(&mut self, ut: &UTab) {
        let n = self.pe_pos.min(RING);
        for k in 0..n {
            let i = (self.pe_pos - n + k) & (RING - 1);
            let (mut r, h, s, p) = self.pe[i];
            r.user = ut.get_or_insert(h, s, &mut self.alloc);
            self.ev.push(p as usize, r);
        }
        self.pe_pos = 0;
        let n = self.pc_pos.min(RING);
        for k in 0..n {
            let i = (self.pc_pos - n + k) & (RING - 1);
            let (mut r, h, s) = self.pc[i];
            r.user = ut.get_or_insert(h, s, &mut self.alloc);
            self.cl.push(0, r);
        }
        self.pc_pos = 0;
    }
}

#[inline(always)]
fn parse_event_line(line: &'static [u8], cx: &Ctx, st: &mut PState, ut: &UTab) {
    let mut f: [&'static [u8]; 7] = [&[]; 7];
    if !split_fields(line, &mut f) {
        return;
    }
    event_fields(&f, cx, st, ut);
}

#[inline(always)]
fn event_fields(f: &[&'static [u8]; 7], cx: &Ctx, st: &mut PState, ut: &UTab) {
    let eid = f[0];
    if eid.is_empty() {
        return;
    }
    let h128 = xxh3_128(eid);
    let key = h128 as u64;
    let part = part_of((h128 >> 64) as u64, cx.pshift);
    let ingest_s = parse_f1_fast(f[3], &cx.dates);
    let ingest = match ingest_s {
        Some(s) => (s - cx.ingest_base).clamp(0, u32::MAX as i64 - 1) as u32,
        None => u32::MAX,
    };
    let mut rec = EvA { key, user: 0, ingest, ts: u32::MAX, meta: 0 };
    'valid: {
        let uid = f[1];
        if uid.is_empty() {
            break 'valid;
        }
        let ts = match parse_ts_fast(f[2], &cx.dates) {
            Some(t) => t,
            None => break 'valid,
        };
        let rel = ts - cx.base;
        if rel < cx.ev_lo as i64 || rel >= cx.ev_hi as i64 {
            break 'valid;
        }
        let is_purchase = match f[4] {
            b"view" | b"cart" => false,
            b"purchase" => true,
            _ => break 'valid,
        };
        let prod = f[5];
        if prod.is_empty() {
            break 'valid;
        }
        let mut meta = 0u32;
        if is_purchase {
            let price = match parse_price(f[6]) {
                Some(p) => p,
                None => break 'valid,
            };
            let cat = cx.products.get(prod).unwrap_or(cx.unknown_cat);
            meta = price | (cat << 24);
        }
        let ingest_s = match ingest_s {
            Some(s) => s,
            None => break 'valid,
        };
        if ingest_s * 1000 - ts > LATE_MS {
            break 'valid;
        }
        rec.ts = rel as u32;
        rec.meta = meta;
        st.ev_pending(ut, rec, nz(xxh3_64(uid)), sref(uid), part as u32);
        return;
    }
    st.ev.push(part, rec);
}

#[inline(always)]
fn parse_click_line(line: &'static [u8], cx: &Ctx, st: &mut PState, ut: &UTab) {
    let mut f: [&'static [u8]; 4] = [&[]; 4];
    if !split_fields(line, &mut f) {
        return;
    }
    click_fields(&f, cx, st, ut);
}

#[inline(always)]
fn click_fields(f: &[&'static [u8]; 4], cx: &Ctx, st: &mut PState, ut: &UTab) {
    let cid = f[0];
    if cid.is_empty() {
        return;
    }
    let uid = f[1];
    if uid.is_empty() {
        return;
    }
    let ts = match parse_ts_fast(f[3], &cx.dates) {
        Some(t) => t,
        None => return,
    };
    let rel = ts - cx.base;
    if rel < 0 || rel >= cx.cl_hi as i64 {
        return;
    }
    let camp = match cx.campaigns.get(f[2]) {
        Some(c) => c,
        None => return,
    };
    st.cl_pending(ut, ClA { cid: sref(cid), user: 0, ts: rel as u32, camp, _pad: 0 }, nz(xxh3_64(uid)), sref(uid));
}

// ---------------------------------------------------------------- small CSV tables
fn read_table2(p: &Path) -> Vec<(&'static [u8], &'static [u8])> {
    let data = map_file(p);
    let b = body(data);
    let mut v = Vec::new();
    for_lines(b, 0, b.len(), |line| {
        let mut f: [&'static [u8]; 2] = [&[]; 2];
        if split_fields(line, &mut f) {
            v.push((f[0], f[1]));
        }
    });
    v
}

fn manifest_value(text: &str, key: &str) -> Option<String> {
    let i = text.find(&format!("\"{key}\""))?;
    let rest = &text[i + key.len() + 2..];
    let c = rest.find(':')?;
    let rest = &rest[c + 1..];
    let q1 = rest.find('"')?;
    let rest = &rest[q1 + 1..];
    let q2 = rest.find('"')?;
    Some(rest[..q2].to_string())
}

// ---------------------------------------------------------------- users and links
/// Zero-initialised vector (T must be valid as all-zero bytes). Big ones come from mmap lazily.
fn zeroed_vec<T>(n: usize) -> Vec<T> {
    if n == 0 {
        return Vec::new();
    }
    unsafe {
        let layout = std::alloc::Layout::array::<T>(n).unwrap();
        let p = std::alloc::alloc_zeroed(layout) as *mut T;
        if p.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        Vec::from_raw_parts(p, n, n)
    }
}

#[repr(C)]
struct USlot {
    key: AtomicU64,
    idx: AtomicU32, // user index + 1; 0 = being published
    _pad: u32,
}

const UBLOCK: u32 = 1024;
const MAXPROBE: usize = 256;

struct IdxAlloc {
    next: u32,
    end: u32,
}

/// Concurrent, insert-only table: uid hash -> dense user index (linear probing, CAS on the key).
/// Keys whose probe window is full go to a locked spill list (never expected in practice).
struct UTab {
    slots: Vec<USlot>,
    mask: usize,
    blocks: AtomicU32,
    uref: Vec<AtomicU64>, // user index -> sref of the uid text
    spill: Mutex<std::collections::HashMap<u64, u32>>,
}

impl UTab {
    fn new(cap_slots: usize, cap_users: usize) -> Self {
        UTab {
            slots: zeroed_vec(cap_slots),
            mask: cap_slots - 1,
            blocks: AtomicU32::new(0),
            uref: zeroed_vec(cap_users),
            spill: Mutex::new(std::collections::HashMap::new()),
        }
    }
    #[inline(always)]
    fn slot_ptr(&self, h: u64) -> *const USlot {
        unsafe { self.slots.as_ptr().add((h as usize) & self.mask) }
    }
    #[inline(always)]
    fn new_index(&self, a: &mut IdxAlloc) -> u32 {
        if a.next == a.end {
            let b = self.blocks.fetch_add(1, Ordering::Relaxed);
            a.next = b * UBLOCK;
            a.end = a.next + UBLOCK;
        }
        let i = a.next;
        a.next += 1;
        assert!((i as usize) < self.uref.len(), "user table overflow");
        i
    }
    #[inline(always)]
    fn wait(slot: &USlot) -> u32 {
        loop {
            let v = slot.idx.load(Ordering::Acquire);
            if v != 0 {
                return v - 1;
            }
            std::hint::spin_loop();
        }
    }
    #[inline(always)]
    fn get_or_insert(&self, h: u64, s: u64, a: &mut IdxAlloc) -> u32 {
        let mut i = (h as usize) & self.mask;
        for _ in 0..MAXPROBE {
            let slot = unsafe { self.slots.get_unchecked(i) };
            let k = slot.key.load(Ordering::Acquire);
            if k == h {
                return Self::wait(slot);
            }
            if k == 0 {
                match slot.key.compare_exchange(0, h, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => {
                        let u = self.new_index(a);
                        self.uref[u as usize].store(s, Ordering::Relaxed);
                        slot.idx.store(u + 1, Ordering::Release);
                        return u;
                    }
                    Err(actual) => {
                        if actual == h {
                            return Self::wait(slot);
                        }
                    }
                }
            }
            i = (i + 1) & self.mask;
        }
        self.spill_get(h, s, a)
    }
    #[cold]
    fn spill_get(&self, h: u64, s: u64, a: &mut IdxAlloc) -> u32 {
        let mut g = self.spill.lock().unwrap();
        if let Some(&u) = g.get(&h) {
            return u;
        }
        let u = self.new_index(a);
        self.uref[u as usize].store(s, Ordering::Relaxed);
        g.insert(h, u);
        u
    }
    /// Upper bound on user indices handed out.
    fn n_users(&self) -> usize {
        self.blocks.load(Ordering::Relaxed) as usize * UBLOCK as usize
    }
    #[inline(always)]
    fn name(&self, u: u32) -> u64 {
        self.uref[u as usize].load(Ordering::Relaxed)
    }
}

/// A parsed link line: both side hashes and string refs.
#[derive(Clone, Copy)]
#[repr(C)]
struct LinkRec {
    ha: u64,
    hb: u64,
    a: u64,
    b: u64,
}

#[inline(always)]
fn key8(s: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    let n = s.len().min(8);
    buf[..n].copy_from_slice(&s[..n]);
    u64::from_be_bytes(buf)
}

#[inline(always)]
fn link_fields(f: &[&'static [u8]; 2], out: &mut Vec<LinkRec>) {
    let (a, b) = (f[0], f[1]);
    if a.is_empty() || b.is_empty() {
        return;
    }
    out.push(LinkRec { ha: nz(xxh3_64(a)), hb: nz(xxh3_64(b)), a: sref(a), b: sref(b) });
}

/// Intern both sides of every link line; returns user-index edges.
fn intern_links(recs: &[LinkRec], ut: &UTab, a: &mut IdxAlloc) -> Vec<(u32, u32)> {
    let mut edges = Vec::with_capacity(recs.len());
    let n = recs.len();
    for i in 0..n {
        if i + 8 < n {
            prefetch(ut.slot_ptr(recs[i + 8].ha));
            prefetch(ut.slot_ptr(recs[i + 8].hb));
        }
        let r = recs[i];
        let ua = ut.get_or_insert(r.ha, r.a, a);
        let ub = ut.get_or_insert(r.hb, r.b, a);
        edges.push((ua, ub));
    }
    edges
}

/// Union-find over user indices. parent[u] = 0 for a root (and for every user in no link),
/// else root + 1 (flattened). Each root is the byte-wise smallest ID of its person.
fn build_dsu(edges: &[Vec<(u32, u32)>], ut: &UTab, cap_users: usize) -> Vec<u32> {
    let mut parent: Vec<u32> = zeroed_vec(cap_users);
    let mut nkey: Vec<u64> = zeroed_vec(cap_users); // key8 + 1 (0 = not computed)
    #[inline(always)]
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        unsafe {
            loop {
                let p = *parent.get_unchecked(x as usize);
                if p == 0 {
                    return x;
                }
                let px = p - 1;
                let gp = *parent.get_unchecked(px as usize);
                if gp != 0 {
                    *parent.get_unchecked_mut(x as usize) = gp;
                }
                x = px;
            }
        }
    }
    let mut key = |nkey: &mut Vec<u64>, i: u32| -> u64 {
        let k = nkey[i as usize];
        if k != 0 {
            return k;
        }
        let k = key8(sget(ut.name(i))).wrapping_add(1);
        nkey[i as usize] = k;
        k
    };
    for v in edges {
        let m = v.len();
        for i in 0..m {
            if i + 8 < m {
                let (a, b) = v[i + 8];
                prefetch(unsafe { parent.as_ptr().add(a as usize) });
                prefetch(unsafe { parent.as_ptr().add(b as usize) });
            }
            let (a, b) = v[i];
            let ra = find(&mut parent, a);
            let rb = find(&mut parent, b);
            if ra != rb {
                let ka = key(&mut nkey, ra);
                let kb = key(&mut nkey, rb);
                let a_smaller = if ka != kb { ka < kb } else { sget(ut.name(ra)) < sget(ut.name(rb)) };
                if a_smaller {
                    parent[rb as usize] = ra + 1;
                } else {
                    parent[ra as usize] = rb + 1;
                }
            }
        }
    }
    // flatten
    for v in edges {
        for &(a, b) in v.iter() {
            for u in [a, b] {
                let r = find(&mut parent, u);
                if r != u {
                    parent[u as usize] = r + 1;
                }
            }
        }
    }
    drop(nkey);
    parent
}

#[inline(always)]
fn person_of(parent: &[u32], u: u32) -> u32 {
    let p = unsafe { *parent.get_unchecked(u as usize) };
    if p == 0 {
        u
    } else {
        p - 1
    }
}

// ---------------------------------------------------------------- per-person processing
struct UserRow {
    name: u64,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: u64,
    top: u32, // u32::MAX = null
}

/// Credited-campaign accumulator for one (campaign, day).
#[derive(Clone, Copy)]
#[repr(C)]
struct CStat {
    sessions: u32,
    purchasing: u32,
    revenue: u64,
}

/// Click as stored per person: sorted by (ts, click_id)
#[derive(Clone, Copy)]
#[repr(C)]
struct ClC {
    cid: u64,
    ts: u32,
    camp: u32,
}

struct Worker {
    // campaign stats export: [camp * ndays + day] -> sessions, purchasing, revenue, assisted
    // (filled by finalize_stats from the compact arrays below)
    stats: Vec<[u64; 4]>,
    // compact, cache-friendly accumulators over event-window days only
    cstat: Vec<CStat>,
    casg: Vec<u32>,
    nd: usize,
    dshift: u32,
    ndays: usize,
    ncat: usize,
    cnt: Vec<u32>,
    open: Vec<u32>,
    touched: Vec<u32>,
    catrev: Vec<u64>,
    sess_start: Vec<u32>,
    sess_day: Vec<u32>,
    sess_rev: Vec<u64>,
    run_of: Vec<u32>,
    runs: Vec<(u32, u32)>, // (day, first session idx)
    rows: Vec<UserRow>,
    // grouping scratch
    pmap: U64Map,
    pname: Vec<u64>,
    pnh: Vec<u64>,
    pcount: Vec<u32>,
    pcl: Vec<u32>,
    pidx: Vec<u32>,
    ev_sorted: Vec<u64>,
    cl_sorted: Vec<ClC>,
    cl_pidx: Vec<u32>,
    // 30-minute cells (sessions can only break between non-empty cells)
    cmin: Vec<u32>,
    cmax: Vec<u32>,
    ccnt: Vec<u32>,
    crev: Vec<u64>,
    csess: Vec<u32>,
    cbits: Vec<u64>,
    sess_cnt: Vec<u32>,
    dcat: Vec<u64>, // [day * ncat + cat] purchase revenue
}

impl Worker {
    fn new(ncamp: usize, ndays: usize, ncat: usize) -> Self {
        Worker {
            stats: vec![[0; 4]; (ncamp + 1) * ndays],
            cstat: Vec::new(),
            casg: Vec::new(),
            nd: 0,
            dshift: 0,
            ndays,
            ncat,
            cnt: vec![0; ncamp + 1],
            open: vec![0; ncamp + 1],
            touched: Vec::new(),
            catrev: vec![0; ncat],
            sess_start: Vec::new(),
            sess_day: Vec::new(),
            sess_rev: Vec::new(),
            run_of: Vec::new(),
            runs: Vec::new(),
            rows: Vec::new(),
            pmap: U64Map::with_capacity(1024),
            pname: Vec::new(),
            pnh: Vec::new(),
            pcount: Vec::new(),
            pcl: Vec::new(),
            pidx: Vec::new(),
            ev_sorted: Vec::new(),
            cl_sorted: Vec::new(),
            cl_pidx: Vec::new(),
            cmin: Vec::new(),
            cmax: Vec::new(),
            ccnt: Vec::new(),
            crev: Vec::new(),
            csess: Vec::new(),
            cbits: Vec::new(),
            sess_cnt: Vec::new(),
            dcat: vec![0; ndays * ncat],
        }
    }

    /// Event-window day range: base-relative day of the window's first day, and its day count.
    fn set_window(&mut self, ncamp: usize, dshift: u32, nd: usize) {
        self.dshift = dshift;
        self.nd = nd;
        self.cstat = vec![CStat { sessions: 0, purchasing: 0, revenue: 0 }; (ncamp + 1) * nd];
        self.casg = vec![0; (ncamp + 1) * nd];
    }

    /// Fold the compact accumulators into the export layout used by the writer.
    fn finalize_stats(&mut self) {
        let nd = self.nd;
        let ncamp1 = self.cstat.len() / nd.max(1);
        for c in 0..ncamp1 {
            for d in 0..nd {
                let cs = self.cstat[c * nd + d];
                let a = self.casg[c * nd + d];
                if cs.sessions != 0 || a != 0 {
                    let st = &mut self.stats[c * self.ndays + d + self.dshift as usize];
                    st[0] += cs.sessions as u64;
                    st[1] += cs.purchasing as u64;
                    st[2] += cs.revenue;
                    st[3] += a as u64;
                }
            }
        }
    }

    fn ensure_cells(&mut self, ncells: usize) {
        if self.cmin.len() < ncells {
            self.cmin = vec![0; ncells];
            self.cmax = vec![0; ncells];
            self.ccnt = vec![0; ncells];
            self.crev = vec![0; ncells];
            self.csess = vec![0; ncells];
            self.cbits = vec![0; (ncells + 63) / 64];
        }
    }

    /// Sessions from unsorted events (ts << 32 | meta) via 30-minute cells; fills sess_* and runs,
    /// and emits the user_daily rows.
    fn sessionize_cells(&mut self, name: u64, evs: &[u64], base: i64, base_day_off: i64) {
        self.sess_start.clear();
        self.sess_day.clear();
        self.sess_rev.clear();
        self.sess_cnt.clear();
        self.runs.clear();
        self.run_of.clear();
        let mut cmin_c = usize::MAX;
        let mut cmax_c = 0usize;
        for &e in evs {
            let ts = (e >> 32) as u32;
            let price = (e as u32 & 0xFF_FFFF) as u64;
            let c = (ts / GAP_MS) as usize;
            let w = c >> 6;
            let b = 1u64 << (c & 63);
            unsafe {
                let bw = self.cbits.get_unchecked_mut(w);
                if *bw & b == 0 {
                    *bw |= b;
                    *self.cmin.get_unchecked_mut(c) = ts;
                    *self.cmax.get_unchecked_mut(c) = ts;
                    *self.ccnt.get_unchecked_mut(c) = 1;
                    *self.crev.get_unchecked_mut(c) = price;
                } else {
                    let m = self.cmin.get_unchecked_mut(c);
                    *m = (*m).min(ts);
                    let x = self.cmax.get_unchecked_mut(c);
                    *x = (*x).max(ts);
                    *self.ccnt.get_unchecked_mut(c) += 1;
                    *self.crev.get_unchecked_mut(c) += price;
                }
            }
            if c < cmin_c {
                cmin_c = c;
            }
            if c > cmax_c {
                cmax_c = c;
            }
        }
        // walk cells in time order
        let mut prev_max: u32 = 0;
        let mut have = false;
        for w in (cmin_c >> 6)..=(cmax_c >> 6) {
            let mut bits = self.cbits[w];
            self.cbits[w] = 0;
            while bits != 0 {
                let c = (w << 6) | bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let lo = self.cmin[c];
                if !have || lo - prev_max > GAP_MS {
                    have = true;
                    let day = (((base + lo as i64).div_euclid(DAY_MS)) - base_day_off) as u32;
                    if self.runs.last().map(|r| r.0) != Some(day) {
                        self.runs.push((day, self.sess_start.len() as u32));
                    }
                    self.run_of.push((self.runs.len() - 1) as u32);
                    self.sess_start.push(lo);
                    self.sess_day.push(day);
                    self.sess_rev.push(0);
                    self.sess_cnt.push(0);
                }
                let k = self.sess_start.len() - 1;
                self.sess_cnt[k] += self.ccnt[c];
                self.sess_rev[k] += self.crev[c];
                self.csess[c] = k as u32;
                prev_max = self.cmax[c];
            }
        }
        // purchases -> category revenue of the session's day
        let ncat = self.ncat;
        let mut any_purchase = false;
        for &e in evs {
            let meta = e as u32;
            let price = meta & 0xFF_FFFF;
            if price != 0 {
                let ts = (e >> 32) as u32;
                let k = self.csess[(ts / GAP_MS) as usize] as usize;
                let d = self.sess_day[k] as usize;
                self.dcat[d * ncat + (meta >> 24) as usize] += price as u64;
                any_purchase = true;
            }
        }
        // user_daily rows: one per day run
        let nruns = self.runs.len();
        for r in 0..nruns {
            let (day, first) = self.runs[r];
            let end = if r + 1 < nruns { self.runs[r + 1].1 as usize } else { self.sess_start.len() };
            let first = first as usize;
            let mut events = 0u32;
            let mut revenue = 0u64;
            for k in first..end {
                events += self.sess_cnt[k];
                revenue += self.sess_rev[k];
            }
            let mut top = u32::MAX;
            if any_purchase && revenue > 0 {
                let row = &mut self.dcat[day as usize * ncat..(day as usize + 1) * ncat];
                let mut best = 0u64;
                for (ci, v) in row.iter_mut().enumerate() {
                    if *v > best {
                        best = *v;
                        top = ci as u32;
                    }
                    *v = 0;
                }
            }
            self.rows.push(UserRow {
                name,
                day: (base_day_off + day as i64) as i32,
                sessions: (end - first) as u32,
                events,
                revenue,
                top,
            });
        }
    }

    #[inline]
    fn close(&mut self, c: usize, k0: usize, k1: usize) {
        if k0 >= k1 {
            return;
        }
        let mut r = self.run_of[k0] as usize;
        let nruns = self.runs.len();
        while r < nruns {
            let (day, first) = self.runs[r];
            let first = first as usize;
            if first >= k1 {
                break;
            }
            let end = if r + 1 < nruns { self.runs[r + 1].1 as usize } else { self.sess_start.len() };
            let lo = k0.max(first);
            let hi = k1.min(end);
            if hi > lo {
                unsafe {
                    *self.casg.get_unchecked_mut(c * self.nd + (day - self.dshift) as usize) += (hi - lo) as u32;
                }
            }
            r += 1;
        }
    }

    /// Process one partition of persons: person indices [qbase, qbase + qsize).
    fn partition(
        &mut self,
        evp: &[&[EvB]],
        clp: &[&[ClB]],
        qbase: u32,
        qsize: usize,
        base: i64,
        base_day_off: i64,
        organic: usize,
        ut: &UTab,
    ) {
        let nev: usize = evp.iter().map(|s| s.len()).sum();
        if nev == 0 {
            return;
        }
        let t_g = Instant::now();
        // counting sort of events by person
        self.pcount.clear();
        self.pcount.resize(qsize, 0);
        for s in evp {
            for e in s.iter() {
                unsafe { *self.pcount.get_unchecked_mut((e.person - qbase) as usize) += 1 };
            }
        }
        self.pidx.clear();
        let mut acc = 0u32;
        for c in self.pcount.iter_mut() {
            self.pidx.push(acc);
            let n = *c;
            *c = acc;
            acc += n;
        }
        self.pidx.push(acc);
        self.ev_sorted.clear();
        self.ev_sorted.resize(nev, 0);
        for s in evp {
            for e in s.iter() {
                let p = (e.person - qbase) as usize;
                unsafe {
                    let w = *self.pcount.get_unchecked(p) as usize;
                    *self.pcount.get_unchecked_mut(p) += 1;
                    *self.ev_sorted.get_unchecked_mut(w) = ((e.ts as u64) << 32) | e.meta as u64;
                }
            }
        }
        // clicks by person
        self.pcl.clear();
        self.pcl.resize(qsize, 0);
        for s in clp {
            for c in s.iter() {
                unsafe { *self.pcl.get_unchecked_mut((c.person - qbase) as usize) += 1 };
            }
        }
        self.cl_pidx.clear();
        let mut acc = 0u32;
        for c in self.pcl.iter_mut() {
            self.cl_pidx.push(acc);
            let n = *c;
            *c = acc;
            acc += n;
        }
        self.cl_pidx.push(acc);
        self.cl_sorted.clear();
        self.cl_sorted.resize(acc as usize, ClC { cid: 0, ts: 0, camp: 0 });
        for s in clp {
            for c in s.iter() {
                let p = (c.person - qbase) as usize;
                unsafe {
                    let w = *self.pcl.get_unchecked(p) as usize;
                    *self.pcl.get_unchecked_mut(p) += 1;
                    *self.cl_sorted.get_unchecked_mut(w) = ClC { cid: c.cid, ts: c.ts, camp: c.camp };
                }
            }
        }
        tadd(3, t_g);
        let t_p = Instant::now();
        let ev_sorted = std::mem::take(&mut self.ev_sorted);
        let mut cl_sorted = std::mem::take(&mut self.cl_sorted);
        let starts = std::mem::take(&mut self.pidx);
        let cstarts = std::mem::take(&mut self.cl_pidx);
        for p in 0..qsize {
            let (e0, e1) = (starts[p] as usize, starts[p + 1] as usize);
            if e0 == e1 {
                continue;
            }
            let cls = &mut cl_sorted[cstarts[p] as usize..cstarts[p + 1] as usize];
            if cls.len() > 1 {
                cls.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| sget(a.cid).cmp(sget(b.cid))));
            }
            let name = ut.name(qbase + p as u32);
            self.person(name, &ev_sorted[e0..e1], cls, base, base_day_off, organic);
        }
        self.ev_sorted = ev_sorted;
        self.cl_sorted = cl_sorted;
        self.pidx = starts;
        self.cl_pidx = cstarts;
        tadd(4, t_p);
    }

    /// evs: one person's events (ts << 32 | meta) sorted; cls: clicks sorted by (ts, cid).
    fn person(&mut self, name: u64, evs: &[u64], cls: &[ClC], base: i64, base_day_off: i64, organic: usize) {
        if PROF.load(Ordering::Relaxed) {
            let t = Instant::now();
            self.sessionize_cells(name, evs, base, base_day_off);
            tadd(5, t);
            let t = Instant::now();
            self.attribute(cls, organic);
            tadd(6, t);
        } else {
            self.sessionize_cells(name, evs, base, base_day_off);
            self.attribute(cls, organic);
        }
    }

    #[allow(dead_code)]
    fn person_sorted(&mut self, name: u64, evs: &[u64], cls: &[ClC], base: i64, base_day_off: i64, organic: usize) {
        self.sess_start.clear();
        self.sess_day.clear();
        self.sess_rev.clear();
        self.runs.clear();
        self.run_of.clear();
        let n = evs.len();
        let mut i = 0;
        let mut cur_day: u32 = u32::MAX;
        let mut d_sess = 0u32;
        let mut d_events = 0u32;
        let mut d_rev = 0u64;
        let mut cat_touched = false;
        while i < n {
            let start = (evs[i] >> 32) as u32;
            let day = (((base + start as i64).div_euclid(DAY_MS)) - base_day_off) as u32;
            let mut last = start;
            let mut rev = 0u64;
            let mut cnt = 0u32;
            if day != cur_day {
                if cur_day != u32::MAX {
                    self.emit_row(name, cur_day, d_sess, d_events, d_rev, cat_touched, base_day_off);
                }
                cur_day = day;
                d_sess = 0;
                d_events = 0;
                d_rev = 0;
                cat_touched = false;
                self.runs.push((day, self.sess_start.len() as u32));
            }
            while i < n {
                let e = evs[i];
                let ts = (e >> 32) as u32;
                if ts - last > GAP_MS {
                    break;
                }
                last = ts;
                cnt += 1;
                let meta = e as u32;
                let price = meta & 0xFF_FFFF;
                if price != 0 {
                    rev += price as u64;
                    self.catrev[(meta >> 24) as usize] += price as u64;
                    cat_touched = true;
                }
                i += 1;
            }
            d_sess += 1;
            d_events += cnt;
            d_rev += rev;
            self.run_of.push((self.runs.len() - 1) as u32);
            self.sess_start.push(start);
            self.sess_day.push(day);
            self.sess_rev.push(rev);
        }
        self.emit_row(name, cur_day, d_sess, d_events, d_rev, cat_touched, base_day_off);
        self.attribute(cls, organic);
    }

    fn attribute(&mut self, cls: &[ClC], organic: usize) {
        let s_n = self.sess_start.len();
        let nc = cls.len();
        let mut lo = 0usize;
        let mut hi = 0usize;
        for k in 0..s_n {
            let s = self.sess_start[k];
            let s_lo = s as i64 - WEEK_MS;
            while hi < nc && cls[hi].ts <= s {
                let c = cls[hi].camp as usize;
                if self.cnt[c] == 0 {
                    self.open[c] = k as u32;
                    self.touched.push(c as u32);
                }
                self.cnt[c] += 1;
                hi += 1;
            }
            while lo < hi && (cls[lo].ts as i64) < s_lo {
                let c = cls[lo].camp as usize;
                self.cnt[c] -= 1;
                if self.cnt[c] == 0 {
                    let o = self.open[c] as usize;
                    self.close(c, o, k);
                }
                lo += 1;
            }
            let credited = if hi > lo {
                let t = cls[hi - 1].ts;
                let mut q = hi - 1;
                while q > lo && cls[q - 1].ts == t {
                    q -= 1;
                }
                cls[q].camp as usize
            } else {
                organic
            };
            let rev = self.sess_rev[k];
            let st = unsafe {
                self.cstat.get_unchecked_mut(credited * self.nd + (self.sess_day[k] - self.dshift) as usize)
            };
            st.sessions += 1;
            st.purchasing += (rev > 0) as u32;
            st.revenue += rev;
        }
        if !self.touched.is_empty() {
            let touched = std::mem::take(&mut self.touched);
            for &c in &touched {
                let c = c as usize;
                if self.cnt[c] > 0 {
                    let o = self.open[c] as usize;
                    self.close(c, o, s_n);
                    self.cnt[c] = 0;
                }
            }
            self.touched = touched;
            self.touched.clear();
        }
    }

    fn emit_row(&mut self, name: u64, day: u32, sessions: u32, events: u32, revenue: u64, cat_touched: bool, base_day_off: i64) {
        let mut top = u32::MAX;
        if cat_touched {
            let mut best = 0u64;
            for c in 0..self.ncat {
                let v = self.catrev[c];
                if v > best {
                    best = v;
                    top = c as u32;
                }
                self.catrev[c] = 0;
            }
        }
        self.rows.push(UserRow { name, day: (base_day_off + day as i64) as i32, sessions, events, revenue, top });
    }
}

/// Sort u64 keys: insertion sort for small, radix (by the high 32 bits) for large.
fn sort_u64(v: &mut [u64]) {
    let n = v.len();
    if n <= 32 {
        for i in 1..n {
            let x = v[i];
            let mut j = i;
            while j > 0 && v[j - 1] > x {
                v[j] = v[j - 1];
                j -= 1;
            }
            v[j] = x;
        }
    } else {
        v.sort_unstable();
    }
}

// ---------------------------------------------------------------- main
fn main() {
    T0.get_or_init(Instant::now);

    let args: Vec<String> = std::env::args().collect();
    let (data_dir, out_dir) = if args.len() >= 3 {
        (PathBuf::from(&args[args.len() - 2]), PathBuf::from(&args[args.len() - 1]))
    } else {
        (
            PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into())),
            PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into())),
        )
    };
    let nthreads: usize = std::env::var("SS_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .or_else(|| std::env::var("NCPU").ok().and_then(|v| v.parse().ok()))
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8))
        .max(2);

    // manifest
    let mtext = fs::read_to_string(data_dir.join("manifest.json")).expect("manifest");
    let ws = parse_ts(manifest_value(&mtext, "window_start").expect("window_start").as_bytes()).expect("ws");
    let we = parse_ts(manifest_value(&mtext, "window_end").expect("window_end").as_bytes()).expect("we");
    let base = ws - WEEK_MS;

    // products / categories
    let prows = read_table2(&data_dir.join("products.csv"));
    let mut cats: Vec<&'static [u8]> = prows.iter().map(|r| r.1).collect();
    cats.push(b"unknown");
    cats.sort();
    cats.dedup();
    assert!(cats.len() <= 256, "too many categories");
    let cat_idx = |c: &[u8]| cats.binary_search_by(|x| (*x).cmp(c)).unwrap() as u32;
    let unknown_cat = cat_idx(b"unknown");
    let pitems: Vec<(&'static [u8], u32)> = prows.iter().map(|r| (r.0, cat_idx(r.1))).collect();
    let products = StrMap::new(&pitems);

    // campaigns (dedup by id, last wins)
    let crows = read_table2(&data_dir.join("campaigns.csv"));
    let mut camp_ids: Vec<&'static [u8]> = Vec::new();
    let mut camp_ch: Vec<&'static [u8]> = Vec::new();
    {
        let tmp: Vec<(&'static [u8], u32)> = crows.iter().enumerate().map(|(i, r)| (r.0, i as u32)).collect();
        let m = StrMap::new(&tmp);
        for (i, r) in crows.iter().enumerate() {
            if m.get(r.0) == Some(i as u32) {
                camp_ids.push(r.0);
                camp_ch.push(r.1);
            }
        }
    }
    let ncamp = camp_ids.len();
    let organic = ncamp;
    let citems: Vec<(&'static [u8], u32)> = camp_ids.iter().enumerate().map(|(i, c)| (*c, i as u32)).collect();
    let campaigns = StrMap::new(&citems);

    let base_day_off = base.div_euclid(DAY_MS);
    let last_day = (we - 1).div_euclid(DAY_MS);
    let ndays = (last_day - base_day_off + 1) as usize;

    let cx = Ctx {
        base,
        ev_lo: (ws - base) as u32,
        ev_hi: (we - base) as u32,
        cl_hi: (we - base) as u32,
        ingest_base: ws / 1000 - (1i64 << 30),
        products,
        campaigns,
        unknown_cat,
        dates: DateTab::new(ws - 9 * DAY_MS, we + 7 * DAY_MS),
        pshift: 0,
    };
    let mut cx = cx;

    // files
    let ev_files: Vec<&'static [u8]> = csv_files(&data_dir.join("events")).iter().map(|p| map_file(p)).collect();
    let cl_files: Vec<&'static [u8]> = csv_files(&data_dir.join("clicks")).iter().map(|p| map_file(p)).collect();
    let ln_files: Vec<&'static [u8]> = csv_files(&data_dir.join("links")).iter().map(|p| map_file(p)).collect();
    let ev_bytes: usize = ev_files.iter().map(|f| f.len()).sum();
    // partitions sized to stay L2-resident: ~12K event rows each
    let est_rows = ev_bytes / 75 + 1;
    let mut pbits = 8u32;
    while pbits < 12 && (est_rows >> pbits) > 12_000 {
        pbits += 1;
    }
    let npart: usize = 1 << pbits;
    let pshift = 64 - pbits;
    cx.pshift = pshift;
    // person partitions: 4x coarser (grouping tables stay small; fewer scatter streams)
    let pbits2: u32 = std::env::var("SS_P2BITS").ok().and_then(|v| v.parse().ok()).unwrap_or(pbits.saturating_sub(2).max(6));
    let npart2: usize = 1 << pbits2;
    let pshift2 = 64 - pbits2;
    let cx = cx;
    let mut chunks = Vec::new();
    make_chunks(Kind::Event, &ev_files, 2 << 20, &mut chunks);
    make_chunks(Kind::Click, &cl_files, 2 << 20, &mut chunks);
    let mut lchunks = Vec::new();
    make_chunks(Kind::Link, &ln_files, 256 << 10, &mut lchunks);
    log(&format!("setup done: {} chunks, {} threads, ndays {}", chunks.len(), nthreads, ndays));
    if let Some(d) = std::env::var("SS_PF").ok().and_then(|v| v.parse().ok()) {
        PF_DIST.store(d, Ordering::Relaxed);
    }
    if std::env::var_os("SS_PROF").is_some() {
        PROF.store(true, Ordering::Relaxed);
    }
    if let Ok(mode) = std::env::var("SS_BENCH") {
        // debug micro-benchmark: one thread parses every event chunk (full work incl. interning)
        for &f in ev_files.iter() {
            let mut acc = 0u8;
            for k in (0..f.len()).step_by(4096) {
                acc ^= f[k];
            }
            std::hint::black_box(acc);
        }
        let est_ev = ev_bytes / 75 + 1024;
        let ut = UTab::new(((est_ev / 10) * 3 / 2).next_power_of_two().max(1 << 16), est_ev);
        let mut st = PState::new(npart, est_ev * 3 / 2 / npart + 64, 1024);
        let mut lines = 0usize;
        let t = Instant::now();
        for c in chunks.iter().filter(|c| c.kind == Kind::Event) {
            #[cfg(target_arch = "x86_64")]
            unsafe {
                let mut cb = |f: &[&'static [u8]; 7]| {
                    lines += 1;
                    if mode == "split" {
                        std::hint::black_box(f);
                        return;
                    }
                    event_fields(f, &cx, &mut st, &ut);
                };
                if std::env::var_os("SS_PX").is_some() {
                    scan_fields_px::<7, _>(c.data, c.start, c.end, &mut cb);
                } else {
                    scan_fields::<7, _>(c.data, c.start, c.end, &mut cb);
                }
            }
        }
        st.flush(&ut);
        st.ev.finish();
        let el = t.elapsed().as_secs_f64() * 1e9;
        eprintln!("bench {mode}: {lines} lines, {:.1} ns/line, users {}", el / lines as f64, ut.n_users());
        std::process::exit(0);
    }
    if std::env::var_os("SS_PRETOUCH").is_some() {
        // debug: fault in all input pages first, to separate page-fault cost from parse cost
        let all: Vec<&'static [u8]> = ev_files.iter().chain(cl_files.iter()).chain(ln_files.iter()).copied().collect();
        let sum = AtomicUsize::new(0);
        let nxt = AtomicUsize::new(0);
        std::thread::scope(|s| {
            for _ in 0..nthreads {
                s.spawn(|| loop {
                    let i = nxt.fetch_add(1, Ordering::Relaxed);
                    if i >= all.len() {
                        break;
                    }
                    let mut acc = 0usize;
                    for k in (0..all[i].len()).step_by(4096) {
                        acc = acc.wrapping_add(all[i][k] as usize);
                    }
                    sum.fetch_add(acc, Ordering::Relaxed);
                });
            }
        });
        log(&format!("pretouch done {}", sum.load(Ordering::Relaxed) & 1));
    }

    // ---------------- phase 1: parse (thread 0 builds the union-find from links first)
    // arena slack is kept small: with huge pages the whole arena gets committed (and zeroed);
    // rare excess spills into per-partition overflow vectors
    let est_ev = ev_bytes / 80 + 1024;
    let per_part = est_ev * 23 / 20 / npart / nthreads + 32;
    let cl_bytes: usize = cl_files.iter().map(|f| f.len()).sum();
    let ln_bytes: usize = ln_files.iter().map(|f| f.len()).sum();
    let est_cl = cl_bytes / 52 + 1024;
    let cl_cap = est_cl * 13 / 10 / nthreads + 1024;
    // distinct user IDs are ~8% of event rows; size for ~2.5x that (load stays < 0.45)
    let est_users = (est_ev + est_cl) / 12 + ln_bytes / 16 + 4096;
    let cap_slots = est_users.next_power_of_two().max(1 << 16);
    // User indices are dense. Size the per-user arrays from a hard upper bound on distinct IDs
    // (a valid row is at least ~24 bytes and names at most two IDs), so they can never overflow;
    // on Linux untouched pages cost only address space.
    let bound_users = ev_bytes / 24 + cl_bytes / 16 + ln_bytes / 3 + 1024;
    let cap_users = if cfg!(target_os = "linux") {
        bound_users.max(cap_slots * 4)
    } else {
        (cap_slots * 4).min(bound_users.max(cap_slots))
    } + (nthreads + 8) * UBLOCK as usize;
    let ut = UTab::new(cap_slots, cap_users);
    let next = AtomicUsize::new(0);
    let parent_cell: OnceLock<Vec<u32>> = OnceLock::new();
    let lnext = AtomicUsize::new(0);
    let ldone = AtomicUsize::new(0);
    let ledges: Mutex<Vec<(usize, Vec<(u32, u32)>)>> = Mutex::new(Vec::new());
    #[cfg(target_arch = "x86_64")]
    let use_simd = std::env::var_os("SS_NOSIMD").is_none() && is_x86_feature_detected!("avx2") && is_x86_feature_detected!("bmi2");
    #[cfg(not(target_arch = "x86_64"))]
    let use_simd = false;
    struct P1 {
        ev: PartBuf<EvA>,
        cl: PartBuf<ClA>,
    }
    let p1: Vec<P1> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|t| {
                let cx = &cx;
                let chunks = &chunks;
                let next = &next;
                let lchunks = &lchunks;
                let lnext = &lnext;
                let ldone = &ldone;
                let ledges = &ledges;
                let parent_cell = &parent_cell;
                let ut = &ut;
                s.spawn(move || {
                    let mut st = PState::new(npart, per_part, cl_cap);
                    // links first: all threads parse and intern link chunks
                    loop {
                        let i = lnext.fetch_add(1, Ordering::Relaxed);
                        if i >= lchunks.len() {
                            break;
                        }
                        let c: Chunk = lchunks[i];
                        let data = c.data;
                        let mut v: Vec<LinkRec> = Vec::with_capacity((c.end - c.start) / 24 + 16);
                        if use_simd {
                            #[cfg(target_arch = "x86_64")]
                            unsafe {
                                let mut cb = |f: &[&'static [u8]; 2]| link_fields(f, &mut v);
                                scan_fields_px::<2, _>(data, c.start, c.end, &mut cb);
                            }
                        } else {
                            for_lines(data, c.start, c.end, |l| {
                                let mut f: [&'static [u8]; 2] = [&[]; 2];
                                if split_fields(l, &mut f) {
                                    link_fields(&f, &mut v);
                                }
                            });
                        }
                        let e = intern_links(&v, ut, &mut st.alloc);
                        ledges.lock().unwrap().push((i, e));
                        ldone.fetch_add(1, Ordering::Release);
                    }
                    if t == 0 {
                        while ldone.load(Ordering::Acquire) < lchunks.len() {
                            std::thread::yield_now();
                        }
                        let mut all = std::mem::take(&mut *ledges.lock().unwrap());
                        all.sort_by_key(|x| x.0);
                        let edges: Vec<Vec<(u32, u32)>> = all.into_iter().map(|x| x.1).collect();
                        log("links interned");
                        let parent = build_dsu(&edges, ut, cap_users);
                        log("links done");
                        let _ = parent_cell.set(parent);
                    }
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= chunks.len() {
                            break;
                        }
                        let c = chunks[i];
                        let data = c.data;
                        if use_simd {
                            #[cfg(target_arch = "x86_64")]
                            unsafe {
                                match c.kind {
                                    Kind::Event => {
                                        let mut cb = |f: &[&'static [u8]; 7]| event_fields(f, cx, &mut st, ut);
                                        scan_fields_px::<7, _>(data, c.start, c.end, &mut cb);
                                    }
                                    Kind::Click => {
                                        let mut cb = |f: &[&'static [u8]; 4]| click_fields(f, cx, &mut st, ut);
                                        scan_fields_px::<4, _>(data, c.start, c.end, &mut cb);
                                    }
                                    Kind::Link => {}
                                }
                            }
                        } else {
                            match c.kind {
                                Kind::Event => for_lines(data, c.start, c.end, |l| parse_event_line(l, cx, &mut st, ut)),
                                Kind::Click => for_lines(data, c.start, c.end, |l| parse_click_line(l, cx, &mut st, ut)),
                                Kind::Link => {}
                            }
                        }
                    }
                    st.flush(ut);
                    st.ev.finish();
                    st.cl.finish();
                    P1 { ev: st.ev, cl: st.cl }
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let parent = parent_cell.into_inner().expect("dsu");
    let n_users = ut.n_users().max(1);
    // person partitions: ranges of person index
    let mut shift2 = 0u32;
    while (n_users >> shift2) > (1usize << pbits2) {
        shift2 += 1;
    }
    let npart2 = (n_users >> shift2) + 1;
    log(&format!("phase 1 parse done ({} user slots, {} person partitions)", n_users, npart2));

    // ---------------- phase 2: dedup events -> person partitions; clicks -> person partitions
    const CL_PIECE: usize = 1 << 16;
    let mut cl_items: Vec<(usize, usize, usize)> = Vec::new();
    for (t, r) in p1.iter().enumerate() {
        let n = r.cl.len(0);
        let mut a = 0;
        while a < n {
            let b = (a + CL_PIECE).min(n);
            cl_items.push((t, a, b));
            a = b;
        }
    }
    let next = AtomicUsize::new(0);
    let total_items = npart + cl_items.len();
    struct P2 {
        ev: PartBuf<EvB>,
        cl: PartBuf<ClB>,
    }
    let ev_out_cap = est_ev * 21 / 20 / npart2 / nthreads + 32;
    let cl_out_cap = est_cl * 11 / 10 / npart2 / nthreads + 32;
    let p2: Vec<P2> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                let p1 = &p1;
                let parent = &parent;
                let cl_items = &cl_items;
                let next = &next;
                s.spawn(move || {
                    let mut r = P2 { ev: PartBuf::new(npart2, ev_out_cap), cl: PartBuf::new(npart2, cl_out_cap) };
                    // dedup table: (key, ingest, idx)
                    // dedup table slots: (key, ingest, gen << 24 | idx); a slot is empty unless its
                    // generation matches, so the table is never cleared between partitions
                    let mut table: Vec<(u64, u32, u32)> = Vec::new();
                    let mut gen: u32 = 0;
                    let mut loser: Vec<u64> = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= total_items {
                            break;
                        }
                        if i < npart {
                            let p = i;
                            let n: usize = p1.iter().map(|t| t.ev.len(p)).sum();
                            if n == 0 {
                                continue;
                            }
                            let t_a = Instant::now();
                            let cap = (n + n / 2).next_power_of_two().max(16);
                            let mask = cap - 1;
                            assert!(n < (1 << 24), "dedup partition too large");
                            if table.len() < cap {
                                table = vec![(0, 0, 0); cap];
                                gen = 0;
                            }
                            gen += 1;
                            if gen == 256 {
                                for t in table.iter_mut() {
                                    t.2 = 0;
                                }
                                gen = 1;
                            }
                            let g = gen << 24;
                            loser.clear();
                            loser.resize((n + 63) / 64, 0);
                            let mut j = 0u32;
                            for sl in p1.iter().flat_map(|t| [t.ev.main(p), t.ev.over(p)]) {
                                for e in sl.iter() {
                                    let key = e.key;
                                    let ing = e.ingest;
                                    let mut slot = (key as usize) & mask;
                                    loop {
                                        let tt = unsafe { table.get_unchecked_mut(slot) };
                                        if (tt.2 >> 24) != gen {
                                            *tt = (key, ing, g | j);
                                            break;
                                        }
                                        if tt.0 == key {
                                            if ing < tt.1 {
                                                let old = tt.2 & 0xFF_FFFF;
                                                loser[(old >> 6) as usize] |= 1u64 << (old & 63);
                                                tt.1 = ing;
                                                tt.2 = g | j;
                                            } else {
                                                loser[(j >> 6) as usize] |= 1u64 << (j & 63);
                                            }
                                            break;
                                        }
                                        slot = (slot + 1) & mask;
                                    }
                                    j += 1;
                                }
                            }
                            tadd(0, t_a);
                            let t_b = Instant::now();
                            let mut j = 0u32;
                            for v in p1.iter().flat_map(|t| [t.ev.main(p), t.ev.over(p)]) {
                                let m = v.len();
                                for k in 0..m {
                                    if k + 16 < m {
                                        let f = unsafe { *v.get_unchecked(k + 16) };
                                        prefetch(unsafe { parent.as_ptr().add(f.user as usize) });
                                    }
                                    let e = unsafe { *v.get_unchecked(k) };
                                    let jj = j;
                                    j += 1;
                                    if e.ts == u32::MAX || (loser[(jj >> 6) as usize] >> (jj & 63)) & 1 != 0 {
                                        continue;
                                    }
                                    let person = person_of(parent, e.user);
                                    r.ev.push((person >> shift2) as usize, EvB { person, ts: e.ts, meta: e.meta });
                                }
                            }
                            tadd(1, t_b);
                        } else {
                            let t_c = Instant::now();
                            let (t, a, b) = cl_items[i - npart];
                            let cm = p1[t].cl.main(0);
                            let co = p1[t].cl.over(0);
                            // items index the concatenation main ++ over
                            let v: &[ClA] = if b <= cm.len() {
                                &cm[a..b]
                            } else if a >= cm.len() {
                                &co[a - cm.len()..b - cm.len()]
                            } else {
                                &cm[a..]
                            };
                            let v2: &[ClA] = if a < cm.len() && b > cm.len() { &co[..b - cm.len()] } else { &[] };
                            for v in [v, v2] {
                                for (k, c) in v.iter().enumerate() {
                                    if k + 16 < v.len() {
                                        prefetch(unsafe { parent.as_ptr().add(v[k + 16].user as usize) });
                                    }
                                    let person = person_of(parent, c.user);
                                    r.cl.push(
                                        (person >> shift2) as usize,
                                        ClB { cid: c.cid, person, ts: c.ts, camp: c.camp, _pad: 0 },
                                    );
                                }
                            }
                            tadd(2, t_c);
                        }
                    }
                    r.ev.finish();
                    r.cl.finish();
                    r
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    log_mem("phase 2 end (p1+p2 live)");
    par_drop(p1);
    log("phase 2 dedup done");

    // ---------------- phase 3: per person partition
    let ncat = cats.len();
    let ncells = (cx.ev_hi / GAP_MS) as usize + 2;
    // sessions start inside the event window: its days, relative to base_day_off
    let win_dshift = (ws.div_euclid(DAY_MS) - base_day_off) as u32;
    let win_nd = ((we - 1).div_euclid(DAY_MS) - ws.div_euclid(DAY_MS) + 1) as usize;
    // largest partitions first (a whale person makes one partition much bigger)
    let mut order: Vec<usize> = (0..npart2).collect();
    let psize: Vec<usize> = (0..npart2).map(|q| p2.iter().map(|t| t.ev.len(q)).sum()).collect();
    order.sort_unstable_by(|a, b| psize[*b].cmp(&psize[*a]));
    let next = AtomicUsize::new(0);
    let workers: Vec<Worker> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                let p2 = &p2;
                let next = &next;
                let ut = &ut;
                let order = &order;
                s.spawn(move || {
                    let mut w = Worker::new(ncamp, ndays, ncat);
                    w.ensure_cells(ncells);
                    w.set_window(ncamp, win_dshift, win_nd);
                    let mut evp: Vec<&[EvB]> = Vec::with_capacity(p2.len() * 2);
                    let mut clp: Vec<&[ClB]> = Vec::with_capacity(p2.len() * 2);
                    loop {
                        let qi = next.fetch_add(1, Ordering::Relaxed);
                        if qi >= npart2 {
                            break;
                        }
                        let q = order[qi];
                        evp.clear();
                        clp.clear();
                        for t in p2.iter() {
                            evp.push(t.ev.main(q));
                            evp.push(t.ev.over(q));
                            clp.push(t.cl.main(q));
                            clp.push(t.cl.over(q));
                        }
                        let qbase = (q << shift2) as u32;
                        let qsize = 1usize << shift2;
                        w.partition(&evp, &clp, qbase, qsize, base, base_day_off, organic, ut);
                    }
                    w.finalize_stats();
                    w
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    par_drop(p2);
    log("phase 3 sessions done");
    log(&format!(
        "cpu ms: dedup-table {:.0}, dedup-emit {:.0}, clicks-map {:.0}, group {:.0}, persons {:.0} (cells {:.0}, attrib {:.0})",
        TACC[0].load(Ordering::Relaxed) as f64 / 1e6,
        TACC[1].load(Ordering::Relaxed) as f64 / 1e6,
        TACC[2].load(Ordering::Relaxed) as f64 / 1e6,
        TACC[3].load(Ordering::Relaxed) as f64 / 1e6,
        TACC[4].load(Ordering::Relaxed) as f64 / 1e6,
        TACC[5].load(Ordering::Relaxed) as f64 / 1e6,
        TACC[6].load(Ordering::Relaxed) as f64 / 1e6
    ));

    // ---------------- phase 4: write
    let ud = out_dir.join("user_daily");
    let cd = out_dir.join("campaign_daily");
    let _ = fs::create_dir_all(&ud);
    let _ = fs::create_dir_all(&cd);

    let mut stats = vec![[0u64; 4]; (ncamp + 1) * ndays];
    for w in &workers {
        for (a, b) in stats.iter_mut().zip(w.stats.iter()) {
            a[0] += b[0];
            a[1] += b[1];
            a[2] += b[2];
            a[3] += b[3];
        }
    }
    std::thread::scope(|s| {
        for (wi, w) in workers.iter().enumerate() {
            let ud = &ud;
            let cats = &cats;
            s.spawn(move || {
                let rows = &w.rows;
                if rows.is_empty() && wi != 0 {
                    return;
                }
                let names: Vec<&[u8]> = rows.iter().map(|r| sget(r.name)).collect();
                let days: Vec<i32> = rows.iter().map(|r| r.day).collect();
                let sess: Vec<i64> = rows.iter().map(|r| r.sessions as i64).collect();
                let evn: Vec<i64> = rows.iter().map(|r| r.events as i64).collect();
                let rev: Vec<i64> = rows.iter().map(|r| r.revenue as i64).collect();
                let top: Vec<Option<&[u8]>> =
                    rows.iter().map(|r| if r.top == u32::MAX { None } else { Some(cats[r.top as usize]) }).collect();
                let cols = [
                    pq::Column { name: "user_id", data: pq::Data::Str(&names) },
                    pq::Column { name: "day", data: pq::Data::Date(&days) },
                    pq::Column { name: "sessions", data: pq::Data::I64(&sess) },
                    pq::Column { name: "events", data: pq::Data::I64(&evn) },
                    pq::Column { name: "revenue_cents", data: pq::Data::I64(&rev) },
                    pq::Column { name: "top_category", data: pq::Data::OptStr(&top) },
                ];
                let bytes = pq::write_file(&cols, rows.len());
                fs::write(ud.join(format!("part-{wi:03}.parquet")), bytes).expect("write user_daily");
            });
        }
        let mut c_id: Vec<&[u8]> = Vec::new();
        let mut c_ch: Vec<&[u8]> = Vec::new();
        let mut c_day: Vec<i32> = Vec::new();
        let mut c_s: Vec<i64> = Vec::new();
        let mut c_p: Vec<i64> = Vec::new();
        let mut c_r: Vec<i64> = Vec::new();
        let mut c_a: Vec<i64> = Vec::new();
        for c in 0..=ncamp {
            for d in 0..ndays {
                let st = stats[c * ndays + d];
                if st[0] == 0 && st[3] == 0 {
                    continue;
                }
                if c == organic {
                    c_id.push(b"organic");
                    c_ch.push(b"organic");
                } else {
                    c_id.push(camp_ids[c]);
                    c_ch.push(camp_ch[c]);
                }
                c_day.push((base_day_off + d as i64) as i32);
                c_s.push(st[0] as i64);
                c_p.push(st[1] as i64);
                c_r.push(st[2] as i64);
                c_a.push(if c == organic { 0 } else { st[3] as i64 });
            }
        }
        let cols = [
            pq::Column { name: "campaign_id", data: pq::Data::Str(&c_id) },
            pq::Column { name: "channel", data: pq::Data::Str(&c_ch) },
            pq::Column { name: "day", data: pq::Data::Date(&c_day) },
            pq::Column { name: "sessions", data: pq::Data::I64(&c_s) },
            pq::Column { name: "purchasing_sessions", data: pq::Data::I64(&c_p) },
            pq::Column { name: "revenue_cents", data: pq::Data::I64(&c_r) },
            pq::Column { name: "assisted_sessions", data: pq::Data::I64(&c_a) },
        ];
        let bytes = pq::write_file(&cols, c_id.len());
        fs::write(cd.join("part-000.parquet"), bytes).expect("write campaign_daily");
    });
    log("phase 4 write done");
    par_drop(workers);
    log("released");
    fast_exit(nthreads);
}
