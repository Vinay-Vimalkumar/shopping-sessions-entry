//! Shopping Sessions pipeline: exact implementation of rules.md.
//! Single binary, multi-threaded, partitioned hash dedup + per-person sweep.

mod pq;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use xxhash_rust::xxh3::{xxh3_128, xxh3_64};

const DAY_MS: i64 = 86_400_000;
const GAP_MS: u32 = 1_800_000;
const WEEK_MS: i64 = 604_800_000;
const LATE_MS: i64 = 86_400_000;

const PBITS: u32 = 8;
const NPART: usize = 1 << PBITS;

// ---------------------------------------------------------------- logging
static T0: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
fn log(msg: &str) {
    if std::env::var_os("SS_LOG").is_some() {
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
    if n == 20 {
        if b[19] != b'Z' {
            return None;
        }
        return Some(parse_dt(b)? * 1000);
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

/// ingest_ts: strict F1 -> epoch seconds
#[inline(always)]
fn parse_f1_secs(b: &[u8]) -> Option<i64> {
    if b.len() != 20 || b[19] != b'Z' {
        return None;
    }
    parse_dt(b)
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

/// Open addressing u64 (nonzero) -> u32.
struct U64Map {
    keys: Vec<u64>,
    vals: Vec<u32>,
    mask: usize,
    len: usize,
}

impl U64Map {
    fn with_capacity(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        U64Map { keys: vec![0; cap], vals: vec![0; cap], mask: cap - 1, len: 0 }
    }
    #[inline(always)]
    fn get(&self, k: u64) -> Option<u32> {
        let mut i = (k as usize) & self.mask;
        loop {
            let kk = self.keys[i];
            if kk == k {
                return Some(self.vals[i]);
            }
            if kk == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    /// returns (value, inserted)
    #[inline(always)]
    fn get_or_insert(&mut self, k: u64, v: u32) -> (u32, bool) {
        if (self.len + 1) * 2 > self.keys.len() {
            self.grow();
        }
        let mut i = (k as usize) & self.mask;
        loop {
            let kk = self.keys[i];
            if kk == k {
                return (self.vals[i], false);
            }
            if kk == 0 {
                self.keys[i] = k;
                self.vals[i] = v;
                self.len += 1;
                return (v, true);
            }
            i = (i + 1) & self.mask;
        }
    }
    fn grow(&mut self) {
        let old_k = std::mem::take(&mut self.keys);
        let old_v = std::mem::take(&mut self.vals);
        let cap = old_k.len() * 2;
        self.keys = vec![0; cap];
        self.vals = vec![0; cap];
        self.mask = cap - 1;
        for (k, v) in old_k.into_iter().zip(old_v) {
            if k != 0 {
                let mut i = (k as usize) & self.mask;
                while self.keys[i] != 0 {
                    i = (i + 1) & self.mask;
                }
                self.keys[i] = k;
                self.vals[i] = v;
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
    Link,
    Event,
    Click,
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
    eid_h: u64,
    uid_h: u64,
    uid: u64, // sref
    ingest: u32,
    ts: u32, // ms since base (ws - 7d); u32::MAX = invalid
    meta: u32, // price (24 bits) | cat << 24 ; price 0 = not a purchase
    chk: u32,
}

/// Surviving event, partitioned by person.
#[derive(Clone, Copy)]
#[repr(C)]
struct EvB {
    name_h: u64,
    name: u64, // sref of person name
    ts: u32,
    meta: u32,
}

/// Valid click (before person mapping).
#[derive(Clone, Copy)]
#[repr(C)]
struct ClA {
    uid_h: u64,
    cid: u64, // sref
    ts: u32,
    camp: u32,
}

/// Valid click, partitioned by person.
#[derive(Clone, Copy)]
#[repr(C)]
struct ClB {
    name_h: u64,
    cid: u64,
    ts: u32,
    camp: u32,
}

struct Ctx {
    base: i64,    // ws - 7d (ms)
    ev_lo: u32,   // ws - base
    ev_hi: u32,   // we - base
    cl_hi: u32,   // we - base
    base_day: i64, // epoch day of base... (not necessarily midnight)
    ingest_base: i64, // seconds
    products: StrMap,
    campaigns: StrMap,
    unknown_cat: u32,
}

#[inline(always)]
fn part_of(h: u64) -> usize {
    (h >> (64 - PBITS)) as usize
}

fn parse_event_line(line: &'static [u8], cx: &Ctx, out: &mut [Vec<EvA>]) {
    let mut f: [&'static [u8]; 7] = [&[]; 7];
    if !split_fields(line, &mut f) {
        return;
    }
    let eid = f[0];
    if eid.is_empty() {
        return;
    }
    let h128 = xxh3_128(eid);
    let eid_h = h128 as u64;
    let chk = (h128 >> 64) as u32;
    let ingest_s = parse_f1_secs(f[3]);
    let ingest = match ingest_s {
        Some(s) => (s - cx.ingest_base).clamp(0, u32::MAX as i64 - 1) as u32,
        None => u32::MAX,
    };
    let mut rec = EvA { eid_h, uid_h: 0, uid: 0, ingest, ts: u32::MAX, meta: 0, chk };
    'valid: {
        let uid = f[1];
        if uid.is_empty() {
            break 'valid;
        }
        let ts = match parse_ts(f[2]) {
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
        rec.uid_h = nz(xxh3_64(uid));
        rec.uid = sref(uid);
        rec.ts = rel as u32;
        rec.meta = meta;
    }
    out[part_of(eid_h)].push(rec);
}

fn parse_click_line(line: &'static [u8], cx: &Ctx, out: &mut Vec<ClA>) {
    let mut f: [&'static [u8]; 4] = [&[]; 4];
    if !split_fields(line, &mut f) {
        return;
    }
    let cid = f[0];
    if cid.is_empty() {
        return;
    }
    let uid = f[1];
    if uid.is_empty() {
        return;
    }
    let ts = match parse_ts(f[3]) {
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
    out.push(ClA { uid_h: nz(xxh3_64(uid)), cid: sref(cid), ts: rel as u32, camp });
}

fn parse_link_line(line: &'static [u8], out: &mut Vec<(&'static [u8], &'static [u8])>) {
    let mut f: [&'static [u8]; 2] = [&[]; 2];
    if !split_fields(line, &mut f) {
        return;
    }
    if f[0].is_empty() || f[1].is_empty() {
        return;
    }
    out.push((f[0], f[1]));
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

// ---------------------------------------------------------------- per-person processing
struct UserRow {
    name: u64,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: u64,
    top: u32, // u32::MAX = null
}

struct Worker {
    // campaign stats: [camp * ndays + day] -> sessions, purchasing, revenue, assisted
    stats: Vec<[u64; 4]>,
    ndays: usize,
    ncat: usize,
    cnt: Vec<u32>,
    open: Vec<u32>,
    touched: Vec<u32>,
    catrev: Vec<u64>,
    sess_start: Vec<u32>,
    sess_day: Vec<u32>,
    sess_rev: Vec<u64>,
    sess_purch: Vec<bool>,
    run_of: Vec<u32>,
    runs: Vec<(u32, u32)>, // (day, first session idx)
    rows: Vec<UserRow>,
}

impl Worker {
    fn new(ncamp: usize, ndays: usize, ncat: usize) -> Self {
        Worker {
            stats: vec![[0; 4]; (ncamp + 1) * ndays],
            ndays,
            ncat,
            cnt: vec![0; ncamp + 1],
            open: vec![0; ncamp + 1],
            touched: Vec::new(),
            catrev: vec![0; ncat],
            sess_start: Vec::new(),
            sess_day: Vec::new(),
            sess_rev: Vec::new(),
            sess_purch: Vec::new(),
            run_of: Vec::new(),
            runs: Vec::new(),
            rows: Vec::new(),
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
                self.stats[c * self.ndays + day as usize][3] += (hi - lo) as u64;
            }
            r += 1;
        }
    }

    /// evs: one person's events sorted by ts; cls: one person's clicks sorted by (ts, cid).
    fn person(&mut self, evs: &[EvB], cls: &[ClB], base_day_off: i64, base: i64, organic: usize) {
        // sessionize
        self.sess_start.clear();
        self.sess_day.clear();
        self.sess_rev.clear();
        self.sess_purch.clear();
        self.runs.clear();
        self.run_of.clear();
        let name = evs[0].name;
        let n = evs.len();
        let mut i = 0;
        // current user-day accumulators
        let mut cur_day: u32 = u32::MAX;
        let mut d_sess = 0u32;
        let mut d_events = 0u32;
        let mut d_rev = 0u64;
        let mut cat_touched = false;
        while i < n {
            let start = evs[i].ts;
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
            while i < n && evs[i].ts - last <= GAP_MS {
                let e = evs[i];
                last = e.ts;
                cnt += 1;
                let price = e.meta & 0xFF_FFFF;
                if price != 0 {
                    rev += price as u64;
                    self.catrev[(e.meta >> 24) as usize] += price as u64;
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
            self.sess_purch.push(rev > 0);
        }
        self.emit_row(name, cur_day, d_sess, d_events, d_rev, cat_touched, base_day_off);

        // attribution
        let s_n = self.sess_start.len();
        let nc = cls.len();
        let mut lo = 0usize;
        let mut hi = 0usize;
        for k in 0..s_n {
            let s = self.sess_start[k];
            let s_lo = s as i64 - WEEK_MS; // may be < 0 relative to base: base is ws-7d and s >= ws-base = 7d, so >= 0
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
            let st = &mut self.stats[credited * self.ndays + self.sess_day[k] as usize];
            st[0] += 1;
            st[1] += self.sess_purch[k] as u64;
            st[2] += self.sess_rev[k];
        }
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
        self.rows.push(UserRow {
            name,
            day: (base_day_off + day as i64) as i32,
            sessions,
            events,
            revenue,
            top,
        });
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
    let nthreads = std::env::var("SS_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .or_else(|| std::env::var("NCPU").ok().and_then(|v| v.parse().ok()))
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8))
        .max(1);

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

    // campaigns
    let crows = read_table2(&data_dir.join("campaigns.csv"));
    // dedup by id, last wins
    let mut camp_ids: Vec<&'static [u8]> = Vec::new();
    let mut camp_ch: Vec<&'static [u8]> = Vec::new();
    {
        let mut tmp: Vec<(&'static [u8], u32)> = Vec::new();
        for (i, r) in crows.iter().enumerate() {
            tmp.push((r.0, i as u32));
        }
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
        base_day: base_day_off,
        ingest_base: ws / 1000 - (1i64 << 30),
        products,
        campaigns,
        unknown_cat,
    };
    let _ = cx.base_day;

    // files
    let ev_files: Vec<&'static [u8]> = csv_files(&data_dir.join("events")).iter().map(|p| map_file(p)).collect();
    let cl_files: Vec<&'static [u8]> = csv_files(&data_dir.join("clicks")).iter().map(|p| map_file(p)).collect();
    let ln_files: Vec<&'static [u8]> = csv_files(&data_dir.join("links")).iter().map(|p| map_file(p)).collect();
    let ev_bytes: usize = ev_files.iter().map(|f| f.len()).sum();
    let mut chunks = Vec::new();
    make_chunks(Kind::Link, &ln_files, 1 << 20, &mut chunks);
    make_chunks(Kind::Event, &ev_files, 4 << 20, &mut chunks);
    make_chunks(Kind::Click, &cl_files, 4 << 20, &mut chunks);
    log(&format!("setup done: {} chunks, {} threads, ndays {}", chunks.len(), nthreads, ndays));

    // ---------------- phase 1: parse
    let est_ev = ev_bytes / 70 + 1024;
    let per_part = est_ev / NPART / nthreads + 64;
    let next = AtomicUsize::new(0);
    struct P1 {
        ev: Vec<Vec<EvA>>,
        cl: Vec<ClA>,
        ln: Vec<(&'static [u8], &'static [u8])>,
    }
    let p1: Vec<P1> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                s.spawn(|| {
                    let mut r = P1 {
                        ev: (0..NPART).map(|_| Vec::with_capacity(per_part)).collect(),
                        cl: Vec::new(),
                        ln: Vec::new(),
                    };
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= chunks.len() {
                            break;
                        }
                        let c = chunks[i];
                        match c.kind {
                            Kind::Event => for_lines(c.data, c.start, c.end, |l| parse_event_line(l, &cx, &mut r.ev)),
                            Kind::Click => for_lines(c.data, c.start, c.end, |l| parse_click_line(l, &cx, &mut r.cl)),
                            Kind::Link => for_lines(c.data, c.start, c.end, |l| parse_link_line(l, &mut r.ln)),
                        }
                    }
                    r
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    log("phase 1 parse done");

    // ---------------- phase 2: links -> persons
    let nlinks: usize = p1.iter().map(|r| r.ln.len()).sum();
    let mut node_of = U64Map::with_capacity(nlinks + 16);
    let mut node_str: Vec<&'static [u8]> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    }
    for r in &p1 {
        for &(a, b) in &r.ln {
            let ha = nz(xxh3_64(a));
            let hb = nz(xxh3_64(b));
            let (ia, new_a) = node_of.get_or_insert(ha, node_str.len() as u32);
            if new_a {
                node_str.push(a);
                parent.push(ia);
            }
            let (ib, new_b) = node_of.get_or_insert(hb, node_str.len() as u32);
            if new_b {
                node_str.push(b);
                parent.push(ib);
            }
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                // keep root = byte-wise smaller name
                if node_str[ra as usize] <= node_str[rb as usize] {
                    parent[rb as usize] = ra;
                } else {
                    parent[ra as usize] = rb;
                }
            }
        }
    }
    let nnodes = node_str.len();
    // person info per node: (name_h, name sref)
    let mut node_person: Vec<(u64, u64)> = Vec::with_capacity(nnodes);
    {
        let mut root_info: Vec<(u64, u64)> = vec![(0, 0); nnodes];
        for i in 0..nnodes {
            let r = find(&mut parent, i as u32) as usize;
            if root_info[r].0 == 0 {
                let s = node_str[r];
                root_info[r] = (nz(xxh3_64(s)), sref(s));
            }
            node_person.push(root_info[r]);
        }
    }
    log(&format!("phase 2 links done: {} links, {} ids", nlinks, nnodes));

    // ---------------- phase 3: dedup events -> person partitions; clicks -> person partitions
    let next = AtomicUsize::new(0);
    let ncl_items = p1.len();
    let total_items = NPART + ncl_items;
    struct P3 {
        ev: Vec<Vec<EvB>>,
        cl: Vec<Vec<ClB>>,
    }
    let p3: Vec<P3> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                s.spawn(|| {
                    let mut r = P3 {
                        ev: (0..NPART).map(|_| Vec::with_capacity(per_part)).collect(),
                        cl: (0..NPART).map(|_| Vec::new()).collect(),
                    };
                    let mut table: Vec<(u64, u32, u32)> = Vec::new();
                    let mut recs: Vec<EvA> = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= total_items {
                            break;
                        }
                        if i < NPART {
                            let p = i;
                            recs.clear();
                            for t in &p1 {
                                recs.extend_from_slice(&t.ev[p]);
                            }
                            let n = recs.len();
                            let cap = (n * 2).next_power_of_two().max(16);
                            let mask = cap - 1;
                            table.clear();
                            table.resize(cap, (0, 0, u32::MAX));
                            for (j, e) in recs.iter().enumerate() {
                                let mut slot = (e.eid_h as usize) & mask;
                                loop {
                                    let t = &mut table[slot];
                                    if t.2 == u32::MAX {
                                        *t = (e.eid_h, e.chk, j as u32);
                                        break;
                                    }
                                    if t.0 == e.eid_h && t.1 == e.chk {
                                        if e.ingest < recs[t.2 as usize].ingest {
                                            t.2 = j as u32;
                                        }
                                        break;
                                    }
                                    slot = (slot + 1) & mask;
                                }
                            }
                            for t in table.iter() {
                                if t.2 == u32::MAX {
                                    continue;
                                }
                                let e = &recs[t.2 as usize];
                                if e.ts == u32::MAX {
                                    continue;
                                }
                                let (name_h, name) = match node_of.get(e.uid_h) {
                                    Some(node) => node_person[node as usize],
                                    None => (e.uid_h, e.uid),
                                };
                                r.ev[part_of(name_h)].push(EvB { name_h, name, ts: e.ts, meta: e.meta });
                            }
                        } else {
                            let t = i - NPART;
                            for c in &p1[t].cl {
                                let name_h = match node_of.get(c.uid_h) {
                                    Some(node) => node_person[node as usize].0,
                                    None => c.uid_h,
                                };
                                r.cl[part_of(name_h)].push(ClB { name_h, cid: c.cid, ts: c.ts, camp: c.camp });
                            }
                        }
                    }
                    r
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    drop(p1);
    log("phase 3 dedup done");

    // ---------------- phase 4: per person
    let ncat = cats.len();
    let next = AtomicUsize::new(0);
    let workers: Vec<Worker> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                s.spawn(|| {
                    let mut w = Worker::new(ncamp, ndays, ncat);
                    let mut evs: Vec<EvB> = Vec::new();
                    let mut cls: Vec<ClB> = Vec::new();
                    loop {
                        let q = next.fetch_add(1, Ordering::Relaxed);
                        if q >= NPART {
                            break;
                        }
                        evs.clear();
                        cls.clear();
                        for t in &p3 {
                            evs.extend_from_slice(&t.ev[q]);
                            cls.extend_from_slice(&t.cl[q]);
                        }
                        evs.sort_unstable_by(|a, b| a.name_h.cmp(&b.name_h).then(a.ts.cmp(&b.ts)));
                        cls.sort_unstable_by(|a, b| {
                            a.name_h.cmp(&b.name_h).then(a.ts.cmp(&b.ts)).then_with(|| sget(a.cid).cmp(sget(b.cid)))
                        });
                        let mut ci = 0usize;
                        let mut i = 0usize;
                        while i < evs.len() {
                            let nh = evs[i].name_h;
                            let mut j = i + 1;
                            while j < evs.len() && evs[j].name_h == nh {
                                j += 1;
                            }
                            while ci < cls.len() && cls[ci].name_h < nh {
                                ci += 1;
                            }
                            let mut cj = ci;
                            while cj < cls.len() && cls[cj].name_h == nh {
                                cj += 1;
                            }
                            w.person(&evs[i..j], &cls[ci..cj], base_day_off, base, organic);
                            i = j;
                            ci = cj;
                        }
                    }
                    w
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    drop(p3);
    log("phase 4 sessions done");

    // ---------------- phase 5: write
    let ud = out_dir.join("user_daily");
    let cd = out_dir.join("campaign_daily");
    let _ = fs::create_dir_all(&ud);
    let _ = fs::create_dir_all(&cd);

    // campaign stats merge
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
        // campaign_daily
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
    log("phase 5 write done");
}
