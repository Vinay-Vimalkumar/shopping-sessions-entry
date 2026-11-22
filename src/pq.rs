//! Minimal Parquet writer: one row group per file, one PLAIN data page (v1) per
//! column, no compression. Supports required BYTE_ARRAY (UTF8), optional
//! BYTE_ARRAY (UTF8), INT32 (DATE) and INT64 columns.

pub enum Data<'a> {
    Str(&'a [&'a [u8]]),
    OptStr(&'a [Option<&'a [u8]>]),
    Date(&'a [i32]),
    I64(&'a [i64]),
}

pub struct Column<'a> {
    pub name: &'static str,
    pub data: Data<'a>,
}

// Thrift compact protocol type ids
const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

fn varint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

fn zz32(v: i32) -> u64 {
    ((v << 1) ^ (v >> 31)) as u32 as u64
}

fn zz64(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

struct Thrift<'b> {
    buf: &'b mut Vec<u8>,
    last: Vec<i16>,
}

impl<'b> Thrift<'b> {
    fn new(buf: &'b mut Vec<u8>) -> Self {
        Thrift { buf, last: vec![0] }
    }
    fn header(&mut self, id: i16, ty: u8) {
        let last = self.last.last_mut().unwrap();
        let delta = id - *last;
        if delta > 0 && delta <= 15 {
            self.buf.push(((delta as u8) << 4) | ty);
        } else {
            self.buf.push(ty);
            varint(self.buf, zz32(id as i32));
        }
        *last = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.header(id, T_I32);
        varint(self.buf, zz32(v));
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.header(id, T_I64);
        varint(self.buf, zz64(v));
    }
    fn bin(&mut self, id: i16, v: &[u8]) {
        self.header(id, T_BINARY);
        varint(self.buf, v.len() as u64);
        self.buf.extend_from_slice(v);
    }
    fn struct_begin(&mut self, id: i16) {
        self.header(id, T_STRUCT);
        self.last.push(0);
    }
    fn struct_end(&mut self) {
        self.buf.push(0);
        self.last.pop();
    }
    fn list_begin(&mut self, id: i16, elem: u8, n: usize) {
        self.header(id, T_LIST);
        self.list_header(elem, n);
    }
    fn list_header(&mut self, elem: u8, n: usize) {
        if n < 15 {
            self.buf.push(((n as u8) << 4) | elem);
        } else {
            self.buf.push(0xF0 | elem);
            varint(self.buf, n as u64);
        }
    }
    // element of a list of structs
    fn elem_begin(&mut self) {
        self.last.push(0);
    }
    fn elem_end(&mut self) {
        self.buf.push(0);
        self.last.pop();
    }
}

struct ChunkMeta {
    ptype: i32,
    num_values: i64,
    size: i64,
    offset: i64,
    optional: bool,
}

fn encode_page(col: &Column, rows: usize, page: &mut Vec<u8>) {
    page.clear();
    match &col.data {
        Data::Str(v) => {
            for s in v.iter() {
                page.extend_from_slice(&(s.len() as u32).to_le_bytes());
                page.extend_from_slice(s);
            }
        }
        Data::OptStr(v) => {
            // definition levels: 4-byte length, then RLE/bit-packed hybrid, bit width 1
            let groups = (rows + 7) / 8;
            let mut lv = Vec::with_capacity(groups + 10);
            varint(&mut lv, ((groups as u64) << 1) | 1);
            for g in 0..groups {
                let mut b = 0u8;
                for k in 0..8 {
                    let i = g * 8 + k;
                    if i < rows && v[i].is_some() {
                        b |= 1 << k;
                    }
                }
                lv.push(b);
            }
            page.extend_from_slice(&(lv.len() as u32).to_le_bytes());
            page.extend_from_slice(&lv);
            for s in v.iter().flatten() {
                page.extend_from_slice(&(s.len() as u32).to_le_bytes());
                page.extend_from_slice(s);
            }
        }
        Data::Date(v) => {
            for x in v.iter() {
                page.extend_from_slice(&x.to_le_bytes());
            }
        }
        Data::I64(v) => {
            for x in v.iter() {
                page.extend_from_slice(&x.to_le_bytes());
            }
        }
    }
}

/// Serialize a complete Parquet file with the given columns (all the same length).
pub fn write_file(cols: &[Column], rows: usize) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::with_capacity(1 << 16);
    out.extend_from_slice(b"PAR1");
    let mut metas: Vec<ChunkMeta> = Vec::new();
    let mut page = Vec::new();
    for col in cols {
        encode_page(col, rows, &mut page);
        let offset = out.len() as i64;
        let mut hdr = Vec::with_capacity(32);
        {
            let mut t = Thrift::new(&mut hdr);
            t.i32(1, 0); // DATA_PAGE
            t.i32(2, page.len() as i32);
            t.i32(3, page.len() as i32);
            t.struct_begin(5);
            t.i32(1, rows as i32);
            t.i32(2, 0); // PLAIN
            t.i32(3, 3); // RLE
            t.i32(4, 3); // RLE
            t.struct_end();
            t.buf.push(0); // end PageHeader
        }
        out.extend_from_slice(&hdr);
        out.extend_from_slice(&page);
        let (ptype, optional) = match col.data {
            Data::Str(_) => (6, false),
            Data::OptStr(_) => (6, true),
            Data::Date(_) => (1, false),
            Data::I64(_) => (2, false),
        };
        metas.push(ChunkMeta {
            ptype,
            num_values: rows as i64,
            size: (hdr.len() + page.len()) as i64,
            offset,
            optional,
        });
    }
    // FileMetaData
    let meta_start = out.len();
    {
        let mut buf = Vec::new();
        {
            let mut t = Thrift::new(&mut buf);
            t.i32(1, 1); // version
            t.list_begin(2, T_STRUCT, cols.len() + 1);
            // root
            t.elem_begin();
            t.bin(4, b"schema");
            t.i32(5, cols.len() as i32);
            t.elem_end();
            for (col, m) in cols.iter().zip(metas.iter()) {
                t.elem_begin();
                t.i32(1, m.ptype);
                t.i32(3, if m.optional { 1 } else { 0 });
                t.bin(4, col.name.as_bytes());
                match col.data {
                    Data::Str(_) | Data::OptStr(_) => {
                        t.i32(6, 0); // converted UTF8
                        t.struct_begin(10); // logicalType
                        t.struct_begin(1); // STRING
                        t.struct_end();
                        t.struct_end();
                    }
                    Data::Date(_) => {
                        t.i32(6, 6); // converted DATE
                        t.struct_begin(10);
                        t.struct_begin(6); // DATE
                        t.struct_end();
                        t.struct_end();
                    }
                    Data::I64(_) => {}
                }
                t.elem_end();
            }
            t.i64(3, rows as i64);
            t.list_begin(4, T_STRUCT, 1);
            t.elem_begin(); // RowGroup
            t.list_begin(1, T_STRUCT, cols.len());
            let mut total = 0i64;
            for (col, m) in cols.iter().zip(metas.iter()) {
                total += m.size;
                t.elem_begin(); // ColumnChunk
                t.i64(2, m.offset);
                t.struct_begin(3); // ColumnMetaData
                t.i32(1, m.ptype);
                t.list_begin(2, T_I32, 2);
                varint(t.buf, zz32(0)); // PLAIN
                varint(t.buf, zz32(3)); // RLE
                t.list_begin(3, T_BINARY, 1);
                varint(t.buf, col.name.len() as u64);
                t.buf.extend_from_slice(col.name.as_bytes());
                t.i32(4, 0); // UNCOMPRESSED
                t.i64(5, m.num_values);
                t.i64(6, m.size);
                t.i64(7, m.size);
                t.i64(9, m.offset);
                t.struct_end();
                t.elem_end();
            }
            t.i64(2, total);
            t.i64(3, rows as i64);
            t.elem_end();
            t.bin(6, b"sessions-engine");
            t.buf.push(0); // end FileMetaData
        }
        out.extend_from_slice(&buf);
    }
    let meta_len = (out.len() - meta_start) as u32;
    out.extend_from_slice(&meta_len.to_le_bytes());
    out.extend_from_slice(b"PAR1");
    out
}
