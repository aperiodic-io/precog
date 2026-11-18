//! Batch encodings compared by the protocol POCs.

use super::{Record, SYMBOLS};
use xxhash_rust::xxh3::xxh3_64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Layout {
    Pcb2Names,
    Pcb2Ids,
    Compact,
    Columnar,
    ColumnarLz4,
    Shadow,
}

pub struct Log<'a> {
    pub stream: &'a str,
    pub writer: &'a str,
    pub stream_id: u32,
    pub writer_id: u32,
    pub epoch: u64,
}

pub enum Key<'a> {
    Id(&'a [u8]),
    Hash(u64),
}

pub struct Rec<'a> {
    pub time_us: u64,
    pub key: Key<'a>,
    pub symbol: Option<&'a [u8]>,
    pub payload: Option<&'a [u8]>,
    pub len: usize,
}

#[derive(Debug)]
pub struct Batch {
    pub names: Option<(String, String)>,
    pub stream_id: u32,
    pub writer_id: u32,
    pub epoch: u64,
    pub first_seq: u64,
    pub count: u32,
}

#[derive(Default)]
pub struct Enc {
    /// Everything before the payload column (all of a PCB2 batch, whose payloads are inline).
    pub head: Vec<u8>,
    /// The payload column of a columnar batch, compressed or not.
    pub blob: Vec<u8>,
    raw: Vec<u8>,
}

macro_rules! ensure {
    ($c:expr, $($m:tt)+) => {
        if !$c {
            return Err(format!($($m)+));
        }
    };
}

const PCB2: &[u8; 4] = b"PCB2";
/// PCB2 with session-interned `u32` stream and writer ids instead of names.
const PCI2: &[u8; 4] = b"PCI2";
/// The columnar family (report §6): a fixed header, uncompressed broker columns, a payload blob.
const PCB3: &[u8; 4] = b"PCB3";
/// `magic | ver u8 | codec u8 | flags u16 | stream_id u32 | writer_id u32 | epoch u64
/// | first_seq u64 | count u32 | base_time_us u64 | cols_len u32 | raw_len u32 | blob_len u32
/// | crc32c u32` (over the header before the crc, the columns and the blob).
pub const HEADER: usize = 60;
const MAX_COUNT: u32 = 65_536;
const MAX_KEY: usize = 1024;

fn codec(layout: Layout) -> u8 {
    match layout {
        Layout::Compact => 0,
        Layout::Columnar => 1,
        Layout::ColumnarLz4 => 2,
        Layout::Shadow => 3,
        Layout::Pcb2Names | Layout::Pcb2Ids => unreachable!(),
    }
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn take_varint(c: &mut &[u8]) -> Result<u64, String> {
    let mut v = 0u64;
    for i in 0..10 {
        let Some((&b, rest)) = c.split_first() else {
            return Err("truncated varint".into());
        };
        *c = rest;
        v |= ((b & 0x7f) as u64) << (7 * i);
        if b < 0x80 {
            return Ok(v);
        }
    }
    Err("varint too long".into())
}

fn take<'a>(c: &mut &'a [u8], n: usize) -> Result<&'a [u8], String> {
    ensure!(c.len() >= n, "truncated");
    let (a, rest) = c.split_at(n);
    *c = rest;
    Ok(a)
}

fn u16_at(b: &[u8], i: usize) -> u16 {
    u16::from_le_bytes(b[i..i + 2].try_into().unwrap())
}
fn u32_at(b: &[u8], i: usize) -> u32 {
    u32::from_le_bytes(b[i..i + 4].try_into().unwrap())
}
fn u64_at(b: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(b[i..i + 8].try_into().unwrap())
}

impl Enc {
    /// Appends one batch of `recs` (sequences `first_seq..`) to `head` and `blob`.
    pub fn encode(&mut self, layout: Layout, log: &Log, first_seq: u64, recs: &[Record]) {
        assert!(!recs.is_empty() && recs.len() <= MAX_COUNT as usize);
        let start = self.head.len();
        let h = &mut self.head;
        if let Layout::Pcb2Names | Layout::Pcb2Ids = layout {
            if layout == Layout::Pcb2Names {
                h.extend_from_slice(PCB2);
                for name in [log.stream, log.writer] {
                    h.extend_from_slice(&(name.len() as u16).to_le_bytes());
                    h.extend_from_slice(name.as_bytes());
                }
            } else {
                h.extend_from_slice(PCI2);
                h.extend_from_slice(&log.stream_id.to_le_bytes());
                h.extend_from_slice(&log.writer_id.to_le_bytes());
            }
            h.extend_from_slice(&log.epoch.to_le_bytes());
            h.extend_from_slice(&first_seq.to_le_bytes());
            h.extend_from_slice(&(recs.len() as u32).to_le_bytes());
            for r in recs {
                h.extend_from_slice(&r.time_us.to_le_bytes());
                h.extend_from_slice(&(r.id.len() as u16).to_le_bytes());
                h.extend_from_slice(&(r.payload.len() as u32).to_le_bytes());
                h.extend_from_slice(r.id.as_bytes());
                h.extend_from_slice(&r.payload);
            }
            let crc = crc32c::crc32c(&h[start..]);
            h.extend_from_slice(&crc.to_le_bytes());
            return;
        }
        let base = recs[0].time_us;
        h.resize(start + HEADER, 0);
        let hash = |r: &Record| xxh3_64(r.id.as_bytes()).to_le_bytes();
        match layout {
            Layout::Compact => {
                for r in recs {
                    let dt = r
                        .time_us
                        .checked_sub(base)
                        .expect("times from the batch base");
                    h.extend_from_slice(&u32::try_from(dt).unwrap().to_le_bytes());
                }
                recs.iter().for_each(|r| h.extend_from_slice(&hash(r)));
                for r in recs {
                    h.extend_from_slice(&u16::try_from(r.payload.len()).unwrap().to_le_bytes());
                }
            }
            Layout::Columnar | Layout::ColumnarLz4 => {
                let mut dict: Vec<u8> = Vec::new(); // symbol indexes in first-seen order
                let keys: Vec<u8> = recs
                    .iter()
                    .map(|r| match dict.iter().position(|&s| s == r.symbol) {
                        Some(i) => i as u8,
                        None => {
                            dict.push(r.symbol);
                            (dict.len() - 1) as u8
                        }
                    })
                    .collect();
                h.push(dict.len() as u8);
                for &s in &dict {
                    let s = SYMBOLS[s as usize];
                    h.push(s.len() as u8);
                    h.extend_from_slice(s.as_bytes());
                }
                h.extend_from_slice(&keys);
                recs.iter().for_each(|r| h.extend_from_slice(&hash(r)));
                let mut prev = base as i64;
                for r in recs {
                    let d = r.time_us as i64 - prev;
                    prev = r.time_us as i64;
                    put_varint(h, ((d << 1) ^ (d >> 63)) as u64); // zigzag
                    put_varint(h, r.payload.len() as u64);
                }
            }
            Layout::Shadow => recs.iter().for_each(|r| h.extend_from_slice(&hash(r))),
            Layout::Pcb2Names | Layout::Pcb2Ids => unreachable!(),
        }
        let cols_len = h.len() - start - HEADER;
        let raw_len: usize = if layout == Layout::Shadow {
            0
        } else {
            recs.iter().map(|r| r.payload.len()).sum()
        };
        let blob_start = self.blob.len();
        match layout {
            Layout::Compact | Layout::Columnar => recs
                .iter()
                .for_each(|r| self.blob.extend_from_slice(&r.payload)),
            Layout::ColumnarLz4 => {
                self.raw.clear();
                recs.iter()
                    .for_each(|r| self.raw.extend_from_slice(&r.payload));
                let max = lz4_flex::block::get_maximum_output_size(raw_len);
                self.blob.resize(blob_start + max, 0);
                let n = lz4_flex::block::compress_into(&self.raw, &mut self.blob[blob_start..])
                    .expect("sized by get_maximum_output_size");
                self.blob.truncate(blob_start + n);
            }
            _ => {}
        }
        let blob = &self.blob[blob_start..];
        let hd = &mut h[start..start + HEADER];
        hd[..4].copy_from_slice(PCB3);
        hd[4] = 1;
        hd[5] = codec(layout);
        hd[8..12].copy_from_slice(&log.stream_id.to_le_bytes());
        hd[12..16].copy_from_slice(&log.writer_id.to_le_bytes());
        hd[16..24].copy_from_slice(&log.epoch.to_le_bytes());
        hd[24..32].copy_from_slice(&first_seq.to_le_bytes());
        hd[32..36].copy_from_slice(&(recs.len() as u32).to_le_bytes());
        hd[36..44].copy_from_slice(&base.to_le_bytes());
        hd[44..48].copy_from_slice(&(cols_len as u32).to_le_bytes());
        hd[48..52].copy_from_slice(&(raw_len as u32).to_le_bytes());
        hd[52..56].copy_from_slice(&(blob.len() as u32).to_le_bytes());
        let crc = crc32c::crc32c(&hd[..56]);
        let crc = crc32c::crc32c_append(crc, &h[start + HEADER..]);
        let crc = crc32c::crc32c_append(crc, blob);
        h[start + 56..start + 60].copy_from_slice(&crc.to_le_bytes());
    }
}

/// Fully validates one batch (checksum, structure, bounds) and calls `f` for every record.
/// The broker's work: it never decompresses a payload.
pub fn visit<'a>(layout: Layout, b: &'a [u8], f: impl FnMut(Rec<'a>)) -> Result<Batch, String> {
    match layout {
        Layout::Pcb2Names | Layout::Pcb2Ids => pcb2(layout == Layout::Pcb2Names, b, f),
        _ => columnar(codec(layout), b, f),
    }
}

fn pcb2<'a>(names: bool, b: &'a [u8], mut f: impl FnMut(Rec<'a>)) -> Result<Batch, String> {
    ensure!(b.len() >= 4 + 8 + 20 + 4, "batch too short");
    let (body, crc) = b.split_at(b.len() - 4);
    ensure!(
        crc32c::crc32c(body) == u32_at(crc, 0),
        "batch checksum mismatch"
    );
    ensure!(&body[..4] == if names { PCB2 } else { PCI2 }, "bad magic");
    let mut c = &body[4..];
    let (mut stream_id, mut writer_id, mut name) = (0, 0, None);
    if names {
        let mut s = || -> Result<String, String> {
            let n = u16_at(take(&mut c, 2)?, 0) as usize;
            ensure!(n > 0 && n <= 255, "bad name length");
            let s = std::str::from_utf8(take(&mut c, n)?).map_err(|e| e.to_string())?;
            Ok(s.to_owned()) // as today's `take_str`: one allocation per name per batch
        };
        name = Some((s()?, s()?));
    } else {
        let ids = take(&mut c, 8)?;
        (stream_id, writer_id) = (u32_at(ids, 0), u32_at(ids, 4));
    }
    let h = take(&mut c, 20)?;
    let (epoch, first_seq, count) = (u64_at(h, 0), u64_at(h, 8), u32_at(h, 16));
    ensure!(count > 0 && count <= MAX_COUNT, "bad event count");
    ensure!(
        first_seq.checked_add(count as u64).is_some(),
        "sequence overflow"
    );
    for _ in 0..count {
        let e = take(&mut c, 14)?;
        let (k, n) = (u16_at(e, 8) as usize, u32_at(e, 10) as usize);
        ensure!(k <= MAX_KEY, "dedup key too long");
        let key = take(&mut c, k)?;
        let payload = take(&mut c, n)?;
        f(Rec {
            time_us: u64_at(e, 0),
            key: Key::Id(key),
            symbol: None,
            payload: Some(payload),
            len: n,
        });
    }
    ensure!(c.is_empty(), "trailing bytes in batch");
    Ok(Batch {
        names: name,
        stream_id,
        writer_id,
        epoch,
        first_seq,
        count,
    })
}

struct Header<'a> {
    batch: Batch,
    codec: u8,
    base: u64,
    raw_len: usize,
    cols: &'a [u8],
    blob: &'a [u8],
}

fn header(b: &[u8]) -> Result<Header<'_>, String> {
    ensure!(b.len() >= HEADER, "batch too short");
    ensure!(&b[..4] == PCB3 && b[4] == 1, "bad magic or version");
    let (cols_len, raw_len, blob_len) = (u32_at(b, 44), u32_at(b, 48), u32_at(b, 52));
    ensure!(
        b.len() as u64 == HEADER as u64 + cols_len as u64 + blob_len as u64,
        "bad batch length"
    );
    let crc = crc32c::crc32c_append(crc32c::crc32c(&b[..56]), &b[HEADER..]);
    ensure!(crc == u32_at(b, 56), "batch checksum mismatch");
    let (first_seq, count) = (u64_at(b, 24), u32_at(b, 32));
    ensure!(count > 0 && count <= MAX_COUNT, "bad event count");
    ensure!(
        first_seq.checked_add(count as u64).is_some(),
        "sequence overflow"
    );
    let (cols, blob) = b[HEADER..].split_at(cols_len as usize);
    Ok(Header {
        batch: Batch {
            names: None,
            stream_id: u32_at(b, 8),
            writer_id: u32_at(b, 12),
            epoch: u64_at(b, 16),
            first_seq,
            count,
        },
        codec: b[5],
        base: u64_at(b, 36),
        raw_len: raw_len as usize,
        cols,
        blob,
    })
}

fn columnar<'a>(codec: u8, b: &'a [u8], mut f: impl FnMut(Rec<'a>)) -> Result<Batch, String> {
    let h = header(b)?;
    ensure!(h.codec == codec, "unexpected codec {}", h.codec);
    let n = h.batch.count as usize;
    let mut c = h.cols;
    let mut sum = 0usize;
    match codec {
        0 => {
            ensure!(
                c.len() == 14 * n && h.blob.len() == h.raw_len,
                "bad column lengths"
            );
            let (dts, rest) = c.split_at(4 * n);
            let (hashes, lens) = rest.split_at(8 * n);
            for i in 0..n {
                let len = u16_at(lens, 2 * i) as usize;
                ensure!(sum + len <= h.raw_len, "lengths exceed the payload column");
                f(Rec {
                    time_us: h.base + u32_at(dts, 4 * i) as u64,
                    key: Key::Hash(u64_at(hashes, 8 * i)),
                    symbol: None,
                    payload: Some(&h.blob[sum..sum + len]),
                    len,
                });
                sum += len;
            }
            c = &[];
        }
        1 | 2 => {
            if codec == 1 {
                ensure!(h.blob.len() == h.raw_len, "bad payload column length");
            } else {
                ensure!(
                    !h.blob.is_empty()
                        && h.blob.len() <= lz4_flex::block::get_maximum_output_size(h.raw_len),
                    "bad compressed column length"
                );
            }
            let ndict = take(&mut c, 1)?[0] as usize;
            let mut dict = [&b""[..]; 256];
            for d in dict.iter_mut().take(ndict) {
                let l = take(&mut c, 1)?[0] as usize;
                ensure!(l > 0, "empty symbol");
                *d = take(&mut c, l)?;
            }
            let keys = take(&mut c, n)?;
            let hashes = take(&mut c, 8 * n)?;
            let mut t = h.base as i64;
            for (i, &k) in keys.iter().enumerate() {
                let k = k as usize;
                ensure!(k < ndict, "symbol index out of range");
                let z = take_varint(&mut c)?;
                t += (z >> 1) as i64 ^ -((z & 1) as i64);
                let len = take_varint(&mut c)? as usize;
                ensure!(len <= h.raw_len - sum, "lengths exceed the payload column");
                f(Rec {
                    time_us: t as u64,
                    key: Key::Hash(u64_at(hashes, 8 * i)),
                    symbol: Some(dict[k]),
                    payload: (codec == 1).then(|| &h.blob[sum..sum + len]),
                    len,
                });
                sum += len;
            }
        }
        3 => {
            ensure!(
                c.len() == 8 * n && h.blob.is_empty() && h.raw_len == 0,
                "bad shadow"
            );
            for i in 0..n {
                f(Rec {
                    time_us: h.base,
                    key: Key::Hash(u64_at(c, 8 * i)),
                    symbol: None,
                    payload: None,
                    len: 0,
                });
            }
            c = &[];
        }
        _ => return Err(format!("unknown codec {codec}")),
    }
    ensure!(c.is_empty(), "trailing bytes in columns");
    ensure!(sum == h.raw_len, "lengths do not sum to the payload column");
    Ok(h.batch)
}

/// The payload column of a columnar batch, decompressed: what a reader does, never the broker.
pub fn payloads(b: &[u8]) -> Result<Vec<u8>, String> {
    let h = header(b)?;
    if h.codec != 2 {
        return Ok(h.blob.to_vec());
    }
    let mut out = vec![0; h.raw_len];
    let n = lz4_flex::block::decompress_into(h.blob, &mut out).map_err(|e| e.to_string())?;
    ensure!(n == h.raw_len, "short decompression");
    Ok(out)
}

/// Session names, sent once per connection: `u16 n | n * (u32 id | u16 len | name)`.
pub fn encode_hello(names: &[(u32, &str)], out: &mut Vec<u8>) {
    out.extend_from_slice(&(names.len() as u16).to_le_bytes());
    for (id, name) in names {
        out.extend_from_slice(&id.to_le_bytes());
        out.extend_from_slice(&(name.len() as u16).to_le_bytes());
        out.extend_from_slice(name.as_bytes());
    }
}

pub fn decode_hello(mut c: &[u8]) -> Result<Vec<(u32, String)>, String> {
    let n = u16_at(take(&mut c, 2)?, 0);
    let names = (0..n)
        .map(|_| {
            let h = take(&mut c, 6)?;
            let name = take(&mut c, u16_at(h, 4) as usize)?;
            let name = std::str::from_utf8(name).map_err(|e| e.to_string())?;
            Ok((u32_at(h, 0), name.to_owned()))
        })
        .collect::<Result<_, String>>()?;
    ensure!(c.is_empty(), "trailing bytes in hello");
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::records;

    const ALL: [Layout; 6] = [
        Layout::Pcb2Names,
        Layout::Pcb2Ids,
        Layout::Compact,
        Layout::Columnar,
        Layout::ColumnarLz4,
        Layout::Shadow,
    ];

    fn log() -> Log<'static> {
        Log {
            stream: "raw.trades.binance-futures",
            writer: "live-producer-binance-futures-0",
            stream_id: 3,
            writer_id: 9,
            epoch: 2,
        }
    }

    fn batch(layout: Layout, recs: &[Record]) -> Vec<u8> {
        let mut e = Enc::default();
        e.head.extend_from_slice(b"prefix"); // encode appends; the caller owns what is before
        e.encode(layout, &log(), 1000, recs);
        let mut b = e.head[6..].to_vec();
        b.extend_from_slice(&e.blob);
        b
    }

    #[test]
    fn pcb2_is_byte_identical_to_todays_mesh_batch() {
        let recs = records(100, 1);
        let l = log();
        let events: Vec<(u64, &[u8], &[u8])> = recs
            .iter()
            .map(|r| (r.time_us, r.id.as_bytes(), &r.payload[..]))
            .collect();
        let mesh = precog_mesh::proto::Batch::encode_keyed(
            &precog_mesh::proto::LogId {
                stream: l.stream.into(),
                writer: l.writer.into(),
                epoch: l.epoch,
            },
            1000,
            &events,
        )
        .unwrap();
        assert_eq!(batch(Layout::Pcb2Names, &recs), &mesh[..]);
    }

    #[test]
    fn every_layout_round_trips_the_records() {
        let recs = records(300, 2);
        for layout in ALL {
            let b = batch(layout, &recs);
            let mut got = Vec::new();
            let h = visit(layout, &b, |r| {
                let key = match r.key {
                    Key::Id(k) => xxh3_64(k),
                    Key::Hash(h) => h,
                };
                got.push((
                    r.time_us,
                    key,
                    r.symbol.map(<[u8]>::to_vec),
                    r.payload.map(<[u8]>::to_vec),
                    r.len,
                ));
            })
            .unwrap_or_else(|e| panic!("{layout:?}: {e}"));
            assert_eq!(
                (h.first_seq, h.count, h.epoch),
                (1000, 300, 2),
                "{layout:?}"
            );
            match layout {
                Layout::Pcb2Names => {
                    assert_eq!(h.names, Some((log().stream.into(), log().writer.into())))
                }
                _ => assert_eq!(
                    (h.names, h.stream_id, h.writer_id),
                    (None, 3, 9),
                    "{layout:?}"
                ),
            }
            assert_eq!(got.len(), 300);
            // The payload column of a compressed batch is only readable after decompression,
            // which the broker never does; readers do.
            let blob = matches!(
                layout,
                Layout::Compact | Layout::Columnar | Layout::ColumnarLz4
            )
            .then(|| payloads(&b).unwrap());
            let mut off = 0;
            for (r, (t, key, sym, payload, len)) in recs.iter().zip(got) {
                assert_eq!(key, xxh3_64(r.id.as_bytes()), "{layout:?}");
                if layout == Layout::Shadow {
                    continue; // only hashes and the batch's sequence range
                }
                assert_eq!(t, r.time_us, "{layout:?}");
                assert_eq!(len, r.payload.len(), "{layout:?}");
                if let Some(blob) = &blob {
                    assert_eq!(&blob[off..off + len], &r.payload[..], "{layout:?}");
                    off += len;
                }
                if let Some(p) = payload {
                    assert_eq!(p, r.payload, "{layout:?}");
                }
                let want_sym = matches!(layout, Layout::Columnar | Layout::ColumnarLz4);
                assert_eq!(
                    sym,
                    want_sym.then(|| SYMBOLS[r.symbol as usize].as_bytes().to_vec())
                );
            }
        }
    }

    #[test]
    fn a_flipped_byte_anywhere_is_rejected() {
        let recs = records(5, 3);
        for layout in ALL {
            let b = batch(layout, &recs);
            for i in 0..b.len() {
                let mut c = b.clone();
                c[i] ^= 0x10;
                assert!(visit(layout, &c, |_| {}).is_err(), "{layout:?} byte {i}");
            }
            assert!(
                visit(layout, &b[..b.len() - 1], |_| {}).is_err(),
                "{layout:?} truncated"
            );
        }
    }

    #[test]
    fn hello_round_trips_session_names() {
        let mut b = Vec::new();
        encode_hello(
            &[(3, "raw.trades.binance-futures"), (9, "writer-0")],
            &mut b,
        );
        assert_eq!(
            decode_hello(&b).unwrap(),
            vec![
                (3, "raw.trades.binance-futures".into()),
                (9, "writer-0".into())
            ]
        );
        assert!(decode_hello(&b[..b.len() - 1]).is_err());
    }
}
