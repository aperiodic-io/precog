//! On-disk record framing.
//!
//! ```text
//! offset size field
//!  0     4   magic "PCR1"
//!  4     4   len: header + key + payload (excluding padding)
//!  8     4   crc32c over bytes [12, len)
//! 12     2   key_len
//! 14     2   flags (0)
//! 16     8   generation   (of the segment file; rejects stale records in recycled files)
//! 24     8   group_start  (segment offset of the first record of this write group)
//! 32     8   lsn
//! 40     8   first_seq    (caller metadata, indexed)
//! 48     4   count        (caller metadata, indexed)
//! 52     4   tag          (caller metadata, indexed; precog stores the batch checksum)
//! 56     8   time_ms      (append time, for retention by age)
//! 64     -   key, payload, zero padding to an 8-byte boundary
//! ```

pub const HEADER: usize = 64;
pub const MAGIC: u32 = u32::from_le_bytes(*b"PCR1");
pub const MAX_KEY: usize = 4096;
pub const MAX_RECORD: usize = 16 << 20;

/// Caller metadata stored with each record and kept in the segment index.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Meta {
    pub key: Vec<u8>,
    pub first_seq: u64,
    pub count: u32,
    pub tag: u32,
    pub time_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    pub len: u32,
    pub key_len: u16,
    pub generation: u64,
    pub group_start: u64,
    pub lsn: u64,
    pub first_seq: u64,
    pub count: u32,
    pub tag: u32,
    pub time_ms: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Decoded<'a> {
    Record {
        header: Header,
        key: &'a [u8],
        payload: &'a [u8],
        next: usize,
    },
    /// Not a valid record of this generation at this offset.
    Invalid,
    /// The buffer ends inside the record.
    Short,
}

/// Bytes a record occupies on disk, padding included.
pub fn encoded_len(key_len: usize, payload_len: usize) -> usize {
    (HEADER + key_len + payload_len).next_multiple_of(8)
}

/// Appends one framed record to `buf`.
pub fn encode(
    buf: &mut Vec<u8>,
    generation: u64,
    group_start: u64,
    lsn: u64,
    meta: &Meta,
    payload: &[u8],
) {
    let len = HEADER + meta.key.len() + payload.len();
    let start = buf.len();
    buf.extend_from_slice(&MAGIC.to_le_bytes());
    buf.extend_from_slice(&(len as u32).to_le_bytes());
    buf.extend_from_slice(&[0; 4]); // crc, filled below
    buf.extend_from_slice(&(meta.key.len() as u16).to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes());
    for v in [generation, group_start, lsn, meta.first_seq] {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    buf.extend_from_slice(&meta.count.to_le_bytes());
    buf.extend_from_slice(&meta.tag.to_le_bytes());
    buf.extend_from_slice(&meta.time_ms.to_le_bytes());
    buf.extend_from_slice(&meta.key);
    buf.extend_from_slice(payload);
    let crc = crc32c::crc32c(&buf[start + 12..start + len]);
    buf[start + 8..start + 12].copy_from_slice(&crc.to_le_bytes());
    buf.resize(start + encoded_len(meta.key.len(), payload.len()), 0);
}

fn u16_at(b: &[u8], o: usize) -> u16 {
    u16::from_le_bytes(b[o..o + 2].try_into().unwrap())
}
fn u32_at(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
fn u64_at(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}

/// Validates and decodes the record at `buf[off..]`.
pub fn decode(buf: &[u8], off: usize, generation: u64) -> Decoded<'_> {
    if buf.len() < off + HEADER {
        return Decoded::Short;
    }
    let b = &buf[off..];
    if u32_at(b, 0) != MAGIC {
        return Decoded::Invalid;
    }
    let len = u32_at(b, 4) as usize;
    let key_len = u16_at(b, 12) as usize;
    if len < HEADER + key_len || len > MAX_RECORD || key_len > MAX_KEY || u16_at(b, 14) != 0 {
        return Decoded::Invalid;
    }
    if u64_at(b, 16) != generation {
        return Decoded::Invalid;
    }
    if b.len() < len {
        return Decoded::Short;
    }
    if crc32c::crc32c(&b[12..len]) != u32_at(b, 8) {
        return Decoded::Invalid;
    }
    let header = Header {
        len: len as u32,
        key_len: key_len as u16,
        generation,
        group_start: u64_at(b, 24),
        lsn: u64_at(b, 32),
        first_seq: u64_at(b, 40),
        count: u32_at(b, 48),
        tag: u32_at(b, 52),
        time_ms: u64_at(b, 56),
    };
    Decoded::Record {
        header,
        key: &b[HEADER..HEADER + key_len],
        payload: &b[HEADER + key_len..len],
        next: off + (len.next_multiple_of(8)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta {
            key: b"stream/writer/1".to_vec(),
            first_seq: 40,
            count: 3,
            tag: 0xdead_beef,
            time_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn roundtrip_and_padding() {
        let mut buf = vec![0xAA; 5]; // records can start anywhere in a buffer
        encode(&mut buf, 7, 4096, 99, &meta(), b"hello");
        assert_eq!(buf.len() - 5, encoded_len(15, 5));
        assert_eq!((buf.len() - 5) % 8, 0);
        match decode(&buf, 5, 7) {
            Decoded::Record {
                header,
                key,
                payload,
                next,
            } => {
                assert_eq!(
                    (
                        header.lsn,
                        header.group_start,
                        header.first_seq,
                        header.count
                    ),
                    (99, 4096, 40, 3)
                );
                assert_eq!(
                    (header.tag, header.time_ms),
                    (0xdead_beef, 1_700_000_000_000)
                );
                assert_eq!(key, b"stream/writer/1");
                assert_eq!(payload, b"hello");
                assert_eq!(next, buf.len());
            }
            d => panic!("{d:?}"),
        }
    }

    #[test]
    fn every_covered_byte_is_checked() {
        let mut buf = vec![];
        encode(&mut buf, 7, 0, 1, &meta(), b"payload bytes");
        let len = HEADER + 15 + 13;
        for i in 0..len {
            let mut v = buf.clone();
            v[i] ^= 0x01;
            // a corrupted length can also read as Short; what matters is it never decodes
            assert!(
                !matches!(decode(&v, 0, 7), Decoded::Record { .. }),
                "flip at byte {i} accepted"
            );
        }
    }

    #[test]
    fn stale_generation_and_zeroes_are_invalid() {
        let mut buf = vec![];
        encode(&mut buf, 7, 0, 1, &meta(), b"x");
        assert_eq!(decode(&buf, 0, 8), Decoded::Invalid);
        assert_eq!(decode(&[0u8; 200], 0, 7), Decoded::Invalid);
        assert_eq!(decode(&[0u8; 10], 0, 7), Decoded::Short);
    }

    /// Re-encodes a record's header field and its checksum, so only the field is wrong.
    fn with_field(mut buf: Vec<u8>, off: usize, bytes: &[u8]) -> Vec<u8> {
        buf[off..off + bytes.len()].copy_from_slice(bytes);
        let len = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
        let end = len.min(buf.len());
        let crc = crc32c::crc32c(&buf[12..end]);
        buf[8..12].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    #[test]
    fn structurally_invalid_records_are_refused_even_with_a_valid_checksum() {
        let mut buf = vec![];
        encode(&mut buf, 7, 0, 1, &meta(), b"payload");
        assert!(matches!(decode(&buf, 0, 7), Decoded::Record { .. }));
        let flags = with_field(buf.clone(), 14, &1u16.to_le_bytes());
        assert_eq!(decode(&flags, 0, 7), Decoded::Invalid, "flags must be zero");
        let key = with_field(buf.clone(), 12, &((MAX_KEY + 1) as u16).to_le_bytes());
        assert_eq!(decode(&key, 0, 7), Decoded::Invalid, "key too long");
        let small = with_field(buf.clone(), 4, &((HEADER + 15 - 1) as u32).to_le_bytes());
        assert_eq!(
            decode(&small, 0, 7),
            Decoded::Invalid,
            "length shorter than header + key"
        );
        let huge = with_field(buf.clone(), 4, &((MAX_RECORD + 1) as u32).to_le_bytes());
        assert_eq!(
            decode(&huge, 0, 7),
            Decoded::Invalid,
            "length beyond the maximum"
        );
    }

    #[test]
    fn a_record_of_header_only_decodes_at_the_end_of_a_buffer() {
        let mut buf = vec![];
        encode(&mut buf, 7, 0, 1, &Meta::default(), b"");
        assert_eq!(buf.len(), HEADER);
        assert!(matches!(
            decode(&buf, 0, 7),
            Decoded::Record { next: 64, .. }
        ));
        let key_only = Meta {
            key: vec![b'k'; MAX_KEY],
            ..Meta::default()
        };
        let mut buf = vec![];
        encode(&mut buf, 7, 0, 1, &key_only, b"");
        assert!(
            matches!(decode(&buf, 0, 7), Decoded::Record { .. }),
            "a key of exactly MAX_KEY bytes"
        );
    }

    #[test]
    fn truncated_record_is_short() {
        let mut buf = vec![];
        encode(&mut buf, 7, 0, 1, &meta(), &[1u8; 100]);
        assert_eq!(decode(&buf[..HEADER + 20], 0, 7), Decoded::Short);
        assert_eq!(decode(&buf[..10], 0, 7), Decoded::Short);
    }
}
