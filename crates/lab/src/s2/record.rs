//! An S2 record as one Precog event payload: headers and body, length-prefixed.

/// An S2 record: headers and a body. A command record has exactly one header, with an empty name.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Record {
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
}

/// What a command record asks for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    /// Sets the stream's fencing token to the body.
    Fence(Vec<u8>),
    /// Asks to drop records below this seq_num (body: u64, big-endian, as S2 encodes it).
    Trim(u64),
}

impl Record {
    pub fn body(body: impl Into<Vec<u8>>) -> Record {
        Record {
            headers: vec![],
            body: body.into(),
        }
    }

    pub fn fence(token: &[u8]) -> Record {
        Record {
            headers: vec![(vec![], b"fence".to_vec())],
            body: token.to_vec(),
        }
    }

    pub fn trim(seq: u64) -> Record {
        Record {
            headers: vec![(vec![], b"trim".to_vec())],
            body: seq.to_be_bytes().to_vec(),
        }
    }

    /// The command this record carries, if it is a command record.
    pub fn command(&self) -> Option<Command> {
        match &self.headers[..] {
            [(name, op)] if name.is_empty() => match &op[..] {
                b"fence" => Some(Command::Fence(self.body.clone())),
                b"trim" => Some(Command::Trim(u64::from_be_bytes(
                    self.body.get(..8)?.try_into().ok()?,
                ))),
                _ => None,
            },
            _ => None,
        }
    }

    /// Headers and body as one payload: `u16 n | (u32 len, name, u32 len, value) * n | body`.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(2 + self.body.len());
        out.extend_from_slice(&(self.headers.len() as u16).to_le_bytes());
        for (k, v) in &self.headers {
            for p in [k, v] {
                out.extend_from_slice(&(p.len() as u32).to_le_bytes());
                out.extend_from_slice(p);
            }
        }
        out.extend_from_slice(&self.body);
        out
    }

    pub fn decode(mut b: &[u8]) -> Option<Record> {
        fn take<'a>(b: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
            let (h, t) = (b.get(..n)?, b.get(n..)?);
            *b = t;
            Some(h)
        }
        let n = u16::from_le_bytes(take(&mut b, 2)?.try_into().ok()?);
        let mut headers = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let mut part = || -> Option<Vec<u8>> {
                let len = u32::from_le_bytes(take(&mut b, 4)?.try_into().ok()?) as usize;
                Some(take(&mut b, len)?.to_vec())
            };
            headers.push((part()?, part()?));
        }
        Some(Record {
            headers,
            body: b.to_vec(),
        })
    }

    /// S2's metered size: 8 bytes + 2 per header + name, value and body lengths.
    pub fn metered_size(&self) -> usize {
        8 + self
            .headers
            .iter()
            .map(|(k, v)| 2 + k.len() + v.len())
            .sum::<usize>()
            + self.body.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_refuses_truncation() {
        let r = Record {
            headers: vec![(b"k".to_vec(), b"v".to_vec()), (vec![], vec![])],
            body: b"hello".to_vec(),
        };
        let e = r.encode();
        assert_eq!(Record::decode(&e), Some(r));
        assert_eq!(Record::decode(&e[..5]), None);
        assert_eq!(Record::decode(&[]), None);
    }

    #[test]
    fn commands_need_exactly_one_unnamed_header() {
        assert_eq!(
            Record::fence(b"t").command(),
            Some(Command::Fence(b"t".to_vec()))
        );
        assert_eq!(Record::trim(7).command(), Some(Command::Trim(7)));
        assert_eq!(Record::body("x").command(), None);
        let mut two = Record::fence(b"t");
        two.headers.push((b"a".to_vec(), vec![]));
        assert_eq!(two.command(), None);
        let named = Record {
            headers: vec![(b"n".to_vec(), b"fence".to_vec())],
            body: vec![],
        };
        assert_eq!(named.command(), None);
    }

    #[test]
    fn metered_size_matches_s2() {
        // 8 + (2 + 1 + 1) + 5
        let r = Record {
            headers: vec![(b"k".to_vec(), b"v".to_vec())],
            body: b"hello".to_vec(),
        };
        assert_eq!(r.metered_size(), 17);
    }
}
