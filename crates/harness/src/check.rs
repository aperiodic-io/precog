//! The workload (synthetic records shaped like live-ingestion's raw trades) and the invariants
//! every scenario asserts. Pure functions: unit tested below.

use serde::Serialize;
use std::collections::{HashMap, HashSet};

/// One event as a producer creates it. `uid` names the event (a resend repeats it); `id` is the
/// dedup id the broker sees (usually the uid, but a scenario may send it empty or reuse it).
#[derive(Clone, Debug)]
pub struct Ev {
    pub uid: String,
    pub id: Vec<u8>,
    pub body: Vec<u8>,
}

/// A synthetic trade, ~230 bytes of JSON like the loadgen's. The uid comes first so a reader
/// can find it without parsing.
pub fn body(uid: &str, producer: &str, symbol: &str, trade: u64, rng: u64) -> Vec<u8> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    format!(
        r#"{{"uid":"{uid}","exchange":"bench","symbol":"{symbol}","trade_id":{trade},"price":{:.4},"qty":{:.3},"side":"{}","time_us":{now},"local_timestamp_us":{now},"producer":"{producer}"}}"#,
        20000.0 + (rng % 40000) as f64 * 1.0001,
        (rng % 5000) as f64 / 1000.0,
        if rng.is_multiple_of(2) { "buy" } else { "sell" },
    )
    .into_bytes()
}

/// The uid of a record written by `body`.
pub fn uid_of(payload: &[u8]) -> Option<&str> {
    let rest = payload.strip_prefix(br#"{"uid":""#)?;
    let end = rest.iter().position(|&c| c == b'"')?;
    std::str::from_utf8(&rest[..end]).ok()
}

/// One producer's events: `symbols` symbols, ids `live-producer-<exchange>-<n>-<trade>`, and a
/// resend rate (a resend repeats one of the 64 latest events byte for byte, as the loadgen does).
pub struct Gen {
    pub producer: String,
    symbols: Vec<String>,
    trade: u64,
    rng: u64,
    ring: Vec<Ev>,
    pub dup_rate: f64,
    /// Send every id empty (never deduplicated).
    pub empty_ids: bool,
    /// Every this many trades carries ~256 KiB of padding (0: never), as capacitycheck's
    /// `-big-every` does: batch sizes swing.
    pub big_every: u64,
}

impl Gen {
    pub fn new(producer: &str, symbols: usize, dup_rate: f64) -> Gen {
        Gen {
            producer: producer.into(),
            symbols: (0..symbols).map(|i| format!("SYM{i}-{producer}")).collect(),
            trade: 0,
            rng: 0x9e37_79b9_7f4a_7c15 ^ xxhash_rust::xxh3::xxh3_64(producer.as_bytes()),
            ring: vec![],
            dup_rate,
            empty_ids: false,
            big_every: 0,
        }
    }

    fn rand(&mut self) -> u64 {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        self.rng
    }

    /// The next event, and whether it is a resend.
    pub fn next(&mut self) -> (Ev, bool) {
        let r = self.rand();
        if self.ring.len() >= 8 && (r % 1000) as f64 / 1000.0 < self.dup_rate {
            return (
                self.ring[(r >> 32) as usize % self.ring.len()].clone(),
                true,
            );
        }
        self.trade += 1;
        let uid = format!("{}-{}", self.producer, self.trade);
        let sym = self.symbols[(r % self.symbols.len() as u64) as usize].clone();
        let mut b = body(&uid, &self.producer, &sym, self.trade, r);
        if self.big_every > 0 && self.trade.is_multiple_of(self.big_every) {
            b.pop(); // the closing brace
            b.extend_from_slice(br#","pad":""#);
            let mut x = r;
            for _ in 0..(256 << 10) / 16 {
                x = x
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                b.extend_from_slice(format!("{x:016x}").as_bytes());
            }
            b.extend_from_slice(b"\"}");
        }
        let e = Ev {
            id: if self.empty_ids {
                vec![]
            } else {
                uid.clone().into_bytes()
            },
            body: b,
            uid,
        };
        if self.ring.len() == 64 {
            self.ring.remove(0);
        }
        self.ring.push(e.clone());
        (e, false)
    }
}

/// A record as read back: its log (a writer log, or a Kafka partition), its position in that
/// log (sequence or offset) and its uid.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Rec {
    pub log: String,
    pub pos: u64,
    pub uid: String,
}

/// What a read-back shows against what producers were told.
#[derive(Debug, Default, Serialize)]
pub struct Verdict {
    pub records: usize,
    pub unique: usize,
    /// Acknowledged events missing from the read.
    pub lost: usize,
    /// Copies beyond what is allowed (one per uid unless `copies` says otherwise).
    pub extra_copies: usize,
    /// Records whose position does not increase within their log, in read order.
    pub out_of_order: usize,
    /// Records no producer sent.
    pub phantom: usize,
    pub examples: Vec<String>,
}

impl Verdict {
    pub fn clean(&self) -> bool {
        self.lost == 0 && self.extra_copies == 0 && self.out_of_order == 0 && self.phantom == 0
    }
}

/// Checks `read` (in read order) against the acknowledged and sent uids. `copies` names uids
/// allowed more than one copy (a resend after the dedup window, an empty id sent twice).
pub fn verify(
    read: &[Rec],
    acked: &HashSet<String>,
    sent: &HashSet<String>,
    copies: &HashMap<String, usize>,
) -> Verdict {
    let mut v = Verdict {
        records: read.len(),
        ..Default::default()
    };
    let mut count: HashMap<&str, usize> = HashMap::new();
    let mut last: HashMap<&str, u64> = HashMap::new();
    for r in read {
        *count.entry(&r.uid).or_default() += 1;
        if let Some(&p) = last.get(r.log.as_str())
            && r.pos <= p
        {
            v.out_of_order += 1;
            note(&mut v.examples, format!("out of order: {r:?} after {p}"));
        }
        last.insert(&r.log, r.pos);
        if !sent.contains(&r.uid) {
            v.phantom += 1;
            note(&mut v.examples, format!("phantom: {r:?}"));
        }
    }
    v.unique = count.len();
    for (uid, &n) in &count {
        let allowed = copies.get(*uid).copied().unwrap_or(1);
        if n > allowed {
            v.extra_copies += n - allowed;
            note(
                &mut v.examples,
                format!("{uid}: {n} copies, {allowed} allowed"),
            );
        }
    }
    let mut lost: Vec<&String> = acked
        .iter()
        .filter(|u| !count.contains_key(u.as_str()))
        .collect();
    lost.sort();
    v.lost = lost.len();
    for u in lost.iter().take(5) {
        note(&mut v.examples, format!("lost: {u}"));
    }
    v
}

fn note(ex: &mut Vec<String>, s: String) {
    if ex.len() < 10 {
        ex.push(s);
    }
}

/// Records one node holds and another does not (both ways), compared as sets.
pub fn divergence(a: &[Rec], b: &[Rec]) -> usize {
    let a: HashSet<&Rec> = a.iter().collect();
    let b: HashSet<&Rec> = b.iter().collect();
    a.symmetric_difference(&b).count()
}

/// The `p` quantile of `xs` (sorted in place).
pub fn quantile(xs: &mut [f64], p: f64) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    xs.sort_by(|a, b| a.total_cmp(b));
    xs[((xs.len() as f64 * p) as usize).min(xs.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(log: &str, pos: u64, uid: &str) -> Rec {
        Rec {
            log: log.into(),
            pos,
            uid: uid.into(),
        }
    }

    fn set(xs: &[&str]) -> HashSet<String> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn verify_flags_loss_copies_disorder_and_phantoms() {
        let read = [
            rec("w", 0, "a"),
            rec("w", 2, "b"),
            rec("w", 1, "c"), // goes backwards
            rec("v", 0, "a"), // a second copy
            rec("v", 1, "z"), // never sent
        ];
        let sent = set(&["a", "b", "c", "d"]);
        let v = verify(&read, &set(&["a", "b", "d"]), &sent, &HashMap::new());
        assert_eq!(
            (v.lost, v.extra_copies, v.out_of_order, v.phantom),
            (1, 1, 1, 1)
        );
        assert!(!v.clean());
        let allow = HashMap::from([("a".to_string(), 2)]);
        let v = verify(&read[..2], &set(&["a", "b"]), &sent, &allow);
        assert!(v.clean(), "{v:?}");
    }

    #[test]
    fn divergence_counts_both_sides() {
        let a = [rec("w", 0, "a"), rec("w", 1, "b")];
        let b = [rec("w", 0, "a"), rec("w", 2, "c")];
        assert_eq!(divergence(&a, &b), 2);
        assert_eq!(divergence(&a, &a), 0);
    }

    #[test]
    fn generated_records_look_like_the_loadgen_and_resend_byte_for_byte() {
        let mut g = Gen::new("live-producer-bench-0", 3, 0.5);
        let mut seen = HashMap::new();
        let mut resends = 0;
        for _ in 0..2000 {
            let (e, resend) = g.next();
            assert_eq!(uid_of(&e.body), Some(e.uid.as_str()));
            assert_eq!(e.id, e.uid.as_bytes());
            assert!((180..300).contains(&e.body.len()), "{}", e.body.len());
            if let Some(b) = seen.insert(e.uid.clone(), e.body.clone()) {
                assert!(resend);
                assert_eq!(b, e.body, "a resend repeats the bytes");
                resends += 1;
            }
        }
        assert!((600..1400).contains(&resends), "{resends}");
        let mut xs = [3.0, 1.0, 2.0];
        assert_eq!(quantile(&mut xs, 0.5), 2.0);
    }
}
