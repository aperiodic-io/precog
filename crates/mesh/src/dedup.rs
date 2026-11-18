//! Broker-side dedup: drops an event whose dedup key was seen within the window on this node.
//!
//! The index is a few generations of hash sets of 64-bit digests (`xxh3(key)` seeded by the
//! stream), each sized for the last one's count; the oldest is dropped whole. The sets are std's
//! SwissTable with the digest as its own hash: 16 tags compared per probe, so a new id (the common
//! case) is rejected fast even when full. Chosen by measurement (`results/dedup.md`, row H, and
//! the broker profile: open addressing at 7/8 load cost 22% of a replica's CPU in long probe
//! runs). ~28 bytes per live id; an id is remembered for at least the window (up to
//! window * k / (k - 1)). At `max_entries` the oldest generation goes early: the window shortens
//! instead of memory growing (`effective_window_s` reports it).

use std::collections::{HashSet, VecDeque};
use std::hash::{BuildHasherDefault, Hasher};

/// The digest is already xxh3: hash it no further.
#[derive(Default)]
struct Pass(u64);

impl Hasher for Pass {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!("only u64 keys")
    }
    fn write_u64(&mut self, x: u64) {
        self.0 = x;
    }
}

type Set = HashSet<u64, BuildHasherDefault<Pass>>;

pub struct Dedup {
    k: usize,
    span_s: u64,
    max_entries: usize,
    /// (start, indexed digests, recorded digests not yet indexed) per generation
    gens: VecDeque<(u64, Set, Vec<u64>)>,
}

/// One index per stream (a node accepts some streams and replicates others; a replica only
/// records, so the cost of indexing lands where the stream is accepted). `max_entries` is per
/// stream, as the fork's cap is per partition.
pub struct Streams {
    window_s: u64,
    k: usize,
    max_entries: usize,
    by_stream: std::collections::HashMap<String, Dedup>,
}

impl Streams {
    pub fn new(window_s: u64, k: usize, max_entries: usize) -> Streams {
        Streams {
            window_s,
            k,
            max_entries,
            by_stream: Default::default(),
        }
    }

    fn of(&mut self, stream: &str) -> &mut Dedup {
        if !self.by_stream.contains_key(stream) {
            self.by_stream.insert(
                stream.to_string(),
                Dedup::new(self.window_s, self.k, self.max_entries),
            );
        }
        self.by_stream.get_mut(stream).unwrap()
    }

    pub fn check(&mut self, stream: &str, digests: &[u64], now_s: u64, dup: &mut Vec<bool>) {
        self.of(stream).check(digests, now_s, dup)
    }

    pub fn record(&mut self, stream: &str, digests: &[u64], now_s: u64) {
        self.of(stream).record(digests, now_s)
    }

    pub fn len(&self) -> usize {
        self.by_stream.values().map(Dedup::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The shortest window any stream remembers now.
    pub fn effective_window_s(&self, now_s: u64) -> u64 {
        self.by_stream
            .values()
            .map(|d| d.effective_window_s(now_s))
            .min()
            .unwrap_or(0)
    }
}

/// A key's digest within its stream.
pub fn digest(stream_seed: u64, key: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64_with_seed(key, stream_seed)
}

pub fn stream_seed(stream: &str) -> u64 {
    xxhash_rust::xxh3::xxh3_64(stream.as_bytes())
}

impl Dedup {
    /// `k` generations (at least 2) over `window_s`, at most `max_entries` ids.
    pub fn new(window_s: u64, k: usize, max_entries: usize) -> Dedup {
        assert!(k >= 2 && window_s > 0);
        Dedup {
            k,
            span_s: window_s.div_ceil(k as u64 - 1),
            max_entries,
            gens: VecDeque::new(),
        }
    }

    fn roll(&mut self, now_s: u64) {
        // a new generation each span, or early once the newest holds its share of the cap
        if self.gens.back().is_none_or(|(t, s, p)| {
            now_s >= t + self.span_s || s.len() + p.len() >= self.max_entries / self.k
        }) {
            self.gens.push_back((now_s, Set::default(), vec![]));
        }
        // by count, by age after an idle gap, and by the entry cap
        while self.gens.len() > self.k
            || self
                .gens
                .front()
                .is_some_and(|(t, _, _)| now_s >= t + self.span_s * self.k as u64)
            || (self.gens.len() > 1 && self.len() > self.max_entries)
        {
            self.gens.pop_front();
        }
    }

    /// For each digest in order: was it seen within the window (a duplicate)? New ones are
    /// recorded, so a batch's later events see its earlier ones.
    pub fn check(&mut self, digests: &[u64], now_s: u64, dup: &mut Vec<bool>) {
        self.roll(now_s);
        for (_, set, pending) in &mut self.gens {
            set.extend(pending.drain(..)); // what this node recorded as a replica
        }
        dup.clear();
        for &h in digests {
            let seen = self.gens.iter().rev().any(|(_, s, _)| s.contains(&h));
            if !seen {
                self.gens.back_mut().unwrap().1.insert(h);
            }
            dup.push(seen);
        }
    }

    /// Records digests without asking (replicated copies, a restart's rebuild): appended, and
    /// indexed by the next `check` (a node that only replicates the stream never pays for it).
    pub fn record(&mut self, digests: &[u64], now_s: u64) {
        self.roll(now_s);
        self.gens.back_mut().unwrap().2.extend_from_slice(digests);
    }

    pub fn len(&self) -> usize {
        self.gens.iter().map(|(_, s, p)| s.len() + p.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How far back ids are remembered now: the window, or less once the cap bites.
    pub fn effective_window_s(&self, now_s: u64) -> u64 {
        self.gens
            .front()
            .map_or(0, |(t, _, _)| now_s.saturating_sub(*t))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dups(d: &mut Dedup, keys: &[u64], now: u64) -> Vec<bool> {
        let mut out = vec![];
        d.check(keys, now, &mut out);
        out
    }

    #[test]
    fn a_resend_within_the_window_is_dropped_and_one_far_past_it_is_not() {
        let mut d = Dedup::new(300, 3, 1 << 30);
        assert_eq!(
            dups(&mut d, &[1, 2, 1], 10),
            [false, false, true],
            "a batch sees itself"
        );
        assert_eq!(
            dups(&mut d, &[2, 3], 300),
            [true, false],
            "still inside the window"
        );
        // remembered at least the window, at most window * k / (k - 1): long after, forgotten
        assert_eq!(dups(&mut d, &[1, 2], 10 + 451), [false, false]);
    }

    #[test]
    fn it_is_exact_through_growth_and_expiry() {
        let mut d = Dedup::new(10, 3, 1 << 30);
        for i in 0..300_000u64 {
            let h = digest(7, &i.to_le_bytes());
            let now = i / 5_000;
            assert_eq!(dups(&mut d, &[h, h], now), [false, true], "{i}");
        }
        // 5k ids a second for at least 10 s are held, at most 15 s
        assert!((50_000..=80_000).contains(&d.len()), "{}", d.len());
    }

    #[test]
    fn the_entry_cap_shortens_the_window_instead_of_growing() {
        let mut d = Dedup::new(300, 3, 10_000);
        for i in 0..100_000u64 {
            d.record(&[digest(1, &i.to_le_bytes())], i / 1_000);
        }
        assert!(d.len() <= 11_000, "{}", d.len());
        let now = 100;
        assert!(
            d.effective_window_s(now) < 300,
            "{}",
            d.effective_window_s(now)
        );
        // the newest ids are still caught
        assert_eq!(
            dups(&mut d, &[digest(1, &99_999u64.to_le_bytes())], now),
            [true]
        );
    }

    #[test]
    fn recorded_ids_are_duplicates_and_streams_do_not_share_ids() {
        let mut d = Dedup::new(300, 3, 1 << 30);
        d.record(&[digest(stream_seed("a"), b"id-1")], 5);
        assert_eq!(
            dups(&mut d, &[digest(stream_seed("a"), b"id-1")], 6),
            [true]
        );
        assert_eq!(
            dups(&mut d, &[digest(stream_seed("b"), b"id-1")], 6),
            [false]
        );
    }

    #[test]
    fn recording_only_appends_and_the_first_check_indexes_what_was_recorded() {
        // replicas record every event the origin kept: that must cost a push, not a hash insert
        let mut d = Dedup::new(300, 3, 1 << 30);
        let ids: Vec<u64> = (0..1000u64).map(|i| digest(3, &i.to_le_bytes())).collect();
        d.record(&ids, 5);
        let (_, set, pending) = d.gens.back().unwrap();
        assert!(
            set.is_empty() && pending.len() == 1000,
            "{} {}",
            set.len(),
            pending.len()
        );
        assert_eq!(d.len(), 1000);
        assert!(
            dups(&mut d, &ids, 6).iter().all(|&x| x),
            "a node taking over still drops them"
        );
        assert!(d.gens.iter().all(|(_, _, p)| p.is_empty()));
    }

    #[test]
    fn each_stream_has_its_own_index_and_cap() {
        let mut s = Streams::new(300, 3, 100);
        let ids: Vec<u64> = (0..50u64).map(|i| digest(1, &i.to_le_bytes())).collect();
        s.record("a", &ids, 1);
        let mut out = vec![];
        s.check("a", &ids, 2, &mut out);
        assert!(out.iter().all(|&x| x));
        s.check("b", &ids, 2, &mut out);
        assert!(
            out.iter().all(|&x| !x),
            "another stream's ids are not duplicates"
        );
        assert_eq!(s.len(), 100);
        assert_eq!(s.effective_window_s(2), 0, "the shortest: b started at 2");
    }
}
