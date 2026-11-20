//! Broker-side dedup: drops an event whose dedup key was seen within the window on this node.
//!
//! One map per stream from a key's 64-bit digest (`xxh3(key)` seeded by the stream) to the second
//! it was last seen: one lookup per event (a new id, the common case, misses once instead of once
//! per generation). It is std's SwissTable with the digest as its own hash. Expired entries count
//! as absent and are dropped in one pass every quarter window. Chosen by measurement
//! (`results/dedup.md` and the broker profile in bench/compare: open addressing at 7/8 load, then
//! three generations of sets, cost 22% and 11% of a node's CPU). A node that only replicates a
//! stream appends the origin's kept ids and indexes them at the stream's next check. At
//! `max_entries` the oldest quarter goes: the window shortens instead of memory growing
//! (`effective_window_s` reports it).

use std::collections::{HashMap, VecDeque};
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

pub struct Dedup {
    window_s: u64,
    max_entries: usize,
    /// digest -> the second it was last seen, plus one; an entry past the window counts as absent
    seen: HashMap<u64, u32, BuildHasherDefault<Pass>>,
    /// recorded (replicated) digests not yet indexed (a deque: dropping its oldest quarter at the
    /// cap moves nothing)
    pending: VecDeque<(u64, u32)>,
    swept_s: u64,
    /// the oldest second any kept entry may carry (the window, or less after the cap bit)
    oldest_s: u64,
    /// when this index first saw an id
    started_s: Option<u64>,
    /// the newest second it saw an id
    last_s: u64,
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

    /// Drops the index of every stream that saw no id within the window: none of its ids can
    /// be a duplicate any more, so an idle (dormant) stream holds no RAM for them.
    pub fn sweep(&mut self, now_s: u64) {
        let w = self.window_s;
        self.by_stream.retain(|_, d| d.last_s + w >= now_s);
        if self.by_stream.capacity() > 2 * self.by_stream.len() {
            self.by_stream.shrink_to_fit();
        }
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
    /// Remembers ids for `window_s`, at most `max_entries` of them. (`_k` is kept for callers
    /// that tuned the old generations; one map with a timestamp per id needs no generations.)
    pub fn new(window_s: u64, _k: usize, max_entries: usize) -> Dedup {
        assert!(window_s > 0);
        Dedup {
            window_s,
            max_entries,
            seen: HashMap::default(),
            pending: VecDeque::new(),
            swept_s: 0,
            oldest_s: 0,
            started_s: None,
            last_s: 0,
        }
    }

    /// Every quarter window, drops expired entries in one pass; at the cap, the oldest quarter
    /// of what is kept too (the window shortens instead of memory growing).
    fn sweep(&mut self, now_s: u64) {
        // the cap raises the floor at most to the current second: past it nothing would be a
        // duplicate. ponytail: within one second the cap is soft (that second's ids are all
        // kept); evict within a second (per-entry order) if a single second can outgrow memory
        let full = self.seen.len() > self.max_entries && self.oldest_s < now_s;
        if !full && now_s < self.swept_s + (self.window_s / 4).max(1) {
            return;
        }
        let window_start = now_s.saturating_sub(self.window_s);
        self.oldest_s = self.oldest_s.max(window_start);
        if full {
            self.oldest_s += (now_s.saturating_sub(self.oldest_s) / 4).max(1);
        }
        let oldest = self.oldest_s;
        self.seen.retain(|_, t| (*t - 1) as u64 >= oldest);
        self.swept_s = now_s;
    }

    /// For each digest in order: was it seen within the window (a duplicate)? New ones are
    /// recorded, so a batch's later events see its earlier ones.
    pub fn check(&mut self, digests: &[u64], now_s: u64, dup: &mut Vec<bool>) {
        self.started_s.get_or_insert(now_s);
        self.last_s = self.last_s.max(now_s);
        for (h, t) in self.pending.drain(..) {
            self.seen.insert(h, t + 1); // what this node recorded as a replica
        }
        self.sweep(now_s);
        dup.clear();
        let oldest = self.oldest_s.max(now_s.saturating_sub(self.window_s));
        for &h in digests {
            // one probe: a new id (the common case) is inserted where the lookup ended
            let t = self.seen.entry(h).or_insert(0); // stored as second + 1: 0 is never seen
            let fresh = *t != 0 && (*t - 1) as u64 >= oldest;
            if !fresh {
                *t = now_s as u32 + 1;
            }
            dup.push(fresh);
        }
    }

    /// Records digests without asking (replicated copies, a restart's rebuild): appended, and
    /// indexed by the next `check` (a node that only replicates the stream never pays for it).
    pub fn record(&mut self, digests: &[u64], now_s: u64) {
        self.started_s.get_or_insert(now_s);
        self.last_s = self.last_s.max(now_s);
        if self.pending.len() >= self.max_entries {
            // the oldest quarter goes and the window shortens past it (as `sweep` does at the
            // cap), but never this second's ids: within a second the cap is soft there too
            let n = (self.pending.len() / 4)
                .min(self.pending.partition_point(|e| (e.1 as u64) < now_s));
            if n > 0 {
                let t = self.pending[n - 1].1 as u64;
                self.pending.drain(..n);
                self.oldest_s = self.oldest_s.max(t + 1);
            }
        }
        self.pending
            .extend(digests.iter().map(|&h| (h, now_s as u32)));
    }

    pub fn len(&self) -> usize {
        self.seen.len() + self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How far back ids are remembered now: the window, or less once the cap bites.
    pub fn effective_window_s(&self, now_s: u64) -> u64 {
        let since = self.oldest_s.max(self.started_s.unwrap_or(now_s));
        now_s.saturating_sub(since).min(self.window_s)
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
    fn at_the_cap_the_newest_ids_are_caught_even_when_they_arrived_within_one_second() {
        // Why: a burst past the cap within one second raised the floor past the clock, one second
        // per batch, and from then on nothing was a duplicate, not even within a batch
        let mut d = Dedup::new(300, 3, 10_000);
        let ids =
            |r: std::ops::Range<u64>| r.map(|i| digest(1, &i.to_le_bytes())).collect::<Vec<_>>();
        let now = 1_000;
        for b in 0..60u64 {
            dups(&mut d, &ids(b * 500..(b + 1) * 500), now);
        }
        assert!(
            d.oldest_s <= now,
            "the floor ran ahead of the clock: {}",
            d.oldest_s
        );
        assert_eq!(
            dups(&mut d, &ids(29_500..30_000), now),
            vec![true; 500],
            "the newest batch"
        );
        assert_eq!(
            dups(&mut d, &[7, 7], now),
            [false, true],
            "a batch still sees itself"
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
        assert!(d.seen.is_empty() && d.pending.len() == 1000);
        assert_eq!(d.len(), 1000);
        assert!(
            dups(&mut d, &ids, 6).iter().all(|&x| x),
            "a node taking over still drops them"
        );
        assert!(d.pending.is_empty());
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
        assert_eq!(s.effective_window_s(10), 8, "b's 8 s, not a's 9 s");
    }

    #[test]
    fn an_id_is_remembered_for_exactly_the_window() {
        let mut d = Dedup::new(300, 3, 1 << 30);
        assert_eq!(dups(&mut d, &[7], 10), [false]);
        assert_eq!(dups(&mut d, &[7], 310), [true], "300 s later: still in");
        assert_eq!(dups(&mut d, &[7], 311), [false], "301 s later: out");
        // a recorded (replicated) id counts from the second it was recorded, the same way
        let mut d = Dedup::new(300, 3, 1 << 30);
        d.record(&[8, 9], 0);
        assert_eq!(dups(&mut d, &[8], 300), [true]);
        assert_eq!(dups(&mut d, &[9], 301), [false]);
    }

    #[test]
    fn expired_ids_are_swept_from_memory_every_quarter_window() {
        let mut d = Dedup::new(300, 3, 1 << 30);
        dups(&mut d, &[1], 9);
        dups(&mut d, &[2], 10);
        dups(&mut d, &[3], 310); // a sweep: 9 is past the window, 10 is not yet
        assert_eq!(d.len(), 2);
        dups(&mut d, &[4], 384); // 2 has expired, but the next sweep is due at 385
        assert_eq!(d.len(), 3);
        dups(&mut d, &[5], 385);
        assert_eq!(d.len(), 3, "2 swept");
    }

    #[test]
    fn past_the_cap_the_oldest_quarter_of_the_window_goes() {
        let mut d = Dedup::new(300, 3, 4);
        for (id, t) in [(1, 0), (2, 40), (3, 80), (4, 120), (5, 160)] {
            dups(&mut d, &[id], t);
        }
        assert_eq!(d.len(), 5, "at the cap nothing goes");
        dups(&mut d, &[6], 170); // over it: the oldest quarter of 170 s (ids before 42) goes
        assert_eq!(d.len(), 4);
        assert_eq!(dups(&mut d, &[1, 3], 170), [false, true]);
        assert_eq!(d.effective_window_s(170), 128);
    }

    #[test]
    fn recorded_ids_past_the_cap_lose_their_oldest_quarter() {
        let mut d = Dedup::new(300, 3, 100);
        assert!(d.is_empty());
        for i in 0..3 {
            d.record(&(i * 10..i * 10 + 10).collect::<Vec<u64>>(), 1 + i);
        }
        assert_eq!(d.len(), 30, "under the cap all are kept");
        assert!(!d.is_empty());
        d.record(&(30..100).collect::<Vec<u64>>(), 4);
        d.record(&[100], 5); // 100 pending: a quarter goes first
        assert_eq!(d.len(), 76);
        // and the window says so: part of second 3 went, so ids from second 4 on are kept
        assert_eq!(d.effective_window_s(5), 1);
        assert_eq!(
            dups(&mut d, &[24, 25, 30, 100], 5),
            [false, false, true, true]
        );
        // this second's ids are never dropped: within a second the cap is soft
        let mut d = Dedup::new(300, 3, 4);
        d.record(&[1, 2, 3, 4], 7);
        d.record(&[5], 7);
        assert_eq!(dups(&mut d, &[1, 5], 7), [true, true]);
    }

    #[test]
    fn streams_are_empty_until_an_id_is_recorded() {
        let mut s = Streams::new(300, 3, 100);
        assert!(s.is_empty());
        s.record("a", &[1], 1);
        assert!(!s.is_empty());
    }

    #[test]
    #[should_panic(expected = "only u64 keys")]
    fn the_pass_through_hasher_takes_only_u64_digests() {
        // another key type would be hashed by `write`, which would put every key in one bucket
        Pass::default().write(b"x");
    }
}

/// Model-based test: random checks and records over a nondecreasing clock against a map of
/// each id's last-kept second. A duplicate is never claimed for an id not kept within the
/// window, and every id kept within `effective_window_s` is caught.
#[cfg(test)]
mod model {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashMap;

    #[derive(Clone, Debug)]
    enum Op {
        Check(Vec<u64>),
        Record(Vec<u64>),
        Tick(u64),
    }

    fn ops() -> impl Strategy<Value = Vec<Op>> {
        let ids = prop::collection::vec(0u64..24, 1..4);
        prop::collection::vec(
            prop_oneof![
                4 => ids.clone().prop_map(Op::Check),
                2 => ids.prop_map(Op::Record),
                2 => (0u64..6).prop_map(Op::Tick),
            ],
            1..80,
        )
    }

    fn run(window: u64, cap: usize, ops: Vec<Op>) -> Result<(), TestCaseError> {
        let mut d = Dedup::new(window, 0, cap);
        let mut last: HashMap<u64, u64> = HashMap::new();
        let mut now = 100;
        let mut dup = vec![];
        for op in ops {
            match op {
                Op::Tick(dt) => now += dt,
                Op::Record(ids) => {
                    d.record(&ids, now);
                    for h in ids {
                        last.insert(h, now);
                    }
                }
                Op::Check(ids) => {
                    d.check(&ids, now, &mut dup);
                    let eff = d.effective_window_s(now);
                    for (&h, &got) in ids.iter().zip(&dup) {
                        let seen = last.get(&h).copied();
                        let within = seen.is_some_and(|t| t + window >= now);
                        prop_assert!(!got || within, "id {} claimed a duplicate at {}", h, now);
                        if cap >= 1 << 20 {
                            prop_assert_eq!(got, within, "id {} at {}", h, now);
                        } else if seen.is_some_and(|t| t + eff >= now) {
                            prop_assert!(
                                got,
                                "id {} kept at {:?} within {}s missed at {}",
                                h,
                                seen,
                                eff,
                                now
                            );
                        }
                        if !got {
                            last.insert(h, now);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Why: a stream idle past the window leaves its index to keep RAM per stream bounded; no
    /// answer may change for it: per stream, exact against the reference, sweeps or not.
    fn streams(window: u64, ops: Vec<(usize, Op, bool)>) -> Result<(), TestCaseError> {
        let mut d = Streams::new(window, 3, 1 << 20);
        let mut last: HashMap<(usize, u64), u64> = HashMap::new();
        // the last second each stream was asked or told of an id
        let mut used: HashMap<usize, u64> = HashMap::new();
        let (mut now, mut dup) = (100, vec![]);
        let name = |s: usize| ["a", "b", "c"][s];
        for (s, op, sweep) in ops {
            match op {
                Op::Tick(dt) => now += 3 * dt,
                Op::Record(ids) => {
                    used.insert(s, now);
                    d.record(name(s), &ids, now);
                    last.extend(ids.into_iter().map(|h| ((s, h), now)));
                }
                Op::Check(ids) => {
                    used.insert(s, now);
                    d.check(name(s), &ids, now, &mut dup);
                    for (&h, &got) in ids.iter().zip(&dup) {
                        let within = last.get(&(s, h)).is_some_and(|&t| t + window >= now);
                        prop_assert_eq!(got, within, "{} id {} at {}", name(s), h, now);
                        if !got {
                            last.insert((s, h), now);
                        }
                    }
                }
            }
            if sweep {
                d.sweep(now);
                // swept: only streams used within the window are left
                let live = used.values().filter(|&&t| t + window >= now).count();
                prop_assert_eq!(d.by_stream.len(), live);
            }
        }
        Ok(())
    }

    proptest! {
        #[test]
        fn streams_idle_past_the_window_leave_ram_and_answer_as_before(
            window in 1u64..12,
            ops in prop::collection::vec((0usize..3, ops().prop_map(|v| v[0].clone()), any::<bool>()), 1..80),
        ) {
            streams(window, ops)?;
        }

        #[test]
        fn without_the_cap_dedup_is_exact(window in 1u64..12, ops in ops()) {
            run(window, 1 << 20, ops)?;
        }

        #[test]
        fn at_the_cap_dedup_is_sound_and_keeps_its_effective_window(
            window in 1u64..12, cap in 1usize..8, ops in ops(),
        ) {
            run(window, cap, ops)?;
        }
    }
}
