//! Broker-side dedup: drops an event whose dedup key was seen within the window on this node.
//!
//! The index is a few generations of open-addressing tables of 64-bit digests (`xxh3(key)`
//! seeded by the stream), each sized for the last one's count; the oldest is dropped whole.
//! Chosen in `results/dedup.md` against eight other exact indexes: ~15 bytes per live id at
//! k = 3, ~100-250 ns per event with a batch's probes prefetched. It remembers an id for at least
//! the window (up to window * k / (k - 1)). At `max_entries` the oldest generation goes early:
//! the window shortens instead of memory growing (`effective_window_s` reports it).

use std::collections::VecDeque;

pub struct Dedup {
    k: usize,
    span_s: u64,
    max_entries: usize,
    gens: VecDeque<Table>,
    sized: usize,
}

struct Table {
    slots: Vec<u64>,
    len: usize,
    start_s: u64,
}

/// 0 marks an empty slot.
fn nz(h: u64) -> u64 {
    h | (h == 0) as u64
}

/// A key's digest within its stream.
pub fn digest(stream_seed: u64, key: &[u8]) -> u64 {
    nz(xxhash_rust::xxh3::xxh3_64_with_seed(key, stream_seed))
}

pub fn stream_seed(stream: &str) -> u64 {
    xxhash_rust::xxh3::xxh3_64(stream.as_bytes())
}

impl Table {
    fn new(cap: usize, start_s: u64) -> Table {
        Table {
            slots: vec![0; cap.max(1024)],
            len: 0,
            start_s,
        }
    }
    /// Any capacity: multiply-shift instead of a power-of-two mask.
    fn home(&self, h: u64) -> usize {
        ((h as u128 * self.slots.len() as u128) >> 64) as usize
    }
    fn next(&self, i: usize) -> usize {
        if i + 1 == self.slots.len() { 0 } else { i + 1 }
    }
    fn contains(&self, h: u64) -> bool {
        let mut i = self.home(h);
        loop {
            match self.slots[i] {
                0 => return false,
                x if x == h => return true,
                _ => i = self.next(i),
            }
        }
    }
    fn insert(&mut self, h: u64) {
        if (self.len + 1) * 8 > self.slots.len() * 7 {
            let cap = self.slots.len() + self.slots.len() / 2;
            let old = std::mem::replace(&mut self.slots, vec![0; cap]);
            self.len = 0;
            for x in old.into_iter().filter(|&x| x != 0) {
                self.insert(x);
            }
        }
        let mut i = self.home(h);
        while self.slots[i] != 0 {
            i = self.next(i);
        }
        self.slots[i] = h;
        self.len += 1;
    }
}

fn prefetch<T>(p: &T) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: a prefetch is a hint; it never faults and reads nothing into the program.
    unsafe {
        std::arch::x86_64::_mm_prefetch(p as *const T as *const i8, std::arch::x86_64::_MM_HINT_T0)
    }
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
            sized: 1024,
        }
    }

    fn roll(&mut self, now_s: u64) {
        // a new generation each span, or early once the newest holds its share of the cap
        if self
            .gens
            .back()
            .is_none_or(|t| now_s >= t.start_s + self.span_s || t.len >= self.max_entries / self.k)
        {
            // sized for the last generation's count at 7/8 load, plus 10%
            self.sized = self
                .gens
                .back()
                .map_or(self.sized, |t| t.len * 8 / 7 * 11 / 10 + 1);
            self.gens.push_back(Table::new(self.sized, now_s));
        }
        // by count, by age after an idle gap, and by the entry cap
        while self.gens.len() > self.k
            || self
                .gens
                .front()
                .is_some_and(|t| now_s >= t.start_s + self.span_s * self.k as u64)
            || (self.gens.len() > 1 && self.len() > self.max_entries)
        {
            self.gens.pop_front();
        }
    }

    /// For each digest in order: was it seen within the window (a duplicate)? New ones are
    /// recorded, so a batch's later events see its earlier ones.
    pub fn check(&mut self, digests: &[u64], now_s: u64, dup: &mut Vec<bool>) {
        self.roll(now_s);
        for t in &self.gens {
            for &h in digests {
                prefetch(&t.slots[t.home(h)]);
            }
        }
        dup.clear();
        for &h in digests {
            let seen = self.gens.iter().rev().any(|t| t.contains(h));
            if !seen {
                self.gens.back_mut().unwrap().insert(h);
            }
            dup.push(seen);
        }
    }

    /// Records digests without asking (replicated copies, a restart's rebuild).
    pub fn record(&mut self, digests: &[u64], now_s: u64) {
        self.roll(now_s);
        for &h in digests {
            if !self.gens.iter().any(|t| t.contains(h)) {
                self.gens.back_mut().unwrap().insert(h);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.gens.iter().map(|t| t.len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn bytes(&self) -> usize {
        self.gens.iter().map(|t| t.slots.len() * 8).sum()
    }

    /// How far back ids are remembered now: the window, or less once the cap bites.
    pub fn effective_window_s(&self, now_s: u64) -> u64 {
        self.gens
            .front()
            .map_or(0, |t| now_s.saturating_sub(t.start_s))
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
}
