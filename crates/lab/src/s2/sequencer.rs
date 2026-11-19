//! A stream's sequencer: dense seq_nums, `match_seq_num`, fencing tokens, monotone timestamps.

use super::record::{Command, Record};

/// What one admitted append was given: seq_nums `start..end` and one timestamp per record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Assigned {
    pub start: u64,
    pub end: u64,
    pub timestamps: Vec<u64>,
}

/// Why an append was refused; nothing was assigned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reject {
    /// `match_seq_num` was not the tail: the tail is returned.
    SeqNumMismatch(u64),
    /// The fencing token did not match: the stream's token is returned.
    FencingTokenMismatch(Vec<u8>),
    Invalid(&'static str),
}

/// S2's limits on one append.
pub const MAX_RECORDS: usize = 1000;
pub const MAX_BATCH_BYTES: usize = 1 << 20;
pub const MAX_FENCING_TOKEN: usize = 36;

#[derive(Clone, Debug, Default)]
pub struct Sequencer {
    tail: u64,
    last_ts: u64,
    token: Vec<u8>,
    trim: u64,
}

impl Sequencer {
    pub fn new() -> Sequencer {
        Sequencer::default()
    }

    /// The next seq_num and the newest timestamp.
    pub fn tail(&self) -> (u64, u64) {
        (self.tail, self.last_ts)
    }

    pub fn fencing_token(&self) -> &[u8] {
        &self.token
    }

    /// The highest trim point requested so far.
    pub fn trim_point(&self) -> u64 {
        self.trim
    }

    /// Assigns seq_nums and timestamps to `records`, or refuses the whole append.
    ///
    /// Timestamps: a client timestamp is kept but capped at `now_ms` (S2's default, `uncapped =
    /// false`); none means arrival time. Either way they never go below the previous record's.
    /// A fence command in the batch changes the token for the *next* append: this one is checked
    /// against the token before it.
    pub fn admit(
        &mut self,
        records: &[(Option<u64>, &Record)],
        match_seq_num: Option<u64>,
        fencing_token: Option<&[u8]>,
        now_ms: u64,
    ) -> Result<Assigned, Reject> {
        if records.is_empty() || records.len() > MAX_RECORDS {
            return Err(Reject::Invalid("an append carries 1 to 1000 records"));
        }
        if records.iter().map(|(_, r)| r.metered_size()).sum::<usize>() > MAX_BATCH_BYTES {
            return Err(Reject::Invalid("an append carries at most 1 MiB"));
        }
        if let Some(t) = fencing_token
            && t != self.token
        {
            return Err(Reject::FencingTokenMismatch(self.token.clone()));
        }
        if let Some(m) = match_seq_num
            && m != self.tail
        {
            return Err(Reject::SeqNumMismatch(self.tail));
        }
        for (_, r) in records {
            if let Some(Command::Fence(t)) = r.command()
                && t.len() > MAX_FENCING_TOKEN
            {
                return Err(Reject::Invalid("a fencing token is at most 36 bytes"));
            }
        }
        let start = self.tail;
        let mut timestamps = Vec::with_capacity(records.len());
        for (ts, r) in records {
            self.last_ts = self.last_ts.max(ts.unwrap_or(now_ms).min(now_ms));
            timestamps.push(self.last_ts);
            self.apply(r);
        }
        self.tail += records.len() as u64;
        Ok(Assigned {
            start,
            end: self.tail,
            timestamps,
        })
    }

    fn apply(&mut self, r: &Record) {
        match r.command() {
            Some(Command::Fence(t)) => self.token = t,
            Some(Command::Trim(s)) => self.trim = self.trim.max(s),
            None => {}
        }
    }

    /// Rebuilds the state a new owner takes over from the stream's records, in seq_num order
    /// (a failover replays the log, or a snapshot plus its suffix).
    pub fn recover<'a>(records: impl IntoIterator<Item = (u64, u64, &'a Record)>) -> Sequencer {
        let mut s = Sequencer::new();
        for (seq, ts, r) in records {
            s.tail = seq + 1;
            s.last_ts = s.last_ts.max(ts);
            s.apply(r);
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bodies(n: usize) -> Vec<Record> {
        (0..n).map(|i| Record::body(format!("r{i}"))).collect()
    }
    fn arrival(rs: &[Record]) -> Vec<(Option<u64>, &Record)> {
        rs.iter().map(|r| (None, r)).collect()
    }

    #[test]
    fn seq_nums_are_dense_across_appends() {
        let mut s = Sequencer::new();
        let (a, b) = (bodies(3), bodies(2));
        assert_eq!(s.admit(&arrival(&a), None, None, 10).unwrap().start, 0);
        let x = s.admit(&arrival(&b), None, None, 11).unwrap();
        assert_eq!((x.start, x.end), (3, 5));
        assert_eq!(s.tail(), (5, 11));
    }

    #[test]
    fn match_seq_num_refuses_anything_but_the_tail() {
        let mut s = Sequencer::new();
        let a = bodies(2);
        s.admit(&arrival(&a), Some(0), None, 1).unwrap();
        assert_eq!(
            s.admit(&arrival(&a), Some(0), None, 1),
            Err(Reject::SeqNumMismatch(2))
        );
        assert_eq!(s.tail().0, 2, "a refused append assigns nothing");
        assert!(s.admit(&arrival(&a), Some(2), None, 1).is_ok());
    }

    #[test]
    fn fence_takes_effect_on_the_next_append() {
        let mut s = Sequencer::new();
        let f = [Record::fence(b"owner-2")];
        // the empty token is the initial one: any token but "" is refused before the fence
        assert_eq!(
            s.admit(&arrival(&f), None, Some(b"owner-2"), 1),
            Err(Reject::FencingTokenMismatch(vec![]))
        );
        s.admit(&arrival(&f), None, Some(b""), 1).unwrap();
        let b = bodies(1);
        assert_eq!(
            s.admit(&arrival(&b), None, Some(b"owner-1"), 2),
            Err(Reject::FencingTokenMismatch(b"owner-2".to_vec()))
        );
        assert!(s.admit(&arrival(&b), None, Some(b"owner-2"), 2).is_ok());
        // an append without a token is not fenced (S2: the token is optional per append)
        assert!(s.admit(&arrival(&b), None, None, 2).is_ok());
    }

    #[test]
    fn fencing_is_checked_before_match_seq_num() {
        let mut s = Sequencer::new();
        s.admit(&arrival(&[Record::fence(b"t")]), None, None, 1)
            .unwrap();
        let b = bodies(1);
        assert_eq!(
            s.admit(&arrival(&b), Some(9), Some(b"x"), 1),
            Err(Reject::FencingTokenMismatch(b"t".to_vec()))
        );
    }

    #[test]
    fn limits() {
        let mut s = Sequencer::new();
        assert!(matches!(
            s.admit(&[], None, None, 1),
            Err(Reject::Invalid(_))
        ));
        assert!(matches!(
            s.admit(&arrival(&bodies(1001)), None, None, 1),
            Err(Reject::Invalid(_))
        ));
        assert!(s.admit(&arrival(&bodies(1000)), None, None, 1).is_ok());
        let big = [Record::body(vec![0u8; MAX_BATCH_BYTES])];
        assert!(matches!(
            s.admit(&arrival(&big), None, None, 1),
            Err(Reject::Invalid(_))
        ));
        let long = [Record::fence(&[b'x'; 37])];
        assert!(matches!(
            s.admit(&arrival(&long), None, None, 1),
            Err(Reject::Invalid(_))
        ));
        assert!(
            s.admit(&arrival(&[Record::fence(&[b'x'; 36])]), None, None, 1)
                .is_ok()
        );
    }

    #[test]
    fn timestamps_are_capped_and_monotone() {
        let mut s = Sequencer::new();
        let r = Record::body("x");
        let a = s
            .admit(&[(Some(50), &r), (Some(5), &r), (None, &r)], None, None, 40)
            .unwrap();
        // 50 is in the future: capped at 40; 5 is behind: raised to 40; arrival: 40
        assert_eq!(a.timestamps, vec![40, 40, 40]);
        let b = s.admit(&[(Some(45), &r)], None, None, 60).unwrap();
        assert_eq!(b.timestamps, vec![45]);
        let c = s.admit(&[(None, &r)], None, None, 30).unwrap();
        assert_eq!(
            c.timestamps,
            vec![45],
            "a clock going back never lowers them"
        );
    }

    #[test]
    fn trim_point_only_rises() {
        let mut s = Sequencer::new();
        s.admit(&arrival(&[Record::trim(5)]), None, None, 1)
            .unwrap();
        s.admit(&arrival(&[Record::trim(3)]), None, None, 1)
            .unwrap();
        assert_eq!(s.trim_point(), 5);
    }

    #[test]
    fn recovery_restores_tail_token_and_clock() {
        let mut s = Sequencer::new();
        let mut log = vec![];
        let rs = [
            Record::body("a"),
            Record::fence(b"t1"),
            Record::trim(1),
            Record::body("b"),
        ];
        for (i, r) in rs.iter().enumerate() {
            let a = s.admit(&[(None, r)], None, None, 100 + i as u64).unwrap();
            log.push((a.start, a.timestamps[0], r));
        }
        let t = Sequencer::recover(log.iter().map(|(s, t, r)| (*s, *t, *r)));
        assert_eq!(t.tail(), s.tail());
        assert_eq!(t.fencing_token(), b"t1");
        assert_eq!(t.trim_point(), 1);
        assert_eq!(Sequencer::recover([]).tail(), (0, 0));
    }
}
