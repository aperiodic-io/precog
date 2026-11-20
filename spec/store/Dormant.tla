------------------------------ MODULE Dormant ---------------------------------
(***************************************************************************)
(* Dormant streams (`crates/mesh/src/store.rs`, `summary.rs`): a stream    *)
(* leaves RAM as a summary on disk and comes back from it; a checkpoint    *)
(* (`Store::begin_checkpoint` under the store's lock, `end_checkpoint`      *)
(* later, without it) writes the changed streams' summaries and where they *)
(* cover the log; a crash loses RAM and a checkpoint not yet written, and  *)
(* the restart (`Store::open`) is the summaries plus the log past them.    *)
(*                                                                         *)
(* A record is its LSN (1..Len(log)) and belongs to a stream. A stream's   *)
(* state is the set of its records it serves: marked durable and not       *)
(* forgotten by retention. Absorption (`absorb`) moves the oldest records   *)
(* out of RAM in LSN order once all of them are marked; only a stream with  *)
(* none of its records past that point, marked or not, may leave RAM.      *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    Streams,
    MaxLen,             \* records appended at most
    MaxCrashes,
    ClearAtCollect,     \* TRUE: a stream is clean once collected (the fix); FALSE: once written
    SkipForgotten,      \* TRUE: the restart skips what was forgotten past `contiguous` (the fix)
    EvictInFlightGuard  \* TRUE: no eviction while a checkpoint is in flight (the fix)

VARIABLES
    log,        \* the segment log: record i belongs to stream log[i]
    marked,     \* records marked durable
    forgot,     \* records retention forgot
    absorbed,   \* records 1..absorbed are out of RAM (in absorbed segments)
    hot,        \* streams in RAM
    ram,        \* per stream in RAM: the records it serves
    dirty,      \* per stream: changed since its summary was collected
    db,         \* per stream: its summary on disk (the records it serves)
    cp,         \* the checkpoint on disk
    pend,       \* the checkpoint collected and not yet written: one, or none ({})
    crashes

vars == <<log, marked, forgot, absorbed, hot, ram, dirty, db, cp, pend, crashes>>

Idx == 1 .. Len(log)
Of(s) == {i \in Idx : log[i] = s}
Serves(s) == (Of(s) \cap marked) \ forgot

\* every record below n marked: `contiguous` (an LSN, here a count)
RECURSIVE Contig(_)
Contig(n) == IF n + 1 \in marked \cup forgot /\ n + 1 <= Len(log) THEN Contig(n + 1) ELSE n

NoCp == [cold |-> 0, contig |-> 0, ram |-> {}, dead |-> {}]

Init ==
    /\ log = <<>>
    /\ marked = {}
    /\ forgot = {}
    /\ absorbed = 0
    /\ hot = Streams
    /\ ram = [s \in Streams |-> {}]
    /\ dirty = [s \in Streams |-> FALSE]
    /\ db = [s \in Streams |-> {}]
    /\ cp = NoCp
    /\ pend = {}
    /\ crashes = 0

\* An append reserves in RAM: the stream is awake (`reserve` wakes it).
Write(s) ==
    /\ Len(log) < MaxLen
    /\ s \in hot
    /\ log' = Append(log, s)
    /\ UNCHANGED <<marked, forgot, absorbed, hot, ram, dirty, db, cp, pend, crashes>>

\* Marked durable in any order (a pending record keeps its stream awake).
Mark(i) ==
    /\ i \in Idx \ marked
    /\ marked' = marked \cup {i}
    /\ ram' = [ram EXCEPT ![log[i]] = @ \cup {i}]
    /\ dirty' = [dirty EXCEPT ![log[i]] = TRUE]
    /\ UNCHANGED <<log, forgot, absorbed, hot, db, cp, pend, crashes>>

\* A stream's own retention forgets one of its records (`trim_stream`; it wakes the stream).
Forget(i) ==
    /\ i \in marked \ forgot
    /\ log[i] \in hot
    /\ forgot' = forgot \cup {i}
    /\ ram' = [ram EXCEPT ![log[i]] = @ \ {i}]
    /\ dirty' = [dirty EXCEPT ![log[i]] = TRUE]
    /\ UNCHANGED <<log, marked, absorbed, hot, db, cp, pend, crashes>>

\* The next record leaves RAM for its segment's index once it and all before it are marked.
Absorb ==
    /\ absorbed < Len(log)
    /\ absorbed + 1 \in marked \cup forgot
    /\ absorbed' = absorbed + 1
    /\ UNCHANGED <<log, marked, forgot, hot, ram, dirty, db, cp, pend, crashes>>

\* Under the lock: the changed streams' summaries and the checkpoint (`ram`: records in RAM,
\* `dead`: forgotten ones; the restart replays past `cold` and counts records below `contig`
\* and in `ram` as covered).
Collect ==
    /\ pend = {}
    /\ LET ch == {s \in hot : dirty[s]}
       IN /\ pend' = {[streams |-> ch,
                      sums |-> [s \in ch |-> ram[s]],
                      cp |-> [cold |-> absorbed, contig |-> Contig(0),
                              ram |-> {i \in (marked \ forgot) : i > absorbed},
                              dead |-> forgot]]}
          /\ dirty' = IF ClearAtCollect
                      THEN [s \in Streams |-> IF s \in ch THEN FALSE ELSE dirty[s]]
                      ELSE dirty
    /\ UNCHANGED <<log, marked, forgot, absorbed, hot, ram, db, cp, crashes>>

\* Without the lock: the transaction, durable at once.
Commit == \E p \in pend :
    /\ db' = [s \in Streams |-> IF s \in p.streams THEN p.sums[s] ELSE db[s]]
    /\ cp' = p.cp
    /\ pend' = {}
    /\ dirty' = IF ClearAtCollect THEN dirty
                ELSE [s \in Streams |-> IF s \in p.streams THEN FALSE ELSE dirty[s]]
    /\ UNCHANGED <<log, marked, forgot, absorbed, hot, ram, crashes>>

\* A write that fails (the log's sync, or the transaction): its streams count as changed again.
CommitFails == \E p \in pend :
    /\ pend' = {}
    /\ dirty' = [s \in Streams |-> dirty[s] \/ s \in p.streams]
    /\ UNCHANGED <<log, marked, forgot, absorbed, hot, ram, db, cp, crashes>>

\* A clean stream with no record in RAM (none past `absorbed`, marked or not) leaves it; not while
\* a checkpoint is in flight (its streams count as clean already, and are not on disk yet).
Evict(s) ==
    /\ pend = {} \/ ~EvictInFlightGuard
    /\ s \in hot
    /\ ~dirty[s]
    /\ \A i \in Of(s) : i <= absorbed
    /\ hot' = hot \ {s}
    /\ ram' = [ram EXCEPT ![s] = {}]
    /\ UNCHANGED <<log, marked, forgot, absorbed, dirty, db, cp, pend, crashes>>

\* Any use wakes it from its summary.
Wake(s) ==
    /\ s \notin hot
    /\ hot' = hot \cup {s}
    /\ ram' = [ram EXCEPT ![s] = db[s]]
    /\ UNCHANGED <<log, marked, forgot, absorbed, dirty, db, cp, pend, crashes>>

\* The restart's view of stream s: its summary, and each record past the checkpoint's `cold`
\* replayed: covered (below `contig` or in `ram`: in the summary already), else new unless it
\* was forgotten.
Replayed(s) ==
    {i \in Of(s) : i > cp.cold /\ i > cp.contig /\ i \notin cp.ram
                   /\ (~SkipForgotten \/ i \notin cp.dead)}

\* A power loss: RAM and a checkpoint not yet written are gone; every record in the log is
\* recovered, marked.
Crash ==
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ marked' = Idx
    /\ forgot' = cp.dead
    /\ absorbed' = cp.cold
    /\ hot' = Streams
    /\ ram' = [s \in Streams |-> db[s] \cup Replayed(s)]
    /\ dirty' = [s \in Streams |-> TRUE]
    /\ pend' = {}
    /\ UNCHANGED <<log, db, cp>>

Next ==
    \/ \E s \in Streams : Write(s) \/ Evict(s) \/ Wake(s)
    \/ \E i \in 1 .. MaxLen : Mark(i) \/ Forget(i)
    \/ Absorb \/ Collect \/ Commit \/ CommitFails \/ Crash

Spec == Init /\ [][Next]_vars

\* A stream in RAM serves exactly its marked records retention did not forget: whether it never
\* left RAM, left it and came back, or the node restarted (where what retention forgot after the
\* last checkpoint is back: retention forgets it again at its next pass).
Exact == \A s \in hot : ram[s] = Serves(s)

TypeOK ==
    /\ marked \subseteq Idx /\ forgot \subseteq marked
    /\ absorbed \in 0 .. Len(log)
=============================================================================
