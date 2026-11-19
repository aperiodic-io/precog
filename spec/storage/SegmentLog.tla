------------------------------ MODULE SegmentLog ------------------------------
(***************************************************************************)
(* One active segment of precog-storage's log (crates/storage/src/log.rs): *)
(* group commit, the page cache, a power loss that keeps any subset of the *)
(* unsynced writes (whole, torn or not at all), media damage, and the      *)
(* recovery scan of `scan()`, step for step.                               *)
(*                                                                         *)
(* A slot holds one record; slot i holds LSN i (the segment's base is 0).  *)
(* Record sizes, the 8-byte search stride and the read window are          *)
(* abstracted: every record start is a slot and the search window covers   *)
(* the whole segment (the window is twice the largest group, so a later    *)
(* group always falls inside it). CRC-32C is assumed to catch every torn   *)
(* or damaged record (a record is Valid, Torn or absent).                  *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    N,             \* slots in the segment
    MaxGroup,      \* records per write group, at most
    WriteCaching,  \* Config::flush_delay_ms > 0: ack once written, fsync deferred
    FlushSlots,    \* flush_bytes in slots: unsynced plus in-flight reaching twice this syncs inline
    Bound,         \* recovery's bound on the unsynced tail (Log::unsynced_bound:
                   \* 2 * flush_bytes + MAX_GROUP), in slots
    Fixed,         \* recovery uses Bound under write caching (the fix); FALSE: the old scan
    MediaFaults,   \* allow damage to persisted records
    MaxCrashes

Slots == 0 .. N - 1
Gen == 1                       \* the segment's generation; stale records carry 0

\* Slot contents. A record: generation, LSN, start of its write group, and a unique
\* id so a recovered record can be told from an older one written at the same place.
Zero == [k |-> "zero"]
Stale == [k |-> "stale"]       \* a record of an older generation (recycled file)
Torn == [k |-> "torn"]         \* this generation's magic, fails validation
Rec(lsn, gs, id) == [k |-> "rec", lsn |-> lsn, gs |-> gs, id |-> id]

VARIABLES
    disk,      \* what stable storage holds
    mem,       \* what the page cache holds (reads, and the next writeback)
    dirty,     \* slots written since they were last persisted
    end,       \* next free slot = next LSN
    synced,    \* every slot below this is on disk as written
    acked,     \* ids acknowledged to callers
    last,      \* ids of the most recent write group
    newest,    \* ids of the most recently acknowledged group
    forgiven,  \* newest groups at past recoveries: the most damage may cost (FailClosed)
    hist,      \* per slot, the id of the record last written there (or 0)
    nextId,
    up,        \* running (FALSE: crashed, recovery pending)
    failed,    \* recovery refused to open (fail closed)
    damaged,   \* a media fault hit a persisted record
    crashes,
    flushTo    \* the end an outstanding deferred flush covers (-1: none)

vars == <<disk, mem, dirty, end, synced, acked, last, newest, forgiven, hist, nextId, up, failed,
          damaged, crashes, flushTo>>

Valid(c, i) == c.k = "rec" /\ c.lsn = i      \* decodes, and is LSN i
WrittenAt(c) == c.k \in {"rec", "torn"}      \* `Window::written_at`: this generation's magic

Init ==
    /\ disk \in [Slots -> {Zero, Stale}]     \* a zero-filled spare, or a recycled file
    /\ mem = disk
    /\ dirty = {}
    /\ end = 0
    /\ synced = 0
    /\ acked = {}
    /\ last = {}
    /\ newest = {}
    /\ forgiven = {}
    /\ hist = [i \in Slots |-> 0]
    /\ nextId = 1
    /\ up = TRUE
    /\ failed = FALSE
    /\ damaged = FALSE
    /\ crashes = 0
    /\ flushTo = -1

\* `Log::append`: one pwrite of the whole group at the end. Without write caching the
\* group is acked only after its fsync (Sync), and the next group waits for that
\* (the WAL thread commits groups one at a time). With write caching it is acked once
\* written; the deferred flush runs outside the log's lock while appends go on
\* (BeginFlush, EndFlush), and once unsynced plus in-flight bytes reach twice
\* flush_bytes the next append first syncs inline (Sync).
Write(n) ==
    /\ up
    /\ end + n <= N
    /\ ~WriteCaching => synced = end
    /\ WriteCaching => end - synced < 2 * FlushSlots
    /\ LET new == end .. end + n - 1
           id(i) == nextId + i - end
       IN /\ mem' = [i \in Slots |-> IF i \in new THEN Rec(i, end, id(i)) ELSE mem[i]]
          /\ hist' = [i \in Slots |-> IF i \in new THEN id(i) ELSE hist[i]]
          /\ dirty' = dirty \cup new
          /\ last' = {id(i) : i \in new}
          /\ acked' = IF WriteCaching THEN acked \cup {id(i) : i \in new} ELSE acked
          /\ newest' = IF WriteCaching THEN {id(i) : i \in new} ELSE newest
    /\ end' = end + n
    /\ nextId' = nextId + n
    /\ UNCHANGED <<disk, synced, up, failed, damaged, crashes, forgiven, flushTo>>

\* fdatasync of the active segment (per group, deferred, or at a roll); without write
\* caching the group it covers is acknowledged now.
Sync ==
    /\ up
    /\ synced < end
    /\ disk' = [i \in Slots |-> IF i \in dirty THEN mem[i] ELSE disk[i]]
    /\ dirty' = {}
    /\ synced' = end
    /\ acked' = IF WriteCaching THEN acked ELSE acked \cup last
    /\ newest' = IF WriteCaching THEN newest ELSE last
    /\ UNCHANGED <<mem, end, last, forgiven, hist, nextId, up, failed, damaged, crashes, flushTo>>

\* `Log::begin_flush`: a deferred flush takes what is written so far; `Flush::sync`
\* then fsyncs outside the lock while appends go on, and `Log::end_flush` records it.
BeginFlush ==
    /\ up /\ WriteCaching
    /\ flushTo = -1
    /\ synced < end
    /\ flushTo' = end
    /\ UNCHANGED <<disk, mem, dirty, end, synced, acked, last, newest, forgiven, hist, nextId, up,
                   failed, damaged, crashes>>

EndFlush ==
    /\ up
    /\ flushTo >= 0
    /\ disk' = [i \in Slots |-> IF i \in dirty /\ i < flushTo THEN mem[i] ELSE disk[i]]
    /\ dirty' = {i \in dirty : i >= flushTo}
    /\ synced' = IF flushTo > synced THEN flushTo ELSE synced
    /\ flushTo' = -1
    /\ UNCHANGED <<mem, end, acked, last, newest, forgiven, hist, nextId, up, failed, damaged, crashes>>

\* The kernel writes one dirty page back, in any order.
Writeback(i) ==
    /\ up
    /\ i \in dirty
    /\ disk' = [disk EXCEPT ![i] = mem[i]]
    /\ dirty' = dirty \ {i}
    /\ UNCHANGED <<mem, end, synced, acked, last, newest, forgiven, hist, nextId, up, failed, damaged, crashes, flushTo>>

\* Power loss: each slot still dirty reaches disk whole, torn, or not at all.
Crash ==
    /\ up
    /\ crashes < MaxCrashes
    /\ \E f \in [dirty -> {"old", "new", "torn"}] :
          disk' = [i \in Slots |->
                     IF i \notin dirty THEN disk[i]
                     ELSE CASE f[i] = "old" -> disk[i]
                            [] f[i] = "new" -> mem[i]
                            [] OTHER -> Torn]
    /\ dirty' = {}
    /\ up' = FALSE
    /\ crashes' = crashes + 1
    /\ flushTo' = -1
    /\ UNCHANGED <<mem, end, synced, acked, last, newest, forgiven, hist, nextId, failed, damaged>>

\* A persisted record goes bad on the medium.
Damage(i) ==
    /\ MediaFaults
    /\ ~damaged
    /\ disk[i].k = "rec"
    /\ i \notin dirty
    /\ disk' = [disk EXCEPT ![i] = Torn]
    /\ mem' = [mem EXCEPT ![i] = Torn]
    /\ damaged' = TRUE
    /\ UNCHANGED <<dirty, end, synced, acked, last, newest, forgiven, hist, nextId, up, failed, crashes, flushTo>>

-----------------------------------------------------------------------------
(* Recovery: `scan(file, generation, base, None)` on the active segment.   *)

\* The forward scan stops at the first slot that is not the next record of the log or
\* whose group is neither a new one starting here nor the group in progress.
RECURSIVE Fwd(_, _)
Fwd(i, g) ==
    IF i < N /\ Valid(disk[i], i) /\ (disk[i].gs = i \/ disk[i].gs = g)
    THEN Fwd(i + 1, disk[i].gs)
    ELSE i

X == Fwd(0, 0)
After == {i \in Slots : i > X}
Later == {i \in After : disk[i].k = "rec" /\ disk[i].gs > X}    \* a later group
TornOwn == {i \in After : disk[i].k = "rec" /\ disk[i].gs <= X} \* the torn group's own
Furthest == IF Later = {} THEN X ELSE CHOOSE m \in Later : \A j \in Later : j <= m
\* The fix: with write caching, damage whose later records all lie within the unsynced
\* bound is an unsynced tail that reached disk out of order: truncate it.
InBound == Fixed /\ WriteCaching /\ Furthest + 1 <= X + Bound
NewEnd ==
    IF TornOwn = {} THEN X
    ELSE CHOOSE m \in {disk[i].gs : i \in TornOwn} :
            \A j \in TornOwn : m <= disk[j].gs

Recover ==
    /\ ~up
    /\ ~failed
    /\ IF Later # {} /\ ~InBound
       THEN /\ failed' = TRUE      \* Error::Corrupt: damage followed by a later group
            /\ UNCHANGED <<disk, mem, end, synced, up, forgiven, flushTo>>
       ELSE \* zero every leftover record of this generation past the new end, then go on
            LET e == NewEnd
                d == [i \in Slots |-> IF i >= e /\ WrittenAt(disk[i]) THEN Zero ELSE disk[i]]
            IN /\ disk' = d
               /\ mem' = d
               /\ end' = e
               /\ synced' = e
               /\ up' = TRUE
               /\ forgiven' = forgiven \cup newest
               /\ UNCHANGED failed
    /\ UNCHANGED <<dirty, acked, last, newest, hist, nextId, damaged, crashes, flushTo>>

Next ==
    \/ \E n \in 1 .. MaxGroup : Write(n)
    \/ Sync \/ BeginFlush \/ EndFlush
    \/ \E i \in Slots : Writeback(i) \/ Damage(i)
    \/ Crash
    \/ Recover

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties                                                              *)

TypeOK ==
    /\ end \in 0 .. N
    /\ synced \in 0 .. end
    /\ dirty \subseteq Slots

Live == {mem[i].id : i \in {j \in Slots : j < end /\ mem[j].k = "rec"}}
\* Still in the log below the end, readable or refused loudly on read (damaged in place).
Kept == {hist[i] : i \in {j \in Slots : j < end /\ (mem[j].k = "rec" \/ mem[j] = Torn)}}

\* The log is a prefix of what was written: every record below the end is the one
\* last written at its place (no stale generation, no leftover of a dropped group).
Prefix ==
    up => \A i \in Slots :
            i < end => \/ mem[i].k = "rec" /\ mem[i].id = hist[i]
                       \/ damaged /\ mem[i] = Torn   \* a read reports Error::Corrupt

\* Ack means durable: without write caching, an acknowledged record survives any
\* power loss.
AckedSurvive == (up /\ ~WriteCaching /\ ~damaged) => acked \subseteq Live

\* Recovery refuses to open only when the medium really damaged something.
NoFalseAlarm == ~damaged => ~failed

\* Damage is refused, or costs at most the newest acknowledged group (damage there,
\* followed only by writes a power loss discarded, looks exactly like a torn tail).
FailClosed == (up /\ ~WriteCaching) => (acked \ (newest \cup forgiven)) \subseteq Kept

=============================================================================
