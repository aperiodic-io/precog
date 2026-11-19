------------------------------ MODULE SegmentRoll -----------------------------
(***************************************************************************)
(* A segment roll of precog-storage (`Log::roll`, crates/storage/src/log.rs) *)
(* and the restart that follows a power loss at any point of it: the       *)
(* directory entries, the new segment's header, the two-slot MANIFEST      *)
(* (a torn slot write leaves that slot invalid), the sealed segment's      *)
(* index file, and `Log::open`'s adoption of a successor segment the       *)
(* manifest missed.                                                        *)
(*                                                                         *)
(* Records are abstracted to counts: the sealed segment holds its synced   *)
(* records (SegmentLog checks what a scan recovers from one segment); the  *)
(* new one gets at most one more group. Spare preallocation, retention and *)
(* recycling of the old files are left out: a spare is a durable file.     *)
(***************************************************************************)
EXTENDS Naturals, Sequences

CONSTANTS
    DirSync    \* fsync the directory right after renaming the spare (TRUE in the code)

Names == {"spare", "seg1", "seg2", "idx1", "idxtmp"}
Steps == <<"rename", "dirsync", "header", "manifest", "switch", "idxtmp", "idxrename",
           "idxdirsync", "done">>

VARIABLES
    pc,        \* index into Steps of the roll's next step (1: not started)
    dirMem,    \* directory entries the running process sees
    dirDisk,   \* directory entries a power loss keeps
    hdr2,      \* the new segment's header: "none", "mem" (written), "disk" (synced)
    slots,     \* the MANIFEST's two slots: [seq, segs, nextId], or Bad
    recs2,     \* records durable in seg2 (appended after the switch)
    acked,     \* records acknowledged: seg1's are, seg2's once synced
    idx,       \* seg1's index file content: "none", "mem", "disk"
    up, failed, crashes

vars == <<pc, dirMem, dirDisk, hdr2, slots, recs2, acked, idx, up, failed, crashes>>

\* A manifest lists segments as <<id, sealed>>.
M0 == [seq |-> 3, segs |-> <<<<1, FALSE>>>>, nextId |-> 2]  \* an older copy
M1 == [seq |-> 4, segs |-> <<<<1, FALSE>>>>, nextId |-> 2]
M2 == [seq |-> 5, segs |-> <<<<1, TRUE>>, <<2, FALSE>>>>, nextId |-> 3]
Bad == [seq |-> 0, segs |-> <<>>, nextId |-> 0]   \* fails its checksum

Init ==
    /\ pc = 1
    /\ dirMem = {"spare", "seg1"}
    /\ dirDisk = dirMem
    /\ hdr2 = "none"
    /\ slots = <<M1, M0>>   \* manifest seq s lives in slot (s % 2) + 1
    /\ recs2 = 0
    /\ acked = {"r1"}
    /\ idx = "none"
    /\ up = TRUE
    /\ failed = FALSE
    /\ crashes = 0

At(s) == pc <= Len(Steps) /\ Steps[pc] = s
Step == pc' = pc + 1
Unch == UNCHANGED <<up, failed, crashes>>

Rename == At("rename") /\ Step
    /\ dirMem' = (dirMem \ {"spare"}) \cup {"seg2"}
    /\ UNCHANGED <<dirDisk, hdr2, slots, recs2, acked, idx>> /\ Unch

\* sync_dir after the rename (skipped when DirSync is FALSE)
DirSyncStep == At("dirsync") /\ Step
    /\ dirDisk' = (IF DirSync THEN dirMem ELSE dirDisk)
    /\ UNCHANGED <<dirMem, hdr2, slots, recs2, acked, idx>> /\ Unch

\* write the header, then sync_data (one step each would add nothing: a crash
\* between them is the same as before the write)
Header == At("header") /\ Step
    /\ hdr2' = "disk"
    /\ UNCHANGED <<dirMem, dirDisk, slots, recs2, acked, idx>> /\ Unch

\* write_manifest: the slot seq % 2, then sync_data
Manifest == At("manifest") /\ Step
    /\ slots' = [slots EXCEPT ![(M2.seq % 2) + 1] = M2]
    /\ UNCHANGED <<dirMem, dirDisk, hdr2, recs2, acked, idx>> /\ Unch

\* the active segment is now seg2: appends go there (one group, fsynced, acked)
Switch == At("switch") /\ Step
    /\ UNCHANGED <<dirMem, dirDisk, hdr2, slots, recs2, acked, idx>> /\ Unch

Append2 == up /\ pc > 5 /\ recs2 = 0
    /\ recs2' = 1 /\ acked' = acked \cup {"r2"}
    /\ UNCHANGED <<pc, dirMem, dirDisk, hdr2, slots, idx>> /\ Unch

\* write_index: a tmp file written and synced, renamed, then the directory synced
IdxTmp == At("idxtmp") /\ Step
    /\ dirMem' = dirMem \cup {"idxtmp"}
    /\ idx' = "disk"
    /\ UNCHANGED <<dirDisk, hdr2, slots, recs2, acked>> /\ Unch
IdxRename == At("idxrename") /\ Step
    /\ dirMem' = (dirMem \ {"idxtmp"}) \cup {"idx1"}
    /\ UNCHANGED <<dirDisk, hdr2, slots, recs2, acked, idx>> /\ Unch
IdxDirSync == At("idxdirsync") /\ Step
    /\ dirDisk' = dirMem
    /\ UNCHANGED <<dirMem, hdr2, slots, recs2, acked, idx>> /\ Unch

\* Power loss at any point; while the manifest slot is being written it may tear.
Crash == up /\ crashes < 1
    /\ up' = FALSE /\ crashes' = crashes + 1
    /\ \/ UNCHANGED slots
       \/ At("manifest") /\ slots' = [slots EXCEPT ![(M2.seq % 2) + 1] = Bad]
    /\ UNCHANGED <<pc, dirMem, dirDisk, hdr2, recs2, acked, idx, failed>>

-----------------------------------------------------------------------------
(* `Log::open` after the power loss, on what the disk kept.                *)

Valid(s) == s.seq > 0
Best == IF Valid(slots[1]) /\ (~Valid(slots[2]) \/ slots[1].seq >= slots[2].seq)
        THEN slots[1] ELSE slots[2]
Exists(n) == n \in dirDisk
SegName(i) == IF i = 1 THEN "seg1" ELSE "seg2"
HeaderOk(i) == i = 1 \/ hdr2 = "disk"

\* Listed segments must exist with a matching header (a missing sealed head segment
\* is retention's case, not reachable here). Then the successor the manifest missed:
\* adopted if its header is valid and continues the log, else returned to the pool.
Outcome ==
    LET m == Best
        listedOk == \A k \in 1 .. Len(m.segs) :
                        Exists(SegName(m.segs[k][1])) /\ HeaderOk(m.segs[k][1])
        succ == m.nextId = 2 /\ Exists("seg2")
    IN IF ~(Valid(slots[1]) \/ Valid(slots[2])) \/ ~listedOk THEN "fail"
       ELSE IF succ /\ hdr2 = "disk" THEN "adopt"
       ELSE "ok"

\* The records a successful open serves: seg1's always (sealed with its index, or
\* rebuilt by a scan when the index file is missing), and seg2's when it is part of
\* the log (listed, or adopted).
Recovered ==
    {"r1"} \cup (IF recs2 > 0 /\ (Len(Best.segs) = 2 \/ Outcome = "adopt")
                 THEN {"r2"} ELSE {})

Recover == ~up /\ ~failed
    /\ failed' = (Outcome = "fail")
    /\ UNCHANGED <<pc, dirMem, dirDisk, hdr2, slots, recs2, acked, idx, up, crashes>>

Next == Rename \/ DirSyncStep \/ Header \/ Manifest \/ Switch \/ Append2
        \/ IdxTmp \/ IdxRename \/ IdxDirSync \/ Crash \/ Recover

Spec == Init /\ [][Next]_vars

\* A power loss anywhere in a roll never stops the node from opening, and the open
\* serves every acknowledged record.
OpensAfterAnyCrash == failed = FALSE
AckedSurvive == (~up /\ ~failed /\ Outcome # "fail") => acked \subseteq Recovered

=============================================================================
