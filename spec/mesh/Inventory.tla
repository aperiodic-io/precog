----------------------------- MODULE Inventory --------------------------------
(***************************************************************************)
(* Repair by following a peer's log (`Node::repair_peer`, `Control::       *)
(* Inventory`, `Store::inventory`, `rejoin` in crates/mesh/src/node.rs): a *)
(* follower asks a peer for what it logged from a cursor (an LSN) and      *)
(* takes what it lacks; the cursor is kept with the follower's checkpoint. *)
(* Faults: the peer crashes under write caching and loses its unsynced     *)
(* tail, then logs other records at those LSNs; the follower crashes back  *)
(* to its last saved cursor, or loses its disk (cursor 0, nothing held).   *)
(*                                                                         *)
(* A record is an id; the peer's log is a sequence of ids (LSN = index).   *)
(* One inventory page is one record.                                        *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS MaxIds, MaxCrashes,
    Rejoin   \* "all": the peer answers a follower of an earlier boot from where its log
             \* ended at the open of the first boot after it (the fix); "last": at its own
             \* open only (before: two crashes between requests skip a boot's records);
             \* "none": from the cursor

VARIABLES
    plog,      \* the peer's log
    synced,    \* the peer's synced prefix
    boot,      \* the peer's incarnation
    recovered, \* per boot, where its log ended when it opened (kept in its checkpoint)
    nextId,
    cur,       \* the follower's cursor: next LSN to ask for (0-based)
    seen,      \* the boot of the peer the cursor is in
    saved,     \* <<cur, seen>> as of the follower's last checkpoint
    has,       \* ids the follower holds
    crashes

vars == <<plog, synced, boot, recovered, nextId, cur, seen, saved, has, crashes>>

Range(s) == {s[i] : i \in 1 .. Len(s)}
Min(a, b) == IF a < b THEN a ELSE b

Init ==
    /\ plog = <<>>
    /\ synced = 0
    /\ boot = 1
    /\ recovered = <<0>>
    /\ nextId = 1
    /\ cur = 0
    /\ seen = 0
    /\ saved = <<0, 0>>
    /\ has = {}
    /\ crashes = 0

PeerLogs ==
    /\ nextId <= MaxIds
    /\ plog' = Append(plog, nextId)
    /\ nextId' = nextId + 1
    /\ UNCHANGED <<synced, boot, recovered, cur, seen, saved, has, crashes>>

PeerSyncs ==
    /\ synced' = Len(plog)
    /\ UNCHANGED <<plog, boot, recovered, nextId, cur, seen, saved, has, crashes>>

\* A power loss under write caching: the unsynced tail is gone; a new boot.
PeerCrashes ==
    /\ crashes < MaxCrashes
    /\ plog' = SubSeq(plog, 1, synced)
    /\ boot' = boot + 1
    /\ recovered' = Append(recovered, synced)
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<synced, nextId, cur, seen, saved, has>>

\* One inventory request and its answer: the peer starts at the cursor, or for a follower of an
\* earlier boot no later than `recovered`; the follower takes the record there, if any, and moves
\* its cursor past it (or stays at the end).
MinFrom(b) == CHOOSE m \in {recovered[i] : i \in b .. boot} :
                  \A i \in b .. boot : m <= recovered[i]
Follow ==
    /\ LET from == CASE seen = boot \/ Rejoin = "none" -> cur
                      [] Rejoin = "last" -> Min(cur, recovered[boot])
                      [] seen = 0 -> 0
                      [] OTHER -> Min(cur, MinFrom(seen + 1))
       IN /\ has' = IF from < Len(plog) THEN has \cup {plog[from + 1]} ELSE has
          /\ cur' = IF from < Len(plog) THEN from + 1 ELSE from
    /\ seen' = boot
    /\ UNCHANGED <<plog, synced, boot, recovered, nextId, saved, crashes>>

\* The follower's checkpoint keeps its cursor.
Save ==
    /\ saved' = <<cur, seen>>
    /\ UNCHANGED <<plog, synced, boot, recovered, nextId, cur, seen, has, crashes>>

\* The follower crashes: back to its saved cursor (what it took is on its disk).
FollowerCrashes ==
    /\ crashes < MaxCrashes
    /\ cur' = saved[1]
    /\ seen' = saved[2]
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<plog, synced, boot, recovered, nextId, saved, has>>

\* The follower's disk is replaced: it holds nothing and follows from the start.
FollowerLosesItsDisk ==
    /\ crashes < MaxCrashes
    /\ has' = {}
    /\ cur' = 0
    /\ seen' = 0
    /\ saved' = <<0, 0>>
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<plog, synced, boot, recovered, nextId>>

Next ==
    \/ PeerLogs \/ PeerSyncs \/ PeerCrashes \/ Follow \/ Save
    \/ FollowerCrashes \/ FollowerLosesItsDisk

Spec == Init /\ [][Next]_vars

\* Caught up with the peer's boot and log, the follower holds everything the peer holds: a
\* record the peer logged is never skipped, whatever crashed in between.
Covered == (seen = boot /\ cur = Len(plog)) => Range(plog) \subseteq has

\* The cursor never points past what the peer of that boot logged.
InLog == seen = boot => cur <= Len(plog)
=============================================================================
