------------------------------- MODULE Delete --------------------------------
(***************************************************************************)
(* Deleting a stream (`Control::Delete`, `Node::delete_stream` in node.rs;  *)
(* `Inner::set_floor`, `Inner::settle`, `Store::reserve` in store.rs of    *)
(* crates/mesh/src): a delete is an epoch floor for                        *)
(* the stream. Every writer log of the stream with an epoch below the      *)
(* floor is deleted, on every node, for good: a node holding the floor     *)
(* forgets what it holds of them and refuses what it is handed of them     *)
(* later. The floor is a record in a stream of the node's own, so it       *)
(* replicates and is repaired as any batch is. A new stream of the same    *)
(* name is writer logs with epochs at or above the floor.                  *)
(*                                                                         *)
(* The accepting node takes the floor above every epoch that `EndNeed`     *)
(* nodes, itself included, hold of the stream, so above every batch        *)
(* acknowledged before (as `Client::end` covers every acknowledged batch), *)
(* and no lower than its own floor. No clock is involved (`delete_floor`). *)
(*                                                                         *)
(* One stream. A writer log is its epoch and holds one batch. A batch      *)
(* reaches a node from a client or from a peer that holds it (replicate    *)
(* and repair are the same step here): it is reserved, then becomes        *)
(* durable. Repair also replaces a batch a node holds with the majority's   *)
(* bytes under the same identity (`Node::replace_with`): it finds the      *)
(* batch durable, writes the replacement, then indexes it. Nodes go down   *)
(* and come back (a batch reserved at the crash may or may not have        *)
(* reached the log), or lose their disk for good.                          *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Nodes,
    MaxEpoch,    \* writer logs have epochs 1 .. MaxEpoch
    Copies,      \* nodes that must hold a batch for its append to be acknowledged
    EndNeed,     \* nodes a delete asks for the stream's epochs: Nodes - Copies + 1
    MaxDeletes,
    MaxLost,     \* disks lost for good (the node is replaced by an empty one)
    Refuse,      \* a node refuses a batch below its floor when it is handed it (the
                 \* design: the client is told). FALSE: it takes it
    Recheck,     \* a batch below the floor is dropped when it becomes durable (the design:
                 \* one reserved before the floor arrived). FALSE: it is indexed. This check
                 \* alone keeps DeletedStaysDeleted; with neither, a peer that was down
                 \* during the delete hands the batch back
    Reapply,     \* a restart applies the floor to what its log replays (the design: the
                 \* floor is in the stream's summary, or its record is replayed first)
    Quorum,      \* TRUE: the floor is above what EndNeed nodes hold (the design).
                 \* FALSE: above what the accepting node holds
    Resettle     \* a replacement is settled as any batch that becomes durable (the design:
                 \* its log may have been deleted while it was written). FALSE: it is
                 \* indexed as it is, as `Store::replace` first did

Epochs == 1 .. MaxEpoch
Floors == 0 .. MaxEpoch + 1

VARIABLES
    has,      \* has[n]: epochs of the batches durable on n
    pend,     \* pend[n]: epochs of the batches reserved on n, queued to its log
    repl,     \* repl[n]: epochs of the batches n is replacing: found durable, the
              \* replacement queued to its log
    floor,    \* floor[n]: the stream's floor on n (0: never deleted)
    up,
    acked,    \* epochs of the batches acknowledged to a client
    issued,   \* deletes accepted: [f |-> the floor, before |-> what was acknowledged by then]
    lost

vars == <<has, pend, repl, floor, up, acked, issued, lost>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x

Init ==
    /\ has = [n \in Nodes |-> {}]
    /\ pend = [n \in Nodes |-> {}]
    /\ repl = [n \in Nodes |-> {}]
    /\ floor = [n \in Nodes |-> 0]
    /\ up = [n \in Nodes |-> TRUE]
    /\ acked = {}
    /\ issued = {}
    /\ lost = 0

\* `Store::reserve`: a node is handed batch e, by a client (`Node::append`) or from a peer
\* that holds it (`Node::replicate`, repair's `fetch_missing`).
Reserve(n, e) ==
    /\ up[n]
    /\ e \notin has[n] \cup pend[n]
    /\ ~Refuse \/ e >= floor[n]
    /\ pend' = [pend EXCEPT ![n] = @ \cup {e}]
    /\ UNCHANGED <<has, repl, floor, up, acked, issued, lost>>

FromClient(n, e) == Reserve(n, e)
FromPeer(m, n, e) == m # n /\ up[m] /\ e \in has[m] /\ Reserve(n, e)

\* `Store::mark_durable` (`Inner::settle`): the batch is on stable storage.
Durable(n, e) ==
    /\ up[n]
    /\ e \in pend[n]
    /\ pend' = [pend EXCEPT ![n] = @ \ {e}]
    /\ has' = [has EXCEPT ![n] = IF Recheck /\ e < floor[n] THEN @ ELSE @ \cup {e}]
    /\ UNCHANGED <<repl, floor, up, acked, issued, lost>>

\* `Store::replaceable`, then `Wal::append_replacing`: n holds the batch with bytes a majority
\* does not hold, and writes theirs (peer m's) in its place.
ReplaceBegin(m, n, e) ==
    /\ m # n
    /\ up[m] /\ up[n]
    /\ e \in has[m] /\ e \in has[n]
    /\ repl' = [repl EXCEPT ![n] = @ \cup {e}]
    /\ UNCHANGED <<has, pend, floor, up, acked, issued, lost>>

\* `Store::replace`: the replacement is on stable storage and takes the batch's place.
ReplaceEnd(n, e) ==
    /\ up[n]
    /\ e \in repl[n]
    /\ repl' = [repl EXCEPT ![n] = @ \ {e}]
    /\ has' = [has EXCEPT ![n] = IF Resettle /\ e < floor[n] THEN @ \ {e} ELSE @ \cup {e}]
    /\ UNCHANGED <<pend, floor, up, acked, issued, lost>>

\* A client learns its append is on `Copies` nodes.
Ack(e) ==
    /\ e \notin acked
    /\ Cardinality({n \in Nodes : e \in has[n]}) >= Copies
    /\ acked' = acked \cup {e}
    /\ UNCHANGED <<has, pend, repl, floor, up, issued, lost>>

\* `Inner::set_floor`: the node holds floor f.
Apply(n, f) ==
    /\ floor' = [floor EXCEPT ![n] = f]
    /\ has' = [has EXCEPT ![n] = {e \in @ : e >= f}]

\* `Node::delete_stream` on n, having asked the nodes of Q for the stream's epochs.
Delete(n, Q, f) ==
    /\ Cardinality(issued) < MaxDeletes
    /\ n \in Q
    /\ \A q \in Q : up[q]
    /\ Cardinality(Q) >= (IF Quorum THEN EndNeed ELSE 1)
    /\ f > Max({0} \cup UNION {has[q] : q \in Q})
    /\ f >= Max({floor[q] : q \in Q})
    /\ f > floor[n]
    /\ Apply(n, f)
    /\ issued' = issued \cup {[f |-> f, before |-> acked]}
    /\ UNCHANGED <<pend, repl, up, acked, lost>>

\* The floor's record reaches a peer (replicate or repair).
FloorFromPeer(m, n) ==
    /\ m # n
    /\ up[m] /\ up[n]
    /\ floor[m] > floor[n]
    /\ Apply(n, floor[m])
    /\ UNCHANGED <<pend, repl, up, acked, issued, lost>>

\* A node stops. What it had reserved, or was replacing, may or may not have reached its log;
\* the restart replays the log past its checkpoint and settles each record as when it became
\* durable (`Store::open`).
Crash(n) ==
    /\ up[n]
    /\ up' = [up EXCEPT ![n] = FALSE]
    /\ \E logged \in SUBSET (pend[n] \cup repl[n]) :
        has' = [has EXCEPT ![n] =
            IF Reapply THEN {e \in @ \cup logged : e >= floor[n]} ELSE @ \cup logged]
    /\ pend' = [pend EXCEPT ![n] = {}]
    /\ repl' = [repl EXCEPT ![n] = {}]
    /\ UNCHANGED <<floor, acked, issued, lost>>

Restart(n) ==
    /\ ~up[n]
    /\ up' = [up EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<has, pend, repl, floor, acked, issued, lost>>

\* A node's disk is lost: it is replaced by an empty node.
Lose(n) ==
    /\ lost < MaxLost
    /\ lost' = lost + 1
    /\ has' = [has EXCEPT ![n] = {}]
    /\ pend' = [pend EXCEPT ![n] = {}]
    /\ repl' = [repl EXCEPT ![n] = {}]
    /\ floor' = [floor EXCEPT ![n] = 0]
    /\ up' = [up EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<acked, issued>>

Next ==
    \/ \E n \in Nodes, e \in Epochs : FromClient(n, e) \/ Durable(n, e)
    \/ \E m, n \in Nodes, e \in Epochs : FromPeer(m, n, e) \/ ReplaceBegin(m, n, e)
    \/ \E n \in Nodes, e \in Epochs : ReplaceEnd(n, e)
    \/ \E e \in Epochs : Ack(e)
    \/ \E n \in Nodes, Q \in SUBSET Nodes, f \in Floors : Delete(n, Q, f)
    \/ \E m, n \in Nodes : FloorFromPeer(m, n)
    \/ \E n \in Nodes : Crash(n) \/ Restart(n) \/ Lose(n)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ has \in [Nodes -> SUBSET Epochs]
    /\ pend \in [Nodes -> SUBSET Epochs]
    /\ repl \in [Nodes -> SUBSET Epochs]
    /\ floor \in [Nodes -> Floors]

\* A node that holds a floor holds nothing below it: what a delete deleted is not served by a
\* node that knows of the delete, whatever arrives later and from whomever.
DeletedStaysDeleted == \A n \in Nodes : \A e \in has[n] : e >= floor[n]

\* A delete's floor is above every batch acknowledged before it was accepted.
AckedIsDeleted == \A d \in issued : \A e \in d.before : e < d.f
===============================================================================
