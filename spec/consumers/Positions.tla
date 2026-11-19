------------------------------ MODULE Positions -------------------------------
(***************************************************************************)
(* Reader-group positions and compacted streams (crates/mesh/src/node.rs): *)
(* a reader commits its position through any node (`commit_position`: a  *)
(* keyed batch in the node's own `__positions` writer log, acked at 2      *)
(* copies); replication and repair bring every commit to every node, in   *)
(* any order; each node serves, per key, the newest value it holds         *)
(* (`apply_compacted`). And a consumer reading a stream by cursor, failing *)
(* over between nodes that hold different parts of each writer log         *)
(* (`Store::read_cursor`, `Consumer::take`).                               *)
(*                                                                         *)
(* A commit is its number: the reader commits 1, 2, ... in order, each one *)
(* past the last. The order a node uses to pick the newest is `Order`:     *)
(* the node-local LSN (arrival order) before the fix, the read order       *)
(* (event time, writer, epoch, seq) after it, with the commit time as the  *)
(* event time.                                                             *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Nodes, MaxCommit, ByReadOrder, MaxSeq

VARIABLES
    held,      \* held[n]: commits n holds, in its log (arrival) order
    last,      \* the newest commit the reader made
    served,    \* served[n]: the position n serves (0: none)
    store,     \* store[n]: sequences of one writer log durable on n
    cursor,    \* the consumer's next sequence
    delivered  \* what the consumer read, in order

vars == <<held, last, served, store, cursor, delivered>>

Range(s) == {s[i] : i \in 1 .. Len(s)}
Max(S) == CHOOSE x \in S : \A y \in S : y <= x

\* `apply_compacted` over a node's log: the newest by LSN keeps the last arrival; by read
\* order, the latest commit (commit times increase on the node that took each commit, and
\* the reader commits one at a time).
Newest(s) == IF s = <<>> THEN 0
             ELSE IF ByReadOrder THEN Max(Range(s)) ELSE s[Len(s)]

Init ==
    /\ held = [n \in Nodes |-> <<>>]
    /\ last = 0
    /\ served = [n \in Nodes |-> 0]
    /\ store = [n \in Nodes |-> {}]
    /\ cursor = 0
    /\ delivered = <<>>

\* The reader commits the next position through node n.
Commit(n) ==
    /\ last < MaxCommit
    /\ last' = last + 1
    /\ held' = [held EXCEPT ![n] = Append(@, last + 1)]
    /\ served' = [served EXCEPT ![n] = Newest(held'[n])]
    /\ UNCHANGED <<store, cursor, delivered>>

\* Replication or repair brings a commit to a node that lacks it, in any order.
Deliver(c, n) ==
    /\ c \in 1 .. last
    /\ c \notin Range(held[n])
    /\ held' = [held EXCEPT ![n] = Append(@, c)]
    /\ served' = [served EXCEPT ![n] = Newest(held'[n])]
    /\ UNCHANGED <<last, store, cursor, delivered>>

\* A writer log's batches reach nodes in any order (appends, replication, repair).
Store(n, s) ==
    /\ s \in 0 .. MaxSeq
    /\ s \notin store[n]
    /\ store' = [store EXCEPT ![n] = @ \cup {s}]
    /\ UNCHANGED <<held, last, served, cursor, delivered>>

\* The consumer polls any node: `read_cursor` returns the log's batches from the cursor on,
\* stopping at the first one the node lacks (it never skips a predecessor), up to max_bytes
\* (any number of them); `take` advances the cursor past each.
Poll(n, k) ==
    /\ k \in 1 .. MaxSeq + 1
    /\ \A s \in cursor .. cursor + k - 1 : s \in store[n]
    /\ cursor' = cursor + k
    /\ delivered' = delivered \o [i \in 1 .. k |-> cursor + i - 1]
    /\ UNCHANGED <<held, last, served, store>>

Next ==
    \/ \E n \in Nodes : Commit(n)
    \/ \E c \in 1 .. MaxCommit, n \in Nodes : Deliver(c, n)
    \/ \E n \in Nodes, s \in 0 .. MaxSeq : Store(n, s)
    \/ \E n \in Nodes, k \in 1 .. MaxSeq + 1 : Poll(n, k)

Spec == Init /\ [][Next]_vars

\* A node serves the newest commit it holds: a reader resuming there repeats nothing it
\* committed past.
ServesNewestHeld == \A n \in Nodes : served[n] = IF held[n] = <<>> THEN 0 ELSE Max(Range(held[n]))

\* A node's served position never goes back.
NoRegression == [][\A n \in Nodes : served'[n] >= served[n]]_vars

\* A consumer failing over between nodes reads each sequence once, in order, none skipped.
NoLossNoRepeat == delivered = [i \in 1 .. cursor |-> i - 1]

=============================================================================
