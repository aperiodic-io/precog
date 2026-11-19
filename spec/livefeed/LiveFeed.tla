------------------------------ MODULE LiveFeed --------------------------------
(***************************************************************************)
(* A live subscription (not from the start) to one node, for one writer    *)
(* log whose batches become durable on that node in any order (pipelined   *)
(* appends, replication, repair): `Store::live_start` (the start position  *)
(* and, per log, the end of its run below it), the empty push that confirms *)
(* the start before `subscribe` returns, `push_loop` reading the node's     *)
(* durable order from the start and `in_log_order` holding a batch until    *)
(* its predecessor came, and the client's `Subscription::accept` joining    *)
(* the log at the first batch it sees and holding batches out of order.     *)
(*                                                                         *)
(* A batch is its sequence number (one event each). The node's durable     *)
(* order is the order batches arrive; `tail` serves it in that order.      *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS MaxSeq, SendBelowStart

Seqs == 0 .. MaxSeq

VARIABLES
    order,     \* the node's durable order (LSN order) of the log's batches
    start,     \* LSN position of the feed's start (-1: not subscribed)
    next,      \* the feed's next sequence for the log
    held,      \* batches the feed holds for a missing predecessor
    fed,       \* LSNs the feed has read
    wire,      \* batches pushed, not yet received by the client
    cnext,     \* the client's next sequence (-1: not joined)
    cheld,     \* batches the client holds
    delivered  \* what the client delivered, in order

vars == <<order, start, next, held, fed, wire, cnext, cheld, delivered>>

Range(s) == {s[i] : i \in 1 .. Len(s)}
Below(s) == s \in {order[i] : i \in 1 .. start}      \* durable before the feed started

\* live_start: the run from 0 of batches durable below the start.
RECURSIVE Run(_, _)
Run(n, S) == IF n \in S THEN Run(n + 1, S) ELSE n

Init ==
    /\ order = <<>>
    /\ start = -1
    /\ next = 0
    /\ held = {}
    /\ fed = 0
    /\ wire = <<>>
    /\ cnext = -1
    /\ cheld = {}
    /\ delivered = <<>>

Arrive(s) ==
    /\ s \notin Range(order)
    /\ order' = Append(order, s)
    /\ UNCHANGED <<start, next, held, fed, wire, cnext, cheld, delivered>>

\* `subscribe` returns once the feed's start is set (the empty push).
Subscribe ==
    /\ start = -1
    /\ start' = Len(order)
    /\ next' = Run(0, Range(order))
    /\ fed' = Len(order)
    /\ UNCHANGED <<order, held, wire, cnext, cheld, delivered>>

\* in_log_order after emitting up to n: batches below the start are skipped (the code
\* before the fix) or pushed too (SendBelowStart), held ones are released.
RECURSIVE Release(_, _, _)
Release(n, h, out) ==
    IF Below(n) THEN Release(n + 1, h, IF SendBelowStart THEN Append(out, n) ELSE out)
    ELSE IF n \in h THEN Release(n + 1, h \ {n}, Append(out, n))
    ELSE <<n, h, out>>

\* push_loop: the next batch in durable order past the start.
Feed ==
    /\ start >= 0
    /\ fed < Len(order)
    /\ LET b == order[fed + 1] IN
       IF b > next THEN /\ held' = held \cup {b}
                        /\ UNCHANGED <<next, wire>>
       ELSE IF b < next THEN UNCHANGED <<next, held, wire>>   \* below where the feed joined
       ELSE LET r == Release(b + 1, held, <<b>>) IN
            /\ next' = r[1]
            /\ held' = r[2]
            /\ wire' = wire \o r[3]
    /\ fed' = fed + 1
    /\ UNCHANGED <<order, start, cnext, cheld, delivered>>

\* Subscription::accept: join at the first batch seen; hold until the predecessor came.
RECURSIVE Drain(_, _, _)
Drain(n, h, out) == IF n \in h THEN Drain(n + 1, h \ {n}, Append(out, n)) ELSE <<n, h, out>>

Receive ==
    /\ wire # <<>>
    /\ LET b == Head(wire)
           n == IF cnext = -1 THEN b ELSE cnext
       IN IF b < n THEN UNCHANGED <<cnext, cheld, delivered>>
          ELSE IF b > n THEN /\ cheld' = cheld \cup {b}
                             /\ cnext' = n
                             /\ UNCHANGED delivered
          ELSE LET r == Drain(b + 1, cheld, <<b>>) IN
               /\ cnext' = r[1]
               /\ cheld' = r[2]
               /\ delivered' = delivered \o r[3]
    /\ wire' = Tail(wire)
    /\ UNCHANGED <<order, start, next, held, fed>>

Next == (\E s \in Seqs : Arrive(s)) \/ Subscribe \/ Feed \/ Receive

Spec == Init /\ [][Next]_vars

\* In order: the client delivers consecutive sequences.
InOrder == \A i \in 1 .. Len(delivered) - 1 : delivered[i + 1] = delivered[i] + 1

\* Once every batch arrived and the feed and client caught up: nothing is stuck, and every
\* batch durable after `subscribe` returned was delivered.
Done == start >= 0 /\ Len(order) = MaxSeq + 1 /\ fed = Len(order) /\ wire = <<>>
NothingStuck ==
    Done => /\ cheld = {} /\ held = {}
            /\ \A i \in start + 1 .. Len(order) : order[i] \in Range(delivered)

=============================================================================
