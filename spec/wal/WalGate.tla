---- MODULE WalGate ----
\* The write-caching gate of `crates/mesh/src/wal.rs`: workers append their own batches under
\* the log's lock, except a batch that rolls the segment, and every batch submitted while the WAL
\* thread rolls or runs a retention job, which is queued to the thread under the gate. The thread
\* lifts the gate once nothing is queued behind it.
\*   InOrder:          each worker's batches reach the log in its submit order
\*   NeverWaitsOnSlow: no worker waits for the log's lock while a slow step (a roll, a
\*                     retention job) holds it
\*   Complete:         once nothing can happen, every batch is logged and every job ran
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Workers, N, Jobs,
          Gated,      \* FALSE: the code before the gate (a worker rolls on itself)
          ClearEarly  \* TRUE: the thread lifts the gate with batches still queued

None == "none"
Thread == "thread"

VARIABLES next,    \* next[w]: the sequence number w submits next (1..N, N+1 when done)
          pc,      \* pc[w]: where w is in its submit
          gate,    \* the flag the gate's mutex guards
          gateBy,  \* who holds the gate's mutex
          logBy,   \* who holds the log's mutex
          queue,   \* the channel to the WAL thread
          log,     \* batches in the log, in LSN order
          tpc,     \* where the WAL thread is
          pending, \* the batch the thread took from the queue
          jobs     \* retention jobs still to come

vars == <<next, pc, gate, gateBy, logBy, queue, log, tpc, pending, jobs>>

Init == /\ next = [w \in Workers |-> 1]
        /\ pc = [w \in Workers |-> "idle"]
        /\ gate = FALSE
        /\ gateBy = None
        /\ logBy = None
        /\ queue = <<>>
        /\ log = <<>>
        /\ tpc = "top"
        /\ pending = <<>>
        /\ jobs = Jobs

b(w) == <<w, next[w]>>
Done(w) == [next EXCEPT ![w] = @ + 1]

\* ---- a worker's submit
TakeGate(w) == /\ pc[w] = "idle" /\ next[w] <= N
               /\ IF Gated
                  THEN /\ gateBy = None
                       /\ gateBy' = w
                       /\ pc' = [pc EXCEPT ![w] = "gated"]
                  ELSE /\ pc' = [pc EXCEPT ![w] = "waitlog"]
                       /\ UNCHANGED gateBy
               /\ UNCHANGED <<next, gate, logBy, queue, log, tpc, pending, jobs>>

\* the gate is up: queue the batch while holding the gate
Divert(w) == /\ pc[w] = "gated" /\ gate
             /\ queue' = Append(queue, b(w))
             /\ gateBy' = None
             /\ next' = Done(w)
             /\ pc' = [pc EXCEPT ![w] = "idle"]
             /\ UNCHANGED <<gate, logBy, log, tpc, pending, jobs>>

ToLog(w) == /\ pc[w] = "gated" /\ ~gate
            /\ pc' = [pc EXCEPT ![w] = "waitlog"]
            /\ UNCHANGED <<next, gate, gateBy, logBy, queue, log, tpc, pending, jobs>>

GotLog(w) == /\ pc[w] = "waitlog" /\ logBy = None
             /\ logBy' = w
             /\ pc' = [pc EXCEPT ![w] = "haslog"]
             /\ UNCHANGED <<next, gate, gateBy, queue, log, tpc, pending, jobs>>

\* `Log::rolls` says no: drop the gate, write
Fits(w) == /\ pc[w] = "haslog"
           /\ gateBy' = IF Gated THEN None ELSE gateBy
           /\ pc' = [pc EXCEPT ![w] = "writing"]
           /\ UNCHANGED <<next, gate, logBy, queue, log, tpc, pending, jobs>>

\* it rolls: raise the gate and queue it (gated), or roll on the worker (before the gate)
Rolls(w) == /\ pc[w] = "haslog"
            /\ IF Gated
               THEN /\ gate' = TRUE
                    /\ queue' = Append(queue, b(w))
                    /\ gateBy' = None
                    /\ logBy' = None
                    /\ next' = Done(w)
                    /\ pc' = [pc EXCEPT ![w] = "idle"]
               ELSE /\ pc' = [pc EXCEPT ![w] = "rolling"]
                    /\ UNCHANGED <<gate, queue, gateBy, logBy, next>>
            /\ UNCHANGED <<log, tpc, pending, jobs>>

Write(w) == /\ pc[w] \in {"writing", "rolling"}
            /\ log' = Append(log, b(w))
            /\ logBy' = None
            /\ next' = Done(w)
            /\ pc' = [pc EXCEPT ![w] = "idle"]
            /\ UNCHANGED <<gate, gateBy, queue, tpc, pending, jobs>>

\* ---- the WAL thread
\* top of its loop: with the gate up, take the next queued batch, or lift the gate
TopGate == /\ tpc = "top" /\ gateBy = None
           /\ IF gate /\ queue # <<>>
              THEN /\ pending' = Head(queue)
                   /\ queue' = Tail(queue)
                   /\ tpc' = "append"
                   /\ UNCHANGED gate
              ELSE /\ gate' = FALSE \* nothing queued behind it
                   /\ tpc' = "idle"
                   /\ UNCHANGED <<pending, queue>>
           /\ UNCHANGED <<next, pc, gateBy, logBy, log, jobs>>

\* the lifted-early variant also clears with batches queued, then serves them as ordinary requests
EarlyClear == /\ ClearEarly /\ tpc = "top" /\ gateBy = None /\ gate /\ queue # <<>>
              /\ gate' = FALSE
              /\ tpc' = "idle"
              /\ UNCHANGED <<next, pc, gateBy, logBy, queue, log, pending, jobs>>

\* blocking receive: a queued batch, or a retention job
Recv == /\ tpc = "idle" /\ queue # <<>>
        /\ pending' = Head(queue)
        /\ queue' = Tail(queue)
        /\ tpc' = "append"
        /\ UNCHANGED <<next, pc, gate, gateBy, logBy, log, jobs>>

Job == /\ tpc = "idle" /\ jobs > 0 /\ gateBy = None
       /\ gate' = IF Gated THEN TRUE ELSE gate
       /\ jobs' = jobs - 1
       /\ tpc' = "joblog"
       /\ UNCHANGED <<next, pc, gateBy, logBy, queue, log, pending>>

JobLog == /\ tpc = "joblog" /\ logBy = None
          /\ logBy' = Thread
          /\ tpc' = "slow"
          /\ UNCHANGED <<next, pc, gate, gateBy, queue, log, pending, jobs>>

JobDone == /\ tpc = "slow"
           /\ logBy' = None
           /\ tpc' = "top"
           /\ UNCHANGED <<next, pc, gate, gateBy, queue, log, pending, jobs>>

\* a queued batch may roll: slow
AppendLog == /\ tpc = "append" /\ logBy = None
             /\ logBy' = Thread
             /\ tpc' = "appending"
             /\ UNCHANGED <<next, pc, gate, gateBy, queue, log, pending, jobs>>

AppendDone == /\ tpc = "appending"
              /\ log' = Append(log, pending)
              /\ logBy' = None
              /\ pending' = <<>>
              /\ tpc' = "top"
              /\ UNCHANGED <<next, pc, gate, gateBy, queue, jobs>>

Next == \/ \E w \in Workers : TakeGate(w) \/ Divert(w) \/ ToLog(w) \/ GotLog(w)
                              \/ Fits(w) \/ Rolls(w) \/ Write(w)
        \/ TopGate \/ EarlyClear \/ Recv \/ Job \/ JobLog \/ JobDone \/ AppendLog \/ AppendDone

Spec == Init /\ [][Next]_vars

\* ---- invariants
Mine(w) == SelectSeq(log, LAMBDA x : x[1] = w)
InOrder == \A w \in Workers : Mine(w) = [i \in 1..Len(Mine(w)) |-> <<w, i>>]

SlowHolder == \/ logBy = Thread /\ tpc \in {"slow", "appending"}
              \/ \E v \in Workers : logBy = v /\ pc[v] = "rolling"
NeverWaitsOnSlow == \A w \in Workers : pc[w] = "waitlog" => ~SlowHolder

Complete == ~ENABLED Next => /\ Len(log) = N * Cardinality(Workers)
                             /\ jobs = 0 /\ queue = <<>>
====
