------------------------------ MODULE Replication -----------------------------
(***************************************************************************)
(* Leaderless appends of precog-mesh (crates/mesh/src/node.rs): any node   *)
(* accepts a batch, deduplicates its keyed events against its own index   *)
(* (`begin_local`), queues it to its log, replicates the bytes it kept to  *)
(* one peer per other failure domain, and acknowledges once `copies`       *)
(* distinct domains, its own included, hold a durable copy. A client       *)
(* retries the same bytes on any node until acknowledged. Anti-entropy     *)
(* repair copies durable batches a peer lacks. Nodes power-fail (losing   *)
(* what was not durable, and with write caching what was not fsynced),     *)
(* restart (rebuilding the dedup index from their log), or lose their disk *)
(* for good.                                                               *)
(*                                                                         *)
(* A batch is its identity (stream, writer, epoch, first_seq), one per     *)
(* element of Ids, and its events' dedup keys. Two producer replicas are   *)
(* two writer logs whose batches carry the same keys. A stored copy is the *)
(* identity and the keys its origin kept (`Batch::without` turns dropped   *)
(* events into tombstones but keeps the identity checksum). A conflicting  *)
(* reuse of an identity is a second content variant of it.                 *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS
    Nodes, Dom,       \* nodes and their failure-domain labels
    Entry,            \* nodes clients send to (a stream's producers prefer one node)
    Copies,           \* copies an append asks for (the client's `copies`)
    Ids, KeysOf,      \* batch identities and the dedup keys of each one's events
    Variants,         \* content variants per identity: {"v"}, or {"v", "x"} for a conflicting reuse
    WriteCaching,     \* ack once written; fsync deferred
    Dedup,            \* broker-side dedup on
    MaxSends,         \* client attempts per identity and variant
    MaxFaults,        \* power losses (with write caching) plus disk losses
    MaxCrashes,       \* power losses in all
    EndNeed,          \* nodes `Client::end` merges (the code: min(nodes, 2))
    RepairRecords     \* repair records the keys of what it stores (the fix); FALSE: before it

None == [v |-> "none"]
Domains(S) == {Dom[n] : n \in S}

\* Copy states on a node: queued to its log, written (write caching, not fsynced), durable.
VARIABLES
    st,        \* st[n][i]: None, or [v: variant, kept: keys, s: "queued"|"written"|"synced"]
    q,         \* q[n]: identities queued to n's log, in log order (the WAL thread commits in order)
    idx,       \* idx[n]: dedup keys n remembers
    ops,       \* appends in progress at their origin: [id, i, v, n (origin), kept, acks]
    msgs,      \* replicate requests in flight: [to, id, i, v, kept]
    sends,     \* attempts used per <<identity, variant>>
    acked,     \* <<identity, variant>> pairs acknowledged to a client
    up, lost, faults, crashes

vars == <<st, q, idx, ops, msgs, sends, acked, up, lost, faults, crashes>>

Durable(n, i) == st[n][i] # None /\ st[n][i].s \in {"written", "synced"}

Init ==
    /\ st = [n \in Nodes |-> [i \in Ids |-> None]]
    /\ q = [n \in Nodes |-> <<>>]
    /\ idx = [n \in Nodes |-> {}]
    /\ ops = {}
    /\ msgs = {}
    /\ sends = [p \in Ids \X Variants |-> 0]
    /\ acked = {}
    /\ up = [n \in Nodes |-> TRUE]
    /\ lost = {}
    /\ faults = 0
    /\ crashes = 0

\* `targets()`: one peer per other domain (target_copies covers every domain here).
Targets(n) == CHOOSE T \in SUBSET (Nodes \ {n}) :
                /\ Domains(T) = Domains(Nodes) \ {Dom[n]}
                /\ Cardinality(T) = Cardinality(Domains(T))

\* A client sends identity i (variant v) to node n: `append` -> `begin_local`.
\* A new batch is deduplicated (unless its identity is already here: a retry) and
\* queued to the log under one lock; a retry or conflict goes to `store_local`.
Send(i, v, n) ==
    /\ n \in Entry
    /\ up[n]
    /\ sends[<<i, v>>] < MaxSends
    /\ sends' = [sends EXCEPT ![<<i, v>>] = @ + 1]
    /\ IF st[n][i] = None
       THEN LET kept == IF Dedup THEN KeysOf[i] \ idx[n] ELSE KeysOf[i]
            IN /\ st' = [st EXCEPT ![n][i] = [v |-> v, kept |-> kept, s |-> "queued"]]
               /\ q' = [q EXCEPT ![n] = Append(@, i)]
               /\ idx' = IF Dedup THEN [idx EXCEPT ![n] = @ \cup KeysOf[i]] ELSE idx
               /\ ops' = ops \cup {[id |-> <<i, v, sends[<<i, v>>]>>, i |-> i, v |-> v, n |-> n,
                                     kept |-> kept, acks |-> {}]}
       ELSE IF st[n][i].v = v
       THEN \* a retry of bytes this node holds: counts once durable, replicates again
            /\ ops' = ops \cup {[id |-> <<i, v, sends[<<i, v>>]>>, i |-> i, v |-> v, n |-> n,
                                  kept |-> st[n][i].kept, acks |-> {}]}
            /\ UNCHANGED <<st, q, idx>>
       ELSE UNCHANGED <<st, q, idx, ops>>    \* Conflict: refused, nothing stored
    /\ UNCHANGED <<msgs, acked, up, lost, faults, crashes>>

\* The WAL thread commits the head of the queue: fsynced, or with write caching written.
Commit(n) ==
    /\ up[n]
    /\ q[n] # <<>>
    /\ LET i == Head(q[n]) IN
       st' = [st EXCEPT ![n][i].s = IF WriteCaching THEN "written" ELSE "synced"]
    /\ q' = [q EXCEPT ![n] = Tail(@)]
    /\ UNCHANGED <<idx, ops, msgs, sends, acked, up, lost, faults, crashes>>

\* The deferred fsync.
Flush(n) ==
    /\ up[n]
    /\ \E i \in Ids : st[n][i] # None /\ st[n][i].s = "written"
    /\ st' = [st EXCEPT ![n] = [i \in Ids |->
                IF st[n][i] # None /\ st[n][i].s = "written"
                THEN [st[n][i] EXCEPT !.s = "synced"] ELSE st[n][i]]]
    /\ UNCHANGED <<q, idx, ops, msgs, sends, acked, up, lost, faults, crashes>>

\* The origin sends the bytes it kept to every target at once (`encode_replicate`),
\* once per attempt: a client retry is a new attempt.
Replicate(o) ==
    /\ o \in ops
    /\ ~\E m \in msgs : m.id = o.id
    /\ o.acks = {}
    /\ msgs' = msgs \cup {[to |-> t, id |-> o.id, i |-> o.i, v |-> o.v, kept |-> o.kept] :
                          t \in Targets(o.n)}
    /\ ops' = (ops \ {o}) \cup {[o EXCEPT !.acks = {o.n}]}   \* marks it sent
    /\ UNCHANGED <<st, q, idx, sends, acked, up, lost, faults, crashes>>

OpOf(id) == {o \in ops : o.id = id}
AddAck(id, t) == (ops \ OpOf(id)) \cup {[o EXCEPT !.acks = @ \cup {t}] : o \in OpOf(id)}

\* A replica stores the origin's decision (`replicate` -> `store_local`) and records the
\* keys it kept; it acks Ok/Duplicate once durable, or Conflict. Storing and acking are
\* one step here: a power loss in between is a lost reply, as `Drop` gives.
Receive(m) ==
    /\ m \in msgs
    /\ LET t == m.to IN
       /\ up[t]
       /\ msgs' = msgs \ {m}
       /\ idx' = IF Dedup THEN [idx EXCEPT ![t] = @ \cup m.kept] ELSE idx
       /\ IF st[t][m.i] = None
          THEN /\ st' = [st EXCEPT ![t][m.i] =
                          [v |-> m.v, kept |-> m.kept,
                           s |-> IF WriteCaching THEN "written" ELSE "synced"]]
               /\ ops' = AddAck(m.id, t)
          ELSE /\ UNCHANGED st
               /\ ops' = IF st[t][m.i].v = m.v /\ Durable(t, m.i) THEN AddAck(m.id, t) ELSE ops
    /\ UNCHANGED <<q, sends, acked, up, lost, faults, crashes>>

\* A request or its reply is lost (timeout, connection reset).
Drop(m) ==
    /\ m \in msgs
    /\ msgs' = msgs \ {m}
    /\ UNCHANGED <<st, q, idx, ops, sends, acked, up, lost, faults, crashes>>

\* Early completion: `copies` distinct domains confirmed, the origin's own copy among them.
Ack(o) ==
    /\ o \in ops
    /\ up[o.n]
    /\ Durable(o.n, o.i) /\ st[o.n][o.i].v = o.v
    /\ Cardinality(Domains(o.acks)) >= Copies
    /\ acked' = acked \cup {<<o.i, o.v>>}
    /\ ops' = ops \ {o}
    /\ UNCHANGED <<st, q, idx, msgs, sends, up, lost, faults, crashes>>

\* Anti-entropy: n fetches a durable batch peer p holds and n lacks (`repair_peer` ->
\* `store_local`), and records the keys it kept, as a replica does.
Repair(n, p, i) ==
    /\ up[n] /\ up[p] /\ n # p
    /\ Durable(p, i)
    /\ st[n][i] = None
    /\ st' = [st EXCEPT ![n][i] = [st[p][i] EXCEPT !.s =
                                     IF WriteCaching THEN "written" ELSE "synced"]]
    /\ idx' = IF Dedup /\ RepairRecords THEN [idx EXCEPT ![n] = @ \cup st[p][i].kept] ELSE idx
    /\ UNCHANGED <<q, ops, msgs, sends, acked, up, lost, faults, crashes>>

\* What a restart recovers: synced copies (and, without write caching, written ones).
Kept(n) == [i \in Ids |-> IF st[n][i] # None /\ st[n][i].s = "synced" THEN st[n][i] ELSE None]

\* Power loss: unsynced copies, the queue, appends in progress and requests to it are gone.
\* With write caching it counts as a fault: acked copies may go with it.
Crash(n) ==
    /\ up[n]
    /\ crashes < MaxCrashes
    /\ WriteCaching => faults < MaxFaults
    /\ up' = [up EXCEPT ![n] = FALSE]
    /\ st' = [st EXCEPT ![n] = Kept(n)]
    /\ q' = [q EXCEPT ![n] = <<>>]
    /\ ops' = {o \in ops : o.n # n}
    /\ msgs' = {m \in msgs : m.to # n /\ OpOf(m.id) # {} => TRUE}
    /\ crashes' = crashes + 1
    /\ faults' = IF WriteCaching THEN faults + 1 ELSE faults
    /\ UNCHANGED <<idx, sends, acked, lost>>

\* Restart: the dedup index is rebuilt from the keys the log kept (`Node::open`).
Restart(n) ==
    /\ ~up[n]
    /\ up' = [up EXCEPT ![n] = TRUE]
    /\ idx' = [idx EXCEPT ![n] =
                 IF Dedup THEN UNION {st[n][i].kept : i \in {j \in Ids : st[n][j] # None}}
                 ELSE {}]
    /\ UNCHANGED <<st, q, ops, msgs, sends, acked, lost, faults, crashes>>

\* A disk is lost for good; the node comes back empty.
Lose(n) ==
    /\ faults < MaxFaults
    /\ n \notin lost
    /\ lost' = lost \cup {n}
    /\ faults' = faults + 1
    /\ st' = [st EXCEPT ![n] = [i \in Ids |-> None]]
    /\ q' = [q EXCEPT ![n] = <<>>]
    /\ idx' = [idx EXCEPT ![n] = {}]
    /\ ops' = {o \in ops : o.n # n}
    /\ msgs' = {m \in msgs : m.to # n}
    /\ UNCHANGED <<sends, acked, up, crashes>>

Next ==
    \/ \E i \in Ids, v \in Variants, n \in Nodes : Send(i, v, n)
    \/ \E n \in Nodes : Commit(n) \/ Flush(n) \/ Crash(n) \/ Restart(n) \/ Lose(n)
    \/ \E o \in ops : Replicate(o) \/ Ack(o)
    \/ \E m \in msgs : Receive(m) \/ Drop(m)
    \/ \E n, p \in Nodes, i \in Ids : Repair(n, p, i)

Spec == Init /\ [][Next]_vars

\* Repair keeps running and every node eventually restarts and commits what it queued.
Fair == /\ \A n \in Nodes : WF_vars(Commit(n)) /\ WF_vars(Flush(n)) /\ WF_vars(Restart(n))
        /\ \A n, p \in Nodes, i \in Ids : WF_vars(Repair(n, p, i))
FairSpec == Spec /\ Fair

-----------------------------------------------------------------------------
(* Properties                                                              *)

\* No node has lost an acked copy: no disk lost, no power loss under write caching.
NoCopyLost == lost = {} /\ (WriteCaching => crashes = 0)

Holders(i, v) == {n \in Nodes : Durable(n, i) /\ st[n][i].v = v}

\* Durability: with at most Copies - 1 faults, an acknowledged batch has a durable copy.
AckedSurvive == \A p \in acked : Holders(p[1], p[2]) # {}

\* Without faults, an acknowledged batch is durable in Copies distinct domains.
AckedInCopiesDomains ==
    faults = 0 /\ crashes = 0 =>
        \A p \in acked : Cardinality(Domains(Holders(p[1], p[2]))) >= Copies

\* A conflicting reuse of an identity is never acknowledged next to the original, while
\* no node has lost a copy (copies > nodes / 2: two acked copy sets share a node, and a
\* node refuses the second variant). After a lost disk the empty node can accept it.
NoConflictingAcks == NoCopyLost => \A i \in Ids : Cardinality({v \in Variants : <<i, v>> \in acked}) <= 1

\* Dedup never loses an event: every key of an acknowledged batch is kept (not a
\* tombstone) in some durable copy.
DedupLosesNothing ==
    \A p \in acked : \A k \in KeysOf[p[1]] :
        \E n \in Nodes, i \in Ids : Durable(n, i) /\ k \in st[n][i].kept

\* A running node's dedup index holds the keys of every batch it holds: whichever way a copy
\* arrived (accepted, replicated, repaired, recovered), a later duplicate of its events on
\* this node is dropped.
IndexCoversHeld ==
    Dedup => \A n \in Nodes : up[n] => \A i \in Ids : st[n][i] # None => st[n][i].kept \subseteq idx[n]

\* Two producer replicas meeting one dedup index keep one copy of each key.
OneCopyPerKey ==
    \A k \in UNION {KeysOf[i] : i \in Ids} :
        Cardinality({i \in Ids : \E n \in Nodes : st[n][i] # None /\ k \in st[n][i].kept}) <= 1

\* Convergence: every node ends up with every batch some node holds durably.
Converge == <>[](\A n, p \in Nodes, i \in Ids : Durable(p, i) => Durable(n, i))

\* `Client::end`: the ends of any EndNeed nodes, merged per log, cover every acked batch,
\* while no node has lost acked copies (a disk loss, or a power loss under write caching;
\* then the end can be short until repair, and a reader starting there re-reads a little).
EndCoversAcked ==
    NoCopyLost =>
    \A S \in {T \in SUBSET Nodes : Cardinality(T) = EndNeed} :
        \A p \in acked : \E n \in S : Durable(n, p[1])

=============================================================================
