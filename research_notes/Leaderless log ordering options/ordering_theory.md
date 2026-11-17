# Ordering without (or with minimal) coordination: theory and practice for a leaderless append-log mesh

Scope note: these notes cover the theory (TOB/consensus equivalence, the TOB taxonomy, communication-history ordering, causal/CRDT ordering) and the practice (Mencius, Calvin, Scalog, Narwhal/Bullshark/Mysticeti, Spanner commit wait, Kafka per-key order). Inferences that apply this to precog are labelled as inferences. The main survey source is the EPFL technical-report version of Défago/Schiper/Urbán ("Total order broadcast and multicast algorithms: taxonomy and survey"). It was published in ACM Computing Surveys 36(4), 2004, pp. 372–421. I read the report text directly. Section numbers below follow the report and may differ slightly from the journal version.

---

## Q1. What is the minimal coordination needed for (1) total order agreement, (2) finality, (3) real-time order, given crash faults and a leaderless durable write layer?

### Takeaway
Total order broadcast (TOB, also called atomic broadcast) with agreement is equivalent to consensus. In an asynchronous system with even one crash, it therefore needs a failure detector of class ◇S, partial synchrony, or randomization somewhere. You cannot avoid that, but you can confine it. In failure-free runs, ordering needs no per-message consensus: a communication-history or deterministic-merge scheme only waits to hear from every member. Consensus is needed only to agree on membership changes (views or epochs) and on what a removed member contributed. Real-time order (linearizability) needs one more thing: an append is acked only after its position is fixed, or the system waits out the clock uncertainty (commit wait).

### Cited Findings
**Equivalence and impossibility**
- "Total order broadcast and consensus are equivalent problems, i.e., if there exists an algorithm that solves one problem, then it can be transformed to solve the other problem." Dolev et al. 1987 show TOB → consensus; Chandra & Toueg 1996 show consensus → TOB. — [Défago, Schiper, Urbán, §2.4.5](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- The equivalence "also holds in asynchronous systems with arbitrary failures" (Chandra & Toueg 1996). — [Défago et al., footnote 7](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- FLP: there is "no deterministic solution to the problem of consensus in asynchronous systems if just a single process can crash". Consensus becomes solvable with failure detectors (Chandra–Toueg), partial synchrony (Dolev et al.; Dwork et al.), or randomization. "The weakest failure detector to solve consensus in an asynchronous system is of class ◇S" (Chandra, Hadzilacos, Toueg 1996). — [Défago et al., §2.4.5](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- The survey says claims of solving TOB "in asynchronous systems with process failures" are "of course incorrect ... or incomplete at best." — [Défago et al., §2.5](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- Constructions of the equivalence. To get consensus from TOB, "the first message delivered by total order broadcast is taken to be the decided value." To get TOB from consensus, "use a separate instance of the consensus protocol to decide on the first, second, third, ... message." Consensus decisions are defined as final: "a node will not change its mind once it has decided." — [Kleppmann, Cambridge Distributed Systems notes, §6](https://www.cl.cam.ac.uk/teaching/2122/ConcDisSys/dist-sys-notes.pdf)
- "The FLP result ... showed us that consensus and total order broadcast require partial synchrony." A linearizable CAS is equivalent to consensus (Herlihy 1991). The ABD linearizable get/set register is asynchronous. Eventual and strong eventual consistency need no waiting and no timing assumptions. — [Kleppmann notes, Slide 141 discussion](https://www.cl.cam.ac.uk/teaching/2122/ConcDisSys/dist-sys-notes.pdf)

**Where the coordination can be pushed: membership and views**
- The basic algorithms of every class "are not tolerant to failures: if a single process crashes, the properties ... are not satisfied." To become fault-tolerant they add one or more of: failure detection, a group membership service, resilient communication patterns (waiting for n−f messages), or message stability. — [Défago et al., §8](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- "While failure detectors provide unreliable and inconsistent failure notifications, a group membership service provides consistent failure notifications." In the primary-partition model, accuracy is enforced by "process-controlled crash": falsely suspected processes are forced to crash. View synchrony "ensures that between two successive views v and v′, processes in the two views deliver the same set of messages." Isis, Totem, Transis and Phoenix are built this way. — [Défago et al., §8.2](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- A group membership service with process-controlled crash "typically emulates" a perfect-ish failure detector. The membership service is itself an agreement problem: views are "notified in the same order to all its members". — [Défago et al., §2.6/§8.2](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- The fixed-sequencer algorithms surveyed "all rely on process-controlled crash to cope with failures; either explicitly (e.g., Tandem), or through a group membership and exclusion (e.g., Isis, Rampart)." — [Défago et al., §9.1](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)

**Uniformity and message stability (relevant to "finality" after crashes)**
- Uniform Agreement and Uniform Total Order constrain faulty processes too: "a process is not allowed to deliver any message out of order, even if it is faulty". Non-uniform variants cover only correct processes. Enforcing uniformity "often has a considerable performance cost." — [Défago et al., §4.1](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- Uniform agreement is achieved with message stability: a message is k-stable once k processes have it, and it is delivered only when stable. — [Défago et al., §8.4](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)

**Real-time order**
- Spanner's commit wait: after a commit timestamp is chosen, the transaction waits until TrueTime guarantees the timestamp is in the past. The expected wait is about 2ε, where ε is the clock-uncertainty bound. This gives external consistency, which is the same as linearizability. (Secondary course slides; the original OSDI 2012 paper was not fetched.) — [UW CSEP552 Spanner slides](https://courses.cs.washington.edu/courses/csep552/13sp/lectures/6/spanner.pdf)
- Scalog acks an append only after a global cut covers the record. The storage server "acknowledge[s] the corresponding append requests by returning the record's global sequence number to the client" once a global cut arrives. — [Scalog, NSDI 2020, §4](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf)
- With FIFO and causal broadcast, a node can deliver its own message immediately. "This is no longer true in total order broadcast": a node's delivery of its own message must wait for others. — [Kleppmann notes, §4.2](https://www.cl.cam.ac.uk/teaching/2122/ConcDisSys/dist-sys-notes.pdf)

### Inferences
- Total order agreement: any scheme in which all readers see the same cross-writer order and the order survives crashes solves TOB, so it solves consensus. The consensus need not sit on the data path. The minimum is consensus over views or epochs. Each membership change must be agreed and must fix the frontier, the last included sequence number, of every writer or node that is removed. Between changes, a deterministic function of what every member has published is enough. Options (b) and (c) are TOB only if they include such a consensus-managed view or epoch change. Without one they are either unsafe (readers may disagree about the crashed node's tail) or not live (they block forever on the crashed node).
- Finality: an order is final at a reader once (i) every member of the current view has published past position P, and (ii) all records before P are stable (in precog, at 2 of 3 copies). The only non-final case is a member that stops publishing. Resolving it takes an agreed view change that seals that member's frontier. Scalog's "emergency finalization", Mencius's revocation and Isis's flush all do this.
- Real-time order (A acked before B started ⇒ A before B): this does not follow from total order alone. A cheap sufficient rule for round-based ordering:
  1. Assign each append to the node's current open round.
  2. Never accept appends into a round that node has closed.
  3. Ack A only after round(A) is globally closed, meaning every view member has closed it.

  Then any B issued after A's ack lands in a later round on every node. The cost is ack latency of at least the time to close the round globally. The alternative is clock-based commit wait (≈2ε). Acking at 2 of 3 durable copies before ordering, as precog does today, gives durability but not real-time order.
- Per-key or per-writer order (option d) needs no agreement at all beyond the single writer log's own sequence. Its durability ack at 2 of 3 copies is enough, as long as the writer's epoch fencing prevents two concurrent incarnations. Epoch fencing is itself a small agreement on who owns the log.

### Gaps
- I did not fetch Chandra & Toueg (JACM 1996) directly. The equivalence is cited through the survey and Kleppmann's notes.
- I did not fetch the original Spanner paper. The commit-wait ≈2ε figure comes from secondary slides.

---

## Q2. TOB algorithm classes (Défago et al.) — mechanism, latency, failure behaviour; where precog's options fall

### Takeaway
There are five classes: fixed sequencer, moving sequencer, privilege-based, communication history (with causal-history and deterministic-merge variants), and destinations agreement. Precog's options map onto them:
- (a) Scalog cuts: a fixed (replicated) sequencer that orders frontiers rather than messages. It is close to destinations agreement on a message set.
- (b) node-stamped clocks + watermarks: communication history.
- (c) (round, node, seq): communication history, deterministic-merge variant. It is close to Mencius/Calvin round-robin.
- (d) per-key routing: avoids TOB altogether.

### Cited Findings
- The five classes are "fixed sequencer, moving sequencer, privilege-based, communication history, and destinations agreement". Privilege-based and moving sequencer are together "token-based". — [Défago et al., §7](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- **Fixed sequencer**: "one process is elected as the sequencer and is responsible for ordering messages". It has three variants: UB (unicast-broadcast), BB (broadcast-broadcast) and UUB. Fixed sequencers are preferred to moving ones because they are "considerably simpler", their "latency ... is often better, as shown by [Urbán et al. 2000]", and there may be a natural sequencer machine. — [Défago et al., §7.1, Note 9](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- **Moving sequencer**: works like a fixed sequencer, but the sequencer role rotates, usually with a token. The main motivation is "to distribute the load among several" processes. All the surveyed algorithms follow the BB variant. In moving-sequencer algorithms token passing is "mostly used for improving performance". In privilege-based algorithms it is "necessary to ensure the liveness". — [Défago et al., §7.2, Notes 8, 9, 12](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- **Privilege-based (token ring, e.g. Totem)**: "senders can broadcast messages only when they are granted the privilege", and "the order is defined by the senders". Senders must know each other, so the class is poorly suited to open groups. Fairness is hard. Latency is good at low load but "increases at high load and in the presence of processor crashes". — [Défago et al., §7.3, Notes 10, 13; §9.3](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- **Communication history**: "processes can broadcast messages at any time, and total order is ensured by delaying the delivery of messages"; destinations "observe ... the history of communication in the system, to learn when delivering a message will no longer violate the total order." Two variants:
  - "causal history" turns the causal partial order into a total order.
  - "deterministic merge", where "processes send messages timestamped independently ... and delivery takes place according to a deterministic policy of merging the streams", e.g. round-robin over processes.

  — [Défago et al., §7.4](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- **Destinations agreement**: "the delivery order results from an agreement between destination processes". Variants agree on (1) a per-message sequence number (Skeen-style: each destination proposes a local timestamp and the final timestamp is the max), (2) a message set (consensus-based, as in Chandra–Toueg), or (3) acceptance of a proposed order. — [Défago et al., §7.5](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- Time-free versus time-based ordering is a separate axis. Communication-history algorithms can use logical clocks or physical, synchronized clocks. — [Défago et al., §7.6](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- Kleppmann's summary of the two simple approaches. The single leader (sequencer) has the problem that a "leader crash ⇒ no more messages delivered; changing the leader safely is difficult". With Lamport clocks, the question is "how do you know if you have seen all messages with timestamp < T? Need to use FIFO links and wait for message with timestamp ≥ T from every node". "Neither of these approaches is fault tolerant: in both cases, the crash of a single node can stop all other nodes from being able to deliver messages." — [Kleppmann notes, Slide 86](https://www.cl.cam.ac.uk/teaching/2122/ConcDisSys/dist-sys-notes.pdf)
- Scalog: "The totally ordered sequence of cuts can be used to induce a total ordering on individual records ... We use a deterministic function that specifies how to order the records in between two consecutive cuts", namely lexicographic by shard and then storage server. — [Scalog §4](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf)

### Inferences
- (a) Scalog cuts are a fixed sequencer made fault-tolerant by Paxos/Raft. The sequencer orders compact frontier vectors, not messages, and a deterministic merge orders records between cuts. Consensus sits on the ordering path, but not on the per-record data path.
- (b) Node-stamped Lamport/HLC clocks plus watermarks are the classical causal-history communication-history algorithm (Lamport 1978). It is total and needs no sequencer while failure-free. It blocks on a crashed or silent node and needs heartbeats (empty messages) for liveness. It needs membership or failure detection for fault tolerance.
- (c) Ordering by (round, node, local seq) is the deterministic-merge variant (Aguilera & Strom; Calvin's round-robin epoch interleave). It is structurally like a moving sequencer or Mencius without the per-instance Paxos. It has the same blocking and fault-tolerance needs as (b).
- (d) Per-key routing to one writer log turns cross-key total order into "not needed". Each log is already a FIFO broadcast from a single sender.

### Gaps
- The survey has no common quantitative latency table across classes. It mentions Urbán et al. 2000 for fixed versus moving sequencer, but I did not fetch that paper's numbers.

---

## Q3. Communication-history ordering: why it is total without a sequencer, why a crash blocks it, and how virtual synchrony fixes it

### Takeaway
Lamport timestamps with ties broken by sender ID, delivered once a timestamp ≥ T has been seen on FIFO links from every process, give a total order with no sequencer. Delivery waits for the slowest or quietest process. Idle processes must send empty messages or heartbeats. A crashed process blocks delivery forever unless a failure detector or a membership service (view change with a flush) removes it. Removal is itself an agreement step.

### Cited Findings
- The algorithm in Figure 11 of the survey: each message carries a Lamport timestamp. Process p tracks LC_p[q] for every q. A received message m′ is deliverable when "ts(m′) ≤ min_{q∈Π} LC_p[q]". Messages are delivered "in increasing order of (ts(m), sender(m))", the lexicographic tie-break. It assumes FIFO channels. — [Défago et al., §7.4, Fig. 11](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- Note 14 says the algorithm "is not live": a process q "can deliver m only after it has received, from every process, a message that was broadcast after the reception of m". To fix this, the algorithms in the literature "usually send empty messages when no application messages are broadcast." — [Défago et al., §7.4, Note 14](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- Note 15: with synchronized clocks in synchronous systems, physical timestamps can replace logical ones and empty messages become unnecessary, "an example of the use of time to communicate". — [Défago et al., §7.4, Note 15](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- HAS (Cristian et al.) is time-based. A message stamped T is delivered at local time T+Δ, where Δ depends on topology, the number of tolerated failures and clock drift. "If the synchronous assumptions do not hold, the algorithms could violate the safety of the protocol (i.e., Total Order) rather than just its liveness." — [Défago et al., §9.4.10](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- Deterministic merge (Aguilera & Strom 2000): senders stamp messages with physical timestamps, and each received message determines which sender is accepted next. It is most efficient with synchronized clocks and known fixed send rates. "To make the algorithm live, senders need to send empty messages if they have no message to send (these messages divide the execution into independent epochs). The algorithm, as described, is not fault-tolerant." — [Défago et al., §9.4.9](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- Newtop (symmetric) makes Lamport's algorithm fault-tolerant by relying on "a partitionable group membership service". It preserves causal order and satisfies only Weak Total Order. — [Défago et al., §9.4.3](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- Resilient patterns are an alternative: waiting for n−f messages instead of n is non-blocking without failure detectors. — [Défago et al., §8.3](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- Virtual synchrony view change, in the Isis style: members flush the old view and exchange flush messages before installing the new view, so that old-view messages are delivered before the switch. Waiting for flushes from all members is the main source of view-change delay. — [Rutgers CS417 virtual synchrony slides](https://people.cs.rutgers.edu/~pxk/417/notes/content/08-virtual_synchrony-slides.pdf)
- Strong virtual synchrony, as in Horus, requires the application to block sending during view changes. Isis's definition is weaker. — [Friedman & van Renesse, "Strong and Weak Virtual Synchrony in Horus" (Cornell)](https://www.cs.cornell.edu/Info/Projects/spinglass/public_pdfs/Strong%20and%20Weak.pdf)
- Gbcast, the Isis membership and ordered multicast primitive, delivers to "all the non-failed members listed in the view during which delivery is scheduled". — [Wikipedia: Gbcast](https://en.wikipedia.org/wiki/Gbcast)
- In WANs, failure detection is less accurate, and inaccurate detection "may cause a membership algorithm to change views frequently". — [Search summary of WAN membership literature (escholarship)](https://escholarship.org/content/qt5z0922rb/qt5z0922rb.pdf?t=qvrc6u)

### Inferences
- Latency of communication history in failure-free runs, for a message m sent at time t (my derivation from Fig. 11 and Note 14, not a published formula). m is delivered at a node once every other node's later message or heartbeat has arrived. Delivery latency ≈ max over q of (time until q next sends, which is ≤ heartbeat interval H) + one-way delay d_q→p. In the worst case that is about H + d_max, plus skew from the clocks. With HLC or physical time, Note 15 lets each node bound the wait more aggressively. Tight bounds still need clock-sync assumptions, and a bad bound breaks safety (HAS), not just liveness.
- Under a crash, delivery stalls for the failure-detection timeout plus a consensus-based view change plus a flush. The flush must agree on exactly which of the crashed node's messages are included. In precog, the copies at 2 of 3 nodes make the crashed node's durable prefix recoverable. The view change only has to agree on its frontier.
- I found no measured costs for Isis/Spread view changes in the sources I fetched.

### Gaps
- Quantitative Isis, Spread or JGroups latency and view-change figures were not found. Spread's own papers (JHU DSN) would be the place to look.

---

## Q4. Causal broadcast, vector clocks, CRDTs and deterministic merge: what they give and what they don't

### Takeaway
Causal broadcast and CRDTs give causal order and convergence (strong eventual consistency) without any waiting. They do not give an agreed, final total order: replicas may apply concurrent operations in different orders, and correctness depends on those operations commuting. A "deterministic merge" by timestamp, as in Automerge or a merge by event time, gives the same order only once every replica has seen the same set of operations. Without a membership/frontier agreement, no replica can know that set is complete, so the merged order is never provably final.

### Cited Findings
- Replication over weaker broadcast. Total order broadcast needs only deterministic updates (SMR). Causal broadcast needs "deterministic, concurrent updates commute". Reliable broadcast needs "all updates commute". Best-effort broadcast needs updates that are also idempotent and tolerate message loss. — [Kleppmann notes, Slide 104](https://www.cl.cam.ac.uk/teaching/2122/ConcDisSys/dist-sys-notes.pdf)
- Strong eventual consistency means eventual delivery plus convergence: "any two replicas that have processed the same set of updates are in the same state (even if updates were processed in a different order)". It "does not require waiting for network communication"; "causal broadcast (or weaker) can disseminate updates"; and "concurrent updates ⇒ conflicts need to be resolved". — [Kleppmann notes, Slide 140](https://www.cl.cam.ac.uk/teaching/2122/ConcDisSys/dist-sys-notes.pdf)
- A Lamport-timestamp total order is consistent with causality, but "given the Lamport timestamps of two events, it is in general not possible to tell whether those events" are concurrent. Vector clocks are needed for that. — [Kleppmann notes, §3](https://www.cl.cam.ac.uk/teaching/2122/ConcDisSys/dist-sys-notes.pdf)
- Gomes, Kleppmann, Mulligan & Beresford verify SEC in Isabelle. Their framework frames convergence as "an abstract convergence theorem, a property of order relations". — [Kleppmann blog, 2017](https://kleppmann.com/2017/07/07/isabelle-crdt-proof.html); [arXiv 1707.01747](https://arxiv.org/abs/1707.01747)
- With FIFO and causal broadcast a node delivers its own message immediately. Total order broadcast forces waiting. — [Kleppmann notes, §4.2](https://www.cl.cam.ac.uk/teaching/2122/ConcDisSys/dist-sys-notes.pdf)

### Inferences
- Precog's current approach, a merge by source event time, is a deterministic merge with no completeness criterion. Two readers can produce different prefixes, and a late-arriving record with an older event time reorders history, so the order is never final. It meets the SEC "same set ⇒ same order" property only eventually, and only for readers that wait indefinitely.
- What a CRDT-style merge can achieve without consensus: causal order across writers (with vector or HLC stamps), per-writer FIFO, and eventual convergence of state for commutative operations. It cannot achieve agreement on a prefix together with finality without a frontier or membership agreement.

### Gaps
- I did not fetch Automerge or Kafka Streams merge documentation. Statements about their internal ordering are not sourced here.

---

## Q5. Round/epoch-based deterministic interleaving: Mencius, Calvin, DAG-BFT (Narwhal/Bullshark/Mysticeti); what transfers to crash-fault settings

### Takeaway
Mencius is the closest published analogue of option (c). Sequence slots are pre-assigned round-robin to servers. Each server proposes in its own slots and fills idle slots with cheap skips (no-ops). A crashed server's slots are revoked through Paxos run by another server. Calvin uses the same shape: 10 ms epochs, with each sequencer's batch per epoch interleaved round-robin by node ID. Narwhal, Bullshark and Mysticeti separate data dissemination and availability from ordering, and order only small references or certificates. Scalog does the same in the crash-fault world. Every one of these systems still uses consensus (or BFT agreement) to decide skips, revocations, epochs or cuts when something fails.

### Cited Findings
**Mencius (OSDI 2008, crash-recovery, 2f+1 servers)**
- Instances are pre-assigned, e.g. "assign to server 0 the consensus instances 0, 3, 6 etc". Only an instance's coordinator may propose a real value. Others may propose only a no-op, so "a server can learn a skipped no-op without a majority agreeing first". — [Mencius, Mao, Junqueira, Marzullo, OSDI 2008](https://www.usenix.org/legacy/event/osdi08/tech/full_papers/mao/mao_html/)
- Skip rule: a server that sees a suggestion for a higher instance skips its own unused lower turns, so slow servers don't hold back fast ones. Skips are piggybacked on accept and suggest messages. Skip announcements are bounded by a count (α=20) and a time limit (τ=50 ms). — [Mencius](https://www.usenix.org/legacy/event/osdi08/tech/full_papers/mao/mao_html/)
- Failure handling: a server that suspects a peer runs Paxos phases 1 and 2 on the peer's instances to revoke them, proposing no-ops. Future instances can be revoked in blocks (β=100,000). The paper says "Mencius temporarily stalls when any of the servers fails while Paxos temporarily stalls only when the leader fails." Safety holds even with an inaccurate failure detector. — [Mencius](https://www.usenix.org/legacy/event/osdi08/tech/full_papers/mao/mao_html/)
- Latency: a request "can commit ... in just one round trip delay when there is no contention", and delayed commits can add up to 2 more steps. With a 50 ms one-way delay, Paxos averaged 167 ms (100 ms at the leader, 200 ms elsewhere) and Mencius about 155 ms. Network-bound throughput was about 1,550 ops/s for Mencius versus about 540 for Paxos on 3 sites, and about 340 versus about 50 on 7 sites. — [Mencius](https://www.usenix.org/legacy/event/osdi08/tech/full_papers/mao/mao_html/)

**Calvin (SIGMOD 2012)**
- "Calvin divides time into 10-millisecond epochs during which every machine's sequencer component collects transaction requests". Each batch is replicated and then sent with the "sequencer's unique node ID, the epoch number ... and all transaction inputs". Each scheduler builds the global order "by interleaving (in a deterministic, round-robin manner) all sequencers' batches for that epoch." — [Thomson et al., Calvin, SIGMOD 2012 §3.1](https://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf)
- Replication is either asynchronous, where master failover needs agreement on the failed sequencer's last batch, or Paxos-based, using ZooKeeper, where "all sequencers within a replication group use Paxos to agree on a combined batch of transaction requests for each epoch". — [Calvin §3.1](https://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf)

**Scalog (NSDI 2020)**
- Each storage server "periodically reports to the ordering layer a local cut—an integer vector". The ordering layer takes the element-wise minimum across the replicas of a shard to find the durable prefix, then assembles a global cut. A Paxos-replicated ordering layer sits behind a tree of soft-state aggregators. — [Scalog §4](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf)
- The interleaving interval was 0.1 ms in the evaluation. A longer interval increases throughput "at the expense of higher latency". Measured: 52M writes/s across 3,500 shards with about 1.6 ms latency, made up of 1.3 ms with no aggregators plus about 0.1 ms per aggregator layer. — [Scalog §6.3](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf); [NSDI page](https://www.usenix.org/conference/nsdi20/presentation/ding)
- On a server failure, the shard is finalized ("emergency finalization"). "Because the ordering layer totally orders finalization operations and cuts, the latest cut before s is finalized reveals which" records are included. Records after that cut are retransmitted by clients to other shards. — [Scalog §4.4](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf)

**Narwhal/Tusk, Bullshark, Shoal++, Mysticeti (BFT)**
- Narwhal "splits reliable transaction dissemination from transaction ordering". The mempool stores causal histories, and consensus orders only references. Narwhal-HotStuff reached over 130k tx/s at under 2 s WAN latency, versus 1.8k tx/s for HotStuff alone. Throughput scaled linearly to 600k tx/s with more workers. — [Danezis et al., Narwhal & Tusk (EuroSys 2022), arXiv 2105.11827](https://arxiv.org/abs/2105.11827)
- Bullshark orders the Narwhal DAG with no extra messages. Anchors sit in every second round, and an anchor commits when f+1 nodes in the next round reference it. Shoal++ reports Bullshark needs up to 4 rounds (about 12 message delays expected) to commit fault-free. Shoal reduces this to 3 rounds (10.5 message delays), and Shoal++ brings anchor commit to 4 message delays. — [Bullshark arXiv 2209.05633 (CCS 2022)](https://arxiv.org/abs/2209.05633); [Shoal++ arXiv 2405.20488](https://arxiv.org/abs/2405.20488)
- Mysticeti-C uses an uncertified DAG. It is the first DAG BFT protocol to reach the 3-message-round latency lower bound in steady state and under crash failures, with 0.5 s WAN commit at over 200k TPS. — [Babel et al., Mysticeti, arXiv 2310.14821](https://arxiv.org/abs/2310.14821)

### Inferences
- What transfers to precog in a crash-fault, non-BFT setting:
  1. Data availability is already separated from ordering. The writer logs plus the 2-of-3 durable WAL are the "Narwhal" layer. The ordering layer then only has to agree on small frontiers per round.
  2. The DAG idea reduces to this: each node's round-r announcement lists its local frontier for round r (and optionally the peers' round-(r−1) frontiers it has seen). Given that set from all members of the view, the order is the deterministic interleave (round, node, seq). This is Calvin/Mencius without per-slot Paxos.
  3. BFT certificates (2f+1 signatures) are unnecessary under crash faults. "Durable at 2 of 3" plays the role of the availability certificate.
- The cheapest correct construction (inference, combining Mencius, Calvin, Scalog and virtual synchrony):
  - Each node closes numbered rounds, every X ms or after N records. It broadcasts a "round-close(r, node, frontier)" message, which is an empty message (Note 14) when idle.
  - A reader delivers round r once it holds round-close(r) from every member of the current epoch and all records up to those frontiers are durable at 2 of 3. The order within round r is (node, writer, seq).
  - Membership and epochs live in a Raft group. Removing node X at epoch e commits "X's last round = r_X, frontier = F_X". That is Mencius revocation or a Scalog emergency finalization, decided once per failure rather than per round.
  - Failure-free runs need no consensus at all. The cost of consensus is paid only on reconfiguration, and the failed node's rounds stall reads until it completes. Mencius documents the same "stall when any server fails" behaviour.
- Compared with Scalog, this removes the Raft commit from every cut. Every node must hear from every other node (all-to-all, n² small messages per round, fine for n=3). The trade-off is that a slow node (not just a crashed one) gates delivery for everyone. Scalog has the same property: a cut can't advance a shard beyond its slowest replica's report. Mencius mitigates it with skips.
- Subtlety: a deterministic interleave by round is safe only if a node's round-close(r) is final. The node must never later add records to round r. Round-close messages must therefore be durable (logged in the WAL before sending), so a restarted node does not contradict itself.

### Gaps
- I did not find any published crash-fault-only DAG protocol, i.e. a "Narwhal-CFT", among the sources fetched. The CFT adaptation above is inference.
- I did not fetch the Bullshark full-paper text with its exact latency evaluation. The round counts come from the Shoal++ paper.

---

## Q6. Latency formulas (inferences built from the cited mechanisms)

### Takeaway
Writing d for the one-way delay, H for the heartbeat or round interval, T for the cut interval and C for a consensus commit (≈ 1 RTT to a quorum in steady-state Raft/Multi-Paxos):
- communication history ≈ H/2 average (H worst) + d_max
- round interleave ≈ round interval/2 + d_max + durability lag
- Scalog cuts ≈ T/2 + report hop + C + broadcast hop
- Mencius ≈ 1 RTT uncontended
- per-key order = the durability ack only

### Cited Findings
- Mencius commits in one round trip without contention. Concurrent suggestions can add up to 2 more steps. Skip-batching uses τ=50 ms. — [Mencius](https://www.usenix.org/legacy/event/osdi08/tech/full_papers/mao/mao_html/)
- Scalog: a 0.1 ms interleaving interval gave about 1.3 ms client latency with no aggregators, plus about 0.1 ms per aggregator layer. A longer interval means higher throughput and higher latency. — [Scalog §6.3](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf)
- Calvin uses a fixed 10 ms epoch. Ordering must wait for every sequencer's batch for the epoch. — [Calvin §3.1](https://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf)
- Communication history must wait for a message with timestamp ≥ T from every node. — [Kleppmann notes, Slide 86](https://www.cl.cam.ac.uk/teaching/2122/ConcDisSys/dist-sys-notes.pdf); [Défago et al., Note 14](https://infoscience.epfl.ch/record/49913/files/DSU03b.pdf)
- Spanner-style commit wait is about 2ε. — [UW Spanner slides](https://courses.cs.washington.edu/courses/csep552/13sp/lectures/6/spanner.pdf)

### Inferences
- **(b) communication history with HLC and heartbeats every H:** ordered-delivery latency ≈ (time until each peer's next stamped message, ≤ H) + max d + clock-skew allowance. That is about H/2 + d_max on average and H + d_max in the worst case. The ack-to-ordered latency adds the 2-of-3 durability wait in parallel. Under a crash, add the failure-detection timeout plus the Raft membership commit.
- **(c) rounds of length R:** a record waits on average R/2 for its round to close. The reader then waits for the last peer's round-close, ≈ d_max plus that peer's lag. Total ≈ R/2 + d_max + skew between nodes' round boundaries. Aligning boundaries to HLC time reduces skew. Real-time ordered acks (Q1 rule) cost the same plus one hop back to the client.
- **(a) Scalog-style cuts with interval T:** ≈ T/2 (average wait for the next report) + d (report to the ordering leader) + C (Raft commit, ≈1 RTT among the ordering replicas) + d (cut broadcast). This is roughly the round scheme plus one consensus RTT, in exchange for not needing all-to-all completeness at readers.
- **(d) per-key order:** the latency is just the 2-of-3 durable ack (≈1 RTT to the fastest peer plus fsync). There is no ordering wait.

### Gaps
- None of these formulas is published in this exact form. They are derived from the mechanisms cited above and should be validated by measuring the POC.

---

## Q7. Is per-key order enough? Industry evidence

### Takeaway
Kafka, the dominant streaming log, deliberately offers only per-partition total order, with key → partition routing. That is evidence that per-key order is the accepted contract for most streaming workloads. I found no quantitative survey of what fraction of use cases need more.

### Cited Findings
- Kafka: "Events with the same event key (e.g., a customer or vehicle ID) are written to the same partition", and Kafka guarantees "any consumer of a given topic-partition will always read that partition's events in exactly the same order as they were written." The guarantee is stated per topic-partition, with no global cross-partition order. — [Apache Kafka Introduction](https://kafka.apache.org/intro)
- Scalog motivates total order with the claim that the shared-log abstraction underlies many systems, including Kafka, Corfu and FuzzyLog. Its design also lets applications control which shard stores which records, giving data locality like vCorfu's materialized streams. — [Scalog §1](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf)
- When updates commute, causal broadcast (or weaker) suffices for convergence. Total order is needed only for non-commutative SMR. — [Kleppmann notes, Slide 104](https://www.cl.cam.ac.uk/teaching/2122/ConcDisSys/dist-sys-notes.pdf)

### Inferences
- Precog's per-writer logs with epoch fencing already deliver Kafka-equivalent per-key semantics if each key is routed to one writer log (option d). Cross-key total order is needed only by consumers doing non-commutative cross-key state machines: multi-key transactions, global constraints, or deterministic replay across keys. For those, a round/epoch interleave with consensus-managed membership is the cheapest construction that gives agreement and finality.

### Gaps
- I found no published statistic on the share of streaming use cases that need cross-key total order. Claims like "most use cases need only per-key order" remain industry opinion that these sources do not quantify.
