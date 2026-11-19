---------------------------- MODULE MCReplication -----------------------------
(* Constants for the model-checking configs of Replication.tla.             *)
EXTENDS Replication

Three == {1, 2, 3}
ThreeDomains == [n \in Three |-> n]
Four == {1, 2, 3, 4}
FourDomains == [n \in Four |-> n]
\* two producer replicas: writer logs whose batches carry the same event key
Replicas == {"a", "b"}
SameKey == [i \in Replicas |-> {"k"}]
One == {"a"}
OneKey == [i \in One |-> {"k"}]
NoKey == [i \in One |-> {}]
First == {1}
=============================================================================
