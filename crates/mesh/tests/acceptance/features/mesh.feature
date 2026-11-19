@mesh
Feature: The mesh replicates every acknowledged batch and keeps serving through node faults
  Any node accepts any append and acknowledges once the requested number of failure domains
  hold a durable copy. A node that is down neither stops the others nor loses what it missed:
  repair copies it back when it returns. Subscribers see every acknowledged batch, per writer
  in sequence order.

  Scenario: Durable-2 appends continue while a node crashes, and the node catches up
    Given a 3-node cluster with 4 MiB segments
    When a writer appends 2000 batches of 1000 bytes with 2 copies while node 1 is killed 2 times
    Then every acknowledged batch is held by at least 2 nodes
    When repair has run on every node
    Then every acknowledged batch is held by all 3 nodes

  Scenario: Live subscribers receive every acknowledged batch in writer order
    Given a 3-node cluster with 4 MiB segments
    And a subscriber on node 0
    And a subscriber on node 2
    When 3 writers each append 300 batches of 500 bytes with 2 copies
    Then every subscriber received every acknowledged batch in writer order
