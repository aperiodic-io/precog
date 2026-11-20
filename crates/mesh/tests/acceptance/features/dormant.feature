@dormant
Feature: Streams nobody uses leave RAM and come back from disk unchanged
  A node holds in RAM only the streams in use; an idle one is a summary on disk. Many streams,
  most of them idle, survive a node's crash and the loss of a node's disk: every acknowledged
  batch is still served, and repair refills a replaced node without every peer holding every
  stream in RAM.

  Scenario: Many streams, mostly dormant, survive a restart and a node replacement
    Given a 3-node cluster with 1 MiB segments, where idle streams leave RAM
    When 2000 streams each get a batch of 300 bytes with 2 copies
    And a writer appends 1000 batches of 1000 bytes with 2 copies
    Then node 0 holds at least 1500 streams only on disk
    When node 1 is killed and restarted
    Then every acknowledged batch is readable from node 1
    And node 1 holds at least 1500 streams only on disk
    When node 2 is replaced by an empty one
    And repair has run on every node
    Then every acknowledged batch is held by all 3 nodes
    And every acknowledged batch is readable from node 2
