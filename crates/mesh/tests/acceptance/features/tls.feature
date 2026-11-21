@tls
Feature: Nodes serve TLS, to clients and to each other
  With `tls` configured a node accepts only TLS and dials its peers with it, checked against the
  cluster's CA. A client that does not speak TLS gets nothing. A certificate replaced on disk is
  served without a restart.

  Scenario: A cluster over TLS replicates, repairs and survives a restart
    Given a 3-node cluster with 1 MiB segments, where the nodes serve TLS
    When a writer appends 30 batches of 1000 bytes to stream "session-1" with 3 copies
    Then a cursor reads every batch of stream "session-1" from every node
    And a client that does not speak TLS gets no answer from any node
    When node 2 is stopped
    And a writer appends 30 batches of 1000 bytes to stream "session-2" with 2 copies
    And node 2 is started
    And repair has run on every node
    Then a cursor reads every batch of stream "session-2" from every node

  Scenario: A certificate replaced on disk is served without a restart
    Given a 3-node cluster with 1 MiB segments, where the nodes serve TLS
    When a writer appends 10 batches of 1000 bytes to stream "session-1" with 3 copies
    And the nodes' certificate is replaced on disk
    Then every node serves the new certificate to its next connection
    When a writer appends 10 batches of 1000 bytes to stream "session-2" with 3 copies
    Then a cursor reads every batch of stream "session-2" from every node
