@tokens
Feature: A tenant's token reaches its own streams and nothing else
  Tenants share a cluster. A tenant's token reads and writes the streams under its prefixes on
  every node, and nothing else: not another tenant's stream, not the cluster's state, not a feed
  of every stream. Nodes replicate among themselves with a token of their own, so no client
  token passes for a node.

  Scenario: A tenant works within its prefix on every node, through a node's restart
    Given a 3-node cluster with 1 MiB segments, where tenant "acme" holds a token for "acme/" and peers have their own token
    When the tenant "acme" appends 30 batches of 500 bytes to stream "acme/session-1" with 2 copies
    Then the tenant "acme" reads 30 batches of stream "acme/session-1" from every node
    When node 1 is killed and restarted
    Then the tenant "acme" reads 30 batches of stream "acme/session-1" from every node

  Scenario: A tenant is refused everything outside its prefix
    Given a 3-node cluster with 1 MiB segments, where tenant "acme" holds a token for "acme/" and peers have their own token
    When a writer appends 5 batches of 500 bytes to stream "other/session-9" with 2 copies
    Then the tenant "acme" is refused an append to stream "acme-not/session-1" and nothing is stored
    And the tenant "acme" is refused a read and a live feed of stream "other/session-9"
    And the tenant "acme" is refused the status of every node and a live feed of every stream
    And no client token makes a peer of node 0
