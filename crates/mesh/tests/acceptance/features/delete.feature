@delete
Feature: A deleted stream is gone from every node, for good
  Deleting a stream removes every writer log of it from every node. A node that was down during
  the delete comes back holding the stream: it hands nothing back to the nodes that deleted it,
  and learns of the delete from them. Nothing of the stream is taken again, across restarts.
  The name stays usable: a writer that starts after the delete writes a new stream of it.

  Scenario: A stream deleted while a node was down is deleted there too and never comes back
    Given a 3-node cluster with 1 MiB segments
    When a writer appends 40 batches of 1000 bytes to stream "session-1" with 3 copies
    And a writer appends 40 batches of 1000 bytes to stream "session-2" with 3 copies
    And node 2 is stopped
    And stream "session-1" is deleted through node 0
    Then stream "session-1" holds no batch on node 0
    And stream "session-1" holds no batch on node 1
    When node 2 is started
    Then node 2 still serves 40 batches of stream "session-1"
    When repair has run on every node
    Then stream "session-1" holds no batch on any node
    And a retry of a batch of the deleted stream "session-1" is refused by every node
    And a cursor reads every batch of stream "session-2" from every node
    When node 0 is killed and restarted
    And node 2 is killed and restarted
    Then stream "session-1" holds no batch on any node
    And a cursor reads every batch of stream "session-2" from every node

  Scenario: A deleted stream's name works again for a writer that starts after the delete
    Given a 3-node cluster with 1 MiB segments
    When a writer appends 20 batches of 1000 bytes to stream "session-1" with 3 copies
    And stream "session-1" is deleted through node 1
    Then stream "session-1" holds no batch on any node
    When a writer that starts after the delete appends 10 batches of 1000 bytes to stream "session-1" with 2 copies
    Then every node serves exactly the new writer's 10 batches of stream "session-1"
    When node 1 is killed and restarted
    Then every node serves exactly the new writer's 10 batches of stream "session-1"

  Scenario: A tenant deletes its own streams and no one else's
    Given a 3-node cluster with 1 MiB segments, where tenant "acme" holds a token for "acme/" and peers have their own token
    When the tenant "acme" appends 10 batches of 500 bytes to stream "acme/session-1" with 2 copies
    And a writer appends 5 batches of 500 bytes to stream "other/session-9" with 3 copies
    Then the tenant "acme" is refused a delete of stream "other/session-9"
    And a cursor reads every batch of stream "other/session-9" from every node
    When the tenant "acme" deletes stream "acme/session-1" through node 2
    Then stream "acme/session-1" holds no batch on any node
