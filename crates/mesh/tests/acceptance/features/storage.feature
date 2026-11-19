@storage
Feature: A node's log keeps every acknowledged batch through crashes, damage and retention
  An append is acknowledged only after its batch is on stable storage. A crash at any moment
  loses nothing acknowledged. A torn final write is truncated on restart; damage followed by
  later data stops the node instead of silently losing records. Retention bounds the log by
  dropping its oldest sealed segments, after copying forward what streams still retain in them
  under their own retention, and restarting scans only the active segment.

  Scenario: Acknowledged appends survive repeated SIGKILL
    Given a 1-node cluster with 4 MiB segments
    When a writer appends 3000 batches of 2000 bytes with 1 copy while node 0 is killed 4 times
    Then every acknowledged batch is readable from node 0

  Scenario: A restart scans only the active segment
    Given a 1-node cluster with 2 MiB segments
    When a writer appends 6000 batches of 2000 bytes with 1 copy
    And node 0 is killed and restarted
    Then node 0 reports at least 5 segments
    And node 0 recovered by scanning at most 2 MiB
    And every acknowledged batch is readable from node 0

  Scenario: Retention bounds the log and keeps the newest batches
    Given a 1-node cluster with 1 MiB segments and 3 MiB retention
    When a writer appends 4000 batches of 2000 bytes with 1 copy
    And retention has run on node 0
    Then node 0 keeps at most 6 segments
    And the newest 500 batches are readable from node 0
    And the oldest batch is gone from node 0

  Scenario: A low-volume stream keeps its own retention on every node while the node cap turns over
    Given a 3-node cluster with 1 MiB segments and 4 MiB retention, where stream "low" retains 2 MiB
    When a writer appends 300 batches of 2000 bytes to stream "low" with 3 copies
    And a writer appends 8000 batches of 2000 bytes to stream "high" with 2 copies
    And retention has run on every node
    Then the oldest batch of stream "high" is gone from every node
    And a cursor reads every batch of stream "low" from every node

  Scenario: A torn final write is truncated and the node carries on
    Given a 1-node cluster with 4 MiB segments
    When a writer appends 200 batches of 2000 bytes with 1 copy
    And node 0 is stopped
    And a torn, unacknowledged write is left after the last record of node 0's active segment
    And node 0 is started
    Then node 0 holds exactly the writer's 200 batches
    When the writer appends 50 more batches
    Then every acknowledged batch is readable from node 0

  Scenario: Damage followed by later data stops the node
    Given a 1-node cluster with 4 MiB segments
    When a writer appends 200 batches of 2000 bytes with 1 copy
    And node 0 is stopped
    And an early record of node 0's active segment is damaged
    Then node 0 refuses to start, saying "corrupt"

  @stress @nightly
  Scenario: Acknowledged appends survive SIGKILL under sustained load
    Given a 1-node cluster with 8 MiB segments
    When a writer appends 30000 batches of 4000 bytes with 1 copy while node 0 is killed 20 times
    Then every acknowledged batch is readable from node 0
