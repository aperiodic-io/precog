@large
Feature: Events larger than a batch
  A batch is at most 8 MiB. The SDK cuts a larger event into chunks, each an event of the
  stream, and a chunked reader puts it back together. The broker stores the chunks as any
  events: they replicate, repair and survive a restart as such.

  Scenario: An event of 64 MiB round-trips, through a node's restart, and a reader resumes inside the next
    Given a 3-node cluster with 8 MiB segments
    When the SDK sends events of 65000, 3000, 1 KiB to stream "session-1" as a chunked producer
    Then a chunked SDK reader reads those events from stream "session-1", byte for byte
    And a reader that is not chunked sees more than 100 events in stream "session-1"
    When node 1 is killed and restarted
    Then a chunked SDK reader reads those events from stream "session-1", byte for byte
    And a chunked SDK reader that stops after its first poll and resumes from its position reads every event of stream "session-1" once
