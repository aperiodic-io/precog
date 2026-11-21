@limits
Feature: A tenant's appends are held to its rate, and no one else's
  A token may carry an append rate. Over it, the tenant's appends wait their turn; past what an
  append may wait they are refused before anything is stored, with an answer the client
  retries. Nothing acknowledged is lost, and a writer without a limit is not held.

  Scenario: A tenant over its rate is slowed, then refused and retried, and loses nothing
    Given a 3-node cluster with 1 MiB segments, where tenant "acme" holds a token for "acme/" at 40 KiB a second and peers have their own token
    When the tenant "acme" appends 80 batches of 1000 bytes to stream "acme/session-1" through node 0, one at a time
    Then that took at least 800 ms
    And node 0's metrics count appends under "acme/" that waited for its rate
    When the tenant "acme" appends 150 batches of 1000 bytes to stream "acme/session-2" through node 0, all at once
    Then node 0's metrics count appends under "acme/" that were refused for its rate
    And the tenant "acme" reads 80 batches of stream "acme/session-1" from every node
    And the tenant "acme" reads 150 batches of stream "acme/session-2" from every node

  Scenario: A writer without a limit is not held while a tenant is over its rate
    Given a 3-node cluster with 1 MiB segments, where tenant "acme" holds a token for "acme/" at 40 KiB a second and peers have their own token
    When the tenant "acme" appends 150 batches of 1000 bytes to stream "acme/session-1" through node 0, all at once
    And a writer appends 100 batches of 1000 bytes to stream "other/session-9" with 2 copies
    Then node 0's metrics count appends under "*" that neither waited for nor were refused for a rate
    And a cursor reads every batch of stream "other/session-9" from every node
