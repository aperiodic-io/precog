@metrics
Feature: A node's metrics and its readers' positions do not grow with the number of sessions
  With a stream per agent session, a series per reader group and stream would grow for as long
  as the node runs, and so would a position per group and stream. Readers of a class's streams
  are counted under the class, each tenant's traffic under its token's prefixes, and a position
  expires with its stream's age limit.

  Scenario: Readers of a class's streams are one series, however many sessions there are
    Given a 3-node cluster with 1 MiB segments, where stream "acme/*" retains 2 MiB
    When 5 sessions of 20 batches are written under "acme/" and a reader group commits a position in each through node 0
    Then node 0's metrics count 5 positions under class "acme/*"
    And the number of series in node 0's metrics is noted
    When 20 more sessions of 20 batches are written under "acme/" and a reader group commits a position in each through node 0
    Then node 0's metrics count 25 positions under class "acme/*"
    And node 0's metrics have as many series as noted

  Scenario: A tenant's traffic is counted under its prefix, and no token is in the metrics
    Given a 3-node cluster with 1 MiB segments, where tenant "acme" holds a token for "acme/" and peers have their own token
    When the tenant "acme" appends 10 batches of 500 bytes to stream "acme/session-1" with 2 copies
    Then node 0's metrics count the tenant's appends under "acme/" and name no token
    And node 1's metrics count the tenant's appends under "acme/" and name no token

  Scenario: A reader's position expires with its stream's age limit, on every node
    Given a 3-node cluster with 1 MiB segments, where stream "acme/*" keeps 5 seconds
    When 3 sessions of 5 batches are written under "acme/" and a reader group commits a position in each through node 1
    Then node 1's metrics count 3 positions under class "acme/*"
    And within 20 seconds no node holds a position of the reader group in a stream under "acme/"
    When node 1 is killed and restarted
    Then within 20 seconds no node holds a position of the reader group in a stream under "acme/"
