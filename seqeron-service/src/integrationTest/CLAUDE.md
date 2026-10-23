# Integration suite

**The integration suite** (`seqeron-service/src/integrationTest`) launches members in the test's own JVM through
`SequencerServer.launch` and a gateway host through `ReplayerServer.launchGatewayHost`, on Aeron's
`aeron-test-support`. `FailoverConfirmTest` is `failover-test.sh`'s confirm producer across a leader stop;
`FailoverPropertyTest` runs a producer on the gateway host through seeded rounds of member stops and UDP cuts
(`SequencerServer.launch`'s `udpFaults` overload, which picks frames by stream id and direction), and `-Pseed=<n>`
replays a run's draws. Both then check every queue recording on every archive frame for frame (`QueueRecordings`),
which a client's `ReplayerRecovery` would paper over. `CommittedEntryLossTest` scripts the fault sequence under which
Aeron Cluster 1.53.2 loses a committed entry. Its check that the entry survives fails on that Aeron, so only
`-PknownAeronFaults` runs it, against an upgrade; its check that the member that truncated it takes itself down and
rejoins on restart runs by default. `SystemTestWatcher` fails
a test on any error a driver, archive or consensus module logs, less the ones a test names and explains. It runs on
JUnit 6, which `aeron-test-support` is built against, on cluster port base 9500, and in CI's `java integration` job;
neither `test` nor `check` runs it. A member stopped in-process is closed rather than killed, and a cut drops frames
rather than delaying, reordering or duplicating them, so the harnesses stay the evidence for process death and for
the network faults faulteron injects.
