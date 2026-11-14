# Integration suite

**The integration suite** (`seqeron-service/src/integrationTest`) launches members in the test's own JVM through
`SequencerServer.launch` and a gateway host through `ReplayerServer.launchGatewayHost`, on Aeron's
`aeron-test-support`. `FailoverConfirmTest` is `FailoverProcessTest`'s confirm producer across a leader stop;
`FailoverPropertyTest` runs a producer on the gateway host through seeded rounds of member stops and UDP cuts
(`SequencerServer.launch`'s `udpFaults` overload, which picks frames by stream id and direction), and `-Pseed=<n>`
replays a run's draws. Both then check every queue frame for frame (`QueueRecordings`): a member's twice, as its
Raft log derives it and as its Replayer replays it, and a host's recording off its archive, which a client's
`ReplayerRecovery` would paper over. `CommittedEntryLossTest` scripts the fault sequence under which
Aeron Cluster 1.53.2 loses a committed entry. Its check that the entry survives fails on that Aeron, so only
`-PknownAeronFaults` runs it, against an upgrade; its check that the member that truncated it takes itself down and
rejoins on restart runs by default. `ReplaySlotsTest` holds the Replayer's two counter readings to a real driver, which
`ReplayerServiceTest` takes from a fake: a replay keeps its slot until its subscriber lets it go or stops reading it for
the stall timeout, each queue's backlog
is matched to its own recording, and a backlog past a quarter of the window holds a second replay. That backlog is the
gateway host's: `GatedChecksum` holds its archive's recorder, which a member's cannot spare, since it records the Raft
log. `ScanDeadlineTest` drives `AeronReplayer.scan` on a bare driver, since an archive's
replay always connects. `LogTrimmerAllocationTest` holds `LogTrimmer.doWork`, which the Replayer calls on every turn of
its duty cycle, to allocating nothing while idle, measured before C2 compiles it. `PausedWalkTest` stops polling a client mid-walk, under and past its 5 s replay stall
timeout, under a flood that evicts it from the queue, and with its `Replaying` unread past the archive's connect
timeout, and holds it to converging in order without being caught up short of the queue's tip, and a mid-walk
pause to starting no other replay. `ClusterClockStepTest` steps the leader's consensus clock back under a stream, through
`InProcessCluster.stepClock` and `SequencerServer.launch`'s clock overload: 5 s holds the heartbeat 5 s and costs no
term, and 30 s terminates the stepped member at the driver timeout on Aeron 1.53.2. It also fails over onto followers
started with clocks behind the leader's, which costs only the election, since the new leader's heartbeat is due on its
own clock. `ReplayerFaultsTest` makes a member's Replayer fail or stall on cue, through `FaultableReplayer`
(a decorator over `AeronReplayer`, installed by `SequencerServer.launch`'s `replayerWrapper` overload): a control
stream that back-pressures drops its reply at the spin bound, a Replayer that reads no requests answers them once it does,
resends queued behind a slow `startReplay` start one replay, a `startReplay` that throws is counted and answered
`ReplayPending`, a replay whose progress freezes loses its slot only after the stall timeout, and ended replays free theirs
at once. `ReceiverFaultsTest` drops a client's `Replaying` or a run of its replay frames through
`ReplayerStreamReceiver`'s fault hooks, and holds it to a gap-free, in-order walk that is never caught up short of the
tip. A run it drops from every replay is a recording with a hole: the client retries at the resend pace, raises
`recoveryStalled` at the 30 s no-progress timeout, and heals once the hole is covered. `StalledHostReplayerTest` stops a gateway host's Replayer reading requests while a client cold-starts on it, and holds
the client to waiting without a fault, then walking in order once the Replayer runs. In every case no queue goes silent for the queue-stall fence's default, and no frame is lost.
A member records no queue (`doc/plans/disk-load-plan.md`, step 2a), so a test that needs a whole queue to compare a derived
replay with asks `InProcessCluster.withQueueRecordings()`, which has each member's archive record its queues at each
start; a member restarted mid-test records from where its queue has reached, and the test checks that start against
the member's `DerivationStart`. `QueueFrames` reads a member's queue through its Replayer where its archive keeps no
recording. `DerivedQueueTest` derives a member's queues from its Raft log and checks them against those recordings
frame for frame and position for position; `DerivationStartTest` holds a start the log's recording does not hold to
ending the Replayer's duty cycle, and an announcement the archive refuses to being recorded once it may;
`DerivedReplayTest` cold-starts a client on a member, and on a gateway host
while the member the host reads stops, and prints each rate; `CheckpointedReplayTest` runs a `DerivingReplayer` of its
own, started from the member's `DerivationStart`, beside the member's recordings and compares its replay with the
archive's fragment for fragment, from the start, mid-history, inside a fragmented frame, and on a member restarted
from a cluster snapshot, at the same and at another term length, and its scans and its tip with the recording's, and
holds its checkpoints to their bound and to a purge's release, its runs to a count that a `FramePredictor` out of step
with the sequencer exceeds, and its replays to the recording past the run bound. `ClusterSnapshotTest` restores
gateways on a member that has run throughout, on one restarted from a cluster snapshot, and on one whose queue start a
purge has moved, and holds a gateway host to the markers it reports, to its purge, to starting at the cut of a member
restarted from a snapshot, and to stopping once every member trimmed its log past it, which a restart recovers. It
also holds a member to trimming its log past its first segment by itself once its cuts are restart-safe, above what
its queues need, and to starting a host after, and a snapshot toggled with no policy to being refused while the
cluster sequences on.
`SystemTestWatcher` fails
a test on any error a driver, archive or consensus module logs, less the ones a test names and explains. It runs on
JUnit 6, which `aeron-test-support` is built against, on cluster port base 9500, and in CI's `java integration` job;
neither `test` nor `check` runs it. A member stopped in-process is closed rather than killed, and a cut drops frames
rather than delaying, reordering or duplicating them, so the harnesses stay the evidence for process death and for
the network faults faulteron injects.
