# Service review: derived queues as implemented

Reviewed 2026-11-13, at `95191c23` on `main` (v0.18.0). Paths are under
`seqeron-service/src/main/java/org/limitless/seqeron/`.

**Scope.** Read: `SequencerService`, `QueuePublisher`, `DerivingReplayer`, `RecordingReader`, `LogTrimmer`,
`AeronLogRelay`, and the wiring in `ReplayerServer` and `SequencerServer`: the half of the service tier that
`disk-load-plan.md`, step 2a, changed. Not read: `Sequencer`, `ReplayerService`, `ClientProxy`, `tools` and `metrics`,
about 7,000 of the tier's 13,600 lines. Not run: no test was executed, and no finding has a test that reproduces it.

**Verdict.** The derivation, the log relay and the trim hold together, and the three interactions most likely to lose
history were checked and hold ("Checked and sound"). Two defects are in how a failure is handled rather than in the
path itself: a member whose derivation cannot start stays up serving no history (1), and the fatal lines an operator
reads name the archive where the cause is the driver (2). The rest are a silent stall on a damaged segment, one
question about Aeron left open, and four simplifications. 1, 2, 3, 5, 6, 7 and 8 are fixed, and 4 is settled: the cluster sequences on.

## Defects

**1. A member whose derivation cannot start stays up and never serves history.** Fixed: a start the log's recording
does not hold, or a follower that cannot be built, throws out of the duty cycle, which exits the node with 70, and
`followerDecided` and its `Long.MAX_VALUE` floor are gone. The announcement publication is created with the follower
and its recording is asked for again every second until the archive takes it, logged once per refusal.
`DerivationStartTest` covers both, and fails on the code as found. As found:
`DerivingReplayer.startFollower` (`replayer/server/DerivingReplayer.java:252-272`) sets `followerDecided` before it
knows the start worked, and both failures are logged at info and never retried:

- **The log's recording starts after the service's start, or the `Follower` constructor throws.** No queue is ever
  listed, so the Replayer is never ready. `firstCheckpointLogPosition()` then answers `Long.MAX_VALUE`, so the trim
  runs with no floor from the derivation.
- **`recordAnnouncements()` throws after `follower` is assigned**, as on an archive call that times out. The line says
  "replays are not derived", but they are. The announcement publication stays null, so no gateway host can start on
  this member.

A member has no recorded path to fall back on, so the first should be a fatal (exit 70) like the other conditions
under which a node cannot serve its history, and the second should retry the announcement rather than latch.
`seqeron_replayer_ready` at 0 is the only sign today.

**2. The fatal lines of a queue failure name the archive.** Fixed: they name the service's subscriber and the media
driver, and end "so this node cannot publish its queues"; `PausedSubscriberTest`, which counted a fatal line no
version of the code wrote, counts the stall's. The event code stays `QueueRecordingFailure`, which a gateway host's
relay logs too. As found: `QueuePublisher` watches the service's own subscriber to
each queue, but its messages are the recorded path's (`sequencer/QueuePublisher.java:148,155,290,313`):

- "the local archive did not start recording group N's queue within 5000ms"
- "group N's queue recording made no progress for …ms of continuous back-pressure"
- "…, so it can no longer record the history it is responsible for"

The cause is now the media driver: a subscriber that did not connect, or one the driver did not drop. An operator
reading these inspects the disk. `seqeron-service/CLAUDE.md` notes the wording as a naming matter; these are the lines
read during an incident. `AeronLogRelay` logs under `CoreComponent.QueueRelay` and `QueueRecordingFailure` the same
way, where a gateway host's recording is at least what is meant.

**3. A damaged log segment stalls the derivation without a line.** Fixed: below the limit, a frame length shorter
than a header or a segment file that ends short throws, as a missing file does, which exits the member with 70 from
the follower and closes a replay. Aeron bounds the commit position by the member's own log recording on leader and
follower alike, so the bytes below it are written. `RecordingReaderTest` covers both, and fails on the code as found,
whose test held a file cut short to giving what it holds. As found: `RecordingReader.poll`
(`replayer/server/RecordingReader.java:97`) breaks on a frame length below the header's and is polled again for
ever. Below the commit position that length is a corrupt or truncated segment. The follower stops advancing, replays
stop short of the tip, and at start the member never becomes ready. A missing segment file already throws; this
should too.

## Open question

**4. A cluster snapshot with no `<snapshots>` policy.** Settled: the cluster records no snapshot and sequences on.
Aeron 1.53.2's `ClusteredServiceAgent.executeAction` catches the throw, acks with no recording id and rethrows; the
rethrow reaches `Image.boundedControlledPoll`'s error handler, which has already passed the action. The consensus
module records the null id as "failed to take snapshot" in its error log, appends no snapshot to `recording.log`,
returns to `ACTIVE` and resets the toggle. `ClusterSnapshotTest.aSnapshotToggledWithNoPolicyIsRefusedAndTheClusterSequencesOn`
toggles one on a cluster with no policy and holds it to that, and to sequencing a producer's frames after.
`fault-tolerance.md` §1 says so. As asked: `SequencerService.onTakeSnapshot`
(`sequencer/SequencerService.java:388`) throws while no policy is loaded. `clusterctl` refuses `snapshot`, but
Aeron's `ClusterTool snapshot` and the control toggle reach the same callback on every member at once. If Aeron
Cluster 1.53.2 then waits for the service's ack, the consensus module stays in `SNAPSHOT` and sequences nothing.
Aeron's handling was not read. A test that toggles a snapshot on a cluster with no policy settles it.

## Simplifications

**5. `QueuePublisher`'s stall detection is one timer written as two.** Fixed: the stall clock anchors at the first
evaluation of an offer's back-pressure and runs until the offer lands; `Actions.recordedPosition` is gone, with the
two `QueuePublisherTest` cases that drove a position the real subscriber cannot move, and `ops.md` and
`fault-tolerance.md` §1.3 describe one offer back-pressured for the timeout. As found: `SequencerService.offerFrame`
(`sequencer/SequencerService.java:537-546`) reads the service's subscriber up to the publication's position after
every offer that lands. While an offer is back-pressured that position cannot move, so the "slow but advancing"
branch (`sequencer/QueuePublisher.java:282`) is unreachable, and the rule is: one offer back-pressured for the stall
timeout is fatal. `recordedPosition`, `stallRecordedPosition` and `Actions.recordedPosition` can go. `ops.md`, "A node
that terminates itself", and `fault-tolerance.md` §1.3 still describe a queue that is slow but advancing.

**6. A scan's stall wait cannot end early.** Fixed: a poll that reads no entry ends the scan `STALLED` at once, and
`SCAN_STALL_TIMEOUT_MS` is gone. As found: `DerivedScan.run` (`replayer/server/DerivingReplayer.java:859-874`) yields
for up to 2 s when a poll reads no entry. Its reader is bounded by `follower.entryEnd`, which only `Follower.doWork`
advances, on the thread the scan holds. The wait only delays every other replay on the member; returning when a poll
comes back empty is equivalent.

**7. `DerivingReplayer` is 970 lines, four inner classes, and has integration tests alone.** Fixed: `LogFollower`
holds the checkpoints and the runs, with `Checkpoint`, `Run` and `Frame`, and reads the commit position through a
`LongSupplier`. `LogFollowerTest` writes a Raft log's segment files and covers the runs, the commit bound, the
checkpoint spacing, thinning, purges and the runs' bound. As found: `Follower` keeps the
checkpoints and the runs, and decides what a purge releases and what thinning drops, which is what the trim may
delete below. It reads one counter and the segment files and is otherwise free of Aeron. As a class of its own, like
`LogRelay` and `QueuePublisher`, those rules have unit tests.

**8. Recording lookups are repeated, as `long[]`.** Fixed: `ArchiveRecording` is the record, with `ofStream`,
`activeOf` and `of`, and the five callers use it. The member's active log recording is now the newest active one by
id, as the relay's was, where it was the last listed. As found: "The active recording of a stream" is written three times
(`DerivingReplayer.activeLogRecording`, `AeronLogRelay.activeRecording`, `LogTrimmer.trimLogRecording`) and returned
as two or four longs by position, and "purge this stream's earlier recordings" twice
(`DerivingReplayer.recordAnnouncements`, `AeronLogRelay.openQueue`). One helper and a record replace the five.

## Checked and sound

- **The trim against readers the archive does not know.** `RecordingReader` reads segment files directly, so the
  archive does not refuse a purge under it. A replay whose segment is purged gets an I/O error and closes, and its
  client asks again; the follower reads at or above the first checkpoint, which caps the trim.
- **A relay's failover.** It resumes at the end of the last whole log entry it applied, and each connect has a
  fragment assembler of its own, so a half-read entry is not carried over.
- **A stale announcement.** A host that reads an announcement below the member's trimmed log is answered
  `NOT_SUITABLE`, leaves that member and tries again.
