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

# Second pass: the rest of the tier

Reviewed 2026-11-13, at `dc830fa9` on `derivation-start-fatal`.

**Scope.** Read: `Sequencer`, `ReplayerService`, `AeronReplayer`, `CutLookup`, `RestartSafety`, `RestartSafeLog`,
`ReplaySlotAllocator`, `QueueResolver`, `LogRelay`, `DerivationAnnouncement`, `LogTrimPlan`, `SegmentPurge`,
`NodeDriver`, `FramePredictor`, `SnapshotChoice`, `QueueStart`, `QueuePositions`, `LogVersion`, the `proxy` package,
`tools` and `metrics`. With the first pass, the whole tier. Not run: no finding has a test that reproduces it.

**Verdict.** The state machine is sound: `Sequencer` validates every condition before it changes state, iterates only
lists and sorted keys, and checks its snapshot's counts on restore. Three defects follow from inputs the code does not
expect: a control reply with no reader stalls the Replayer for a second (9), the topology admits the gateway id the
sequencer uses as "none" (10), and a torn line in `restart-safe.log` swallows the next one (11). One misclassified
error fences a client (12). The documents describe `clusterctl` and the stall wording as they were before derived
queues (13). The rest is small. All nine are fixed.

## Defects

**9. A control reply nobody reads stalls the Replayer for about a second.** Fixed: `NOT_CONNECTED` drops the reply at
once, and `ReplayerServiceTest`'s case for it counts one offer where it found 1,001. As found: `ReplayerService.offerControl`
(`replayer/server/ReplayerService.java:849`) idles up to 1,000 times before it drops a reply, `NOT_CONNECTED` included.
The duty cycle's idle strategy is `backoff` by default, which parks up to 1 ms per idle, so a reply offered with no
subscriber on stream 203 holds the duty cycle for about a second. `openRound` offers a `SnapshotRound` at every cut, so
on a member or a gateway host with no client attached each round stops the derivation, every replay and the trim for
that long. Nobody can read a reply while nothing is connected, and every requester resends: `NOT_CONNECTED` should
drop it at once.

**10. The topology admits gateway id -1, which the sequencer reads as "no gateway".** Fixed in the sequencer rather
than the XSD, since spec §7.1 types `gatewayId` as any int32: `promotionTarget` returns the row and
`takeOverdueActivation` the pending activation, null for none, and `NO_GATEWAY_ID` is gone; the row's key masks the
id. Two `SequencerTest` cases, a standby and a primary with id -1, fail on the code as found. As found: `topology.xsd` types a gateway's
`id` as `xs:int`, where `SourceId` has `minInclusive 0`. `Sequencer.NO_GATEWAY_ID` is -1, so an instance with id -1 is
never chosen by `promotionTarget`, and `takeOverdueActivation` drops its overdue activation without handing the role on:
its pair fails over to nobody. A negative id also sign-extends in the topology row's key (`Sequencer.keepTopologyRow`
and `topologyKey`), where the application row's key masks it. The XSD should restrict the id as it does `SourceId`,
and the key should mask it.

**11. A torn line in `restart-safe.log` swallows the next one.** Fixed: `open` truncates the file to its last newline
before it appends; `RestartSafeLogTest` appends after a torn line and reads both back. As found: `RestartSafeLog.read` ignores what follows the last
newline as a write a crash cut short, but `open` appends after it without ending it, so the next line joins the fragment
and is ignored too (`replayer/server/RestartSafeLog.java:48`). A lost `safe` costs a longer replay. A lost `cut` makes
the snapshot action at that position look live when the log replays it, and it is handed to the Replayer as a new round.
`open` should truncate the file to its last newline.

**12. On a member, a replay the driver cannot start fences its client.** Fixed: a resume is refused only on an
`IllegalArgumentException`, a position the `Replayer` does not hold, or on an `ArchiveException` while the archive still
answers; anything else is held. `DerivingReplayer` refuses a position off a frame boundary itself, which the driver
would otherwise reject as a failure to add the publication. `ReplayerServiceTest` holds a driver's failure and refuses
the other two. As found: `ReplayerService.startReplayForClient` answers
`NO_REPLAY_NEEDED`, which a client treats as fatal, when the replay throws and `archiveAnswers` says the archive still
answers; otherwise it answers `ReplayPending` and the client resends. On a member `DerivingReplayer.listQueueRecordings`
reads memory and never throws, so any failure of `startReplay`, such as a driver that cannot add the replay
publication, is taken for a bad request. Only a position the queue does not hold should be refused there.

## Documents

**13. The documents describe `clusterctl` and the stall as they were before derived queues.** Fixed, the code
taken as right: each place below now says what it does, and so do `SeqeronCounters`' doc comments for the two
counters, Java and C#.
- `snapshot.md` §10.2 ("when") says that with a policy `clusterctl snapshot` is allowed and `clusterctl shutdown` uses
  `SHUTDOWN`. `ClusterCtl` refuses `snapshot` and always uses `ABORT`.
- `clusterctl.md`, "Why `ABORT` and not `SHUTDOWN`", says the cluster never snapshots and that replay keeps every
  member's recording complete, and after it that every member's archive holds the same queues. A member records no
  queue, and with a policy the cluster snapshots.
- `NodeDriver`'s Javadoc (`EXIT_FATAL`, `DEFAULT_UNTETHERED_TIMEOUT_NS`, `DEFAULT_QUEUE_STALL_TIMEOUT_MS`,
  `warnIfTmpfs`) and `MetricsExporter`'s help for `seqeron_replayer_ready` and `seqeron_replayer_throttled_requests_total`
  speak of the queue's recording, as `QueuePublisher` does. On a member the throttle cannot fire:
  `DerivingReplayer.queueBacklog` is always null.

## Minor

**14. Dead code.** Fixed. `ClientProxy.connectionOrLive()` had no caller. `ReplyAddresses.of` has `@return@return`.

**15. Two tools exit 0 having done nothing.** Fixed: `clusterctl snapshot` exits 2, a usage error, and
`scanAndDumpLog` returns whether it dumped a recording or derived a queue; `ClusterToolsTest` and `SbeLogPrinterTest`
hold them to it. As found: `clusterctl snapshot` prints that it is not supported and exits 0.
`SbeLogPrinter.scanAndDumpLog` returns true when `--queue` finds no recording and no log, or the catalog holds no valid
recording, so the printer exits 0.

**16. `Hosts.close` can miss a host being rebuilt.** Fixed with an `AtomicReferenceArray`; no test, since the race
needs a rebuild and a close to interleave. As found: `current` is a plain array a host's thread writes when it rebuilds
its `ClientProxy`; `close` may read the one before, stop it, and leave the new one running until the join gives up after
5 s and the shared Aeron client closes under it. An `AtomicReferenceArray` closes the gap.

**17. Two consumer counters can break a scrape.** Fixed: the exporter groups samples by metric name, the first
counter's help and type heading them; `MetricsExporterTest` exports two type ids under one name. As found:
`MetricsExporter.appMetricMeta` names a consumer counter from its
label, and keys the metric on name, help and type, whose help holds the type id. Two type ids whose labels give one name
write two `HELP` and `TYPE` blocks for it, and Prometheus rejects the whole scrape.

## Checked and sound

- **Determinism.** `Sequencer` walks `gatewayRows` and `pendingActivations` by index and snapshots the hash maps in
  sorted key order. A hand-over removes the pending activation before it re-arms one, so the loop in
  `clusterHeartbeat` cannot see its own promotion.
- **A derived replay's slot.** `AeronReplayer.replayProgress` finds a replay by the publisher-limit counter on stream
  201 with its session id, which a `DerivingReplayer` publication has too, so a derived replay that ends or stalls frees
  its slot.
- **Restart safety.** A confirmation for a round the Replayer has not yet seen cut is kept for it, and a source that
  joins under a second client leaves the round unsafe.
- **The ledger.** `CommandLedger.resolved` never passes a `Publish` whose drop it has not reported.
- **A partial announcement.** `DerivationAnnouncement.decode` returns the last announcement the recording holds whole.
