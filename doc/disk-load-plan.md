# Disk load: findings and plan

A review of what a member and a gateway host write to disk, how a slow write reaches a client, and the
changes that would reduce both. Step 2a exists as a prototype, off by default, step 3's warning
and step 4 are in, and nothing else here is implemented. Step 1 is confirmed, and bounds what step 2 is worth:
it reduces the bytes written and leaves the stall to step 3.

## 1. What is written

- **Every message is written at least twice per member.** The Raft log and every queue recording are in the
  one archive directory (`<baseDir>/archive-<memberId>`) and are written by the archive's one recorder thread.
- **Groups multiply it.** An application frame is recorded once per group its `groupMask` names, and every
  system frame once per active group.
- **A gateway host writes each relayed queue again**, to its own archive.
- **A queue recording is derived data.** It is a deterministic function of the log, and a member records it
  afresh at every start, from `groupSeqNo` 1 or from the cut it restored at. It never has to survive a
  restart, yet it shares the log's volume and the kernel's writeback bursts.

At 50k msg/s with an 18-byte payload the archive grows by 190 MB per 30 s (`ec2-tests.md`, test 1), about
23 GB an hour, log and queue together.

## 2. How a slow write reaches a client

A blocked recorder `write()` reaches a client through the Raft log's recording, not through the queue's
publication window, although the recording is the queue's one tethered subscriber:

- The queue's window is 8 MiB at the 16 MiB default term. A 37–70 ms recorder stall at 50k msg/s is a few
  hundred KiB, well inside it, so `QueuePublisher.emit` should not back-pressure.
- Aeron Cluster 1.53.2 bounds the leader's commit position by the leader's own log recording:
  `ConsensusModuleAgent.quorumPositionBoundedByLeaderLog` returns `min(quorumPosition, leaderAppendPosition)`.
- A follower's service polls its log up to its own append position (`ConsensusModuleAgent`, the follower log
  poll and `catchupPoll`).

So a recorder blocked in `pwrite64` on any file, a queue segment included, stops the log's recording
position, and commits stop for as long. That matches the trace: a 37.5 ms recorder wait inside a run whose
maximum latency was 37.6 ms. Two consequences:

- A quorum does not hide a slow disk on the leader.
- A slow disk on a follower does not delay commits, and delays that member's own consumers.

## 3. Plan

### Step 1: confirm the mechanism

Confirmed (`ec2-tests.md`, "Archive writes on EBS"). During a stall on EBS with default writeback at 49k
msg/s, sampled with `CounterSampler`:

- `seqeron.sequencer.queueBackPressureAlerts` stays 0 and the queue keeps at least 7.0 MiB of its window;
- the leader's commit position stops at the Raft log's `RecordingPos` while the log publication runs ahead.

### Step 2: stop writing the queue to the durable volume

Two ways, 2a preferred. 2b is the smaller change and is superseded by 2a.

#### 2a: no queue recording; the Replayer derives history from the log

`Sequencer` has no Aeron dependency and every input it takes is a log entry: session messages, session
closes, timer events, leadership terms and snapshot actions, each with its timestamp. A second instance fed
the Raft log's recording produces the frames the member's own did, by the determinism that already makes
every member's queue the same. The log is then the one copy on disk, whatever the number of groups.

- **Live path.** `SequencerService` still publishes each queue for co-located consumers, and nothing records
  it. The service keeps a subscriber of its own on each queue and moves it past every frame it offers, so the
  queue is always connected and every offer lands: a dropped offer would leave the publication behind the
  positions the log gives, which a client joins the live queue by. Only an untethered consumer can then hold
  the window, until the driver evicts it.
- **Replay.** The Replayer reads the log's recording up to the commit position (the recording can
  hold entries a new leader truncates), decodes Aeron's cluster log codecs, derives the frames and
  publishes the requested group's on the replay stream.
- **Starting mid-history.** A resume needs the sequencer's state at the point it starts from. The Replayer
  follows the log with a `Sequencer` of its own and keeps a checkpoint every so many bytes: log position,
  the state `Sequencer.onSnapshot` already serializes, and each queue's position. A request restores the
  newest checkpoint at or below it and discards frames up to the one asked for. A cold start with a
  `<snapshots>` policy starts from the cluster snapshot at the cut; without one, from the log's start.
- **Positions.** Clients name a replay's start, its catch-up point and a snapshot file's cut by queue
  position, and check the first frame against it. A queue position is a function of the frame lengths before
  it at a given term length and MTU, so a checkpoint can carry it and the replay publication can start at it
  (`initialPosition`), which keeps the replay protocol and the three clients unchanged. Addressing by
  `groupSeqNo` instead is cleaner and changes all three.
- **Gateway host.** The relay takes a bounded replay of a member's log (`startBoundedReplay` on the commit
  position counter) and the host derives its queues as a member's Replayer does. Members' log positions
  agree by construction, so failing over to another member needs no term-length or MTU condition.
- **Retention.** Queue purging goes. The log must be kept back to the oldest cut a client may restore from,
  so the restart-safe cut becomes the log's trim point, which is step 4. The prototype restates
  `snapshot.md` §10 against the log ("Retention" below).
- **Restart.** A member no longer rewrites its whole queue history while it replays the log.

Costs and open questions:

- The Replayer decodes five of Aeron's cluster log templates (`SessionMessageHeader`, `TimerEvent`,
  `SessionCloseEvent`, `NewLeadershipTermEvent`, `ClusterActionRequest`) and reads the commit position
  counter. The codecs are public, in `io.aeron.cluster.codecs`, at the version `versions.properties` pins.
- A group's replay reads every group's log entries and builds each of its frames: more CPU per replayed
  byte than an archive replay, off the live path. The prototype walks a queue at a third to a half of the
  recorded path's rate, which a snapshot start shortens with the log it has to read.
- Whether a derived replay keeps position addressing or moves to `groupSeqNo`. The prototype keeps positions.

Prototype. With `replayer.deriveFromLog`, a member records no queue and its Replayer is a `DerivingReplayer`,
which is to `ReplayerService` what `AeronReplayer` is over recordings: each queue is one recording, named by
its group, that starts where the queue did and has reached where the derivation has. There is no recorded
path to fall back on. A gateway host takes the same flag, and then relays a member's log where it relayed
its queue recordings.

- **Live path.** As above. `QueuePublisher` is unchanged: its liveness check and stall bound read the
  service's own subscriber where they read the recording.
- **Reading the log.** `RecordingReader` reads the log's recording from the archive's segment files, as far as
  the commit position: a member's commit position never passes what its own archive has written, and nothing
  below it is truncated. No archive replay is involved, so an election that stops the log's recording ends
  nothing, and a replay takes none of the archive's sessions. It reads with positional reads, since the archive
  shrinks a segment file when it truncates the log.
- **Derivation.** `LogDeriver` runs a `Sequencer` over the log's entries and `QueuePositions` gives each frame
  its queue position. One derivation follows the commit position and keeps a checkpoint in
  memory every 8 MiB of log.
- **Runs.** The frames of an application message, of a system message and of a heartbeat follow from the log
  entry, the `clusterSeqNo`, each queue's `groupSeqNo` and the active groups, with no other sequencer state.
  `FramePredictor` builds them that way, for every group at once, and the derivation compares its frames with
  the sequencer's for every entry. Where they differ (a rejected message, a system message the sequencer
  synthesizes a frame behind, a session's close, a new term, a snapshot) it keeps a run: the entry's frames as
  the sequencer published them, and the counts after it. It keeps one at each checkpoint too, with no frames.
  The runs are memory, released by a purge as the checkpoints are.
- **Run bound.** The runs hold at most 64 MiB, counting 512 bytes a run besides its frames. Past that, the
  first entry the predictor does not derive after a checkpoint starts a run that holds the sequencer's state,
  no run is kept behind it up to the next checkpoint, and a replay runs a `LogDeriver` over that stretch. A
  flood of such entries then costs replay rate and one state per checkpoint, not memory per entry.
- **Checkpoint bound.** At most 1024 are kept. Past that every other one is released and the log between two
  doubles: 16 MiB once the log passes 8 GiB, 32 MiB past 16 GiB. A replay still starts at a run, and the run
  kept at a released checkpoint stays, so its first frame is no further off. What grows is the stretch a run
  past the run bound covers, and how far below a purged start a gateway host's derivation begins.
- **Start.** The member's service hands its Replayer a `DerivationStart` when it starts: log position 0 and
  nothing sequenced, or the cluster snapshot's log position and the state it restored, with the position it
  created each queue at and the node's term length and MTU.
- **Replay.** A replay runs no sequencer but over a stretch past the run bound: from the newest run at or below the requested position it publishes
  each run's kept frames and the predictor's for the log behind them, as far as the derivation has compared.
  The frames from the requested position on are published on a publication created at it, so the replay protocol and the clients are
  unchanged. A position at a fragment boundary inside a frame above the MTU is served as the archive serves it.
- **Tip and scans.** A queue's tip is where the derivation has brought it, and a scan for a snapshot's cut walks
  the runs from the last at or before where it starts.
- **Readiness.** No queue is listed until the derivation has caught up with the commit position as it stood
  when it started, so the Replayer is not ready before then.
- **Purge.** A purge below a restart-safe cut moves the queue's start, a term at a time as a recording's moves
  a segment at a time, and releases the checkpoints and the runs below it.
- **Gateway host.** One `AeronLogRelay` replaces the relay per group: it replays a member's log over UDP,
  bounded by that member's commit position, runs a `LogDeriver` over it and publishes each relayed group's
  frames on the host's own queue. The host still records those queues and serves them as before, so its
  Replayer, its cut tracking and its purge are unchanged, and so is what it writes. Its decisions are the
  pure `LogRelay`.
- **Announcement.** A host cannot read a member's driver, so the member's Replayer publishes the first
  checkpoint it keeps and the id of its commit position counter on a stream its archive records
  (`DerivationAnnouncement`, stream 207): its `DerivationStart` when it starts, and the first checkpoint left
  after each purge. A host reads the last. The recording is active for as long as that run is.
- **Host start and failover.** The derivation starts where the first member the host reads announces, so the
  host's queues begin at `groupSeqNo` 1, at a cut, or at the checkpoint below that member's purged start, and
  the topology rows reach the host's Replayer from the state it restores. On losing its member the relay resumes on the next at the
  end of the last log entry it applied, whatever that member's term length or MTU.
- **Stall.** On EBS with default writeback at 50k msg/s, two 600 s runs with the flag had no interval of 10 ms or
  more without a new `clusterSeqNo`, where the recorded path had 17 of up to 81 ms, and the archive grew half as fast
  (`ec2-tests.md`, "Archive writes on EBS").
- **Rate.** On a laptop, off the page cache, two million frames with a 150-byte filler on one group and one
  million on another walk at 2.2–2.3 and 1.5–1.7 million frames/s derived and about 5 million recorded. With a
  sequencer per replay over an archive replay of the log it was 1.8 and 1.1 million. What is left is reading and
  counting each entry of every group, about 0.11 µs, and offering each frame, about 0.26 µs, where the archive
  copies recorded frames in blocks.
- **Saturation.** Not measured with the flag. The recorded path's ramps (`ec2-results.md` §3.3) found no ceiling:
  stalls of 2–75 ms ended them at any rate from 40k to 900k msg/s, the size the stall runs above found on the recorded
  path. `disk-session.sh` (`ec2-tests.md`, §3) runs `hop-knee.sh` twice with the flag and twice without in one launch,
  which tells whether it removes them and where the ceiling then is; the same launch ramps the flag under the kernel's
  default writeback and on a default gp3 volume, and replays 4,000,000 frames cold off EBS both ways. At 888k msg/s the log alone writes about 200 MB/s, above the volume's provisioned
  125 MB/s, so each ramp needs the recorder's longest write and the dirty pages beside it to tell a ceiling from the
  volume's. `hop-knee.sh` samples the first with `CounterSampler` and the second with `node.sh meminfo-sampler`.
- **Tests.** `DerivedQueueTest`; `DerivedReplayTest`, a client's cold start and live following on a member
  that records nothing; `CheckpointedReplayTest`, which runs a `DerivingReplayer` beside a recording
  member's archive and compares replays fragment for fragment, and scans and tips; and three in
  `ClusterSnapshotTest` on members that record nothing: a gateway restores its snapshot file and catches up, on
  a member that has run throughout, on one restarted from a cluster snapshot, and on one whose queue start a
  purge has moved. `CheckpointedReplayTest` thins and releases checkpoints, at a spacing and a bound it sets,
  compares replays whose runs are past a bound of 0, and fails if a 200 000-frame history keeps 32 runs, which
  it does once `FramePredictor` and the sequencer encode differently. `RecordingReaderTest` reads segment
  files it writes: fragments, padding, a recording started inside a term, a truncated and a missing file.
  For a host: `LogRelayTest`; in `DerivedReplayTest`, a client's cold start on a host and its walk on across
  the stop of the member the host read; and three in `ClusterSnapshotTest`: the markers the host reports, its
  purge below the cuts its gateway confirms, and its start from a member restarted from a cluster snapshot.
  For retention, one more there: a member trims its log past its first segment, a gateway then restores on
  it, and a host starts on it above the trimmed log.

Retention. `snapshot.md` §10 with the flag on, where it differs:

| §10 | recorded | derived |
| --- | --- | --- |
| a queue starts at a cut (§10.1, §10.3) | a recording created at the marker's position; the earlier one purged | no recording: the service hands over a `DerivationStart`, and the queue's positions are the snapshot's at its term length and MTU, else from 0 |
| Replayer ready (§10.5) | each queue recording, the earlier one purged | the derivation caught up with the commit position |
| purge (§10.6) | whole segments below the newest restart-safe cut | the start moves to the term boundary below that cut; nothing is deleted |
| what holds a queue's history | its recording | the log from the first checkpoint kept, which is at or below the queue's start |
| the log kept (§10.7) | from the older of the two newest restart-safe snapshots, for a restart | from the lower of that and the first checkpoint kept, which the member's Replayer tells its trim |
| a host's start (§10.3, §10.6) | the member's recording start | the member's last announcement |
| a host left behind by a purge (§10.6) | fatal | the member is dropped and the next tried, without end |
| a new member (§10.6) | copies no queue | has none to copy |

The restart-safety rules (§10.8), the client rules (§10.6) and the cut lookup (§10.3) are unchanged: they read
the Replayer's listing, and a derived queue is listed as one recording.

Limits of the prototype:

- **A gateway host still records the queues it derives.** Its disk writes are unchanged. Its disk is not on the
  commit path (§2), so the stall is unaffected.
- **The queue's start holds the trim.** A queue's start is the term boundary below the newest restart-safe
  cut, and the trim keeps the log from the checkpoint below it. On a quiet cluster the start moves seldom, so the
  log kept is up to a queue term and the checkpoint spacing more than the two snapshots need. Moving the start
  to the cut itself would bound that, but a client that is down across one round, on a node where no other
  instance joins it, would then find its file's cut below the start and be fenced (`snapshot.md` §10.6), where
  a recording leaves it up to a segment.
- **A host with no suitable member does not stop.** A member is unsuitable when its log no longer holds the
  position the host needs: its announced start for a host's first read, the host's own position after. The
  relay moves to the next member and retries, where a queue relay left behind stops with a fatal.
- **The announcement recording grows** by one announcement, the sequencer's state, per purge, and a host reads
  it whole at each connect. A member's restart purges it.
- **Checkpoints are memory.** Each holds the sequencer's state, and up to 1024 are kept. They are
  rebuilt at each start by deriving the log from the service's start, and no queue is served before that ends.
- **A replay costs more.** It reads every group's entries, at a third to a half of the recorded path's rate,
  and its first frame waits for the log between the run and the position asked for.
- **The predictor repeats the sequencer's encoding.** A change to one and not the other costs speed, not
  correctness: every entry on which they differ becomes a run, served from the frames the sequencer published.
- **No recorded path.** A member with the flag has no queue recording to fall back on, so `clusterctl` and the
  tools that read one (`QueueRecordings`, `SbeLogPrinter` on a queue) find none.
- **Not run with the flag:** the tests tagged `docker`, the C# client on Windows, and the multi-host EC2
  tests. The integration and system suites and `ChaosTest` run with it
  under `-PderiveFromLog`, the system suite's `Hosted*Test` with the C++ stubs when `HOSTED_PROBE` names them, and
  `csharp-client-test.sh` under `CSHARP_MEMBER_OPTS=-Dreplayer.deriveFromLog=true`; CI runs each both ways, less
  the C++ stubs, which it runs neither way. With the flag `QueueRecordings` reads a member's queues twice, as its
  log derives them and as its Replayer replays them, and the checks of a member's queue recording are skipped.

#### 2b: a second archive for queue recordings

Run a second `Archive` in the member's JVM, with its own directory, control stream ids and recorder thread.
`SequencerService` and the Replayer use it; the consensus module keeps the first for the log.

- With a `<snapshots>` policy, its directory goes on tmpfs. The policy's `bytes` bounds it, and a reboot
  loses nothing the log does not rebuild.
- Without a policy it grows without bound, so it goes on a separate volume.
- A gateway host's archive is the same case: its recording is relayed afresh at every start.

Effect: the durable volume takes half the bytes or less, each writeback burst holds the log alone, and a write
to a queue segment can no longer block the log's recorder. On tmpfs, replays are served from memory and stop
competing with the recorder, so `ReplayerService.replayLimit`'s throttle matters much less.

Cost: a second archive's threads and control streams, memory equal to the retained history, and a segment
length below Aeron's 128 MiB default so a purge follows the cut closely. Since the log's recording is the path
(step 1), this halves the volume written and shortens the bursts, and step 3 is still what removes the stall.

### Step 3: keep the log's own writes from blocking

- `vm.dirty_expire_centisecs=50` and `vm.dirty_writeback_centisecs=20` are the one setting measured clean on
  EBS: 0 of 15 runs stalled. A member on Linux warns at start while both are the kernel's defaults
  (`NodeDriver.warnIfDefaultWriteback`).
- Local NVMe for the log is untested, and belongs to the deferred metal runs.

### Step 4: trim the log automatically

Done. A member's Replayer runs the trim each time a snapshot becomes restart-safe (`LogTrimmer`,
`snapshot.md` §10.7), and `clusterctl trim-log` asks it to and prints its answer. This bounds space and
recovery time, not the write rate. With step 2a's flag the trim stops at the log position of the derivation's
first checkpoint, and trims nothing until the derivation has started.

### Step 5: keep `groupMask` narrow

Each extra bit is one more copy of the frame on every member and gateway host. If wide masks become common,
one queue with filtering in the client writes less.

## 4. Considered and rejected

| change | why not |
| --- | --- |
| `aeron.archive.file.sync.level=1` | measured: about 6.5 ms added at the median (`ec2-tests.md`, test 3) |
| `tryClaim` in `QueuePublisher.emit` | saves a memory copy, no I/O, and an aborted claim moves this member's positions off its peers' |
| recording without checksums | one copy and a CRC on the recorder thread, no I/O |