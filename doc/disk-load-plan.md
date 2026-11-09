# Disk load: findings and plan

A review of what a member and a gateway host write to disk, how a slow write reaches a client, and the
changes that would reduce both. Nothing here is implemented. Step 1 is unconfirmed on a running cluster and
decides how much step 2 is worth.

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

`ec2-tests.md` ("Archive writes on EBS") and `ops.md` ("Term lengths") say a blocked recorder `write()` holds
back the queue's publication limit, because the recording is the queue's one tethered subscriber. The
measurements do not fit that:

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

This is read from Aeron's source and the existing measurements. It has not been observed directly.

## 3. Plan

### Step 1: confirm the mechanism

During a run that stalls (EBS, default writeback, 50k msg/s), sample with `CounterSampler`:

- `seqeron.sequencer.queueBackPressureAlerts`, and the queue's publisher limit minus its position;
- the cluster commit position against the Raft log's `RecordingPos`.

| observation during a stall | conclusion |
| --- | --- |
| alerts stay 0, the queue has window left, commit stops with the log's `RecordingPos` | the log's recording is the path; correct `ec2-tests.md` and `ops.md` |
| the queue's limit is reached, alerts rise | the tether is the path, as documented |

### Step 2: stop writing the queue to the durable volume

Two ways, 2a preferred. 2b is the smaller change and is superseded by 2a.

#### 2a: no queue recording; the Replayer derives history from the log

`Sequencer` has no Aeron dependency and every input it takes is a log entry: session messages, session
closes, timer events, leadership terms and snapshot actions, each with its timestamp. A second instance fed
the Raft log's recording produces the frames the member's own did, by the determinism that already makes
every member's queue the same. The log is then the one copy on disk, whatever the number of groups.

- **Live path.** `SequencerService` still publishes each queue for co-located consumers, and nothing records
  it. With no tethered subscriber `emit` cannot back-pressure, so the recording-liveness and stall fatals in
  `QueuePublisher` go. An offer to a queue with no subscriber returns `NOT_CONNECTED` and is dropped, since
  the frame is in the log.
- **Replay.** The Replayer replays the log's recording, bounded by the commit position (the recording can
  hold entries a new leader truncates), decodes Aeron's cluster log codecs, runs a `Sequencer` over them and
  publishes the requested group's frames on the replay stream.
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
  so the restart-safe cut becomes the log's trim point, which is step 4.
- **Restart.** A member no longer rewrites its whole queue history while it replays the log.

Costs and open questions:

- The Replayer decodes five of Aeron's cluster log templates (`SessionMessageHeader`, `TimerEvent`,
  `SessionCloseEvent`, `NewLeadershipTermEvent`, `ClusterActionRequest`) and reads the commit position
  counter. The codecs are public, in `io.aeron.cluster.codecs`, at the version `versions.properties` pins.
- A group's replay reads every group's log entries and runs the sequencer over them: more CPU per replayed
  byte than an archive replay, off the live path. To be measured against `ReplayColdStartTest`.
- Every restart-safety and purge rule in `snapshot.md` §10 that names a queue recording has to be restated
  against the log.
- Whether a derived replay keeps position addressing or moves to `groupSeqNo`.

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
length below Aeron's 128 MiB default so a purge follows the cut closely. If step 1 shows the log's recording
is the path, this halves the volume written and shortens the bursts, and step 3 is still what removes the stall.

### Step 3: keep the log's own writes from blocking

- `vm.dirty_expire_centisecs=50` and `vm.dirty_writeback_centisecs=20` are the one setting measured clean on
  EBS: 0 of 15 runs stalled. They are a recommendation in `ec2-tests.md` today. Warn at start when a member's
  values are the kernel defaults, as `NodeDriver.warnUnlessTmpfs` does for the Aeron directory.
- Local NVMe for the log is untested, and belongs to the deferred metal runs.

### Step 4: trim the log automatically

Run what `clusterctl trim-log` does once a cut is restart-safe. This bounds space and recovery time, not the
write rate.

### Step 5: keep `groupMask` narrow

Each extra bit is one more copy of the frame on every member and gateway host. If wide masks become common,
one queue with filtering in the client writes less.

## 4. Considered and rejected

| change | why not |
| --- | --- |
| `aeron.archive.file.sync.level=1` | measured: about 6.5 ms added at the median (`ec2-tests.md`, test 3) |
| `tryClaim` in `QueuePublisher.emit` | saves a memory copy, no I/O, and an aborted claim moves this member's positions off its peers' |
| recording without checksums | one copy and a CRC on the recorder thread, no I/O |