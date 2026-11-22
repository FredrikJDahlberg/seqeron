# Service tier

## Aeron Cluster sequencer — `SequencerServer` / `SequencerService` / `Sequencer`
`Sequencer` is the replicated state machine proper — it owns `clusterSeqNo` and every frame encode, has
no Aeron dependency, and is unit-tested directly (`SequencerTest`). `SequencerService`
(`ClusteredService`) is its Aeron adapter: it decides *when* to call the sequencer, holding no replicated state
itself. Every ingress message gets a cluster-wide monotone
`clusterSeqNo` plus the Raft consensus timestamp, then is republished on the **node-local queue** of each group
it reaches (`QUEUE_CHANNEL` = `aeron:ipc?alias=seqeron-queue`, stream `queueStreamId(groupId)` = 2000 + `groupId`),
every system frame on every queue, an application frame on the queue of each group its `groupMask` names,
numbered along the queue by `groupSeqNo`. Nothing records a queue: the node's co-located Aeron Archive records the
Raft log alone, and the Replayer derives each queue's history from it (below). The first topology load
activates the groups and opens the queues, and the sequencer refuses everything before it. Every frame on a
queue is sequenced: it carries `Sequenced` envelopes and nothing else, and the member's queue agent is its only
publisher.

**Every node publishes its own queues** — leader and follower alike, from its own committed log. All nodes process
the same committed log in the same order and keep identical sequencing state (so a new leader resumes
exactly where the last one left off), which gives every node the same frames in the same order on its queues
(queue positions match only at equal IPC MTU and term length), from `groupSeqNo` 1 or from the cut a restarted
member restored at (below), with no cross-node replication.

**The service publishes no queue** (`doc/records/plans/queue-off-duty-cycle-plan.md`, steps 1 and 2): its duty cycle keeps
each queue's position by `QueuePositions`, which is where a snapshot's marker starts, and never waits on a queue's
subscribers. A `QueueAgent`, on a thread of the member's `ReplayerServer`, reads the log's recording from the
archive's segment files up to the commit position, runs a `LogDeriver` over it from the service's `DerivationStart`,
and publishes each queue. A queue publication is created at its first frame and never re-created on a leadership
change (`aeron:ipc` has no port to collide on), so a node's queue is one continuous run spanning every leader tenure.
The agent holds a subscriber of its own on each queue and reads it past every frame it offers, so every offer lands
with no application subscribed, and a frame that lands anywhere but where `QueuePositions` puts it is fatal.

Consumers split live from history: co-located apps subscribe to the queue **directly** for the live feed
(untethered, so a slow app is dropped and heals via replay rather than back-pressuring), while the
co-located `ReplayerService` serves cold-start/gap replay derived from the log. The agent never spins: a frame a
queue does not take is kept, and no log entry is read while one is, so a back-pressured queue holds the agent and
never the service, and only a subscriber its driver has not dropped can hold it. `QueueEmitter` — those decisions,
split off the way `Sequencer` is and unit-tested the same way — **skips** a queue back-pressured for the stall timeout
(doc/records/plans/queue-off-duty-cycle-plan.md, step 3): the agent logs the subscribers at the queue's limit, revokes the
publication (`ExclusivePublication.revoke`, since a close waits for the held subscriber to drain), derives that queue's
frames up to the commit position without offering them, and reopens it there under a new session; consumers heal by
replay, and `QueueSkipTest` holds a tethered subscriber that stops reading to that. A member whose queue's own
subscriber does not connect for the stall timeout, or that the driver closed (checked once a second, since a lost
image back-pressures nothing while applications keep the queue connected), or whose agent throws **terminates
itself** (`NodeDriver.EXIT_FATAL` = 70). Peers keep quorum, and the restart derives its queues
again. The service's own wait on the runtime is the cluster-clock timer, which `HeartbeatTimer` bounds the same way.
A node whose consensus module truncates its log below what it applied terminates itself
too, which Aeron Cluster 1.53.2 does to a committed entry (`SequencerServer.truncatesAppliedLog`,
`doc/architecture/fault-tolerance.md` §1.4), and so does one whose cluster snapshot failed before Aeron called `onTakeSnapshot`, which
left it without the round its peers applied (`SequencerService.onServiceError`, §1.5).

Besides forwarded ingress, the sequencer synthesizes its own frames on the same `clusterSeqNo` counter:
`SessionClosed` (one per cluster session close: nothing from that session reaches the log after it),
`LeadershipChanged` (one per term, same leader or not), and a **1 Hz `ClusterHeartbeat`** (`Sequencer.CLUSTER_HEARTBEAT_INTERVAL_MS`) — the cluster
clock, so consumers have a consensus-driven time source that keeps advancing while a producer is silent,
which is exactly when a gateway's keepalive watchdog must probe.

**A cluster snapshot is also an application round** (`doc/architecture/snapshot.md` §10), and exists only while a
`<snapshots>` policy is loaded: without one `onTakeSnapshot` throws, `clusterctl shutdown` uses `ABORT`, and a node's
recovery is full-log replay from `clusterSeqNo` 1. With a policy the `Sequencer` reports a round due (its interval,
its `bytes`, or an operator's `SnapshotRequested`) as replicated state, and the leader's service sets the
`ClusterControl` toggle to `SNAPSHOT`. The cluster's snapshot action then calls `onTakeSnapshot` on every member at
the same log position: `Sequencer.takeSnapshot` publishes the marker `SnapshotStarted` on every queue, and
`Sequencer` (a `SequencerSnapshot`, its own Java-only schema 216) writes its state, which acks. Nothing waits for an
application. **`onStart` restores that state and starts each queue again at the cut** (`QueueStart`: at the marker's
position when the node's IPC term length and MTU are the snapshot's, else at 0), and hands the Replayer a
`DerivationStart` (the snapshot's log position, the restored state and each queue's start), from which the queue
agent re-creates each queue there with the re-emitted marker as its first frame, and the topology rows the log no
longer replays. So a restarted member's queue starts at a cut, not at `groupSeqNo` 1, and a consumer that cannot
restore from a snapshot file cannot follow it.

A cut may be restarted from only once it is *restart-safe*: every co-located application that joined the round
(`SnapshotJoined`, within `replayer.snapshotJoinMs`) has confirmed its file (`SnapshotConfirmed`). The member's
`restart-safe.log` in its cluster directory holds `cut` and `safe` lines, forced to disk; the service writes
`cut` before it acks, which is also how a snapshot action replayed from the log is told from a live one, and the
Replayer writes `safe` (`RestartSafety`). Before the consensus module starts, the launcher invalidates every snapshot at
the head of `recording.log` that the file does not name as safe (`SnapshotChoice`), so an unsafe cut costs a longer
replay and never a fenced application. `LogVersion` is the major of Aeron's `appVersion`.

**Rounds start at least the session timeout plus one heartbeat apart.** The consensus module resets every client
session's inactivity clock at each snapshot, so rounds more often than `sequencer.sessionTimeoutMs` would keep a dead
client's session from ever timing out, and a dead gateway from ever being replaced by its standby. The leader's service
asks for a due round only past that gap (`Sequencer.snapshotDue(timestamp, minGapNs)`), with its own session timeout,
which is the one its consensus module enforces; the first round waits for nothing.

**Application snapshots are local files the log confirms instead** (`doc/architecture/snapshot.md`). The sequencer
synthesizes `SnapshotStarted` as a round's cut; every instance of a participating source serializes its state
there through its façade's `SnapshotListener` into a file in its own directory (`replayer.client.SnapshotStore`),
and the one that may publish submits the round's `SnapshotEnd` — record count, length, CRC — the only snapshot
frame a producer sends. Every instance compares its own serialization with the sequenced end and fences on a
difference (spec §16 A-6, A-7); a match confirms its file, which only then is named `<round>.snapshot` and
replaces every older one but the confirmed round before it. A starting client restores in `ReplayerRecovery` from its
newest confirmed file whose records match its trailer once a replay from the file's cut position opens on that cut —
a file from a purged log fails it — then resumes after the cut; an instance with no such file replays its queue from `groupSeqNo` 1. The Replayer holds no snapshot state. This shortens a client's restart, not a node's.

**A member records no queue** (`doc/records/plans/disk-load-plan.md`, step 2a). Its Replayer is a `DerivingReplayer`, which
derives each queue from the Raft log's recording, read from the archive's segment files (`RecordingReader`) up to
the commit position: a `LogFollower` runs a `LogDeriver` over the log once from the service's `DerivationStart`
and keeps a checkpoint every 8 MiB (its rules unit-tested over segment files `LogFollowerTest` writes), and a
replay rebuilds frames with a `FramePredictor` wherever the two agreed. Each queue is listed as one recording at the queue's positions, so the replay protocol and the clients are those of a
recording. The member's Replayer announces its start on stream 207 (`DerivationAnnouncement`), which its archive
records for gateway hosts, and its `LogTrimmer` keeps the log from the first checkpoint. The plan lists the limits.

## The replay protocol — two sides, two namespaces
**`replayer.server`** is Java only: `ReplayerServer`/`ReplayerService` and their pure seams `Replayer`,
`ReplaySlotAllocator` and the gateway host's `LogRelay`, with
`AeronReplayer`, `DerivingReplayer` and `AeronLogRelay` the parts that touch Aeron. **`replayer.client`** is `ReplayerStreamReceiver` and its pure seam
`ReplayerRecovery`, plus `SequencedEvent` and an instance's snapshot files, `SnapshotStore` — Java, C++ in
`org::limitless::seqeron::replayer::client`, and C# in `Org.Limitless.Seqeron.Replayer.Client`. On a member `ReplayerServer` runs inside `SequencerServer`'s
JVM, on its embedded driver, so a fatal in either exits the node with 70; its own `main` is the gateway
host's alone. The two sides share only the protocol's addresses —
`IPC_CHANNEL`, `REPLAY_STREAM_ID` 201, `REQUEST_STREAM_ID` 202, `CONTROL_STREAM_ID` 203,
`FROM_START` and `NO_REPLAY_NEEDED` — and those are `protocol/ReplayProtocol` in every language.

**A gateway host runs clients with no member on it.** `ReplayerServer` with `replayer.archiveEndpoints`
runs its own media driver and archive, and `AeronLogRelay` replays a member's Raft log over UDP, bounded by
that member's commit position, runs a `LogDeriver` over it, and publishes the queue of each group in
`replayer.groups` onto the host's own, which the host's archive records and its `AeronReplayer` serves. Its
decisions are the pure **`LogRelay`**, unit-tested the way `QueueEmitter` is. The derivation starts where the
first member it reads announces, `groupSeqNo` 1, a cut or the checkpoint below a purged start, into new local
recordings. On losing its member it resumes on the next at the end of the last log entry it applied, since
members' log positions agree by construction; once every member's log starts past that position it stops with a
fatal (`HISTORY_LOST`), and a restart derives again from a member's start. Clients there are unchanged: they attach to the host's
Aeron directory with the host's node id (one no member uses) where they take a member id, and submit over UDP
ingress — an `Application` with `offCluster` set, since no leadership there is its own, which makes it the
one instance whose gate opens once caught up. The member pays one archive replay per gateway host and its sequencer nothing.
The relay reports the snapshot markers it publishes, and the host's Replayer tracks rounds and purges its queue
recordings below the newest restart-safe cut (`doc/architecture/snapshot.md` §10.6). A restarted host purges the earlier recordings of each
relayed queue before its Replayer starts.

## Two kinds of producer, and only one of them is elected
A **gateway** is an edge producer deployed as an **active/hot-standby pair**: it is named in the topology
list, and the cluster picks which instance is live — `GatewayRegistered` (the list row), `GatewayStarted`
(the instance announcing itself), `GatewayActive` (the cluster's designation),
`GatewayActivationRequested` (the operator asking for one). That vocabulary is this tier's own, not any
edge protocol's: `gateway` names a deployment role seqeron defines and runs the election for. A
**co-located application** is the other kind: one replica per node, in the list nowhere, needing no
election because `LeadershipChanged` already picks one — it publishes only while its own node is leader.
Naming those frames `Producer*` would imply a co-located replica could be listed and designated, which it
cannot. Connections are the generic half: `ConnectionOpened`/`ConnectionClosed` say nothing about which
kind of producer owns the socket, which is why they are not `Client*`.

`clusterctl load-topology <file>` publishes the deployment document — XML validated against
`topology.xsd`, which ships in the jar and `TopologyDocument` — the parse/validate half, split off
and unit-tested the way `Sequencer` and `QueueEmitter` are — resolves off its own classpath. Its three
sections are the complete producer view of a deployment: `<gateways>` (the elected pairs),
`<applications>` (the co-located, leader-gated kind, `sourceId` required so every producer sits in spec
§5's one id space), and `<protocols>` (`PayloadIdRegistered` rows naming each `payloadId`). **Only the
first is acted on**: `GatewayRegistered.remaining` counts down to 0 on the gateway section's last row, and
that row is the sequencer's completeness edge — it synthesizes the bootstrap `GatewayActive` per logical
gateway behind it. The application and protocol rows follow it, carry no countdown, and are labelling for
`SbeLogPrinter`: decoded by nothing, gating nothing. An optional `<snapshots>` element comes last, as one
`SnapshotPolicyRegistered` the sequencer does act on: it turns application snapshot rounds on (spec §7.3,
`doc/architecture/snapshot.md`).
