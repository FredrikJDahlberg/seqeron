# Design overview

seqeron is a replicated sequencer. It takes messages from any number of producers, assigns each one a
place in a single global order, and gives every consumer that order, complete and without gaps, on the
consumer's own host. This document describes how it is built and why. The normative rules are in
[`seqeron-protocol-spec.md`](seqeron-protocol-spec.md); the failure handling is in
[`fault-tolerance.md`](fault-tolerance.md).

## The sequencer architecture

A trading venue, an order management system or a risk engine is a set of services that must agree on
what happened and in what order. The sequencer architecture settles that once, in one place: every input
is stamped with a sequence number by a single sequencer, and every service consumes the resulting stream
as a deterministic state machine. Services never coordinate with each other. Two replicas that apply the
same stream hold the same state; a hot standby is a replica that does not publish; an audit or a
reconciliation is a replay; a restart is a replay from a known point.

The sequencer is then the one component everything depends on, for both correctness and availability.
seqeron makes it a replicated state machine on Aeron Cluster (Raft): the order is decided by consensus,
survives the loss of a minority of nodes, and continues under a new leader with no gap and no
renumbering. It is not tied to any application protocol. A payload is an opaque byte range that the
cluster copies through unopened; what it means is the business of the producers and consumers that use it.

## Architecture

```
  producers: gateway pairs, co-located applications, clusterctl
      │
      │ ingress: UDP, or IPC on the leader's host
      ▼
  ┌─ cluster member ─────────────────────────────────────────────────┐
  │                                                                  │
  │  Aeron Cluster ──▶ Sequencer ──▶ tap (aeron:ipc, stream 205) ────┼──▶ consumers on this host:
  │  (Raft log)                       │                              │      live, untethered
  │      ▲                            ▼                              │
  │      │                      Aeron Archive ──▶ Replayer ──────────┼──▶ consumers on this host:
  │      │                                                           │      history and gaps
  └──────┼───────────────────────────────────────────────────────────┘
         │ Raft replication
         ▼
  every other member: the same Sequencer, tap, Archive and Replayer
```

A deployment is a cluster of **members** — one for development, three or more, up to seven, in
production — each running one JVM: the Aeron Cluster consensus
module, the sequencer, an embedded media driver, an Aeron Archive and a Replayer. Clients run in their own
processes beside a member and attach to its media driver. A **gateway host** is a host that runs clients
but no member; its own Replayer relays a member's tap onto a local one, so its clients see exactly what
they would see on a member.

### The cluster member

- **`Sequencer`** is the replicated state machine. It validates each committed ingress frame, stamps it
  with the next `globalSeqNo` and the Raft consensus timestamp, and synthesizes the frames the cluster
  owns. It has no Aeron dependency and no I/O; it is unit-tested directly.
- **`SequencerService`** is the Aeron Cluster adapter. It decides when to call the sequencer and publishes
  what comes back, and holds no replicated state of its own.
- **The tap** is a node-local `aeron:ipc` publication (stream 205) carrying every sequenced frame. Every
  member publishes its own, leader and follower alike, and since every member applies the same committed
  log in the same order, every tap is byte-identical, frame for frame.
- **The archive** records the tap. Each member therefore holds a complete copy of history, written
  locally, with no cross-node replication and no leader-only recording. The tap is created once and
  survives leadership changes, so a member's recording is one continuous run across every leader it has
  seen.
- **The Replayer** serves history out of that recording to the clients on its host: a cold start from
  `globalSeqNo` 1, or a resume after a gap. It is off the live path, and nothing reads the archive except
  through it.

### The client tier

The client tier is a library in the application's process, in Java, C++ (header-only) and C# (.NET).
All three speak the same frames.

- **`ReplayerStreamReceiver`** follows the stream. It asks the Replayer for history, switches to the live
  tap once caught up, detects a gap on the tap and resumes over it, and delivers every frame once, in
  `globalSeqNo` order.
- **`ClusterStreamSender`** and **`IngressPublisher`** submit: a cluster session over IPC when the
  producer shares a host with the leader, over UDP otherwise.
- **`PendingSends`** confirms ingress on the tap and resends what a failover lost.
- **`Gateway`** and **`Application`** are the two façades, one per kind of producer. Each assembles the
  pieces above into one duty cycle, with the election or leader gate and the fences, so an application
  writes its own logic and none of seqeron's.

## The life of a message

1. A producer encodes its payload in whatever schema it owns. `IngressPublisher` wraps it in an
   `Unsequenced` frame — 28 bytes of envelope — and offers it to the cluster session.
2. The leader appends the frame to the Raft log and replicates it. The entry commits once a majority of
   members hold it.
3. Every member applies the committed entry. `Sequencer` validates the frame against the ten ingress
   conditions of spec §9.2, copies its 18-byte header, appends `globalSeqNo` and the consensus timestamp,
   and republishes it as a `Sequenced` frame on the member's tap. The payload is copied, never decoded or
   re-encoded.
4. The tap offer is reliable: it retries until the frame is published, since a dropped frame would leave
   a permanent hole. Only the archive's recording can hold it back, as the tap's one tethered subscriber.
5. The archive records the frame.
6. Each client on the host reads the frame off the tap, checks that its `globalSeqNo` is the next one,
   and dispatches it. A consumer that falls behind is dropped by the media driver rather than slowing
   the tap, and heals by replay.
7. The producer, which also follows its own tap, sees its frame arrive and stops tracking it. Only now
   is the message known to be sequenced.

A cold start runs steps 6 and 7 against a replay first. The client asks its member's Replayer for the
recording from its start, delivers the replay in order, keeps any live frames that arrive meanwhile, and
switches to the tap when the two meet.

## Frames

Everything on the wire is one SBE schema (210), in two families sharing one header layout.

- The **application family** carries a producer's payload: `Unsequenced` on ingress, `Sequenced` on the
  tap. `header.payloadId` names the payload's protocol. The cluster decodes no payload.
- The **system family** is seqeron's own vocabulary, named by `header.systemEventType` at the same offset:
  connections opening and closing, the gateway list and the election, the cluster's start and stop
  markers, the protocol registry, and snapshot rounds. Producers submit twelve such events; the
  sequencer synthesizes four (`LeadershipChanged`, `ClusterHeartbeat`, `GatewayActive`,
  `SnapshotStarted`).

| offset | field | ingress | tap |
| --- | --- | --- | --- |
| 0 | `sourceId` — the producer | ✓ | ✓ |
| 4 | `connectionId` — the producer's connection, or −1 | ✓ | ✓ |
| 8 | `sessionId` — the cluster session it arrived on | ✓ | ✓ (set by the sequencer) |
| 16 | `payloadId` or `systemEventType` | ✓ | ✓ |
| 18 | `globalSeqNo` | | ✓ |
| 26 | `timestamp` — Raft consensus time, epoch ns | | ✓ |

Every field sits at the same offset in every template, so a consumer tracks continuity without branching
on the frame type. A consumer splits by family first, then dispatches on `(payloadId, templateId)`.
Payloads are at most 8,884 bytes, which keeps a frame within one 8,960-byte IPC MTU.

## Time

The stream carries its own clock. Every frame holds the Raft consensus timestamp it was sequenced under, and
the sequencer emits a `ClusterHeartbeat` once a second whether or not anything else arrives. A timer driven
by the heartbeat decides the same thing on every replica, and keeps advancing while every producer is
silent, which is when a watchdog most needs it. Every deadline that decides what the sequencer emits, such
as a gateway's activation timeout, is measured on this clock rather than a node's local one, so no decision
depends on which node made it.

## Producers

### Gateways

A gateway connects the system to the outside world: an exchange session, a client connection, a market
data feed. It runs as an active/hot-standby pair, and the cluster chooses which instance serves.

The pair is declared in a topology document that `clusterctl load-topology` puts on the log. The
sequencer designates an instance by synthesizing `GatewayActive`: the rank-0 instance at bootstrap, the
standby when the active instance's cluster session closes, the next instance when a designated one does
not announce itself within 5 s, and whichever instance an operator names. Because the designation is a
frame, every instance and every replica sees the same decision at the same point in the order.

A designated instance announces itself with `GatewayStarted` before it serves. It resumes the pair's
connection ids past the highest its predecessor issued, so no id is ever issued twice, and it learns of
every connection its predecessor held from the `ConnectionOpened` and `ConnectionClosed` frames on the log.

### Co-located applications

A co-located application runs one replica per member. Nothing elects it: `LeadershipChanged` already
names the leader, and only the replica on the leader's host publishes. Every replica reads the same
stream and so holds the same state, including which requests are outstanding. When leadership moves,
the new leader's replica dispatches every request still outstanding, in `globalSeqNo` order. A request
is discharged only by its sequenced reply, so work is neither lost nor answered twice by design; across a
failover it is delivered at least once, and a reply that is a pure function of the request can be
de-duplicated by its key.

### Confirmed ingress

A send that succeeds means the frame reached the leader, not the log. A leader that fails takes its
uncommitted ingress with it, and the producer's session survives, so neither side sees an error. The
client tier closes that gap: `PendingSends` holds every frame until the producer's own tap shows it. A
term change identifies exactly which frames were lost; the producer places nothing new until those are
resent, oldest first. Each frame is then sequenced exactly once, in order, across any number of leader
failovers.

## Failure handling

The model is crash faults, not Byzantine ones. The cluster checks that frames are well formed, not who
sent them; authentication belongs at the system's external edges.

| failure | what happens | recovery |
| --- | --- | --- |
| leader lost | Aeron Cluster elects a new leader; the sequencer emits `LeadershipChanged` | the stream continues at the next `globalSeqNo`; producers resend what the old leader had not committed |
| member lost | the others keep quorum | the member restarts and replays the full log, rebuilding its recording |
| a member's archive stops recording | the member terminates (exit 70) rather than sequence history it cannot keep | as for a lost member |
| slow consumer | its media driver drops it within about 0.2 s; the tap is not held back | it resumes over the gap from the Replayer |
| frame missed on the tap | the next frame reveals the gap | the client resumes from the last frame it delivered, holding live frames meanwhile |
| gateway instance fails | its cluster session closes | the sequencer designates the standby |
| gateway can no longer trust its view | a fence: session lost, tap or recovery stalled, ingress or snapshot mismatch | the instance exits, its session closes, and the standby takes over |

The member's own recovery is always the same operation: replay the log from `globalSeqNo` 1. All state
downstream of the log — `globalSeqNo`, the gateway list, which instance is active, the open connections — is
a function of it, with no separate persistence and no separate recovery procedure.

## Snapshots

The cluster takes no snapshots. A member restored from one would hold a recording that starts where the
snapshot did, and could no longer serve full history to its clients. The cost is that a member's
recovery time and archive size grow with uptime.

Applications can snapshot their own state instead, and the log makes those snapshots safe. The sequencer
starts a round by sequencing `SnapshotStarted`, whose position in the order is the round's cut. Every
instance of a participating source serializes its state at that cut into its own local file, and the
instance that may publish submits the round's `SnapshotEnd`: a record count, a length and a CRC32C. Every
instance compares its own file with the sequenced end, and one that differs stops. A restarting
instance restores its newest file that the log confirms, then replays only what follows the cut. This
shortens an application's restart, not a member's. [`snapshot.md`](snapshot.md) is the design in full.

## Deployment

- **Members.** Three is the usual cluster; the port layout allows up to seven. Each member is one JVM with
  its media driver embedded, configured through system properties and the `SEQERON_HOSTS` list of member
  hosts ([`running-a-cluster.md`](running-a-cluster.md)).
- **Clients** attach to a member's media driver, or a gateway host's, and submit over IPC or UDP.
- **Topology.** The gateway pairs, the co-located applications, the payload protocols and the snapshot
  policy are declared in one XML document, validated against a packaged schema and published once onto
  the log.
- **Operations.** `clusterctl` starts and stops the cluster, designates gateways, loads the topology and
  requests snapshot rounds ([`clusterctl.md`](clusterctl.md)). Each member exports its counters to
  Prometheus ([`ops.md`](ops.md)). `SbeLogPrinter` decodes any recording, the Raft log included
  ([`log-printer.md`](log-printer.md)).
- **Versions.** Every process uses the Aeron, Agrona and SBE versions seqeron was built with, since a
  mismatch corrupts frames rather than failing a build. The C# client, on Aeron.NET, is one release behind
  and verified against the cluster on every Aeron upgrade (spec **V-1**).

## Limits

| | |
| --- | --- |
| members | up to seven, bounded by the 70-port cluster block |
| payload | 8,884 bytes per frame; larger messages are split by the application |
| node recovery | full-log replay; time and archive size grow with uptime (the heartbeat alone is about 86,400 frames a day) |
| concurrent replays | four per member; more clients queue |
| ingress confirmation | within one producer process: a restarted producer starts with nothing pending |
| faults | crash, not Byzantine; media driver and network faults are not exercised by the test harnesses |

## Further reading

1. [Getting Started](getting-started.md) — a node and a first consumer and producer
2. [Client API](client-api.md) — Java, C++ and C#
3. [Protocol Specification](seqeron-protocol-spec.md) — normative
4. [Fault Tolerance](fault-tolerance.md) — each failure and its recovery
5. [Application Snapshots](snapshot.md)
6. [Running a Cluster](running-a-cluster.md)
