# Client API

The API of the **client tier**: the `org.limitless:seqeron` artifact (Java), the `seqeron::seqeron_core`
CMake target (C++) and the `Org.Limitless.Seqeron` package (C#). `seqeron-service` (sequencer, Replayer
server, tools, metrics exporter) is the service tier and is not client API.

The types this page names are the API, except those under [Internal types](#internal-types): public only
for cross-package access, with no compatibility guarantee. The wire contract is
[`seqeron-protocol-spec.md`](seqeron-protocol-spec.md); the design rationale is in [`overview.md`](overview.md).

The sections before [Façades](#façades-app) define the model and the contract; the rest is reference.

## Application types

| application type | role | API |
| --- | --- | --- |
| **Gateway** | Edge producer deployed as an active/hot-standby pair, the active instance designated by the cluster: exchange session, client gateway, feed handler | [`Gateway`](#gateway) |
| **Replicated service** | One replica per cluster member, deterministic over the stream, publishing from the leader's replica | [`Application`](#application) |
| **Consumer** | Reads the stream and publishes nothing: audit, risk, downstream feed | [`ReplayerStreamReceiver`](#replayerstreamreceiver) |
| **Offline reader** | Decodes messages from a recording, outside a live client | `SequencedFrameDecoder` (Java, C#), `unwrapFrame` / `ClusterStreamClient` (C++) |

## Guarantees

| API | guarantees | out of scope |
| --- | --- | --- |
| [`ReplayerStreamReceiver`](#replayerstreamreceiver) | Every message on its group's queue exactly once, in `clusterSeqNo` order: archive replay from the local recording, then the live queue, with gap detection and resumption internal. Optional snapshot restore first. | Filtering within a group: every consumer receives every message addressed to its group, and every system message. |
| [`Gateway`](#gateway) | One instance of an active/hot-standby pair; the cluster designates the active instance. A connection id space never reused across handovers, and every connection's open and close on the log, so a successor knows its predecessor's connection state. Nothing an instance sends after a sibling is designated reaches the log (spec **S-6**). Confirmed ingress; fencing. | Edge connection continuity across a handover: the successor reopens the edge and counterparties reconnect. |
| [`Application`](#application) | One replica per cluster member, all holding identical state; only the leader's replica publishes. Work outstanding at a failover is redispatched by the next leader's replica (`OutstandingWork`). Confirmed ingress; fencing. | Exactly-once replies: across a failover, delivery is at-least-once. |
| Confirmed ingress (both façades) | Every placed message is sequenced exactly once, in order, across any number of leader failovers and replaced cluster sessions. | Producer process failure: a restarted producer, or a promoted standby, starts with nothing pending. A gateway instance a sibling superseded: what was pending is dropped. |
| [Snapshots](#snapshots) (both façades) | Restart from the newest log-confirmed snapshot, then only the messages after its cut. Fencing of any instance whose state diverges from its source's. | Member restart: a member always replays its full log. |

A **fence** is `onFenced(ClusterError, detail)`, raised once, after which the instance takes no further
action; the façade releases its cluster session before raising it, and its queue and replay subscriptions at the
end of that `doWork()`, so a fenced instance holds no Replayer slot. Causes: cluster session lost (on a `Gateway` instance that announced its activation on it, or not replaced within 20 s),
ingress confirmation fault, recovery stall, queue stall, snapshot divergence, unrestorable snapshot, an addressed group the
log lacks ([`fault-tolerance.md`](fault-tolerance.md) §2.1). Loss of the media driver
is not a fence; it raises from `doWork()`.

## Execution model

- **Single-threaded, caller-owned duty cycle.** The client tier starts no threads. The application calls a
  façade's `doWork()` (or the receiver's `poll()`), which returns a work count for the caller's idle
  strategy, Agrona `Agent` style. All callbacks run on that thread, inside that call.
- **Co-located media driver.** `start(aeron)` takes an Aeron client connected to the media driver of a
  cluster member or [gateway host](#replayerstreamreceiver) on the same host. The queue and the replay are IPC;
  no client reads the stream from a remote host.
- **Bounded callbacks.** The duty cycle carries the cluster session keep-alive. A session idle for
  `sequencer.sessionTimeoutMs` (default 5 s) is closed by the cluster; for a gateway, that hands over to the
  standby. A callback that must spin calls `keepAlive()`.
- **Back pressure blocks, bounded.** `publish`/`reply` spin through ingress back pressure and leader
  elections until the offer lands, for at most 10 s; beyond that the session is treated as lost. A
  `Gateway` instance that announced its activation on that session is fenced; an `Application`, or any other
  `Gateway` instance, opens a new session, starting an attempt at most once a second without blocking its duty
  cycle, and is fenced only if none opens within the queue-stall timeout and the attempt then under way fails. The result is `protocol.Publish`: `Published` (offered — not yet sequenced; confirmed
  ingress tracks it to the producer's own queue), `Declined` (not placed; retryable), or `Refused` (never
  placeable).
- **Flyweights.** `Payload` and `SequencedEvent` are views over the receive buffer, valid only for the
  callback. Copy out what outlives it.
- **Zero allocation on the hot path.** Dispatch and confirmed publish allocate nothing once warm; the C#
  suite asserts zero bytes per message.
- **Untethered subscription.** A consumer that falls a window behind the queue publication is dropped by the
  media driver and resumes from its Replayer automatically. Ordering holds; the cluster never sees back
  pressure from a consumer.

## Application contract

The guarantees are conditional on these; spec §16 states them normatively (**A-1**–**A-7**).

- **Deterministic state machine.** Every replica and gateway instance derives state from the messages alone:
  no local clock (use `onClusterHeartbeat` and `clusterTimestampNs`), no hash iteration order, no node
  identity, no randomness. Divergence is detected only at a snapshot round.
- **Idempotent leader-only work.** Act only while `canPublish()`, keep each request outstanding until its
  reply is sequenced, and make the reply a pure function of the request.
- **Deterministic snapshot serialization.** Equal state produces equal bytes on every instance.
- **Payload ownership:** schemas, versioning (spec **V-2**), `payloadId` rows in the topology document, and
  fragmentation above 8,874 bytes.
- **Handle `Declined`** by retrying from the duty cycle, not by spinning in a callback. `Refused` is final.
- **Exit when fenced.** The façade has already released the cluster session, so a standby takes over; a
  restart is what brings the instance back. Exit with 70, this repository's convention for "fenced, restart
  me" ([`ops.md`](ops.md)), so a supervisor can tell a fence from a crash.
- **Unique `clientId` per node** ([Client ids](#client-ids)).
- **Authenticate at the edge.** The cluster accepts any well-formed message.

## Model constraints

- **One total order, read by group.** Every message has one `clusterSeqNo`; an instance reads its group's
  queue, which holds the messages addressed to that group and every system message ([Groups](#groups)).
  Within a group there are no topics; consumers filter on `(payloadId, templateId)` or `sourceId`, and all
  producers share one sequencer's throughput.
- **No query path.** The cluster holds no application state. Consumers materialize state from the stream
  and rebuild it on restart from a snapshot or the start of its queue.
- **History is node-local:** from the start, from a snapshot cut, or from a recording position after a gap.
  There is no random access by `clusterSeqNo`; C++'s `ClusterStreamClient` performs bounded archive scans.
- **Opaque, bounded payloads:** at most 8,874 bytes, never decoded or modified by the cluster.
- **A topology first.** The cluster sequences nothing until `clusterctl load-topology` has activated its
  groups, and a pair is elected only once its rows are on the log.
- **Pre-1.0.** The API may change between minor releases.

## Patterns

### Request/reply through the log

```
 external client
      │ request
      ▼
 Gateway (active) ── publish(connectionId, payloadId, request) ──▶ cluster: clusterSeqNo N
                                                                          │
            ┌─────────────────────────────────────────────────────────────┤
            ▼                                   ▼                         ▼
 Application, member 0              Application, member 1 (leader)   Application, member 2
 applies N, outstanding             applies N, outstanding           applies N, outstanding
                                    reply(sourceId, connectionId, …)
                                                │
                                                ▼
                                     cluster: clusterSeqNo M ──▶ every replica: discharged
                                                │
                                                ▼
 Gateway: onSequenced(M), own sourceId, connectionId ──▶ external client
```

1. The gateway allocates a connection id (`openConnection`) and publishes requests on it.
2. Every replica applies the request at the same log position and records it in `OutstandingWork`, keyed
   by its `clusterSeqNo`.
3. The leader's replica replies under the requester's `sourceId`, `connectionId` and `groupMask`, so the
   reply reaches every group the request did. Replicas discharge the request when the reply is sequenced,
   not when it is sent.
4. Every gateway instance receives every application payload addressed to its group, keeps those with its
   own `sourceId`, and the active instance routes each by `connectionId`.
5. On leader failure between 2 and 3, the next leader's replica redispatches. The reply is deterministic, so
   a duplicate is byte-identical and dropped by key.

`GatewayApp` and `ColocatedApp` in `seqeron-examples` implement this in each language.

### Read-only consumers

Audit, risk or downstream feeds take a `ReplayerStreamReceiver` (or an `Application` if they may later
publish): replay from the start of the queue or the newest confirmed snapshot, then follow the queue. Consumers are
invisible to the cluster and unbounded in number per node. `FollowStream` in `seqeron-examples` is the
minimal case.

## Façades (`app`)

A façade composes the full duty cycle for one producer kind — cluster session, queue subscription, confirmed
ingress, fencing, and the election or leader gate — in the required order. Code written against one sees
neither the message envelope nor seqeron's system messages.

Both façades exist in every language with identical operation names. Signatures below are Java's; the
language differences are:

| | Java | C++ | C# |
|---|---|---|---|
| construction | `Gateway.builder()…build()` | `Gateway<Listener>{ config, listener }`; `Config` is an aggregate, one field per builder setter | `new Gateway(new GatewayOptions { … })`, one `init` property per setter; `build()`'s mandatory options are `required` |
| listener | implements `Gateway.Listener` | type satisfying the `GatewayListener` / `ApplicationListener` concept | implements `IGatewayListener` / `IApplicationListener` |
| ingress endpoints | `ingressEndpoints(…)`, default `PortLayout.ingressEndpoints()` | `Config::ingressEndpoints`, default `protocol::ingressEndpointsCsv()` | `IngressEndpoints`, default `PortLayout.IngressEndpoints()` |
| `publish` / `reply` | encoded bytes including the payload's `MessageHeader` | bytes, or `publish<Encoder>(…, fill)` where `fill(Encoder&)` populates a header-wrapped encoder | bytes or `ReadOnlySpan<byte>`, copied once into a façade-owned buffer |
| payload body | `buffer()` at `bodyOffset()`/`bodyLength()` | `body()`/`bodyLength()`, or `decode<Decoder>()` | `Buffer` at `BodyOffset`/`BodyLength` |
| headerless payload (spec §13.2) | `buffer()` at `payloadOffset()`/`payloadLength()` | `payload()`/`payloadLength()` | `Buffer` at `PayloadOffset`/`PayloadLength` |
| release | `close()` / try-with-resources | `close()` | `Dispose()` / `using` |

Java is bytes-only because its SBE codecs share no common interface. C# options and listener types are
top-level rather than nested, per .NET design guidelines.

### Configuration

| option | `Gateway` | `Application` | default | meaning |
|---|---|---|---|---|
| `listener` | required | required | | edge or service logic |
| `gatewayName` | required | | | topology row this instance joins |
| `sourceId` | with `snapshotListener` | required | | `sourceId` declared in the topology row |
| `addressedGroups` | ✓ | ✓ | none | names of the other groups the instance publishes to |
| `clientId` | ✓ | ✓ | 0 | the client's counter label, unique per node |
| `memberId` | ✓ | ✓ | 0 | member (or gateway host node id) whose queue is followed |
| `egressChannel` | required | required | | cluster session egress channel |
| `ingressEndpoints` | ✓ | ✓ | `PortLayout.ingressEndpoints()` | member ingress endpoints |
| `offCluster` | | ✓ | false | single instance on a gateway host ([Application](#application)) |
| `snapshotListener` | ✓ | ✓ | none | participate in snapshot rounds |
| `snapshotDirectory` | with `snapshotListener` | with `snapshotListener` | | this instance's snapshot files |
| `pendingCapacity` | ✓ | ✓ | 1,024 | messages tracked by confirmed ingress; beyond it `publish` returns `Declined` |
| `queueStallTimeoutMs` | ✓ | ✓ | 20,000 | no `ClusterHeartbeat` for this long fences with `QUEUE_STALLED` a designated instance or the leader's replica; any other logs it |
| `recoveryStallTimeoutMs` | ✓ | ✓ | 60,000 | recovery dispatching nothing for this long fences with `RECOVERY_STALLED`, as above |
| `ipcConnectTimeoutMs` | | ✓ | 500 | wait for IPC ingress to the local member while it leads |

The stall defaults are `DEFAULT_QUEUE_STALL_TIMEOUT_MS` (20 heartbeat intervals) and
`DEFAULT_RECOVERY_STALL_TIMEOUT_MS` (3× that).

**Dependency boundary.** A façade's signatures name only `app` and `protocol.Publish` (C++ `protocol::Publish`).
With the default port block, a façade client imports nothing else. `FacadeSurfaceTest` enforces this in Java
and C#.

**`Payload`** is one application message with the envelope stripped: `clusterSeqNo`, `sourceId`, `connectionId`,
`groupMask`, `sourceSessionId`, `clusterTimestampNs`, `groupSeqNo`, `receiveTimeNs`, `position()`, and
`(payloadId, templateId)` for dispatch. System messages are never delivered as `Payload`. `bodyOffset()`/`bodyLength()` also skip the
payload's `MessageHeader`, which is where an SBE decoder wraps. For a headerless payload (spec §13.2) use
`payloadOffset()`/`payloadLength()`; `templateId`, `blockLength` and `version` are then meaningless.
`position()` is the message's recording position on this node — a delivery attribute, and the index for a
consumer that replays the recording itself (for example, to serve its own resends).

**Cluster clock.** `onClusterHeartbeat(clusterTimeNs, receiveTimeNs)` fires at 1 Hz on consensus time. It
advances while all producers are idle and is identical on every node, so deadlines driven by it are
deterministic. Between ticks, use each payload's `clusterTimestampNs()`.

**Node apply lag is not a fence.** It is a node property, raises nothing and changes no behaviour;
[`ops.md`](ops.md) graphs it per member.

### Groups

An instance reads its group's queue (spec §3.1), which nothing configures: before its first replay the
façade asks its node's Replayer, which resolves it from the instance's topology row, a `Gateway`'s by
`gatewayName` and an `Application`'s by `sourceId` (spec R-6). Until that row is on the log the façade
waits, as recovery waits on `ReplayPending`, and is not fenced; the Replayer logs the wait. On a gateway
host the instance's group must be one the host relays (`replayer.groups`), or it waits for good. A
producer addresses other groups by name: `addressedGroups` lists them, and a name the log's group table
lacks fences with `GROUP_UNKNOWN` (spec A-9).

Once the table is resolved, from the log or a restored snapshot, the façade calls
`onGroupsResolved(GroupDirectory)` once, before the publish gate first opens. The directory is immutable:
`mask(name)` is a group's bit, combined with `|`, and `name(groupId)` labels a group. A mask is an `int`
(Java), `std::uint16_t` (C++) or `ushort` (C#).

- `publish` without a mask reaches the instance's own group. With one, the façade adds the own group's
  bit, since confirmed ingress watches the own queue; a mask naming a group the table lacks is `Refused`.
- `reply` takes the request's `groupMask()`, so every group that holds the request outstanding sees the
  reply (spec A-2).

The groups are fixed by the first topology load (spec S-8), so the directory holds for the life of the
archive.

### `Gateway`

One instance of an elected active/hot-standby pair. The cluster designates; the application implements the
edge.

```java
try (Gateway gateway = Gateway.builder()
        .gatewayName("GW-A").clientId(10).memberId(memberId)
        .egressChannel(egressChannel).listener(listener)
        .build()) {
    gateway.start(aeron);
    while (running) { idle.idle(gateway.doWork()); }
}
```

```cpp
Edge edge; // satisfies app::GatewayListener
app::Gateway<Edge> gateway{
    { .gatewayName = "GW-A", .clientId = 10, .memberId = memberId, .egressChannel = egressChannel }, edge
};
gateway.start(aeron);
while (running) { idle.idle(gateway.doWork()); }
gateway.close();
```

| operation | semantics |
|---|---|
| `doWork()` | one duty-cycle iteration: session, queue, confirmed ingress, fences, connection lifecycle, election |
| `gateClosed()` | the edge closed on its own (failed dial, counterparty hang-up). The designation stands; `doWork()` reopens it via `onActivated` without a new `GatewayStarted`. For initiators; acceptors never call it |
| `keepAlive()` | session keep-alive for a callback that spins; `doWork()` already sends one per cycle |
| `canAccept()` | serving, and ingress not held for a post-failover resend |
| `openConnection()` / `openConnection(data, length)` | allocate a connection id and place `ConnectionOpened`, retried by `doWork()` |
| `closeConnection(id)` | place `ConnectionClosed`; dropped for a connection the cluster never saw |
| `publish(connectionId, payloadId, payload, length)`, `publish(connectionId, groupMask, payloadId, payload, length)` | submit a payload under this gateway's `sourceId`, to its own group or to those `groupMask` names ([Groups](#groups)). C++ also `publish<Encoder>(connectionId, [groupMask,] payloadId, fill)` |
| `isActivated()`, `isServing()`, `sourceId()`, `gatewayId()`, `isCaughtUp()`, `lastClusterSeqNo()` | instance state; `sourceId()` and `gatewayId()` are `UNRESOLVED` until a `GatewayRegistered` row names the instance |

| listener callback | trigger |
|---|---|
| `onGroupsResolved(GroupDirectory)` | the group table, once, before the first `onActivated` ([Groups](#groups)) |
| `onActivated(firstConnectionId)` | designated active: open the edge |
| `onStandby()` | another instance designated, or this one fenced while serving: close the edge |
| `onSequenced(Payload)` | application payload, in order |
| `onConnectionOpened` / `onConnectionClosed` | this logical gateway's connection lifecycle from the log, whichever instance issued it — used to rebuild per-connection state during replay, and the only signal for a client that drops without logging out |
| `onCaughtUp(clusterSeqNo)` | each transition to caught up |
| `onClusterHeartbeat(clusterTimeNs, receiveTimeNs)` | 1 Hz cluster clock |
| `onFenced(ClusterError, detail)` | terminal: the session is already released, so the standby takes over, and a serving edge has had `onStandby`; normally exit |

**Snapshots.** With a `snapshotListener` the gateway also requires `sourceId` (the pair's, which labels the
instance's snapshot counter) and `snapshotDirectory` (per instance, not per pair). Every instance writes a
file at each cut; the instance whose `GatewayStarted` for its current activation has been placed submits the
round's `SnapshotEnd`. The façade's file header carries the pair's rows, the active instance and the
highest connection id, replacing the pre-cut messages on restore. See [Snapshots](#snapshots).

**Reference implementations.** `TestGateway` (`seqeron-service/src/test/java/org/limitless/seqeron/tools/TestGateway.java`);
`seqeron-examples/src/java/example/GatewayApp.java` and `src/cpp/GatewayApp.cpp` (one pair, from
`seqeron-examples/topology.xml`, the C++ one handed over by `cpp-client-test.sh`);
`seqeron-examples/src/csharp/GatewayApp` (pair from `topology-csharp.xml`, handed over by
`csharp-client-test.sh`).

### `Application`

One replica of a leader-gated service: one per member, declared in the topology's `<applications>`,
publishing only while its member leads. Same builder as `Gateway`, with `sourceId` in place of
`gatewayName`. Ingress is IPC to the local member while it leads (`DEFAULT_IPC_CONNECT_TIMEOUT_MS`), UDP
otherwise.

| operation | semantics |
|---|---|
| `doWork()` | one duty-cycle iteration: session, queue, confirmed ingress, fences, leader gate |
| `canPublish()` | gate open and ingress not held |
| `isLeading()` | gate open |
| `publish(payloadId, payload, length)`, `publish(groupMask, payloadId, payload, length)` | submit under the application's `sourceId`, no connection, to its own group or to those `groupMask` names. C++ also `publish<Encoder>([groupMask,] payloadId, fill)` |
| `reply(requesterSourceId, connectionId, groupMask, payloadId, payload, length)` | submit under the **requester's** `sourceId`, `connectionId` and `groupMask`, so the originating gateway routes it. C++ also `reply<Encoder>(…, fill)` |
| `sourceId()`, `isCaughtUp()`, `lastClusterSeqNo()` | replica state |

**Leader gate.** `onLeadershipChanged(boolean leading)` replaces `onActivated`/`onStandby`; `onGroupsResolved`,
`onSequenced`, `onCaughtUp`, `onClusterHeartbeat` and `onFenced` are as for `Gateway`, and there is no connection
lifecycle. **Every leadership change closes the gate**, so call `OutstandingWork.onNotLeader()` on `false`:
a reply submitted during the election may be lost, and the next opening redispatches it. The gate is also
shut while a lost cluster session is replaced, for the same reason, and it closes at a fence: an open gate
reports `false` before `onFenced`, and `isLeading()` and `canPublish()` are false from then on. Retain a request's `sourceId`,
`connectionId` and `groupMask`, not its `Payload`.

**Off-cluster.** On a gateway host, `offCluster(true)` makes the replica the sole instance. Its gate opens
once caught up regardless of leader, and closes for one cycle on every leadership change; ingress is UDP
only. Nothing arbitrates between two such instances and the sequencer accepts both, so run exactly one. A
service that needs an off-cluster standby is a `Gateway`.

**Reference implementations.** `ColocatedApp` in `seqeron-examples` (`src/java`, `src/cpp`, `src/csharp`).
With the `GatewayApp`s, these builds reject any reference outside `app` (`checkFacadeOnly`, its CMake
equivalent, and `src/csharp/Directory.Build.targets`).

### Snapshots

[`snapshot.md`](snapshot.md) specifies the protocol. A façade participates when it has a `snapshotListener`
(C++: a `SnapshotListener*` that outlives the façade) and, for an `Application`, its topology row sets
`snapshot="true"`. `snapshotDirectory` holds the instance's confirmed file and the one being written, and must
outlive the process ([`snapshot.md`](snapshot.md) §4.1).

| listener operation | Java | C++ | C# |
|---|---|---|---|
| write next record | `onSnapshot(MutableDirectBuffer buffer, int recordIndex)` | `onSnapshot(std::span<std::uint8_t> buffer, std::int32_t recordIndex)` | `OnSnapshot(IMutableDirectBuffer, int)` |
| read one record | `onRestore(DirectBuffer buffer, int length, int recordIndex, int formatVersion)` | `onRestore(std::span<const std::uint8_t> record, std::int32_t recordIndex, std::uint32_t formatVersion)` | `OnRestore(IDirectBuffer, int length, int recordIndex, int formatVersion)` |
| record format | `formatVersion()` | `formatVersion()` | `FormatVersion` |
| formats it reads (default: its own) | `supportsFormatVersion(int formatVersion)` | `supportsFormatVersion(std::uint32_t formatVersion) const` | `SupportsFormatVersion(int formatVersion)` |

1. **At the cut**, before the next message is dispatched, the façade calls `onSnapshot` from `recordIndex` 0
   until it returns 0. Each call encodes one record (at most 65,535 bytes) and returns its length;
   `recordIndex` 0 restarts iteration, and a length outside 0–65,535 abandons the round.
2. **Every instance writes its own file.** The publishing instance (open gate, or the gateway's active
   instance as above) submits `SnapshotEnd`; every instance compares it with its own, confirms its file on a
   match and fences with `SNAPSHOT_DIVERGED` on a mismatch.
3. **On start**, the newest confirmed file whose cut the recording holds is restored before any dispatch:
   `onRestore` once per record in write order, `recordIndex` 0 meaning clear state, each with the file's
   `formatVersion`. A build that changes its format overrides `supportsFormatVersion` to keep reading the
   formats it replaces ([`snapshot.md`](snapshot.md) §9). Dispatch resumes after the cut; with no such file,
   from the start of the queue.
4. **An unreadable file** (a `formatVersion` that `supportsFormatVersion` refuses, corrupt records) fences with
   `SNAPSHOT_UNRESTORABLE` (C++/C# `ClusterError::SnapshotUnrestorable`; divergence is
   `ClusterError::SnapshotDiverged`).

### `OutstandingWork`

The façades are composed of I/O-free state machines with twins in every language. `OutstandingWork` —
leader-only request/reply tracking, redispatched after failover — is the only one exposed, since it models
the application's work rather than seqeron's. The election, leader gate and stall fences are
package-private (Java), `internal` (C#) or in `app::detail` (C++). Confirmed ingress is
[`PendingSends`](#producer-components), an internal type.

## `ReplayerStreamReceiver`

The single read path (`replayer.client`, every language). It replays local history through the co-located
Replayer (an Aeron Archive replay), switches to the live queue once caught up, recovers gaps internally, and
delivers every message once in `clusterSeqNo` order. Each façade owns one.

The local node is a cluster member or a **gateway host** — a host with no member, running
`start-gateway-host.sh`, whose Replayer relays a member's queues, those of the groups it is configured with, into local recordings
([`fault-tolerance.md`](fault-tolerance.md#33-gateway-host)). A client there attaches to the host's Aeron
directory and passes the host's node id (unused by any member) as `memberId`. `Gateway` is unchanged;
`Application` requires [`offCluster`](#application).

| operation | Java | C++ | C# |
|---|---|---|---|
| construct | `(clientId, onSequenced, onLeadershipChanged, onCaughtUp)` | `(clientId, onSequenced, onConnected, onDisconnected, onLeadershipChanged, onCaughtUp)` | as Java, callbacks as delegates |
| snapshot restore | `restoreFrom(SnapshotStore, SnapshotRestoreHandler)`, before `start` | `restoreFrom(SnapshotStore&, SnapshotRestoreHandler&)`, before `start` | `RestoreFrom(SnapshotStore, ISnapshotRestoreHandler)`, before `Start` |
| attach | `start(aeron, memberId, groupId)`; a consumer names its group, a façade passes the one the Replayer resolved | `start(aeron, memberId, groupId)` | `Start(aeron, memberId, groupId)` |
| duty cycle | `poll()` | `poll()` | `Poll()` |
| state | `isCaughtUp()`, `lastClusterSeqNo()`, `currentLeaderMemberId()`, `restoreFailure()` | same; `restoreFailure()` is `std::optional<std::string>` | same, as properties; `RestoreFailure` null if none |
| release | `close()` | `close()`, or the destructor | `Dispose()` |

Single-threaded, like the façades.

**Snapshot restore.** `restoreFrom` makes the cold start take the newest confirmed file in the store whose
cut a replay finds in the recording, handing header and records to the handler, a bounded number per
`poll()`, before dispatching from the message after the cut ([`snapshot.md`](snapshot.md) §7). A file whose
cut the recording does not hold falls back to the next older, the last to the start of the queue. An unrestorable
snapshot (unsupported format or header version, records failing the trailer) halts recovery permanently;
`restoreFailure()` gives the reason.

### Client ids

`clientId` is the `client` label of the client's counters, and should be unique among clients on a node: two
sharing one share their counters' series ([`ops.md`](ops.md#client-ids), which also lists the ids this
repository uses). The Replayer addresses a client by an id the client draws from its media driver, which
makes it unique.

### Message delivery

| message | Java, C# | C++ |
|---|---|---|
| `LeadershipChanged` | `onLeadershipChanged` only | `onLeadershipChanged` only |
| `ConnectionOpened` / `ConnectionClosed` | `onSequenced`, `isSystem()` | `onConnected` / `onDisconnected` (`LifecycleEvent`) only |
| other system messages | `onSequenced`, `isSystem()` | `onSequenced`, `system` |
| application messages | `onSequenced` | `onSequenced` |

C++'s two extra callbacks keep the callback set interchangeable with `ClusterStreamClient`; a
`LifecycleEvent` carries the message identity and payload, from which `decodeSystem<ConnectionOpened>(event)`
reads `connectionData`. A message whose dedicated callback is null (Java, C#) or empty (C++) is delivered to
`onSequenced` as a system message, so `groupSeqNo` has no holes. `onSequenced` is mandatory.

**`SequencedEvent`** (`replayer.client` in Java and C#, `protocol/SequencedFrame.hpp` in C++) is the
flyweight `onSequenced` receives. Its fields carry the decoder's names, plus the delivery attributes
`receiveTimeNs()` and `position()`. `sourceSessionId`, `clusterTimestampNs` and `groupSeqNo` read the wire
fields `header.sessionId`, `header.timestamp` and `header.groupSeqNo` (spec F-6).

**Decoding.** Dispatch by family (`isSystem()`), then on `(payloadId, templateId)` for application messages or
`systemEventType` for system messages — never on `templateId` alone. System payloads have no `MessageHeader`
(`blockLength` and `version` read 0), so take both from the decoder's compiled constants:

- **C++:** `decodeSystem<Decoder>(event)`; `decodeSequenced<Decoder>(event)` for application payloads.
- **Java:** `decoder.wrap(event.buffer(), event.payloadOffset(), Decoder.BLOCK_LENGTH, Decoder.SCHEMA_VERSION)`.
- **C#:** `SbeBuffers.Wrap(view, event.Buffer, event.PayloadOffset, event.PayloadLength)`, then
  `decoder.WrapForDecode(view, 0, Decoder.BlockLength, Decoder.SchemaVersion)`. The SBE C# runtime has its
  own `DirectBuffer`; `SbeBuffers` aliases it over the same memory without copying.

`seqeron-examples` decodes a submitted `ConnectionOpened` and a synthesized `ClusterHeartbeat` in all three
languages.

**Without a receiver.** `SequencedFrameDecoder` (Java, C#) and `unwrapFrame`/`FrameView` (C++) strip the
envelope from a raw queue message read out of a recording. `ClusterStreamClient` (C++ only,
`sequencer/client/ClusterStreamClient.hpp`) performs bounded archive scans; it is not a live path. A caller
that keeps the archive session `connectLocalArchive` or `connectToArchiveWithClusterStream` returned calls
`pollArchiveSession` every duty cycle, because the archive closes a session whose pings go unread.

## Packages

Aeron's layout: a component's server at the package root, its client in `.client`, shared wire contract in
`protocol`.

| package | tier | contents |
|---|---|---|
| `protocol` | client | `FrameLayer`, `SystemFrame`, `SequencedFrameDecoder`, `PortLayout`, `ReplayProtocol`, `SeqeronCounters`, `Publish` (C++: `SequencedFrame.hpp`, `PortLayout.hpp`, `ReplayProtocol.hpp`, `SeqeronCounters.hpp`, `Publish.hpp`) |
| `sequencer.client` | client | internal: the [producer components](#producer-components) (C++ also `ClusterStreamClient`) |
| `replayer.client` | client | `ReplayerStreamReceiver`, its callback types (`SequencedHandler`, `LeadershipHandler`, `CaughtUpHandler`), `SnapshotRestoreHandler`, `SnapshotStore`; `SequencedEvent` in Java and C# (C++: `protocol`, beside `unwrapFrame` and `decodeSystem`/`decodeSequenced`) |
| `app` | client | the façades and their components |
| `util` | client | support code |
| `sbe.frame`, `sbe.replay` | client | generated codecs |
| `sequencer`, `replayer.server`, `tools`, `metrics`, `sbe.probe` | service | not client API |

- **C++:** same directories and namespaces (`org::limitless::seqeron::sequencer::client`, …); a `detail`
  namespace beneath a package stands in for Java package-private.
- **C#:** PascalCase namespaces (`Org.Limitless.Seqeron.Sequencer.Client`, …), `internal` for
  package-private, properties for getters, `I`-prefixed interfaces (`IIngressTracker`,
  `ISnapshotRestoreHandler`, `ISnapshotListener`), receiver callbacks as delegates. No `ClusterStreamClient`.

## Constants

| type | contents |
|---|---|
| `FrameLayer` (Java, C#), `SequencedFrame.hpp` (C++) | queue identity (`QUEUE_STREAM_ID_BASE` 2000, `queueStreamId(groupId)`, `MAX_GROUPS` 16), spec §12 size limits; Java/C# also the heartbeat interval |
| `SystemFrame` (Java, C#), `SequencedFrame.hpp` (C++) | `systemEventType` values |
| `PortLayout` | cluster port block, per-member ingress endpoints, the producer endpoint list (`ingressEndpoints()`, `ingressEndpoints(parseHosts("h0,h1,h2"))`; C++ `ingressEndpointsCsv`), archive control channel, a node's default Aeron directory (`aeronDir(nodeId)`; C++ `util::resolveAeronDir`); honours `SEQERON_PORT_BASE` and `SEQERON_HOSTS` |
| `ReplayProtocol` | replay protocol channel and stream ids, `NO_REPLAY_NEEDED` |
| `Publish` | offer outcome: `Published`, `Refused` (final), `Declined` (retryable) |

## Wire codecs

The generated `sbe-frame.xml` codecs (`org.limitless.seqeron.sbe.frame`, `org_limitless_seqeron_sbe_frame`,
`Org.Limitless.Seqeron.Sbe.Frame`) are API because the schema is; producers encode system payloads and
consumers decode them with these. They change only with the protocol, under spec **V-3**. The `sbe-replay.xml`
codecs ship but are used only by `ReplayerStreamReceiver`.

## Support code

`util` (`Logger`, `IdleStrategies`/`IdleStrategy.hpp`, `Clocks`, C++ `Env.hpp`, C# `SbeBuffers`) and
`protocol.SeqeronCounters` ship because the API uses them. They are usable but carry no stability
guarantee, except `SbeBuffers`, which C# SBE decoding requires. `Clocks.monotonicMs()` is the time base for
every duty-cycle deadline; C++ uses the free function `nowMs()`.

## Internal types

Visible for cross-package or test access only; no compatibility guarantee.

- **C++ `detail` namespaces** (each in a `detail/` directory beside its package) — the equivalent of Java
  package-private. A public signature naming a `detail` type is a test seam.
  - `replayer/client/detail`: `ReplayerRecovery`, `ReplayerRecoveryActions`.
  - `sequencer/client/detail/Transport.hpp`: `IngressTransport`/`EgressTransport` and their Aeron
    implementations, injected into `ClusterStreamSender::connect` in tests.
  - `app/detail`: `Session`, `GatewayLifecycle`, `LeaderGate`, `RecoveryStallFence`, `QueueStallFence`.
    `makePayload` builds a `Payload` for unit-testing a listener.
- **Java package-private and C# `internal` types** — the same components. `FacadeSurfaceTest` fails the
  build if a public signature names one (C#: any public type outside `App` beyond `Publish`, `Aeron` and the
  two buffer interfaces). The C# test assembly reaches them via `InternalsVisibleTo`.
- **`seqeron-service`**: `Sequencer`, `SequencerService`, `SequencerServer`, `QueuePublisher`,
  `replayer.server`, `tools`, `MetricsExporter`.
- **`sequencer.client`**: the producer components below.

### Producer components

The façades are assembled from these, and seqeron's own tools use them directly. They are public because
those callers sit in other packages, not as a supported way to produce: a producer takes a façade.

| type | role |
|---|---|
| `ClusterStreamSender` | The cluster session (Java and C# wrap `AeronCluster`). `connectColocated(aeron, memberId, …)` uses IPC ingress to the local member, falling back to UDP when it is not leader; `connect(…)` is UDP only. Both take the UDP ingress endpoint list `"0=host:port,1=host:port,…"`, needed for fallback and reconnect. The default (`PortLayout.ingressEndpoints()` in Java/C#, `protocol::ingressEndpointsCsv()` in C++, offset by `SEQERON_PORT_BASE`) covers the hosts in `SEQERON_HOSTS`, else every member on `localhost`; `PortLayout.parseHosts` and `ingressEndpoints(List)` (C++ `parseHosts`, `ingressEndpointsCsv(hosts)`) build any other. C++ connects to all members concurrently and follows a follower's redirect to the leader. `send` spins through back pressure and elections. Call `keepAlive()` and `pollEgress()` every duty cycle. |
| `IngressPublisher` | Encode and offer; returns `protocol.Publish`: `Published`, `Refused` (above `MAX_PAYLOAD_LENGTH`; nothing offered; final), or `Declined` (transport result; retryable). `publishPayload` takes the frame's `groupMask`, own group's bit included. Java/C#: `publishPayload`/`publishSystem` on pre-encoded bytes. C++: free functions templated on the encoder, populated via a `Fill`. |
| `offerFrame` (C++) | Offers a caller-encoded message; both `publish*` functions end here. It carries schemaless payloads (spec §13.2), which C++'s encoder-templated `publishPayload` cannot express. Takes the same `IngressTracker`, so confirmation still applies. |
| `SystemFrame` (Java, C#) | Wraps an encoded payload in its envelope and returns the length; the offer is the caller's. |
| `PendingSends` | Confirmed ingress. A successful offer is not a sequenced message, and a failover or a closed session silently drops what the leader had not committed. Install it as `IngressPublisher`'s tracker and on the sender via `setIngressHold`, feed it every frame off your own queue and each leadership term, and call `resendMissing` every duty cycle (spec §16 A-4, A-5). `isClosed(clusterSessionId)` says the queue has shown that session closed: replace it. |

`IngressTracker` (implemented by `PendingSends`) and `IngressSender` (implemented by `ClusterStreamSender`)
are seams; implement them only to replace those types.

## Distribution

| | |
|---|---|
| Java | `org.limitless:seqeron`, versioned by `org.limitless:seqeron-bom`, from JitPack |
| C++ | `seqeron::seqeron_core`, header-only, via `FetchContent` over the checkout or `find_package(seqeron)` on an installed prefix |
| C# | `Org.Limitless.Seqeron` on nuget.org, `net10.0`, pinning its Aeron.NET and SBE runtime versions exactly (spec **V-1**) |

JitPack builds the Java artifacts from a release tag. `seqeron-bom` pins Aeron, Agrona and SBE to the
versions seqeron was built against; without it Gradle resolves the higher Aeron, and a protocol mismatch
surfaces only as a decode failure at runtime. Applications import the BOM as `enforcedPlatform`; libraries
built on seqeron use `platform`.

```gradle
repositories {
    mavenCentral()
    maven { url 'https://jitpack.io' }
}
dependencies {
    implementation enforcedPlatform('com.github.FredrikJDahlberg.seqeron:seqeron-bom:<tag>')
    implementation 'com.github.FredrikJDahlberg.seqeron:seqeron'   // or seqeron-service
}
```

**Examples.** `seqeron-examples` builds against the published artifacts, outside this build: `src/java`,
`src/cpp`, `src/csharp`. `FollowStream` is the minimal client. `ColocatedApp` uses [`app`](#façades-app)
alone, and its build fails on any reference outside `app` beyond `protocol.Publish` (plus `util` in C++,
which Java and C# get from their standard libraries and Agrona).

**API reference.** Java: `./gradlew :seqeron-client:javadoc` (`seqeron-client/build/docs/javadoc/`, also
the javadoc jar). C++: the CMake `docs` target (`<build>/docs/html`, Doxygen, `detail/` excluded). C#:
`doc/site/csharp.Doxyfile` ([`building.md`](building.md)); the package ships XML docs and a symbols
package. All exclude the generated codecs. Each Java package's `package-info.java` mirrors
[Packages](#packages); keep them in step.
