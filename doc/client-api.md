# Client API

What a process that talks to a seqeron cluster programs against, in Java, C++ and C#. It is the **client
tier**: the `org.limitless:seqeron` artifact in Java, the `seqeron::seqeron_core` CMake target in C++ and
the `Org.Limitless.Seqeron` package in C#. Everything in `seqeron-service` — the sequencer, the Replayer
server, the tools, the metrics exporter — runs the cluster and is not for clients.

The classes this page names are the API. Other classes in the client tier are public only because a class
in another package needs them ([Not API](#not-api)). The wire contract beneath all of it is
[`seqeron-protocol-spec.md`](seqeron-protocol-spec.md), and [`overview.md`](overview.md) explains the design.

## Choosing an entry point

Most clients take one of the two façades and never see the classes beneath them.

| you are writing | take |
| --- | --- |
| an edge producer deployed as an active/standby pair: an exchange session, a client gateway, a feed handler | [`Gateway`](#gateway) |
| an application with one replica per member, which publishes from the leader's replica | [`Application`](#application) |
| a consumer that only reads the stream | [`ReplayerStreamReceiver`](#consuming-the-ordered-stream) |
| a producer with a duty cycle of its own | the receiver with `ClusterStreamSender`, `IngressPublisher` and `PendingSends` ([Producing](#producing)) |
| a reader of a recording, outside a live client | `SequencedFrameDecoder` (Java, C#), `unwrapFrame` or `ClusterStreamClient` (C++) |

## Packages

The packages follow Aeron's layout: a component's server at its root, its client in `.client` beside it,
and what both sides share in `protocol`.

| package | tier | holds |
|---|---|---|
| `protocol` | client | the wire contract in code: `FrameLayer`, `SystemFrame`, `SequencedFrameDecoder`, `PortLayout`, `ReplayProtocol`, `SeqeronCounters` and `Publish` (C++: `SequencedFrame.hpp`, `PortLayout.hpp`, `ReplayProtocol.hpp`, `SeqeronCounters.hpp`, `Publish.hpp`) |
| `sequencer.client` | client | producing: `ClusterStreamSender`, `IngressPublisher`, `PendingSends`, `IngressTracker` (C++ also `ClusterStreamClient`) |
| `replayer.client` | client | consuming: `ReplayerStreamReceiver`, its three callback types (`SequencedHandler`, `LeadershipHandler`, `CaughtUpHandler`), `SnapshotRestoreHandler`, `SnapshotStore`, and in Java and C# `SequencedEvent`. The C++ `SequencedEvent` is in `protocol` (`SequencedFrame.hpp`), beside the `unwrapFrame` that fills it and the `decodeSystem`/`decodeSequenced` that read it |
| `app` | client | the two façades and the blocks they are built from |
| `util` | client | support code |
| `sbe.frame`, `sbe.replay` | client | generated codecs |
| `sequencer`, `replayer.server`, `tools`, `metrics`, `sbe.probe` | service | the cluster itself; not for clients |

The languages differ only in spelling:

- **C++** uses the same directories and namespaces (`org::limitless::seqeron::sequencer::client`, …) for
  the client tier, which is all of it, and a `detail` namespace beneath a package for what Java makes
  package-private.
- **C#** uses the same namespaces in PascalCase (`Org.Limitless.Seqeron.Sequencer.Client`, …), makes
  `internal` what Java makes package-private, and has the Java classes under the Java names, with
  properties where Java has getters. Interfaces take an `I` (`IIngressTracker`, `ISnapshotRestoreHandler`,
  `ISnapshotListener`), and the receiver's three callbacks are delegates. It has no `ClusterStreamClient`.

## Getting it

| | |
|---|---|
| Java | `org.limitless:seqeron`, versioned by `org.limitless:seqeron-bom`, from JitPack (below) |
| C++ | `seqeron::seqeron_core`, header-only, from `FetchContent` over the checkout or `find_package(seqeron)` on an installed prefix |
| C# | `Org.Limitless.Seqeron` on nuget.org, for `net10.0`, naming its Aeron.NET and SBE runtime versions exactly (spec **V-1**) |

The Java artifacts come from JitPack, built from a release tag. `seqeron-bom` pins Aeron, Agrona and SBE at
the versions seqeron was built against. Without it, Gradle takes the higher of your Aeron and seqeron's,
and a version the cluster does not speak fails only when frames do not decode. An application takes the
BOM as `enforcedPlatform`; a library built on seqeron takes `platform`, which leaves the final choice to
its own consumer.

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

**Examples.** `seqeron-examples` builds against these, outside this repository's build: `src/java` against
the artifact, `src/cpp` against the CMake target, `src/csharp` against the package. `FollowStream` is the
smallest complete client in each language. `ColocatedApp` is the same flow written against the
[front door](#the-front-door-app) alone, and its build fails if it names anything outside `app` beyond
`protocol.Publish` (and, in C++, `util`, which Java and C# take from their standard libraries and Agrona).

**Reference documentation.** `./gradlew :seqeron-client:javadoc` renders this surface at
`seqeron-client/build/docs/javadoc/`, and the same pages ship as the artifact's javadoc jar, so an IDE
shows them. The C++ counterpart is the CMake `docs` target (`cmake --build <build> --target docs`, into
`<build>/docs/html`, Doxygen required), over the headers less `detail/`. Both leave out the generated
codecs, which the schema documents. The C# package carries its XML docs beside the assembly, and a symbols
package beside it. Each Java package states its role in its `package-info.java`, so the index and an IDE's
completion show the table above; keep the two in step.

## Consuming the ordered stream

**`ReplayerStreamReceiver`** (`replayer.client`, every language) is the one entry point for reading. It
replays this node's history through the co-located Replayer, switches to the live tap once caught up,
heals gaps by itself, and delivers every frame once, in `globalSeqNo` order. A façade owns one and hands
out `Payload`s, so a producer that takes a façade never constructs it.

"This node" is a cluster member, or a **gateway host**: a host that runs no member but runs
`start-gateway-host.sh`, whose Replayer relays a member's tap onto the host's own
([`fault-tolerance.md`](fault-tolerance.md#33-gateway-host)). A client there attaches to the host's Aeron
directory and passes the host's node id, one no member uses, where it would pass a `memberId`; nothing else
changes. `Gateway` works there as on a member; `Application` needs [`offCluster`](#application), since no
leadership there is its own.

| call | Java | C++ | C# |
|---|---|---|---|
| construct | `(clientId, onSequenced, onLeadershipChanged, onCaughtUp)` | `(clientId, onSequenced, onConnected, onDisconnected, onLeadershipChanged, onCaughtUp)` | as Java, the callbacks delegates |
| restore a snapshot first | `restoreFrom(sourceId, SnapshotStore, SnapshotRestoreHandler)`, before `start` | `restoreFrom(sourceId, SnapshotStore&, SnapshotRestoreHandler&)`, before `start` | `RestoreFrom(sourceId, SnapshotStore, ISnapshotRestoreHandler)`, before `Start` |
| attach | `start(aeron, memberId)` | `start(aeron, memberId)` | `Start(aeron, memberId)` |
| each duty cycle | `poll()` | `poll()` | `Poll()` |
| start over | `restart()` | `restart()` | `Restart()` |
| state | `isCaughtUp()`, `lastGlobalSeqNo()`, `currentLeaderMemberId()`, `restoreFailure()` | the same; `restoreFailure()` is a `std::optional<std::string>` | the same, as properties; `RestoreFailure` is null when there is none |
| release | `close()` | destructor | `Dispose()` |

All calls belong to one thread.

**Restoring a snapshot.** With `restoreFrom`, the cold start first takes the newest snapshot file in the
store that the log confirms: the Replayer holds the source's sequenced `SnapshotEnd` for that round, and the
file matches it. It hands the file's header and records to the handler, a bounded number per `poll()`,
before it dispatches anything, then dispatches from the frame after the round's cut
([`snapshot.md`](snapshot.md) §7). A file the log does not confirm gives way to the next older one, and the
last to a walk from `globalSeqNo` 1. A snapshot it cannot restore — a format or header version the handler
does not read, or records that fail the file's end — stops recovery for good, and `restoreFailure()` says
why. `restart()` starts a receiver over as a cold start, restoring again and dispatching every frame after
the restored cut once more; it is what a passive gateway instance's activation uses.

**Client ids.** The `clientId` must be unique among the clients on one node. Two that share one cancel each
other's replays, and neither ever catches up. The co-located Replayer notices within a couple of seconds:
it logs the collision, sets `seqeron_replayer_client_id_collision` (type id 5108) and tells both clients
(spec **R-4**). Their next `poll()` then throws — `IllegalStateException` in Java, `std::runtime_error` in
C++, `InvalidOperationException` in C# — as does a façade's `doWork()`. The Replayer cannot tell which
client was there first, so both stop.

This repository's own processes use ids 1–22 and 31–43, so an application's should start at 23 and skip
those:

| `clientId` | used by |
|---|---|
| 1 | `start-three-node-cluster.sh`'s per-member probe; `docker-failover-test.sh`'s observer |
| 7 | `replay-bench.sh`'s probe; `seqeron-examples` (Java) |
| 8 | `seqeron-examples` (C++) |
| 9 | `ClusterProbe follow`/`confirm` default |
| 10 | `TestGateway serve` default; `chaos-runner.sh`'s second consumer |
| 11, 12 | `failover-test.sh`'s two producers |
| 13, 14 | `seqeron-examples` `ColocatedApp`, Java then C++ |
| 15, 16 | `seqeron-examples` `GatewayApp`, GW-EX-A then GW-EX-B |
| 17, 18 | `seqeron-examples` `SnapshotApp`, Java then C++ |
| 19 | `seqeron-examples` `FollowStream` (C#) |
| 20 | `seqeron-examples` `ColocatedApp` (C#) |
| 21, 22 | `seqeron-examples` `GatewayApp` (C#), GW-EX-CS-A then GW-EX-CS-B |
| 31–36 | `csharp-client-test.sh`'s C# probes |
| 41–43 | `csharp-windows-test.sh`'s C# probes |

**Where each frame arrives.**

| frame | Java and C# | C++ |
|---|---|---|
| `LeadershipChanged` | `onLeadershipChanged` only | `onLeadershipChanged` only |
| `ConnectionOpened` / `ConnectionClosed` | `onSequenced`, `isSystem()` true | `onConnected` / `onDisconnected` (`LifecycleEvent`) only |
| any other system frame | `onSequenced`, `isSystem()` true | `onSequenced`, `system` true |
| application frame | `onSequenced` | `onSequenced` |

C++ has two more callbacks because its receiver keeps the callback set `ClusterStreamClient` already had,
so a consumer can swap one stream source for the other. They carry a `LifecycleEvent`, the frame's identity
and its payload, from which `decodeSystem<ConnectionOpened>(event)` reads `connectionData`. The Java and C#
receivers are the only source there is, and deliver both events through `onSequenced` like any other
system frame.

A frame whose own callback is null (Java, C#) or empty (C++) arrives in `onSequenced` instead, as a system
frame, so no frame is dropped and `globalSeqNo` has no holes. `onSequenced` itself is required; the
constructor throws without it.

**`SequencedEvent`** is what `onSequenced` receives: `replayer.client` in Java and C#,
`protocol/SequencedFrame.hpp` in C++. It is a flyweight, valid only during the call. In Java it is a view
over the `SequencedFrameDecoder` below, with every field named as the decoder names it, plus
`receiveTimeNs()` and `position()`, which belong to the delivery rather than the frame. The events and the
decoders use one name per concept in every language, on either side of the envelope; `sourceSessionId` and
`clusterTimestampNs` say which session and which clock, and their doc comments name the wire field each
reads (`header.sessionId`, `header.timestamp`).

**Decoding.** Split by family first (`isSystem()`), then dispatch on `(payloadId, templateId)` for an
application frame, or on `systemEventType` for a system one; never on `templateId` alone. A system payload
carries no `MessageHeader`, and the event's `blockLength` and `version` are 0 on a system frame, so every
language takes the block length and version from the decoder's compiled constants, for submitted and
synthesized events alike:

- **C++:** `decodeSystem<Decoder>(event)`, and `decodeSequenced<Decoder>(event)` for an application payload
  (`protocol/SequencedFrame.hpp`).
- **Java:** `decoder.wrap(event.buffer(), event.payloadOffset(), Decoder.BLOCK_LENGTH, Decoder.SCHEMA_VERSION)`.
- **C#:** `SbeBuffers.Wrap(view, event.Buffer, event.PayloadOffset, event.PayloadLength)`, then
  `decoder.WrapForDecode(view, 0, Decoder.BlockLength, Decoder.SchemaVersion)`. The SBE runtime reads its
  own `DirectBuffer`, not Agrona.NET's, and `util.SbeBuffers` points one at the same bytes without a copy.

`seqeron-examples` decodes a submitted `ConnectionOpened` and a synthesized `ClusterHeartbeat` in all three
languages.

**Reading without a receiver.** `SequencedFrameDecoder` (Java, C#) and `unwrapFrame`/`FrameView` (C++)
strip the envelope off a raw tap frame, for reading frames from a recording. `ClusterStreamClient` (C++
only, `sequencer/client/ClusterStreamClient.hpp`) reads the sequenced stream out of an Aeron Archive for
bounded scans; it is not the live path.

## Producing

A façade assembles these classes, so a producer that takes one never sees them. A consumer that writes its
own duty cycle uses them directly.

| class | role |
|---|---|
| `ClusterStreamSender` | The cluster session. `connectColocated(aeron, memberId, …)` uses IPC ingress on the co-located member and falls back to UDP when that member is not leading; `connect(…)` uses UDP. Both take the UDP endpoint set, `"0=host:port,1=host:port,…"`, which the fallback and the reconnect both need. Its default (`PortLayout.ingressEndpoints()` in Java and C#, `protocol::ingressEndpointsCsv()` in C++, under `SEQERON_PORT_BASE`) is the members `SEQERON_HOSTS` names, else every member on `localhost`; `PortLayout.parseHosts` and `ingressEndpoints(List)` (C++ `parseHosts`, `ingressEndpointsCsv(hosts)`) build a set from any other list. C++ dials every member at once and takes the first to answer, following a follower's redirect to the leader. `send` spins through back-pressure and elections. Call `keepAlive()` and `pollEgress()` every duty cycle. |
| `IngressPublisher` | Encode and offer, returning `protocol.Publish`: `Published`; `Refused` (above `MAX_PAYLOAD_LENGTH`, nothing offered, permanent); or `Declined` (the transport's answer, worth retrying). Java and C#: `publishPayload`/`publishSystem` on an instance, the payload pre-encoded. C++: free functions templated on the encoder, filled through a `Fill`. |
| `offerFrame` (C++) | Offers a frame the caller has already encoded, and is where both `publish*` functions end. The C++ `publishPayload` is templated on an SBE encoder, so a payload with no schema at all (spec §13.2) is framed by the caller and offered here; Java's and C#'s take payload bytes and carry any encoding. It takes the same `IngressTracker`, so a hand-framed payload is confirmed like any other. |
| `SystemFrame` (Java, C#) | Wraps an encoded payload in its envelope and returns the length, leaving the offer to the caller. `IngressPublisher` uses it; call it directly only to place frames yourself. |
| `PendingSends` | Confirmed ingress. A send that succeeds is not a frame sequenced, and a failover silently loses what the old leader had not committed. Give it to `IngressPublisher` as its tracker and to the sender with `setIngressHold`, feed it your own tap and each leadership term, and call `resendMissing` (spec §16 A-4, A-5). |

`IngressTracker` is the interface `PendingSends` implements, and `IngressSender` the one
`ClusterStreamSender` implements. Implement them only to replace those classes.

## The front door (`app`)

A façade is the whole duty cycle for one kind of producer: the cluster session, the tap, confirmed ingress,
the fences, and the election or leader gate, in the order they must run. An application that takes one
writes its edge and its payloads, and nothing of the frame layer or of seqeron's system vocabulary appears
in its code.

Every language has both façades, with the same calls under the same names. The tables below give Java's
signatures; C++ and C# differ only here:

| | Java | C++ | C# |
|---|---|---|---|
| construct | `Gateway.builder()…build()` | `Gateway<Listener>{ config, listener }`, where `Config` is an aggregate with one field per builder setter | `new Gateway(new GatewayOptions { … })`, with one `init` property per builder setter; what `build()` requires is `required` |
| listener | implements `Gateway.Listener` | any type satisfying the `GatewayListener` / `ApplicationListener` concept | implements `IGatewayListener` / `IApplicationListener` |
| ingress | `ingressEndpoints(…)`, defaulting to `PortLayout.ingressEndpoints()` | `Config::ingressEndpoints`, defaulting to `protocol::ingressEndpointsCsv()` | `IngressEndpoints`, defaulting to `PortLayout.IngressEndpoints()` |
| `publish` / `reply` | payload bytes, its own `MessageHeader` included | the same bytes, or `publish<Encoder>(…, fill)`, where `fill(Encoder&)` stamps an encoder already wrapped with its header | the same bytes, or a `ReadOnlySpan<byte>`, copied once into a buffer the façade owns |
| payload body | `buffer()` at `bodyOffset()`/`bodyLength()` | `body()`/`bodyLength()`, or `decode<Decoder>()` | `Buffer` at `BodyOffset`/`BodyLength` |
| headerless payload (spec §13.2) | `buffer()` at `payloadOffset()`/`payloadLength()` | `payload()`/`payloadLength()` | `Buffer` at `PayloadOffset`/`PayloadLength` |
| release | `close()`, or try-with-resources | `close()` | `Dispose()`, or `using` |

Java has only bytes because its SBE codecs share no interface, and a payload with no schema needs bytes in
any language. C#'s options classes and listener interfaces are top-level types where Java nests its
builders and listeners, as .NET's design guidelines keep public types out of one another.

**A closed surface.** A façade's whole surface is `app` plus `protocol.Publish`, which `publish` and
`reply` return (`protocol::Publish` in C++). `Publish` sits in `protocol`, not on `IngressPublisher`, so
that taking a façade never means importing from `sequencer.client`, and the ingress endpoints default to
`PortLayout.ingressEndpoints()`, so a deployment on the default port block names nothing outside `app` at
all. Everything else a listener sees — `Payload`, `ClusterError` — is `app`'s own. `FacadeSurfaceTest`
fails the build, in Java and in C#, when that stops holding.

**`Payload`** is what a façade's `onSequenced` receives: one application payload with the envelope off. It
carries `globalSeqNo`, `sourceId`, `connectionId`, `sourceSessionId`, `clusterTimestampNs`,
`receiveTimeNs`, `position()`, and `(payloadId, templateId)` to dispatch on; no system frame ever arrives as
one. `bodyOffset()` and `bodyLength()` take the payload's own `MessageHeader` off as well, which is where an
SBE decoder wraps (C++: `body()`, or `decode<Decoder>()` to wrap one in a single call). A payload with no
header at all (spec §13.2, what C++'s `offerFrame` exists for) is addressed by `payloadOffset()` and
`payloadLength()` (C++: `payload()`), and its `templateId`, `blockLength` and `version` mean nothing.
`position()` is the frame's first byte in this node's recording: a delivery stamp, like `receiveTimeNs()`,
and the anchor for a consumer that replays the recording itself, as a FIX gateway serving its own resends
indexes it per outbound message.

**The cluster clock.** Both façades deliver `onClusterHeartbeat(clusterTimeNs, receiveTimeNs)` once a
second. It is the one time source that keeps advancing while every producer is silent, which is when a
watchdog must still fire, and it is identical on every node, so a timer driven by it decides the same thing
everywhere. Run deadlines off it rather than a local clock; between ticks, every payload carries its own
`clusterTimestampNs()`.

**Tap lag is not a fence.** How far a node runs behind the cluster is a property of the node, not the
client: it raises no fence and changes no behaviour, and [`ops.md`](ops.md) graphs it per member as
**Node apply lag**.

Both façades take `DEFAULT_TAP_STALL_TIMEOUT_MS` (20 heartbeat periods) and
`DEFAULT_RECOVERY_STALL_TIMEOUT_MS` (three times that), and each builder takes overrides.

### `Gateway`

One instance of an elected active/standby pair. The cluster decides which instance serves; the
application supplies the edge.

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

| call | what it does |
|---|---|
| `doWork()` | one duty-cycle iteration: the cluster session, the tap, confirmed ingress, the fences, the connection lifecycle and the election, in the order they require |
| `gateClosed()` | reports that the edge closed by itself — a dial that failed, a counterparty that hung up. The designation stands, so `doWork()` reopens it through `onActivated`, without a second `GatewayStarted`. An acceptor never calls it; an initiator, whose edge is one dial, does |
| `keepAlive()` | tells the cluster this instance is alive. `doWork()` already does it every cycle; this is for a listener callback that spins, since `doWork()` cannot run until it returns, and a session quiet for `sequencer.sessionTimeoutMs` is dropped |
| `canAccept()` | whether a connection may be taken now: serving, and ingress not held behind a failover's resend |
| `openConnection()` / `openConnection(data, length)` | allocates the connection's id and places its `ConnectionOpened`, retried by `doWork()` |
| `closeConnection(id)` | the same for a connection that has gone; one the cluster never heard of is dropped rather than announced |
| `publish(connectionId, payloadId, payload, length)` | submits one payload, stamped with this gateway's `sourceId`; `Declined` is worth retrying. C++ also has `publish<Encoder>(connectionId, payloadId, fill)` |
| `isActivated()`, `isServing()`, `isPassive()`, `sourceId()`, `gatewayId()`, `isCaughtUp()`, `lastGlobalSeqNo()` | what the instance knows about itself; `sourceId()` and `gatewayId()` read `UNRESOLVED` until a `GatewayRegistered` row names it |

**The listener is the edge.**

| callback | when |
|---|---|
| `onActivated(firstConnectionId)` | this instance has been designated and may serve: open the edge |
| `onStandby()` | another instance was designated: close the edge |
| `onSequenced(Payload)` | an application payload, in order |
| `onConnectionOpened` / `onConnectionClosed` | this logical gateway's connection lifecycle, off the log, whichever instance issued it. It is how an instance that keeps per-connection state rebuilds it while replaying, and the only notice of a client that drops its socket without logging out |
| `onCaughtUp(globalSeqNo)` | every transition to caught up |
| `onClusterHeartbeat(clusterTimeNs, receiveTimeNs)` | the cluster clock, once a second |
| `onFenced(ClusterError, detail)` | once, when the instance can no longer act: release the cluster session, usually by exiting, so a standby takes over |

The `ClusterError` values are the cluster session lost, ingress confirmation faulted, recovery stalled, the
tap stalled, and a snapshot diverged or unrestorable ([`fault-tolerance.md`](fault-tolerance.md) §2.1). A
media driver that goes away raises from `doWork()` instead.

**Snapshots.** A gateway takes the same `SnapshotListener` as an [`Application`](#application), through
the builder's `snapshotListener` (C++ `Config::snapshotListener`). It then requires `sourceId` (C++
`Config::sourceId`), the pair's, since the restore asks about that source's snapshot before any row is
dispatched, and `snapshotDirectory`, the instance's own, not its pair's. Every instance serializes its state
at the cut into its directory, and the instance whose `GatewayStarted` for its current activation has been
placed submits the round's end. The façade's own header in each file carries the pair's rows, the active
instance and the highest connection id, which a restore puts in place of the frames before the cut. The
fences are the same as an application's.

**Passive instances.** With `passive(true)` (C++ `Config::passive`) an instance holds no state until it is
activated. It follows the tap for the election alone; its listener sees no payload and no connection; it
takes part in no round, and so writes no file. When activated it restores the newest confirmed file its
directory holds, left from when it last served, or replays from `globalSeqNo` 1 without one, and serves
once caught up. That races the 5 s activation deadline ([`snapshot.md`](snapshot.md) §4), so a passive
instance suits state that catches up well inside it.

**Reference consumers.** `seqeron-service/src/test/java/org/limitless/seqeron/tools/TestGateway.java` is the
reference consumer. Its C++ twin is `seqeron-examples/src/cpp/GatewayApp.cpp`, a pair with one simulated
connection, loaded from `seqeron-examples/topology.xml`. The C# `seqeron-examples/src/csharp/GatewayApp`
runs a pair of its own from `seqeron-examples/topology-csharp.xml`, and `csharp-client-test.sh` hands that
pair over.

### `Application`

One replica of the producer kind nothing elects: one per member, named in the topology's `<applications>`
section, publishing only while its own member leads. It has the same builder as `Gateway`, taking the
`sourceId` its row declares in place of a gateway name, and connects over its member's own `aeron:ipc`
while that member leads (`DEFAULT_IPC_CONNECT_TIMEOUT_MS`).

| call | what it does |
|---|---|
| `doWork()` | one duty-cycle iteration: the cluster session, the tap, confirmed ingress, the fences, then the leader gate |
| `canPublish()` | whether leader-only work may reach ingress now: the gate is open, and ingress is not held |
| `isLeading()` | whether the gate is open at all |
| `publish(payloadId, payload, length)` | submits one payload of the application's own, on its `sourceId` and no connection. C++ also has `publish<Encoder>(payloadId, fill)` |
| `reply(requesterSourceId, connectionId, payloadId, payload, length)` | the same on behalf of the producer that asked, under the **requester's** `sourceId` and `connectionId`, which is how the gateway that took the request routes the answer back out. C++ also has `reply<Encoder>(requesterSourceId, connectionId, payloadId, fill)` |
| `sourceId()`, `isCaughtUp()`, `lastGlobalSeqNo()` | what the replica knows about itself |

**The leader gate.** The listener's `onLeadershipChanged(boolean leading)` takes the place of `Gateway`'s
`onActivated` and `onStandby`; `onSequenced`, `onCaughtUp`, `onClusterHeartbeat` and `onFenced` are the
same, and there is no connection lifecycle, since an application nothing elects owns no connections.
**Every leadership change closes an open gate**, so `false` is where `OutstandingWork.onNotLeader()`
belongs: a reply submitted during the election may have gone with it, and the next opening dispatches it
again. Keep a request's `sourceId` and `connectionId`, not its `Payload`: the flyweight is valid only
during its callback, and a reply is usually dispatched later.

**Off the cluster.** On a gateway host, `offCluster(true)` (C++ `Config::offCluster`) makes the replica the
one instance rather than one of three. Its gate opens once it is caught up, whoever leads, and still closes
for one cycle on every leadership change; ingress is UDP alone. Nothing elects between two such instances,
and the sequencer does not refuse a second one's payloads, so run exactly one. An application that needs a
standby off the cluster is a `Gateway`.

**Snapshots** ([`snapshot.md`](snapshot.md)). The builder's `snapshotListener(SnapshotListener)` (C++
`Config::snapshotListener`, a `SnapshotListener*` that must outlive the replica) makes the application take
part in snapshot rounds, if its topology row also says `snapshot="true"`; without a listener it takes part
in none. It requires `snapshotDirectory(Path)` (C++ `Config::snapshotDirectory`, a
`std::filesystem::path`): the replica's own directory, one file per round, which must outlive the process
([`snapshot.md`](snapshot.md) §4.1).

| listener call | Java | C++ | C# |
|---|---|---|---|
| write the next record | `onSnapshot(MutableDirectBuffer buffer, int recordIndex)` | `onSnapshot(std::span<std::uint8_t> buffer, std::int32_t recordIndex)` | `OnSnapshot(IMutableDirectBuffer, int)` |
| read one record back | `onRestore(DirectBuffer buffer, int length, int recordIndex)` | `onRestore(std::span<const std::uint8_t> record, std::int32_t recordIndex)` | `OnRestore(IDirectBuffer, int length, int recordIndex)` |
| the record format | `formatVersion()` | `formatVersion()` | `FormatVersion` |

1. **At each round's cut**, before the next frame is dispatched, every replica's façade calls `onSnapshot`
   from `recordIndex` 0 until it returns 0. Each call encodes the next record of the state into the buffer,
   at most its 65,535-byte capacity, and returns its length; `recordIndex` 0 is where an iteration starts
   over, and a length outside 0–65,535 drops the round.
2. **Every replica must produce the same records for the same state:** no hash-map iteration order, no
   local time, no node identity.
3. **Every replica writes its own file.** The one whose gate is open at the cut submits the round's
   `SnapshotEnd`; every replica compares the sequenced end with its own, and is fenced with
   `SNAPSHOT_DIVERGED` if it differs.
4. **On start**, a replica with a listener restores the newest file of its own that the log confirms
   before it dispatches anything: `onRestore` once per record, in the order `onSnapshot` wrote them, with
   `recordIndex` 0 where the state is cleared. It then dispatches from the frame after the cut. With no
   confirmed file it replays from `globalSeqNo` 1.
5. **A file it cannot read** — a `formatVersion` the listener does not support, or damaged records — fences
   it with `SNAPSHOT_UNRESTORABLE` (C++ and C# `ClusterError::SnapshotUnrestorable`; the divergence fence is
   `ClusterError::SnapshotDiverged`).

**Reference consumers.** `seqeron-examples/src/java/example/ColocatedApp.java` and its twins,
`ColocatedApp.cpp` and `src/csharp/ColocatedApp`, are the reference consumers. With the two `GatewayApp`s,
they are the only clients in the repository whose builds refuse anything outside `app`
(`checkFacadeOnly`, the same check in `seqeron-examples/CMakeLists.txt`, and
`src/csharp/Directory.Build.targets`).

## Underneath (`app`)

The façades are assembled from pure state machines that do no I/O, each with a twin in every language.
Only one is offered, being about the application's own work rather than seqeron's plumbing:

| class | for |
|---|---|
| `OutstandingWork` | leader-only request/reply work, dispatched again after a failover |

The election, the leader gate and the two stall fences are package-private in Java and `internal` in C#,
since only the façades assemble them; their C++ twins are in `app::detail` ([Not API](#not-api)).
Confirmed ingress, the one block a duty cycle of your own needs, is in [`sequencer.client`](#producing)
beside the `IngressTracker` it implements.

## Constants

| where | what |
|---|---|
| `FrameLayer` (Java, C#), `SequencedFrame.hpp` (C++) | the tap's identity (`FEEDER_STREAM_ID` 205) and the size limits of spec §12; in Java and C# also the heartbeat interval |
| `SystemFrame` (Java, C#), `SequencedFrame.hpp` (C++) | the `systemEventType` values |
| `PortLayout` | the cluster's port block, each member's ingress endpoint, the endpoint set a producer connects with (`ingressEndpoints()`, or `ingressEndpoints(parseHosts("h0,h1,h2"))`; C++ `ingressEndpointsCsv`), and the co-located archive's control channel; honours `SEQERON_PORT_BASE` and `SEQERON_HOSTS` |
| `ReplayProtocol` | the replay protocol's channel and stream ids, and `NO_REPLAY_NEEDED` |
| `Publish` | what a publish did: `Published`, `Refused` (permanent) or `Declined` (retryable); returned by `IngressPublisher` and by both façades' `publish` |

## Wire codecs

The generated SBE codecs for `sbe-frame.xml` (`org.limitless.seqeron.sbe.frame` in Java,
`org_limitless_seqeron_sbe_frame` in C++, `Org.Limitless.Seqeron.Sbe.Frame` in C#) are API, because the
schema is. A producer encodes system payloads with them and a consumer decodes them. They change only when
the protocol does, under spec **V-3**. The `sbe-replay.xml` codecs ship too, but only
`ReplayerStreamReceiver` speaks that protocol.

## Support code

`util` (`Logger`, `IdleStrategies`/`IdleStrategy.hpp`, `Clocks`, `Env.hpp` in C++ and `SbeBuffers` in C#)
and `protocol`'s `SeqeronCounters` ship in the client tier because the classes above use them. You may use
them, but they are infrastructure rather than what seqeron offers, and carry no promise of stability.
`SbeBuffers` is the exception: a C# consumer decoding with SBE codecs needs it (above).
`Clocks.monotonicMs()` is what every duty-cycle deadline here is measured against; C++ has no twin, since
`nowMs()` is a free function in the one header that needs it.

## Not API

Header-only C++ has no package-private, so what Java hides that way C++ puts in a `detail` namespace, in a
`detail/` directory beside the package it serves. **Nothing under a `detail` is API**, and a public
signature that names one is a test seam. Do not build on:

- **`replayer/client/detail`:** `ReplayerRecovery` and `ReplayerRecoveryActions`, the seam the unit suites
  drive.
- **`sequencer/client/detail/Transport.hpp`:** `IngressTransport`/`EgressTransport` and their Aeron
  implementations, the seam `ClusterStreamSender`'s `connect` overloads take in tests.
- **`app/detail`:** the blocks a façade assembles — `Session` (the cluster session, the tap, confirmed
  ingress and the fences in one duty cycle), `GatewayLifecycle`, `LeaderGate`, `RecoveryStallFence` and
  `TapStallFence`. In Java these are package-private, and `FacadeSurfaceTest` fails the build if a public
  signature names one. Beside them, `makePayload` builds the `Payload` a façade would hand a listener, so
  a consumer can unit-test its own handlers.
- **C#'s `internal` types:** the same blocks as Java's package-private ones. The test assembly reaches them
  through `InternalsVisibleTo`, and `FacadeSurfaceTest` fails the build if a public signature names a public
  type outside `App` beyond `Publish`, `Aeron` and the two buffer interfaces.
- **Everything in `seqeron-service`:** `Sequencer`, `SequencerService`, `SequencerServer`, `TapPublisher`,
  `replayer.server`, `tools`, `MetricsExporter`.
