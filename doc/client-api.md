# Client API

What a process that talks to a seqeron cluster programs against. It lives in the **client tier**: the
`org.limitless:seqeron` artifact in Java and the `seqeron::seqeron_core` CMake target in C++. Everything
in `seqeron-service` (the sequencer, the Replayer server, the tools, the metrics exporter) runs the
cluster and is not for clients.

The classes named on this page are the API. Other classes in the client tier are public only because a
class in another package needs them (see [Not API](#not-api)).

The wire contract under all of this is `doc/seqeron-protocol-spec.md`. This page covers the libraries
built on it.

## Packages

Laid out as Aeron's are: a component's server at its root, its client in `.client` beside it, and what
both sides share in `protocol`. C++ uses the same directories and namespaces
(`org::limitless::seqeron::sequencer::client`, …) for the client tier, which is all of it, plus a
`detail` beneath a package for what Java makes package-private ([Not API](#not-api)).

| Package | Tier | Holds |
|---|---|---|
| `protocol` | client | The wire contract in code: `FrameLayer`, `SystemFrame`, `SequencedFrameDecoder`, `PortLayout`, `ReplayProtocol`, `SeqeronCounters`, and `Publish` (C++: `SequencedFrame.hpp`, `PortLayout.hpp`, `ReplayProtocol.hpp`, `SeqeronCounters.hpp`, `Publish.hpp`) |
| `sequencer.client` | client | Producing: `ClusterStreamSender`, `IngressPublisher`, `PendingSends`, `IngressTracker` (C++ also `ClusterStreamClient`) |
| `replayer.client` | client | Consuming: `ReplayerStreamReceiver`, its three callback interfaces (`SequencedHandler`, `LeadershipHandler`, `CaughtUpHandler`), and `SequencedEvent` in Java. The C++ `SequencedEvent` is in `protocol` (`SequencedFrame.hpp`) instead, beside the `unwrapFrame` that fills it and the `decodeSystem`/`decodeSequenced` that read it |
| `app` | client | What a client application is built from: the façades below, and the blocks under them |
| `util` | client | Support code |
| `sbe.frame`, `sbe.replay` | client | Generated codecs |
| `sequencer`, `replayer.server`, `tools`, `metrics`, `sbe.probe` | node | The cluster itself; not for clients |

## Getting it

| | |
|---|---|
| Java | `org.limitless:seqeron`, versioned by `org.limitless:seqeron-bom`; the README's "Example consumer" has the JitPack coordinates |
| C++ | `seqeron::seqeron_core`, header-only, from `FetchContent` over the checkout or `find_package(seqeron)` on an installed prefix |

`seqeron-examples` builds against these, outside this repository's own build — `src/java` against the
artifact and `src/cpp` against the CMake target. `FollowStream` is
the smallest complete client in each language; `ColocatedApp` is the same flow written against the
[front door](#the-front-door-app) alone, in both languages, and its build fails if it names anything
outside `app` beyond `protocol.Publish` (and, in C++, `util`, which Java takes from its own standard
library).

`./gradlew :seqeron-client:javadoc` renders this surface from the sources, at
`seqeron-client/build/docs/javadoc/`, and the same pages ship as the artifact's javadoc jar, so an IDE
resolving `org.limitless:seqeron` shows them. Both leave out the generated SBE codecs, which carry no
comment of their own — the schema is what documents them. The C++ counterpart is the CMake `docs` target
(`cmake --build <build> --target docs`, into `<build>/docs/html`, Doxygen required), over the same surface:
the headers, less `detail/` and the codecs.

Each package states its own role in a `package-info.java`, so the table above is what the javadoc index
and an IDE's package completion show without this page open — `app` says it is the front door, and
`sequencer.client`, `replayer.client` and `util` say what they are for and when to reach past a façade for
them. Keep the two in step: a package whose role changes here changes there.

## Consuming the ordered stream

**`ReplayerStreamReceiver`** (`replayer.client`, both languages) is the entry point. It replays
this node's history through the co-located Replayer, switches to the live tap once caught up, heals gaps
by itself, and delivers every frame once, in `globalSeqNo` order. A producer that takes a
[façade](#the-front-door-app) does not construct one: the façade owns it and hands out `Payload`s.

| Call | Java | C++ |
|---|---|---|
| construct | `(clientId, onSequenced, onLeadershipChanged, onCaughtUp)` | `(clientId, onSequenced, onConnected, onDisconnected, onLeadershipChanged, onCaughtUp)` |
| attach | `start(aeron, memberId)` | `start(aeron, memberId)` |
| each duty cycle | `poll()` | `poll()` |
| state | `isCaughtUp()`, `lastGlobalSeqNo()`, `currentLeaderMemberId()` | the same |
| release | `close()` | destructor |

`clientId` must be unique among the replicas on one node. Two replicas that share one supersede each
other's replays, and neither ever catches up. **Nothing on the client side reports it** — the co-located
`ReplayerService` is what notices the collision, and it says so once in its own log and in the
`seqeron_replayer_client_id_collision` counter (type id 5108), so that node's Replayer is where a replica
that never catches up is diagnosed. `doc/registries.md` §4 records the ids this repo's own processes take.
All calls belong to one thread.

**Where each frame arrives:**

| Frame | Java | C++ |
|---|---|---|
| `LeadershipChanged` | `onLeadershipChanged` only | `onLeadershipChanged` only |
| `ConnectionOpened` / `ConnectionClosed` | `onSequenced`, `isSystem()` true | `onConnected` / `onDisconnected` (`LifecycleEvent`) only |
| any other system frame | `onSequenced`, `isSystem()` true | `onSequenced`, `system` true |
| application frame | `onSequenced` | `onSequenced` |

C++ has the two extra callbacks because its receiver keeps the callback set `ClusterStreamClient` already
had, so a consumer can swap one stream source for the other; the Java receiver is the only source there is
and delivers both events through `onSequenced` like any other system frame. They carry a `LifecycleEvent` —
the frame's identity and no body — so **`ConnectionOpened`'s `connectionData` is reachable in Java and not
through the C++ receiver**. Leaving either one empty is a hole in `globalSeqNo` wherever a connection opens
or closes, the same cost as leaving the leadership callback out.

A frame whose callback is null (Java) or empty (C++) is dropped. A consumer that passes no
leadership callback therefore sees a hole in `globalSeqNo` at every leadership change, `globalSeqNo` 1
included.

**`SequencedEvent`** is what `onSequenced` receives — `replayer.client` in Java,
`protocol/SequencedFrame.hpp` in C++: a flyweight, valid only during the call. In Java it is a view over the
`SequencedFrameDecoder` below and names every field as that decoder does, plus `receiveTimeNs()` and
`position()`, which belong to the delivery rather than to the frame. All four types — both events and both
views — carry one name per concept, so a field reads the same in either language and on either side of the
envelope; `sourceSessionId` and `clusterTimestampNs` say which session and which clock, and each names the
wire field it reads (`header.sessionId`, `header.timestamp`) in its own doc comment. Split by
family first (`isSystem()`), then dispatch on `(payloadId, templateId)` for an application frame or on
`systemEventType` for a system one, never on `templateId` alone. To decode a system body:

- C++: `decodeSystem<Decoder>(event)`, and `decodeSequenced<Decoder>(event)` for an application
  payload (`protocol/SequencedFrame.hpp`). `decodeSystem` takes the block length and version from the
  decoder's own compiled constants for **every** system shape, so C++ has one rule where Java has two.
- Java: the nine system events a producer submits wrap with their decoder's own `BLOCK_LENGTH` and
  `SCHEMA_VERSION`, since the event's `blockLength()` is 0 for them. The three the sequencer synthesizes
  (`LeadershipChanged`, `ClusterHeartbeat`, `GatewayActive`) wrap with the event's `blockLength()` and
  `version()`.

`seqeron-examples` decodes one of each in both languages — a submitted `ConnectionOpened` and a synthesized
`ClusterHeartbeat` — which is the shortest place to read the two rules off working code.

**`SequencedFrameDecoder`** (Java) and **`unwrapFrame`/`FrameView`** (C++) strip the envelope off a raw
tap frame. Use them to read frames without a receiver, for example from a recording.

**`ClusterStreamClient`** (C++ only, `sequencer/client/ClusterStreamClient.hpp`) reads the sequenced stream
out of an Aeron Archive for bounded scans. It is not the live path.

## Producing

A producer takes a [façade](#the-front-door-app) instead of the classes below and never sees them, in
either language; this is what one is assembled from, and what a consumer writing its own duty cycle uses
directly.

| Class | Role |
|---|---|
| `ClusterStreamSender` | The cluster session. `connectColocated(aeron, memberId, …)` uses IPC ingress on the co-located member and falls back to UDP when that member is not leading; `connect(…)` uses UDP. Java: both take the UDP endpoint set — `PortLayout.ingressEndpoints()` is the default one — because the fallback and the reconnect both need it. C++: neither takes one; UDP dials member 0's ingress port on `localhost` (under `SEQERON_PORT_BASE`) and follows the cluster's redirect to the leader, so a C++ producer reaches only a cluster on its own host. `send` spins through back-pressure and elections. Call `keepAlive()` and `pollEgress()` every duty cycle. |
| `IngressPublisher` | Encode and offer. Returns `protocol.Publish`: `Published`; `Refused` (above `MAX_PAYLOAD_LENGTH`, nothing offered, permanent); `Declined` (the transport's answer, worth retrying). Java: `publishPayload`/`publishSystem` on an instance, with the body pre-encoded. C++: free functions templated on the encoder, filled through a `Fill`. |
| `offerFrame` (C++) | Offers a frame the caller has already encoded, and is where both `publish*` functions end. Java's `publishPayload` takes payload bytes, so it carries any encoding; the C++ one is templated on an SBE encoder, and a payload with no schema at all (§13.2) is framed by the caller and offered here. It takes the same `IngressTracker`, so a hand-framed payload is confirmed like any other. |
| `SystemFrame` (Java) | Wraps an encoded body in its envelope and returns the length; the offer is yours. `IngressPublisher` uses it; call it directly only to place frames yourself. |
| `PendingSends` | Confirmed ingress. A send that succeeds is not a frame sequenced, and a failover silently loses what the old leader had not committed. Give it to `IngressPublisher` as its tracker and to the sender with `setIngressHold`, feed it your own tap and each leadership term, and call `resendMissing`. Spec §16 A-4, A-5. |

`IngressTracker` is the interface `PendingSends` implements, and `IngressSender` the one
`ClusterStreamSender` implements. Implement them only to replace those classes.

## The front door (`app`)

The assembled duty cycle, one façade per kind of producer, over the pieces above. A consumer that
takes one of these writes its edge and its payloads, and nothing of the frame layer or of seqeron's
system vocabulary appears in its code. Both languages have both façades, `app/Gateway.hpp` and
`app/Application.hpp` in C++, with the same calls under the same names. The tables below use
Java's shapes; C++ differs only here:

| | Java | C++ |
|---|---|---|
| construct | `Gateway.builder()…build()` | `Gateway<Listener>{ config, listener }`, where `Config` is an aggregate with one field per builder setter |
| listener | implements `Gateway.Listener` | any type satisfying the `GatewayListener` / `ApplicationListener` concept |
| ingress | `ingressEndpoints(…)`, defaulting to `PortLayout.ingressEndpoints()` | none: member 0 on `localhost`, following the cluster's redirect (see [`ClusterStreamSender`](#producing)) |
| `publish` / `reply` | payload bytes, its own `messageHeader` included | the same bytes, or `publish<Encoder>(…, fill)`, where `fill(Encoder&)` stamps an encoder already wrapped with its header |
| payload body | `buffer()` at `bodyOffset()`/`bodyLength()` | `body()`/`bodyLength()`, or `decode<Decoder>()` |
| headerless payload (§13.2) | `buffer()` at `payloadOffset()`/`payloadLength()` | `payload()`/`payloadLength()` |
| release | `close()`, or try-with-resources | `close()` |

Bytes are the only form Java has, because Java's SBE codecs share no interface; a payload with no schema at
all needs them in either language.

A façade's whole surface is `app` plus `protocol.Publish`, which `publish` and `reply` return
(`protocol::Publish` in C++). It sits in `protocol` rather than on `IngressPublisher` so that taking a
façade does not mean importing from `sequencer.client`; the builders' `ingressEndpoints` defaults to
`PortLayout.ingressEndpoints()`, so a deployment on the default port block names nothing outside `app` at
all. Everything else a listener sees — `Payload`, `ClusterError` — is `app`'s own, and `FacadeSurfaceTest` is what
fails the build when that stops holding.

**`Payload`** is what `onSequenced` receives: the envelope is off, and `bodyOffset()`/`bodyLength()` take
the payload's own `MessageHeader` off too, which is where an SBE decoder wraps (C++: `body()`, or
`decode<Decoder>()` to wrap one in a single call). A payload that carries no header at all (§13.2, what
C++'s `offerFrame` exists for) is addressed by `payloadOffset()`/`payloadLength()` (C++: `payload()`)
instead, and its `templateId`, `blockLength` and `version` mean nothing. It carries `globalSeqNo`, `sourceId`,
`connectionId`, `sourceSessionId`, `clusterTimestampNs`, `receiveTimeNs`, `position()`, and
`(payloadId, templateId)` to dispatch on. No system frame ever arrives as one. `position()` is the frame's
first byte in this node's recording — a delivery stamp like `receiveTimeNs()`, and what a consumer that
replays that recording itself anchors on: a FIX gateway serving its own resend indexes it per outbound
message.

### `Gateway`

One instance of an elected active/standby pair.

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

| Call | What it does |
|---|---|
| `doWork()` | one duty-cycle iteration: the cluster session, the tap, confirmed ingress, the fences, the connection lifecycle and the election, in the order they require |
| `gateClosed()` | the edge closed by itself — a dial that failed, a counterparty that hung up; the designation stands, so `doWork()` reopens it through `onActivated` without a second `GatewayStarted`. An acceptor never calls it; an initiator, whose edge is one dial, does |
| `keepAlive()` | tells the cluster this instance is alive; `doWork()` already does it once a cycle, so this is only for a `Listener` callback that spins — `doWork()` cannot run again until it returns, and a session quiet for `sequencer.sessionTimeoutMs` is dropped |
| `canAccept()` | whether a connection may be taken right now — serving, and ingress is not held behind a failover's resend |
| `openConnection()` / `openConnection(data, length)` | allocates the id and places its `ConnectionOpened`, retried by `doWork()` |
| `closeConnection(id)` | the same for a connection that has gone; one the cluster never heard of is dropped rather than announced |
| `publish(connectionId, payloadId, payload, length)` | submits one payload, stamped with this gateway's `sourceId`; `Declined` is worth retrying. C++ also has `publish<Encoder>(connectionId, payloadId, fill)` |
| `isActivated()`, `isServing()`, `sourceId()`, `gatewayId()`, `isCaughtUp()`, `lastGlobalSeqNo()` | what the instance may say about itself; `sourceId()` and `gatewayId()` read `UNRESOLVED` until a `GatewayRegistered` row names it |

`Listener` is the edge: `onActivated(firstConnectionId)` opens it and `onStandby()` closes it,
`onSequenced(Payload)` delivers application payloads in order, `onConnectionOpened`/`onConnectionClosed`
report this logical gateway's connection lifecycle off the log — whichever instance issued it, which is how
an instance that keeps per-connection state rebuilds it while it replays, and the only notice of a client
that drops its socket without logging out — `onCaughtUp(globalSeqNo)` fires on every
transition, and `onFenced(ClusterError, detail)` fires once — release the cluster session, usually by exiting, so
a standby takes over. The four `ClusterError` values are the cluster session lost, ingress confirmation faulted,
recovery stalled, and the tap stalled; a media driver that goes away raises from `doWork()` instead.

Tap lag is deliberately **not** the client tier's business: it raises no fence and changes no
behaviour. How far a node runs behind the cluster is a property of the node, and `doc/ops.md` graphs it
per member (**Node apply lag**).

`onClusterHeartbeat(clusterTimeNs, receiveTimeNs)` is what the heartbeat exists for, and both
façades deliver it. It is the cluster clock's tick: the one time source that keeps advancing while every
producer is silent — exactly when a watchdog must still fire — and identical on every node, so a timer
driven by it decides the same thing everywhere. A producer with a deadline runs it off this rather than
off a local clock. Between ticks, every payload carries its own `clusterTimestampNs()`.

`seqeron-service/src/test/java/org/limitless/seqeron/tools/TestGateway.java` is the reference consumer; its C++
twin is `seqeron-examples/src/cpp/GatewayApp.cpp`, a pair with one simulated connection, loaded from
`seqeron-examples/topology.xml`.

### `Application`

One replica of the producer kind nothing elects: one per node, named in the topology's `<applications>`
section, publishing only while its own node leads. Same builder shape as `Gateway`, taking the
`sourceId` its row declares instead of a gateway name, and connecting over its own member's `aeron:ipc`
while that member leads (`DEFAULT_IPC_CONNECT_TIMEOUT_MS`).

| Call | What it does |
|---|---|
| `doWork()` | one duty-cycle iteration: the cluster session, the tap, confirmed ingress, the fences, then the leader gate |
| `canPublish()` | whether leader-only work may reach ingress right now — the gate is open, and ingress is not held |
| `isLeading()` | whether the gate is open at all |
| `publish(payloadId, payload, length)` | submits one payload of this application's own, on its `sourceId` and no connection. C++ also has `publish<Encoder>(payloadId, fill)` |
| `reply(requesterSourceId, connectionId, payloadId, payload, length)` | the same on behalf of the producer that asked: the **requester's** `sourceId` and `connectionId`, which is how the gateway that took the request routes the answer back out. C++ also has `reply<Encoder>(requesterSourceId, connectionId, payloadId, fill)` |
| `sourceId()`, `isCaughtUp()`, `lastGlobalSeqNo()` | what the replica may say about itself |

`seqeron-examples/src/java/example/ColocatedApp.java` and its C++ twin `ColocatedApp.cpp` are the
reference consumers, and the only clients in the repository whose builds refuse anything outside `app`
(`checkFacadeOnly`, and the same check in `seqeron-examples/CMakeLists.txt`).

`Listener` adds `onLeadershipChanged(boolean leading)` where `Gateway` has `onActivated`/`onStandby`, and
carries the same `onSequenced`/`onCaughtUp`/`onClusterHeartbeat`/`onFenced`. It has no connection
lifecycle: an application nothing elects owns no connections. **Every leadership change closes an open gate**,
so `false` is where `OutstandingWork.onNotLeader()` belongs: a reply submitted during the election may
have gone with it, and the next opening dispatches it again. Keep a request's `sourceId` and
`connectionId` rather than its `Payload` — the flyweight is valid only during its callback, and a reply
is usually dispatched later.

Both façades take `DEFAULT_TAP_STALL_TIMEOUT_MS` (20 heartbeat periods) and
`DEFAULT_RECOVERY_STALL_TIMEOUT_MS` (three times that) — the deployment policy every producer had been
copying; each builder takes overrides.

## Underneath (`app`)

The decisions the façades are assembled from — pure state machines, each with a Java and a C++ twin, each
doing no I/O. Only the one below is offered. The election, the leader gate and the two stall fences are
package-private in Java, the façades being the only thing that assembles them; their C++ twins are in
`app::detail`, which is how C++ spells the same thing (see [Not API](#not-api)). Confirmed ingress, the one block a duty cycle of your own does need, sits in
[`sequencer.client`](#producing) beside the `IngressTracker` it implements.

| Class | For |
|---|---|
| `OutstandingWork` | Leader-only request/reply work, re-dispatched after a failover. The exception here, being about the application's own work rather than seqeron's plumbing |

## Constants

| Where | What |
|---|---|
| `FrameLayer` (Java), `SequencedFrame.hpp` (C++) | The tap's identity (`FEEDER_STREAM_ID` 205) and the size limits of spec §12; in Java also the heartbeat interval |
| `SystemFrame` (Java), `SequencedFrame.hpp` (C++) | The `systemEventType` values |
| `PortLayout` | The cluster's port block, each member's ingress endpoint, the endpoint set a producer connects with (`ingressEndpoints()`), and the co-located archive's control link; honours `SEQERON_PORT_BASE` |
| `ReplayProtocol` | The replay protocol's channel and stream ids, and `NO_REPLAY_NEEDED` |
| `Publish` | What a publish did — `Published`, `Refused` (permanent), `Declined` (retryable); returned by `IngressPublisher` and by both façades' `publish` |

## Wire codecs

The generated SBE codecs for `sbe-frame.xml` (`org.limitless.seqeron.sbe.frame` in Java,
`org_limitless_seqeron_sbe_frame` in C++) are API because the schema is. A producer encodes system bodies
with them and a consumer decodes them. They change only when the protocol does, and spec **V-3** governs
how. The `sbe-replay.xml` codecs also ship, but only `ReplayerStreamReceiver` speaks that protocol.

## Support code

`util` (`Logger`, `IdleStrategies`/`IdleStrategy.hpp`, `Clocks`, and `Env.hpp` in C++) and `protocol`'s
`SeqeronCounters` ship in the client tier because the classes above use them. You may use them, but they
are infrastructure rather than what seqeron offers, and they carry no promise of stability.
`Clocks.monotonicMs()` is what every duty-cycle deadline here is measured against; C++ has no twin,
since `nowMs()` is a free function in the one header that needs it.

## Not API

Header-only C++ has no package-private, so what Java hides that way C++ puts in a `detail` namespace, in
a `detail/` directory beside the package it serves. **Nothing under a `detail` is API**, and a public
signature that names one is a test seam. Don't build on them:

- `replayer/client/detail`: `TapFaultInjector` (test harnesses only, to drop live tap frames and exercise
  gap recovery; Java takes a `BooleanSupplier` on the same constructor instead), `ReplayerRecovery` and
  `ReplayerRecoveryActions` (the seam the unit suites drive).
- `sequencer/client/detail/Transport.hpp`: `IngressTransport`/`EgressTransport` and their Aeron
  implementations, the seam `ClusterStreamSender`'s `connect` overloads take in tests.
- `app/detail`: the blocks a façade assembles — `Session` (the cluster session, the tap, confirmed ingress
  and the fences in one duty cycle), `GatewayLifecycle`, `LeaderGate`, `RecoveryStallFence` and
  `TapStallFence`. In Java these are package-private, and `FacadeSurfaceTest` fails the build if a public
  signature names one. Beside them, `makePayload`: it builds the `Payload` a façade would hand a listener,
  so a consumer can unit-test its own handlers.
- Everything in `seqeron-service`: `Sequencer`, `SequencerService`, `SequencerServer`, `TapPublisher`,
  `replayer.server`, `tools`, `MetricsExporter`.
