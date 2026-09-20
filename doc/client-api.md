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
(`org::limitless::seqeron::sequencer::client`, …) for the client tier, which is all of it.

| Package | Tier | Holds |
|---|---|---|
| `protocol` | client | The wire contract in code: `FrameLayer`, `SystemFrame`, `SequencedFrameDecoder`, `PortLayout`, `ReplayProtocol`, `SeqeronCounters` (C++: `SequencedFrame.hpp`, `PortLayout.hpp`, `ReplayProtocol.hpp`, `SeqeronCounters.hpp`) |
| `sequencer.client` | client | Producing: `ClusterStreamSender`, `IngressPublisher`, `IngressTracker` (C++ also `ClusterStreamClient`) |
| `replayer.client` | client | Consuming: `ReplayerStreamReceiver`, and `SequencedEvent` in Java. The C++ `SequencedEvent` is in `protocol` (`SequencedFrame.hpp`) instead, beside the `unwrapFrame` that fills it and the `decodeSystem`/`decodeSequenced` that read it |
| `app` | client | The building blocks below |
| `util` | client | Support code |
| `sbe.frame`, `sbe.replay` | client | Generated codecs |
| `sequencer`, `replayer.server`, `tools`, `metrics`, `sbe.probe` | node | The cluster itself; not for clients |

## Getting it

| | |
|---|---|
| Java | `org.limitless:seqeron`, versioned by `org.limitless:seqeron-bom`; the README's "Example consumer" has the JitPack coordinates |
| C++ | `seqeron::seqeron_core`, header-only, from `FetchContent` over the checkout or `find_package(seqeron)` on an installed prefix |

`seqeron-examples` builds against these, outside this repository's own build — `src/java` against the
artifact and `src/cpp` against the CMake target. They are
the smallest complete client in each language.

`./gradlew :seqeron-client:javadoc` renders this surface from the sources, at
`seqeron-client/build/docs/javadoc/`, and the same pages ship as the artifact's javadoc jar, so an IDE
resolving `org.limitless:seqeron` shows them. Both leave out the generated SBE codecs, which carry no
comment of their own — the schema is what documents them.

## Consuming the ordered stream

**`ReplayerStreamReceiver`** (`replayer.client`, both languages) is the only entry point. It replays
this node's history through the co-located Replayer, switches to the live tap once caught up, heals gaps
by itself, and delivers every frame once, in `globalSeqNo` order.

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

**`SequencedEvent`** is what `onSequenced` receives — `replayer.client` in Java, `protocol/SequencedFrame.hpp` in C++: a flyweight, valid only during the call. Split by
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

| Class | Role |
|---|---|
| `ClusterStreamSender` | The cluster session. `connectColocated(aeron, memberId, …)` uses IPC ingress on the co-located member and falls back to UDP when that member is not leading; `connect(…)` uses UDP. `send` spins through back-pressure and elections. Call `keepAlive()` and `pollEgress()` every duty cycle. |
| `IngressPublisher` | Encode and offer. Returns `Publish`: `Published`; `Refused` (above `MAX_PAYLOAD_LENGTH`, nothing offered, permanent); `Declined` (the transport's answer, worth retrying). Java: `publishPayload`/`publishSystem` on an instance, with the body pre-encoded. C++: free functions templated on the encoder, filled through a `Fill`. |
| `offerFrame` (C++) | Offers a frame the caller has already encoded, and is where both `publish*` functions end. Java's `publishPayload` takes payload bytes, so it carries any encoding; the C++ one is templated on an SBE encoder, and a payload with no schema at all (§13.2) is framed by the caller and offered here. It takes the same `IngressTracker`, so a hand-framed payload is confirmed like any other. |
| `SystemFrame` (Java) | Wraps an encoded body in its envelope and returns the length; the offer is yours. `IngressPublisher` uses it; call it directly only to place frames yourself. |
| `PendingSends` (`app`) | Confirmed ingress. A send that succeeds is not a frame sequenced, and a failover silently loses what the old leader had not committed. Give it to `IngressPublisher` as its tracker and to the sender with `setIngressHold`, feed it your own tap and each leadership term, and call `resendMissing`. Spec §16 A-4, A-5. |

`IngressTracker` is the interface `PendingSends` implements, and `IngressSender` the one
`ClusterStreamSender` implements. Implement them only to replace those classes.

## Building blocks (`app`)

Pure state machines for the decisions a gateway or co-located application has to get right. Each has a
Java and a C++ twin and does no I/O.

| Class | For |
|---|---|
| `GatewayLifecycle` | A gateway instance's election: when to open its gate, when to stand down (`GatewayRegistered`/`GatewayActive`/`GatewayStarted`) |
| `LeaderGate` | Whether a co-located replica may do leader-only work: caught up, and its node leads |
| `OutstandingWork` | Leader-only request/reply work, re-dispatched after a failover |
| `RecoveryStallFence` | Fences a gateway whose recovery stops making progress |
| `TapLagMonitor` | Observes how far a contiguous tap runs behind the leader; raises no fence |
| `PendingSends` | See [Producing](#producing) |

## Constants

| Where | What |
|---|---|
| `FrameLayer` (Java), `SequencedFrame.hpp` (C++) | The tap's identity (`FEEDER_STREAM_ID` 205) and the size limits of spec §12; in Java also the heartbeat interval |
| `SystemFrame` (Java), `SequencedFrame.hpp` (C++) | The `systemEventType` values |
| `PortLayout` | The cluster's port block, and each member's ingress endpoint; honours `SEQERON_PORT_BASE` |
| `ReplayProtocol` | The replay protocol's channel and stream ids, and `NO_REPLAY_NEEDED` |

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

These are public or in public headers for mechanical reasons. Don't build on them:

- `TapFaultInjector`: test harnesses only, to drop live tap frames and exercise gap recovery.
- C++ `ReplayerRecovery`, `ReplayerRecoveryActions`, and `IngressTransport`/`EgressTransport` with their
  Aeron implementations: the seams the unit suites drive. Header-only C++ has no package-private. In Java
  the same seams are package-private.
- Everything in `seqeron-service`: `Sequencer`, `SequencerService`, `SequencerServer`, `TapPublisher`,
  `replayer.server`, `tools`, `MetricsExporter`.
