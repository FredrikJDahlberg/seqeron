# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

1. Think Before Coding

Don't assume. Don't hide confusion. Surface tradeoffs.

Before implementing:

State your assumptions explicitly. If uncertain, ask.
If multiple interpretations exist, present them - don't pick silently.
If a simpler approach exists, say so. Push back when warranted.
If something is unclear, stop. Name what's confusing. Ask.

2. Simplicity First

Minimum code that solves the problem. Nothing speculative.

No features beyond what was asked.
No abstractions for single-use code.
No "flexibility" or "configurability" that wasn't requested.
No error handling for impossible scenarios.
If you write 200 lines and it could be 50, rewrite it.
Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

3. Surgical Changes

Touch only what you must. Clean up only your own mess.

When editing existing code:

Don't "improve" adjacent code, comments, or formatting.
Don't refactor things that aren't broken.
Match existing style, even if you'd do it differently.
If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:

Remove imports/variables/functions that YOUR changes made unused.
Don't remove pre-existing dead code unless asked.
The test: Every changed line should trace directly to the user's request.

4. Goal-Driven Execution

Define success criteria. Loop until verified.

Transform tasks into verifiable goals:

"Add validation" → "Write tests for invalid inputs, then make them pass"
"Fix the bug" → "Write a test that reproduces it, then make it pass"
"Refactor X" → "Ensure tests pass before and after"
For multi-step tasks, state a brief plan:

1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
   Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.


## What this is

See the [Overview](README.md#overview) in README.md. **seqeron** is the sequencing tier: an Aeron
Cluster (Raft) replicated state machine that assigns a global, gap-free total order to messages from
external producers, plus the replayer that serves history off each node's recording and the client-side
plumbing that follows the ordered stream.

This repository is that tier **alone**. It was carved out of **phixeron**, which keeps the product
edges — the C++/simdfix FIX gateway, `OrderExecServer`, `BasicDataServer`, and the two Artio legs
(`ExchangeGateway`, `OrderGateway`). Those are gone from here, and so are the docs that described them
(`design.md`, `todo.md`, `future-arch.md`, `architecture-primer.md`, `basicdata-design.md`,
`artio-integration.md`, `gap.md`, `audit.md`, `router-design.md`, `seqeron-protocol.md`,
`0-overview.md`, `6-detailed-architecture.md`, and the `review-*.md` notes). **The citations of them
that comments across the tree used to carry have been removed**, so every `doc/<name>.md` reference in
this tree resolves inside it. Do not add a citation of a document that is not here — state the reason
inline instead.

**Every artifact says `seqeron`** — package `org.limitless.seqeron`, Gradle project `seqeron`, the fat
jar `seqeron-0.1.0-uber.jar`, CMake targets `seqeron_core`/`seqeron_flags`, environment variables
`SEQERON_*`, Prometheus metrics `seqeron_*`. The product repo is still `phixeron` and its own
identifiers stay that way; where a doc here cites one of its files or protocol names, that name is
`phixeron` on purpose.

## Module layout

One Gradle project, one CMake project, one source tree — `src/main/{java,cpp,sbe,resources,scripts,ops}`
and `src/test/{java,cpp,resources,scripts}`. The `:cluster`/`:gateways` split and the
core-library/executable target pair are gone with the product half; **the repository boundary is what
enforces the dependency direction now**, so there is nothing to keep on the right side of a line within
this tree.

| | Java | C++ |
| --- | --- | --- |
| the sequencer | `sequencer/` — `Sequencer`, `SequencerService`, `SequencerServer`, `FrameLayer`, `SystemFrame`, `TapPublisher`, `TapStallPolicy` | `sequencer/` — `SequencedFrame`, `ClusterStreamSender`, `ClusterStreamClient`, `IngressPublisher`, `PortLayout` |
| the replayer | `replayer/server/` and `replayer/client/` | `replayer/client/` only |
| the tools | `tools/` — `ClusterCtl`, `TopologyDocument`, `ClusterProbe`, `SbeLogPrinter` | — |
| the ops plane | `metrics/` — `MetricsExporter`, `MetricsAggregator`, `SeqeronCounters` | `util/SeqeronCounters.hpp` |
| the gateway fence | `fixgateway/GatewayRecoveryStallPolicy` | `fix/GatewayRecoveryStallPolicy.hpp` |

`fixgateway`/`fix` hold exactly one class each and are not a FIX implementation: the recovery-stall
policy is a language-port pair that any edge gateway needs, and the pair lives here because the fence
it decides is the cluster tier's contract with its producers. Keep the two files and both
`GatewayRecoveryStallPolicyTest`s in step.

**The C++ half is a client library, not a program.** `seqeron_core` is a header-only INTERFACE
target and the only binary the build produces is `core_tests`. There is **no C++ replay server** —
the server side of the replay protocol is Java only.

`src/main/ops/` holds the Prometheus and Grafana provisioning the metrics scripts feed
(`doc/ops.md`); it is deployment configuration, not code.

## Build

### Java
```bash
./gradlew compileJava
./gradlew uberJar     # build/libs/seqeron-0.1.0-uber.jar — every script's prerequisite
./gradlew test        # JUnit 5, 286 tests, ~1s
./gradlew generateFrameSbe generateReplaySbe generateProbeSbe generateClusterSbeIr
./gradlew compileTestJava   # TestGateway, which chaos-runner.sh needs and no jar carries
```
JDK 21. `SEQERON_JAR` overrides the jar path for every script that resolves it.

### C++
```bash
cmake -B cmake-build-debug -DCMAKE_BUILD_TYPE=Debug      # AddressSanitizer
cmake --build cmake-build-debug
```
C++23, requires Java (Runtime) on PATH for the SBE tool, and fetches Aeron 1.51.0 and GoogleTest from
source. GoogleTest and `core_tests` are gated on `SEQERON_BUILD_TESTS`, which defaults to
`PROJECT_IS_TOP_LEVEL` — a build that adds this one gets neither unless it asks. `-DSEQERON_COVERAGE=ON` adds instrumentation. No simdfix, and therefore **no SSH remote is
needed** — the FetchContent clone that used to require one went with the product half.

**Aeron and SBE versions are pinned twice** — `build.gradle`'s `ext` block and `CMakeLists.txt`'s
`FetchContent`/`SBE_VERSION`. The two sides generate independently from the same schemas and speak the
same wire protocol, so a version that differs across them is a runtime decode failure, not a build
error. Change both together.

## Tests

```bash
cmake --build cmake-build-debug --target run_tests   # 132 GoogleTest cases
./gradlew test                                       # 286 JUnit cases
```
`run_tests` is `ctest --output-on-failure` with the build dependency wired. **Plain `ctest` is fine
here** — the `..._NOT_BUILT` noise that had to be filtered was simdfix's own registered suite, and this
build declares no simdfix. Single suites:
`./cmake-build-debug/core_tests --gtest_filter='ReplayerRecovery*'` and
`./gradlew test --tests '*SequencerTest'`.

The Java suite covers the deterministic decision-making — `Sequencer`, and `ReplayerService` through
its `Replayer` seam — and deliberately touches no Aeron runtime: no media driver, no cluster, no Aeron
mocks. Everything Aeron-shaped is covered by `core_tests` and by the five end-to-end scripts under
`src/test/scripts`. Coverage is one JaCoCo report at `build/reports/jacoco/test/`, excluding the
generated SBE codecs.

**All five harnesses are Java-only.** They drive the cluster through `tools/ClusterProbe`, which
submits `ProbeMarker` payloads at ingress (`submit`), round-trips one through consensus and back off
the tap (`ping`), or replays history through the co-located Replayer and then follows the tap live
(`follow`). The probe attaches to a member's own embedded driver, so three of the five need no
standalone `aeronmd` at all. `chaos-runner` needs a sixth thing the probe cannot supply — a **gateway
pair under the faults** — and `TestGateway` is it: an elected active/standby producer (`GW-T-A`/`GW-T-B`,
`gatewaySourceId` 9, listening on 9200/9201) that speaks no application protocol and holds no session
state, but holds the same four fences a real gateway does, so the recovery-stall policy gets exercised
here. It is in **`src/test/java`** and therefore in no jar: `chaos-runner.sh` puts
`build/classes/java/test` on the classpath beside the uber jar and refuses to start without it. Its
list is `src/test/resources/topology-test-gateway.xml`, the only topology document in this repo.

`start-cluster.sh` and `start-three-node-cluster.sh` both default to the cluster tier alone;
`SEQERON_PRODUCT_APPS=1` additionally launches the product repo's binaries, which must already be
built there.

## Architecture

### Data flow
```
producers ──ingress──▶  Aeron Cluster (Java, Raft-replicated)
                                    │
    IPC (aeron:ipc 205), sequenced frames — recorded on every node
     live tap: read directly · history/gaps: replay via co-located ReplayerService
                                    │
                  co-located application replicas (this repo's is ClusterProbe)
```

### Aeron Cluster sequencer — `SequencerServer` / `SequencerService` / `Sequencer`
`Sequencer` is the replicated state machine proper — it owns `globalSeqNo` and every frame encode, has
no Aeron dependency, and is unit-tested directly (`SequencerTest`). `SequencerService`
(`ClusteredService`) is its Aeron adapter: it decides *when* to call the sequencer and publishes what
comes back, holding no replicated state itself. Every ingress message gets a cluster-wide monotone
`globalSeqNo` plus the Raft consensus timestamp, then is republished on the **node-local tap**
(`FEEDER_CHANNEL` = `aeron:ipc`, `FEEDER_STREAM_ID` = 205), which this node's co-located Aeron Archive
records. Every frame on it is sequenced: the tap carries `Sequenced` envelopes and nothing else, and
the sequencer is the stream's only publisher.

**Every node publishes and records its own tap** — leader and follower alike. All nodes process the
same committed log in the same order and keep identical sequencing state (so a new leader resumes
exactly where the last one left off), which makes the taps byte-identical across nodes: each archive
independently holds complete history, with no cross-node replication. The tap publication is created
once in `onStart` and never re-created on a leadership change (`aeron:ipc` has no port to collide on),
so a node's recording is one continuous run spanning every leader tenure.

Consumers split live from history: co-located apps subscribe to the tap **directly** for the live feed
(untethered, so a slow app is dropped and heals via replay rather than back-pressuring), while the
co-located `ReplayerService` serves cold-start/gap replay off the same recording. `emit` is reliable —
it spins until the offer lands, since a dropped frame would be an unrecoverable hole — and can only
block on local-archive write back-pressure, because the recording is the tap's one tethered subscriber.
Reliable is not unbounded: `TapPublisher` — the reliable-offer discipline, split off the way `Sequencer` is
and unit-tested the same way — applies `TapStallPolicy`'s verdict to the archive's `RecordingPos` counter,
and a node whose recording has stopped or stopped advancing **terminates itself** (`EXIT_TAP_FATAL` = 70) rather
than sequence history it cannot keep. Peers keep quorum, and the restart rebuilds its recording over the
full-log replay it does anyway. The 1 Hz heartbeat runs the same liveness check, because a *stopped*
recording back-pressures nothing at all (the untethered app subscribers keep the publication connected)
and would otherwise be silent.

Note that **no cluster callback may signal failure by throwing**: `Image.boundedControlledPoll` has
already advanced the log position past the message and `AgentRunner` keeps the agent alive, so a throw
drops the frame and carries on. The same holds on the client side — `Image.poll` hands any exception its
fragment handler raises to the Aeron error handler and advances the subscriber position anyway — which is
why a client records the fault and raises it from its own duty cycle instead.

Besides forwarded ingress, the sequencer synthesizes its own frames on the same `globalSeqNo` counter:
`ConnectionOpened`/`ConnectionClosed` (cluster session lifecycle), `LeadershipChanged` (de-duplicated
per leader), and a **1 Hz `ClusterHeartbeat`** (`Sequencer.CLUSTER_HEARTBEAT_INTERVAL_MS`) — the cluster
clock, so consumers have a consensus-driven time source that keeps advancing while a producer is silent,
which is exactly when a gateway's keepalive watchdog must probe.

**Snapshots are not supported**, and both `ClusteredService` hooks refuse: `onTakeSnapshot` throws, and
`onStart` refuses a snapshot image rather than restoring from one. `clusterctl shutdown` uses `ABORT`,
and recovery is always full-log replay from `globalSeqNo` 1. That is deliberate: replaying the whole log
is what keeps each node's tap recording complete and gap-free — a node restored from a snapshot would
record only from wherever it resumed. The cost is that recovery time and archive size grow with uptime
(the 1 Hz heartbeat alone is ~86.4k frames/day).

### The replay protocol — two sides, two namespaces
**`replayer.server`** is Java only: `ReplayerServer`/`ReplayerService` and their pure seams `Replayer`,
`ReplaySlotAllocator`, `ReplayRecordings`, `ReplayClientIdCollisions`, with `AeronReplayer` the only
part that touches Aeron. **`replayer.client`** is `ReplayerStreamReceiver` and its pure seam
`ReplayerRecovery`, plus `RecoveryProgressPolicy`, `SequencedEvent`, `SequencedFrameDecoder` — Java, and
C++ in `org::limitless::seqeron::replayer::client`. The only edge across is client→server: the client
reads `ReplayerService`'s channel and stream-id constants (`IPC_CHANNEL`, `REPLAY_STREAM_ID` 201,
`REQUEST_STREAM_ID` 202, `CONTROL_STREAM_ID` 203), which are the wire contract between them.

`ReplayerStreamReceiver` is the Aeron adapter only — subscriptions, the replay image, the clocks; every
decision it makes about them lives in **`ReplayerRecovery`**, which holds none of them and is where the
unit suite drives the walk/resume/gap state machine — `ReplayerRecoveryTest` names one situation per case,
`ReplayerRecoveryPropertyTest` drives seeded fault sequences against a model of the archive/tap/Replayer and
asserts gap-freedom and convergence over whatever comes out; both tests exist in both languages. The Java
and C++ classes are faithful ports of each other: same protocol, same state machine, same adapter/seam
split. Keep all four files and all four tests in step — the two `ReplayerRecoveryTest`s are case for
case in the same order precisely so a divergence is visible as a missing case rather than as a
runtime decode failure on a live tap.

### Frames: two families, one envelope
**Everything on the wire is `sbe-frame.xml` (schema 210), in two families.** The **application** family
is `Unsequenced` (100) on ingress, republished as `Sequenced` (101) on the tap, carrying one opaque
length-prefixed payload named by `header.payloadId`. The **system** family is seqeron's own vocabulary
(spec §7), named by `header.systemEventType` at the same offset: `UnsequencedSystem` (102) →
`SequencedSystem` (103) for the nine events a producer submits, plus three templates of their own for the
three the sequencer synthesizes — `ClusterHeartbeat` (104), `LeadershipChanged` (105), `GatewayActive`
(106). Sequencing is copy-18/append-16 for both, the body is never re-encoded, and `sequenceFrame`
validates every frame against `doc/seqeron-protocol-spec.md` §9.2.

**The cluster tier decodes no `payloadId` at all** — every application payload is copied through
unopened. `payloadId` 1 is retired and refused on ingress: core was a `payloadId` until the system family
replaced it. The product repo's ids are 2 (`sbe-order.xml`), 3 (`sbe-session.xml`) and 4
(`sbe-basicdata.xml`), and this repo owns 5 — `ProbeMarker`, private to `ClusterProbe`.

**A consumer splits by family first, then dispatches on `(payloadId, templateId)` — never `templateId`
alone**: template ids are unique per schema, so two applications' templates can collide, and the uint16
at offset 16 is a `payloadId` on one family and a `systemEventType` on the other. `unwrapFrame` (C++,
`SequencedFrame.hpp`) and `SequencedFrameDecoder` (Java) are the one place the envelope is stripped; past
them a consumer sees `isSystem()` plus either a `payloadId` and the message's own template, or a
`systemEventType`.

**A system body carries no `MessageHeader`** — `systemEventType` names it, so an encoder `wrap`s rather
than `wrapAndApplyHeader`s and a decoder supplies `BLOCK_LENGTH`/`SCHEMA_VERSION` from its own compiled
constants. `SystemFrame` (Java) and `publishSystem`/`decodeSystem` (C++) are that contract's one place
per language; the `systemEventType` table lives in `SystemFrame.java` and `SequencedFrame.hpp` and the
two must stay in step.

### Two kinds of producer, and only one of them is elected
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
and unit-tested the way `Sequencer` and `TapPublisher` are — resolves off its own classpath. Its three
sections are the complete producer view of a deployment: `<gateways>` (the elected pairs),
`<applications>` (the co-located, leader-gated kind, `sourceId` required so every producer sits in spec
§5's one id space), and `<protocols>` (`PayloadIdRegistered` rows naming each `payloadId`). **Only the
first is acted on**: `GatewayRegistered.remaining` counts down to 0 on the gateway section's last row, and
that row is the sequencer's completeness edge — it synthesizes the bootstrap `GatewayActive` per logical
gateway behind it. The application and protocol rows follow it, carry no countdown, and are labelling for
`SbeLogPrinter`: decoded by nothing, gating nothing.

### The gateway fence
`GatewayRecoveryStallPolicy` (both languages) decides one of the four signals on which an edge gateway
stops serving its counterparties: recovery dispatching nothing for a deadline once the instance has been
caught up at least once. It measures **progress, not elapsed recovery** — a converging re-walk always
advances the `globalSeqNo` it has dispatched, and a non-converging one never does, so timing elapsed
recovery would fence the very path a recovery takes. It never arms before the first catch-up, because a
cold start replays the whole log (no snapshots) and has no useful time bound. `RecoveryProgressPolicy`
alarms on the same predicate at a longer deadline; this one fences.

The other three signals and the gateway behaviour behind them live with the gateways, in the product
repo. What matters here is that the fence deliberately looks to the cluster exactly like the gateway
process dying — that is the state the recovery path is built for.

### SBE code generation
Four schemas, all under `src/main/sbe`, each generating into a distinct namespace so one include path
covers all of them:

- `sbe-frame.xml` (schema 210) — the seven top-level templates, their four header composites, and the
  nine submitted **system** bodies (the connection lifecycle events, the cluster markers, the gateway
  list/election frames, `GatewayActivationRequested`, `ApplicationRegistered`). No system message carries
  a `header` field — the frame's is the only one. Seqeron's own, and the only thing this tier decodes.
- `sbe-replay.xml` (schema 212) — the six **replay control** messages, node-local between a
  `ReplayerService` and its co-located app replicas, never sequenced and never recorded.
- `sbe-probe.xml` (schema 214) — core's own application payload (`payloadId` 5): one message,
  `ProbeMarker`, carrying a submitter-side `seqNo` and variable-length filler. What `tools/ClusterProbe`
  speaks, so the e2e scripts can drive a cluster with no product binary built. A payload rather than a
  system event precisely because the sequencer decodes no `payloadId` at all — a probe frame exercises the
  real copy-through path and needs no change to `sbe-frame.xml`. **Java only**, which keeps the C++
  `generated/sbe/core` root free of any application codec. `payloadId` 5 is private (one publisher, one
  consumer, both the probe), so it needs no `PayloadIdRegistered` row; its `sourceId` is 8
  (`ClusterProbe.PROBE_SOURCE_ID`).
- `sbe-cluster.xml` (schema 111) — trimmed mirror of `io.aeron.cluster.codecs` (SessionConnectRequest,
  SessionEvent, SessionKeepAlive, …), replacing a hand-written `ClusterProtocol.hpp`. Its last section is
  decode-only — the consensus-module log entries (`TimerEvent`, `SessionOpenEvent`, …) that
  `SbeLogPrinter --schema cluster` reads out of a Raft-log recording; the client never sends them. Java
  side is **IR-only** (`generateClusterSbeIr`, no codecs), since the Java side speaks the cluster protocol
  through `io.aeron.cluster.codecs` and the stubs would be dead classes.

`src/main/sbe` is a codegen-input directory, not a resource one, so no jar ships an XML. The one file a
jar does need is `src/main/resources/topology.xsd`.

Both sides regenerate independently from the same XML — keep them in sync when editing a schema. SBE
never deletes generated files for messages you removed, so **each schema owns a disjoint output directory
and each codegen step wipes its own before running**; a regeneration is a replacement, and no manual purge
of `cmake-build-*/generated/sbe` or `build/generated/sources/sbe` is needed. Keep that property when
adding a schema: give it its own package/namespace directory, declare only that as the task's output, and
wipe it in the same step. `collectSbeIr` stages each schema's `.sbeir` into the jar for `SbeLogPrinter`,
which **discovers whatever `.sbeir` resources are on its classpath rather than naming schemas** — so a
product jar beside this one lets the same tool name an application payload, and this jar alone names none,
which is the point.

## Scripts

`src/main/scripts` holds the operator and cluster-lifecycle scripts; `ports.sh` (the port formula, mirrored
by `PortLayout.hpp` and `SequencerServer`'s Javadoc) and `paths.sh` are sourced by every other script.
`src/test/scripts` holds the five harnesses. Both resolve paths relative to the repository root — they
were written when this tree sat under `cluster/`, so check the depth of any `../..` you add.

`clusterctl.sh` commands: `start`, `shutdown`, `activate <gatewayId>`, `load-topology <file>`, `counters`,
`help`; anything unrecognized passes through to `io.aeron.cluster.ClusterTool` against this node's cluster
dir. `snapshot` is refused.

`sbe-log-printer.sh` puts a whole deployment's IR in front of `SbeLogPrinter` (`SEQERON_JAR` picks the
jar); `-o <payloadId>` — the spec §13.1 pipe — is the wrapper's only, because Gradle re-encodes a child's
stdout and would corrupt the payload bytes.

`stop-cluster.sh` stops everything either start script launched, product processes included, so that a
harness cleaning up after a `SEQERON_PRODUCT_APPS=1` run finds nothing left behind.

## Known gaps

The gap list went with the product half, so there is none in this repo. The two structural costs
recorded above are the standing ones: **no snapshots** (recovery time and archive size grow with uptime),
and **the cluster is bounded at three members** by the 9300–9329 port block (`doc/registries.md` §2).

`doc/` holds what survived the split: `seqeron-protocol-spec.md` (normative — the frames, the families,
the system vocabulary, the topology document), `fault-tolerance.md`, `registries.md` (the two shared
namespaces this tier owns — `sourceId`, and the port blocks each repo draws from),
`clusterctl.md` and `ops.md` (runbooks), and `publishing.md` (the standing backlog between
`publishToMavenLocal`/`FetchContent` and a coordinate someone else can resolve). Note that `registries.md` still points at
`src/main/resources/topology.xml`, which left with the product half — the only topology document here
is `src/test/resources/topology-test-gateway.xml`.

## Code Formatting Mandate
- Explicitly respect all style, brace, and indentation configurations found in the local `.clang-format` file.
- Before completing an edit or creating a file, ensure it complies with our Clang-Format criteria.
- Keep code comments short and to the point. Do not explain design that is already documented.
