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
edges — the C++/simdfix FIX gateway, `OrderExecServer`, `BasicDataServer`, and the two Java FIX legs
(`ExchangeGateway`, `OrderGateway`). Those are gone from here, and so are the docs that described them
(`design.md`, `todo.md`, `future-arch.md`, `architecture-primer.md`, `basicdata-design.md`,
`gap.md`, `audit.md`, `router-design.md`, `seqeron-protocol.md`,
`0-overview.md`, `6-detailed-architecture.md`, and the `review-*.md` notes). **The citations of them
that comments across the tree used to carry have been removed**, so every `doc/<name>.md` reference in
this tree resolves inside it. Do not add a citation of a document that is not here — state the reason
inline instead.

**Every artifact says `seqeron`** — package `org.limitless.seqeron`, Gradle project `seqeron`, the fat
jar `seqeron-<version>-uber.jar`, CMake targets `seqeron_core`/`seqeron_flags`, environment variables
`SEQERON_*`, Prometheus metrics `seqeron_*`. The product repo is still `phixeron` and its own
identifiers stay that way; where a doc here cites one of its files or protocol names, that name is
`phixeron` on purpose.

## Module layout

**Two Gradle modules, Aeron's shape**: `:seqeron-client` and `:seqeron-service`, the latter depending on
the former. Each owns a source tree — `<module>/src/main/{java,...}` and `<module>/src/test/{java,...}`
— and one CMake project sits above them, since C++ is the client tier alone
(`seqeron-client/src/main/cpp`). The service module holds `resources`, `scripts` and `ops`; the client
module holds `cpp` and `generated`. Both hold `sbe`.

**The direction is the compiler's to enforce.** A client class that reaches for `Sequencer` does not
compile, because the service module is not on the client's classpath. The single-source-set build that
preceded this caught the same thing afterwards, by scanning the compiled client classes' constant pool
(`checkTierSeparation`, now gone) — which never saw test code, and never saw an import that javac had
already discarded.

**Two audiences, and inside each module the packages Aeron's layout implies.** A component's server sits
at its package root and its client in `.client` beside it (`sequencer` / `sequencer.client`,
`replayer.server` / `replayer.client`), and the wire contract both sides share is `protocol` —
`FrameLayer`, `SystemFrame`, `PortLayout`,
`SequencedFrameDecoder`, `ReplayProtocol`, `SeqeronCounters`, `Publish`. Each module publishes one jar: `seqeron`
(the **client tier**: `protocol`, `sequencer.client`, `replayer.client`, `app`, `util`, the frame and
replay codecs and the cluster mirror's IR) and `seqeron-service` (the **service tier**: `sequencer`,
`replayer.server`, `tools`, `metrics` and the probe codecs). No package is in both, so both jars carry an
`Automatic-Module-Name`. A process that merely talks to a cluster takes the first alone, and
`seqeron-examples` is the proof: it resolves `org.limitless:seqeron` and compiles. Its
`ColocatedApp.java` proves the narrower claim that `app` is sufficient on its own — `checkFacadeOnly`
fails its build if it names anything outside `app` beyond `protocol.Publish`, the way
`FacadeSurfaceTest` checks the same from inside the client tier. Anything the service tier
shares with a client — the tap's identity, the cluster clock, the port block, the replay protocol's
addresses — goes in `protocol`, never in a service-tier class. C++ is the client tier alone, in the
same directories and namespaces.

**The producer side is a language-port pair too.** Java's `ClusterStreamSender`/`IngressPublisher` carry
the C++ files' names and semantics — `connectColocated` (IPC ingress on the co-located member, UDP
endpoints when it is not leading), a `send` that spins through back-pressure and an election rather than
dropping the frame — unless a new leader arrives mid-spin while its `IngressTracker` holds, when it places
nothing and returns false — a self-throttling `keepAlive`, and the three-valued `Publish`. They are far smaller
than their twins because `AeronCluster` already is the cluster protocol that `ClusterStreamSender.hpp`
implements by hand; what the Java side adds is only what that client does not do. Two divergences are
deliberate and documented in the class: the body arrives pre-encoded rather than through a `Fill` over an
encoder (Java's SBE codecs share no interface), and leadership moving off the co-located member costs a
session rather than a publication swap (`AeronCluster` owns its publication). `IngressSender` exists so
`IngressPublisher` has a seam the Java suite can drive without an Aeron runtime — the C++ transport seam
has no Java equivalent, so the state machine those 868 C++ test lines cover is Aeron's here, not ours.
What is left of ours that is easy to get wrong is split off and unit-tested: **`IngressStallPolicy`**
(which offer results are terminal — `CLOSED` is not, it is an election in progress). `ClusterStreamSender`
itself is then the Aeron adapter, whose one other decision — replacing an IPC session whose leader moved
away — is a single comparison, so its low line coverage is the same statement `SequencerService`'s is.

**A send that succeeds is not a frame sequenced.** Nothing confirms ingress on egress, and a leader
failover silently loses whatever the old leader had not committed — the session survives it.
`sequencer/client/PendingSends` is the confirm-on-tap tracker (spec §16 A-4, A-5): a producer gives it to
`IngressPublisher` as its `IngressTracker` (which tracks what it places and declines while it holds or is
full) and to the sender with `setIngressHold`, feeds it its own tap and each `LeadershipChanged`'s term, and
resends what a term change lost. Both languages,
case for case, with a property test asserting exactly-once, in-order delivery across random failovers;
`failover-test.sh` proves the same across a real leader kill.

**The C++ half is a client library, not a program.** `seqeron_core` is a header-only INTERFACE
target and the only binary the build produces is `core_tests`. There is **no C++ replay server** —
the server side of the replay protocol is Java only.

`seqeron-service/src/main/ops/` holds the Prometheus and Grafana provisioning the metrics scripts feed
(`doc/ops.md`); it is deployment configuration, not code.

## Build

### Java
```bash
./gradlew compileJava
./gradlew uberJar     # build/libs/seqeron-<version>-uber.jar — every script's prerequisite
./gradlew test        # JUnit 5, ~1s
./gradlew generateFrameSbe generateReplaySbe generateProbeSbe generateClusterSbeIr
./gradlew :seqeron-service:compileTestJava   # TestGateway, which chaos-runner.sh needs and no jar carries
./gradlew :seqeron-client:jar :seqeron-service:jar  # the two published artifacts
```
JDK 21. `SEQERON_JAR` overrides the jar path for every script that resolves it.

### C++
```bash
cmake -B cmake-build-debug -DCMAKE_BUILD_TYPE=Debug      # AddressSanitizer
cmake --build cmake-build-debug
```
C++23, and fetches Aeron 1.51.0 (unless an installed one is found) and GoogleTest from source. **The core SBE codecs are generated and
committed**, under `seqeron-client/src/main/generated/sbe/core` — the git tag is the C++ artifact, so
shipping them
with it is what lets a consumer build with no SBE tool and no JDK of seqeron's asking (Aeron's own
build still wants a JDK 17+). `find_package(Java)` is therefore `QUIET`, not `REQUIRED`, and is used
only by `RegenerateSbeCodecs` (rewrites the committed tree — run it when a schema changes, then commit
what it produced) and `CheckSbeCodecsCurrent`, which regenerates into the build tree and compares.
SBE's C++ output is deterministic, so that comparison is exact; `run_tests` depends on it, which is
what keeps the committed copy from drifting away from `seqeron-client/src/main/sbe`. GoogleTest and
`core_tests` are gated on `SEQERON_BUILD_TESTS`, which defaults to
`PROJECT_IS_TOP_LEVEL` — a build that adds this one gets neither unless it asks. `-DSEQERON_COVERAGE=ON` adds instrumentation. No simdfix, and therefore **no SSH remote is
needed** — the FetchContent clone that used to require one went with the product half.

**Consumable two ways, under the same target name.** `add_subdirectory`/`FetchContent` over the
checkout (`seqeron-examples`), or `find_package(seqeron)` against a `cmake --install`ed prefix —
`SEQERON_INSTALL` gates the install rules and defaults to `PROJECT_IS_TOP_LEVEL`. The exported target
names no Aeron target, because FetchContent leaves Aeron's in no export set and an `install(EXPORT)`
naming one fails at generate time; they are `$<BUILD_INTERFACE:>`-wrapped and
`cmake/seqeronConfig.cmake.in` re-attaches them under `aeron::`, so an installed consumer brings its
own installed Aeron. CI's `installed` job is the only thing that exercises this path.

**Aeron, Agrona and SBE versions are pinned once**, in `versions.properties` — `build.gradle` loads it
and `CMakeLists.txt` parses it. The two sides generate independently from the same schemas and speak
the same wire protocol, so a version that differs across them is a runtime decode failure, not a build
error. The C++ build uses an installed Aeron when `find_package(aeron)` finds one at or above that
version, and fetches it otherwise; the project links Aeron only by its `aeron::` names, which work
either way.

## Tests

```bash
cmake --build cmake-build-debug --target run_tests   # GoogleTest
./gradlew test                                       # JUnit
```
`run_tests` is `ctest --output-on-failure` with the build dependency wired. **Plain `ctest` is fine
here** — the `..._NOT_BUILT` noise that had to be filtered was simdfix's own registered suite, and this
build declares no simdfix. Single suites:
`./cmake-build-debug/core_tests --gtest_filter='ReplayerRecovery*'` and
`./gradlew test --tests '*SequencerTest'`.

The Java suite covers the deterministic decision-making — `Sequencer`, and `ReplayerService` through
its `Replayer` seam — and deliberately touches no Aeron runtime: no media driver, no cluster, no Aeron
mocks. Everything Aeron-shaped is covered by `core_tests` and by the six end-to-end scripts under
`seqeron-service/src/test/scripts`. Coverage is a JaCoCo report per module, at
`<module>/build/reports/jacoco/test/`, excluding the generated SBE codecs.

**Five of the six harnesses are Java-only.** They drive the cluster through `tools/ClusterProbe`, which
submits `ProbeMarker` payloads at ingress (`submit`), round-trips one through consensus and back off
the tap (`ping`), replays history through the co-located Replayer and then follows the tap live
(`follow`), or streams through `ClusterStreamSender` and `sequencer/client/PendingSends` and checks its own tap shows
every frame exactly once, in order (`confirm`, which `failover-test.sh` runs across the leader kill). The probe attaches to a member's own embedded driver, so three of the five need no
standalone `aeronmd` at all. The sixth, `docker-failover-test.sh`, is the containerized multi-round
failover soak (`docker/compose.yml`, `./gradlew operatorDist`, CI's `failover.yml`). `chaos-runner` needs one more thing the probe cannot supply — a **gateway
pair under the faults** — and `TestGateway` is it: an elected active/standby producer (`GW-T-A`/`GW-T-B`,
`gatewaySourceId` 9, listening on 9200/9201) that speaks no application protocol and holds no session
state, but holds the same fences a real gateway does — including the client tier's recovery-stall
fence, which is why `chaos-runner.sh` can drive a
non-converging recovery to a handover rather than a hang. **It is the reference consumer of `app/Gateway`**,
the client tier's façade for one instance of an elected pair: the election (`app/GatewayLifecycle`), the
connection id space it resumes from its predecessor, the connection lifecycle frames, confirmed ingress
(`sequencer/client/PendingSends` under an `IngressPublisher`, held by its `ClusterStreamSender`) and the four `app/Fence`
values are all behind it, so what is left in the harness is a socket and a line protocol. A media driver that
goes away raises from `doWork()` rather than as a fence. It is in
**`seqeron-service/src/test/java`** and therefore in no jar: `chaos-runner.sh` puts
`seqeron-service/build/classes/java/test` on the classpath beside the uber jar and refuses to start
without it. Its
list is `seqeron-service/src/test/resources/topology-test-gateway.xml`, the only topology document in this repo.

`start-cluster.sh` and `start-three-node-cluster.sh` launch the cluster tier and nothing else — core
starts no process it does not own. A consumer that wants its own replicas or gateways alongside runs
them itself: `SEQERON_NO_CONSUMERS=1` leaves out the probe followers, the caller launches its own
after READY, and names them in `SEQERON_EXTRA_PROCESSES` so `stop-cluster.sh` sweeps them too.

## Architecture

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
and unit-tested the same way — watches the archive's `RecordingPos` counter,
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
`ConnectionOpened`/`ConnectionClosed` (cluster session lifecycle), `LeadershipChanged` (one per term,
same leader or not), and a **1 Hz `ClusterHeartbeat`** (`Sequencer.CLUSTER_HEARTBEAT_INTERVAL_MS`) — the cluster
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
`ReplayerRecovery`, plus `SequencedEvent` — Java, and C++ in
`org::limitless::seqeron::replayer::client`. The two sides share only the protocol's addresses —
`IPC_CHANNEL`, `REPLAY_STREAM_ID` 201, `REQUEST_STREAM_ID` 202, `CONTROL_STREAM_ID` 203 and
`NO_REPLAY_NEEDED` — and those are `protocol/ReplayProtocol` in both languages.

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

### SBE code generation
Four schemas, split by who speaks them — `sbe-frame`, `sbe-replay` and `sbe-cluster` under
`seqeron-client/src/main/sbe`, `sbe-probe` under `seqeron-service/src/main/sbe` — each generating into
a distinct namespace so one include path
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

A module's `sbe` directory is a codegen input, not a resource one, so no jar ships an XML. The one
file a
jar does need is `seqeron-service/src/main/resources/topology.xsd`.

Both sides regenerate independently from the same XML — keep them in sync when editing a schema. SBE
never deletes generated files for messages you removed, so **each schema owns a disjoint output directory
and each codegen step wipes its own before running**; a regeneration is a replacement, and no manual purge
of `cmake-build-*/generated/sbe` or `build/generated/sources/sbe` is needed. Keep that property when
adding a schema: give it its own package/namespace directory, declare only that as the task's output, and
wipe it in the same step. `collectSbeIr` stages each schema's `.sbeir` into the jar under
**`META-INF/seqeron/sbeir/`** for `SbeLogPrinter`, which **discovers whatever `.sbeir` resources are on
its classpath rather than naming schemas** — so a product jar beside this one lets the same tool name an
application payload, and this jar alone names none, which is the point. The staging directory is
`META-INF`, not a package dir, because a consumer contributing its IR must not have to write into
seqeron's own package namespace; `collectSbeIr` wipes its destination first, like every codegen step.

## Scripts

`.claude/rules/operator-scripts.md` and `seqeron-service/src/test/scripts/CLAUDE.md` cover the
scripts. `ports.sh` is
mirrored by `PortLayout` in both languages; change all three together.

## Known gaps

The gap list went with the product half, so there is none in this repo. The two structural costs
recorded above are the standing ones: **no snapshots** (recovery time and archive size grow with uptime),
and **the cluster is bounded at three members** by the 30-port cluster block (`doc/registries.md` §2).
`SEQERON_PORT_BASE` moves that block off its 9300 default — deployment-wide, read by all three mirrors
— but does not widen it.

`doc/` holds what survived the split: `seqeron-protocol-spec.md` (normative — the frames, the families,
the system vocabulary, the topology document), `client-api.md` (what a client programs against, and what in
the client tier is not API — update it when that surface changes), `fault-tolerance.md`, `registries.md` (the two shared
namespaces this tier owns — `sourceId`, and the port blocks each repo draws from),
`clusterctl.md` and `ops.md` (runbooks), and `package.md` (the packaging review list). The only topology
document here is `seqeron-service/src/test/resources/topology-test-gateway.xml`; the product half's
`topology.xml` left with it, and no doc points at it any more.

## Code Formatting Mandate
- Explicitly respect all style, brace, and indentation configurations found in the local `.clang-format` file.
- Before completing an edit or creating a file, ensure it complies with our Clang-Format criteria.
- Keep code comments short and to the point. Do not explain design that is already documented.
