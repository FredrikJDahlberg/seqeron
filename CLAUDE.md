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

5. Commits

Never commit code without permission.


## What this is

See [README.md](README.md). **seqeron** is the sequencing tier: an Aeron
Cluster (Raft) replicated state machine that assigns a global, gap-free total order to messages from
external producers, plus the replayer that serves history off each node's recording and the client-side
plumbing that follows the ordered stream.

This repository is that tier **alone**: it contains no application edges and no application schemas.
Every `doc/<name>.md` reference in this tree resolves inside it. Do not cite a document that is not
here — state the reason inline instead.

**Every artifact says `seqeron`** — package `org.limitless.seqeron`, Gradle project `seqeron`, the fat
jar `seqeron-<version>-uber.jar`, CMake targets `seqeron_core`/`seqeron_flags`, environment variables
`SEQERON_*`, Prometheus metrics `seqeron_*`.

## Module layout

**Two Gradle modules, Aeron's layout**: `:seqeron-client` and `:seqeron-service`, the latter depending on
the former. Each owns a source tree — `<module>/src/main/{java,...}` and `<module>/src/test/{java,...}`
— and one CMake project sits above them, since C++ is the client tier alone
(`seqeron-client/src/main/cpp`). C# is the client tier alone too, a standalone `dotnet` build in
`seqeron-client/src/main/csharp`, its tests in `seqeron-client/src/test/csharp`. The service module holds
`resources`, `scripts` and `ops`; the client module holds `cpp`, `csharp` and `generated`. Both hold `sbe`.

**The direction is the compiler's to enforce.** A client class that reaches for `Sequencer` does not
compile, because the service module is not on the client's classpath.

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
shares with a client — the queue's identity, the cluster clock, the port block, the replay protocol's
addresses — goes in `protocol`, never in a service-tier class. C++ is the hosted client stubs (`hosted`) and the
types they share with the Java client tier (`app`, `protocol`, `util`), under the same names, plus a `detail/`
directory and namespace beneath a package for what Java makes package-private, since header-only C++ has no such
thing. The direct C++ client is retired and kept on the `cpp-direct-client` branch (tag `cpp-direct-client-final`). C# is the Java classes under the Java names in PascalCase namespaces
(`Org.Limitless.Seqeron.Sequencer.Client`), `internal` where Java is package-private, and the façades take an
options class with `init` properties where Java takes a builder. Its `FacadeSurfaceTest` makes Java's check,
and `seqeron-examples/src/csharp` its `checkFacadeOnly`, in `Directory.Build.targets`.

## Build

### Java
```bash
./gradlew compileJava
./gradlew uberJar     # build/libs/seqeron-<version>-uber.jar — every script's prerequisite
./gradlew test        # JUnit 5, ~1s
./gradlew generateFrameSbe generateReplaySbe generateProbeSbe generateClusterSbeIr
./gradlew :seqeron-service:compileTestJava   # TestGateway and TestApplication, which systemTest needs and no jar carries
./gradlew :seqeron-client:jar :seqeron-service:jar  # the two published artifacts
```
JDK 21. `SEQERON_JAR` overrides the jar path for every script that resolves it.

### C++
```bash
cmake -B cmake-build-debug -DCMAKE_BUILD_TYPE=Debug      # AddressSanitizer
cmake --build cmake-build-debug
```

### C#
```bash
dotnet test --project seqeron-client/src/test/csharp/Seqeron.Client.Tests.csproj
dotnet pack -c Release -o build/nuget seqeron-client/src/main/csharp/Seqeron.Client.csproj  # what the C# examples resolve
./gradlew :seqeron-client:generateCSharpFrameSbe :seqeron-client:generateCSharpReplaySbe     # rewrite the committed codecs
./gradlew :seqeron-client:checkCSharpSbeCurrent
```

**Aeron, Agrona and SBE versions are pinned once**, in `versions.properties` — `build.gradle` loads it,
`CMakeLists.txt` parses it and `Directory.Build.props` reads it. The two sides generate independently from the same schemas and speak
the same wire protocol, so a version that differs across them is a runtime decode failure, not a build
error. The C++ build uses an installed Aeron when `find_package(aeron)` finds one at or above that
version, and fetches it otherwise; the project links Aeron only by its `aeron::` names, which work
either way. **C# is the one exception** (spec V-1): Aeron.NET trails Aeron, so `aeronDotnet` pins its own
version, which agrees with Java's on everything a client touches. `csharp-client-test.sh` is the evidence, and
`chaos.yml` runs it on every pull request, so an Aeron upgrade's pull request reruns it before tagging.

## Tests

```bash
cmake --build cmake-build-debug --target run_tests   # GoogleTest
./gradlew test                                       # JUnit
./gradlew :seqeron-service:integrationTest           # JUnit 6, members in the test's JVM
./gradlew :seqeron-service:systemTest                # JUnit 6, members as child JVMs; networkFaultTest adds Docker + faulteron
dotnet test --project seqeron-client/src/test/csharp/Seqeron.Client.Tests.csproj   # xUnit
```
`run_tests` is `ctest --output-on-failure` with the build dependency wired, and also builds
`core_headers`, which compiles each public header alone under `-Werror`. Plain `ctest` works too.
Single suites:
`./cmake-build-debug/core_tests --gtest_filter='OutstandingWork*'` and
`./gradlew test --tests '*SequencerTest'`. The C# suite is the Java client tier's tests case for case, plus the
spec §14 conformance rows `ConformanceTest.cpp` runs, and three of its own: the committed codecs' schema ids,
the session timeouts read back from the context it connects with, and an allocation test that holds the queue
dispatch and the confirmed publish to zero bytes a frame.

The Java suite covers the deterministic decision-making — `Sequencer`, and `ReplayerService` through
its `Replayer` seam — and deliberately touches no Aeron runtime: no media driver, no cluster, no Aeron
mocks. Everything that needs an Aeron runtime is covered by the integration suite, by the
system tests in `seqeron-service/src/systemTest` and by the client-tier scripts under `seqeron-service/src/test/scripts`. Coverage is a JaCoCo report per module, at
`<module>/build/reports/jacoco/test/`, excluding the generated SBE codecs.

## Architecture

Component guidance loads with the files it is about: `seqeron-service/CLAUDE.md` (the sequencer, the
replay server and gateway host, the topology document), `seqeron-client/CLAUDE.md` (the producer side,
confirmed sends, the C++ and C# builds, `ReplayerRecovery` parity),
`seqeron-service/src/integrationTest/CLAUDE.md`, `seqeron-service/src/test/scripts/CLAUDE.md` (the
harnesses, `TestGateway`, `TestApplication`), `.claude/rules/sbe-codegen.md` and
`.claude/rules/operator-scripts.md`. `doc/overview.md` is the design in full.

Note that **no cluster callback may signal failure by throwing**: `Image.boundedControlledPoll` has
already advanced the log position past the message and `AgentRunner` keeps the agent alive, so a throw
drops the frame and carries on. The same holds on the client side — `Image.poll` hands any exception its
fragment handler raises to the Aeron error handler and advances the subscriber position anyway — which is
why a client records the fault and raises it from its own duty cycle instead.

### Frames: two families, one envelope
**Everything on the wire is `sbe-frame.xml` (schema 210), in two families.** The **application** family
is `Unsequenced` (100) on ingress, republished as `Sequenced` (101) on the queue, carrying one opaque
length-prefixed payload named by `header.payloadId`. The **system** family is seqeron's own vocabulary
(spec §7), named by `header.systemEventType` at the same offset: `UnsequencedSystem` (102) →
`SequencedSystem` (103) for the thirteen events a producer submits, plus five templates of their own for
the five the sequencer synthesizes — `ClusterHeartbeat` (104), `LeadershipChanged` (105), `GatewayActive`
(106), `SnapshotStarted` (107), `SessionClosed` (108). Sequencing is copy-20/append-24 for both, the payload is never re-encoded, and `sequenceMessage`
validates every frame against `doc/seqeron-protocol-spec.md` §9.2.

**The cluster tier decodes no `payloadId` at all** — every application payload is copied through
unopened. `payloadId` 1 is retired and refused on ingress: core was a `payloadId` until the system family
replaced it. 2–4 are reserved for an external deployment (spec §6.1), and this repo owns 5 —
`ProbeMarker`, private to `ClusterProbe`.

**A consumer splits by family first, then dispatches on `(payloadId, templateId)` — never `templateId`
alone**: template ids are unique per schema, so two applications' templates can collide, and the uint16
at offset 16 is a `payloadId` on one family and a `systemEventType` on the other. `unwrapFrame` (C++,
`SequencedFrame.hpp`) and `SequencedFrameDecoder` (Java) are the one place the envelope is stripped; past
them a consumer sees `isSystem()` plus either a `payloadId` and the message's own template, or a
`systemEventType`.

**A system payload carries no `MessageHeader`** — `systemEventType` names it, so an encoder `wrap`s rather
than `wrapAndApplyHeader`s and a decoder supplies `BLOCK_LENGTH`/`SCHEMA_VERSION` from its own compiled
constants. `SystemFrame` (Java, C#) and `publishSystem`/`decodeSystem` (C++) are that contract's one place
per language; the `systemEventType` table lives in `SystemFrame.java`, `SequencedFrame.hpp` and
`SystemFrame.cs`, and the three must stay in step.

## Scripts

`.claude/rules/operator-scripts.md` and `seqeron-service/src/test/scripts/CLAUDE.md` cover the
scripts. `ports.sh` is
mirrored by `PortLayout` in Java and C#; change all three together.

## Known gaps

The standing structural cost is that **a cluster with no `<snapshots>` policy takes no cluster
snapshots** (a node's recovery time and archive size grow with the history its archive holds, which suits a
deployment that resets the archive between runs; with a policy they are bounded by the policy's
`bytes`, and every consumer of a member's queue must then be able to restart from a snapshot file, which only an
`Application` or `Gateway` writes).
The cluster is bounded at seven members by the 70-port cluster block (`doc/ops.md`, "Ports"); three or five is usual.
`SEQERON_PORT_BASE` moves that block off its 9300 default — deployment-wide, read by both mirrors
— but does not widen it. `SEQERON_HOSTS` names the members' hosts, deployment-wide too, and is read by
both `PortLayout`s only: the scripts' `ports.sh` lays out localhost clusters.

`doc/` holds `overview.md` (the design, for architects: what each part does and why, the life of a
message, the failure model, the limits — keep it in step with the documents it summarizes),
`getting-started.md` (a release node plus a consumer and a producer; its snippets pin a
release, so bump them when one changes the API they use), `running-a-cluster.md` (the processes, a node's
ports, properties and restart, the operator scripts), `building.md` (each language's build, the unit
suites, the harnesses, the examples), `releasing.md`, `log-printer.md`, `seqeron-protocol-spec.md` (normative — the frames, the families,
the system vocabulary, the topology document), `client-api.md` (what a client programs against, and what in
the client tier is not API — update it when that surface changes), `fault-tolerance.md`, `clusterctl.md` and `ops.md` (runbooks, ports, counters), `snapshot.md`
(application snapshots: local files the log confirms), `upgrades.md` (seqeron and application versions,
and the proposed snapshot-and-upcaster upgrade model), and `versioning-prior-art.md` (how exchanges and
log-based systems version their protocols, as background to `upgrades.md`). The topology documents here are
`seqeron-service/src/test/resources/topology-test-gateway.xml`, `topology-test-snapshot.xml` and
`topology-test-groups.xml` beside it, and
`seqeron-examples/topology.xml` and `topology-csharp.xml`. `client-api.md`'s tables have a column per language,
and a change to the client tier's surface changes all three.

## Code Formatting Mandate
- Explicitly respect all style, brace, and indentation configurations found in the local `.clang-format` file.
- Before completing an edit or creating a file, ensure it complies with our Clang-Format criteria.
- Keep code comments short and to the point. Do not explain design that is already documented.
