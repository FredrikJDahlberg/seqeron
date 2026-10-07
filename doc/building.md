# Building and testing

How to build each language's half of this repository, run its unit suites and its end-to-end harnesses,
and build the examples against the published artifacts. [`releasing.md`](releasing.md) is how a build
becomes a release.

## Requirements

Requires **JDK 21** for the Java half, a **C++23** compiler for the C++ one and the **.NET 10 SDK** for the
C# one, and fetches Aeron 1.53.2 and GoogleTest from source — Aeron only when `find_package` finds no
installed one at that version or newer (built with `-DAERON_INSTALL_TARGETS=ON`, on `CMAKE_PREFIX_PATH`).

The C++ and C# halves need no JDK of seqeron's own making: their codecs are generated and committed under
`seqeron-client/src/main/generated/sbe`, so a consumer compiles them rather than running the SBE tool. Java
is still needed to *change* them — `RegenerateSbeCodecs` and `generateCSharp*Sbe`, then commit — and
Aeron's own build requires a JDK 17+ regardless (`aeron-archive/src/main/c` does
`find_package(Java 17 REQUIRED)` and Aeron's CMake shells out to its Gradle build), so a from-source Aeron
keeps one on the machine either way. Every language generates independently from the same schemas —
`seqeron-client/src/main/sbe` and `seqeron-service/src/main/sbe` — so the Aeron and SBE versions are pinned
once, in `versions.properties`, which every build reads. C#'s Aeron.NET is the one exception, which spec
**V-1** records.

## Java

```bash
./gradlew compileJava
./gradlew uberJar     # fat jar, run without Gradle: build/libs/seqeron-<version>-uber.jar
./gradlew test        # JUnit 5, ~1s
```

Every script needs that jar, so `uberJar` is the prerequisite for all of them. The operator scripts
find it themselves — `build/libs/` in this checkout, `lib/` in an installed distribution — while the
harnesses under `seqeron-service/src/test/scripts` still name `build/libs/` and run from the repository root.
`SEQERON_JAR` overrides the path either way, and `SEQERON_HOME` the root it is resolved from.

The codegen tasks run as part of `compileJava` and can be invoked on their own:

```bash
./gradlew generateFrameSbe generateReplaySbe generateProbeSbe   # Java codecs + IR
./gradlew generateClusterSbeIr                                  # IR only, no codecs
./gradlew compileTestJava                                       # TestGateway, for chaos-runner.sh
```

## C++

```bash
cmake -B cmake-build-debug -DCMAKE_BUILD_TYPE=Debug      # AddressSanitizer
cmake --build cmake-build-debug

cmake -B cmake-build-release -DCMAKE_BUILD_TYPE=Release
cmake --build cmake-build-release
```

`cmake --build cmake-build-debug --target docs` renders the C++ API reference into
`cmake-build-debug/docs/html` when Doxygen is installed; the target does not exist otherwise. The C#
reference is Doxygen too, rendered from the root into `build/docs/csharp/html`:

```bash
mkdir -p build/docs && SEQERON_SITE=doc/site SEQERON_VERSION=$(cat VERSION) doxygen doc/site/csharp.Doxyfile
```

All three API references, Java, C++ and C#, are published for the latest release at
https://fredrikjdahlberg.github.io/seqeron/.
`-DSEQERON_COVERAGE=ON` adds coverage instrumentation. The tree is developed on macOS/arm64 with
Apple clang; CI builds it on Ubuntu with both clang and gcc-14.

## C#

```bash
dotnet build seqeron-client/src/main/csharp/Seqeron.Client.csproj
dotnet pack -c Release -o build/nuget seqeron-client/src/main/csharp/Seqeron.Client.csproj
```

A standalone `dotnet` build, versioned from `VERSION` and pinned by `versions.properties` like the other two.
The frame and replay codecs are committed under `seqeron-client/src/main/generated/sbe/csharp`, so it needs
no JDK; `./gradlew :seqeron-client:generateCSharpFrameSbe :seqeron-client:generateCSharpReplaySbe` rewrites
them after a schema change, and `checkCSharpSbeCurrent` fails CI until they are. The pack is what
`seqeron-examples/src/csharp` builds against, and what a release pushes to nuget.org.

## Unit tests

```bash
cmake --build cmake-build-debug --target run_tests   # C++: 259 cases
./gradlew test                                       # Java: 408 cases
dotnet test --project seqeron-client/src/test/csharp/Seqeron.Client.Tests.csproj   # C#: 298 cases
```

`run_tests` is `ctest --output-on-failure` with the build dependency wired up; plain `ctest` works
too.

Run a single C++ suite by filter, or a single Java test class:

```bash
./cmake-build-debug/core_tests --gtest_filter='ReplayerRecovery*'
./gradlew test --tests '*SequencerTest'
```

The Java suite covers the deterministic decision-making — `Sequencer`, and `ReplayerService` through
its `Replayer` seam — and deliberately touches no Aeron runtime: no media driver, no cluster, no Aeron
mocks. Everything that needs an Aeron runtime is covered by `core_tests` and by the end-to-end harnesses below.
Coverage is a JaCoCo report at `build/reports/jacoco/test/`, written by `./gradlew test`.

## End-to-end harnesses

The end-to-end harnesses live under `seqeron-service/src/test/scripts/`. **Eight of the eleven are Java-only**
— they drive the cluster through `ClusterProbe` or `TestGateway`, which attach to a member's own embedded
media driver or a gateway host's, so six of them need no standalone `aeronmd` at all. Each brings a cluster
up and tears it down again; run them from the repository root, with `./gradlew uberJar` done first. The
ninth, `docker-failover-test.sh`, is the containerized one and wants `./gradlew operatorDist` and Docker
instead. The tenth and eleventh drive the C# client and want the .NET SDK beside the jar:
`csharp-client-test.sh` on Linux, and `csharp-windows-test.sh` on Windows, in Git Bash, with its member in a
WSL1 distribution that has a JDK 21.

| Script | Purpose |
|--------|---------|
| `start-three-node-cluster.sh` | Start a local 3-node Raft cluster with a per-node `ClusterProbe` replica; blocks until Ctrl-C. `SEQERON_NO_CONSUMERS=1` leaves out the replicas, for a caller that runs its own |
| `failover-test.sh` | Force a failover, then cold-start a fresh `ClusterProbe` follower on the new leader and verify it catches up on full history — each node's tap recording is one continuous run spanning both tenures. Two `confirm` producers stream across the kill: the one using `PendingSends` must see every frame exactly once, in order, and an untracked control reports what the kill lost |
| `gap-recovery-test.sh` | Drop a live tap frame on a caught-up consumer (SIGUSR1 fault injection) and verify it re-walks its recording and heals rather than wedging |
| `paused-subscriber-test.sh` | `SIGSTOP` a caught-up consumer while more than two tap windows go by, and verify its member stays up and the resumed consumer heals the hole its eviction left |
| `replayer-restart-test.sh` | Kill and restart a client's own node and verify the client fails fast and a fresh cold start is served from the node's new recording alone |
| `gateway-host-test.sh` | A `confirm` producer on a gateway host (node 3) streams while the member its relay reads, the leader, is killed: every frame must come back exactly once, in order, and the relay must move to another member. The host is then restarted, and a fresh `ClusterProbe` follower there must catch up from its new recording |
| `chaos-runner.sh` | Randomized fault injection against a live 3-node cluster, with the `TestGateway` pair (`GW-T-A`/`GW-T-B`, ports 9200/9201) taking load through its accept gate; every run prints its `SEED` to replay the exact fault sequence. Needs `./gradlew uberJar compileTestJava` |
| `snapshot-test.sh` | Application snapshots against a live 3-node cluster: the `TestGateway` pair and a `TestApplication` replica per member take part in rounds every 2 s, and the script restarts the standby, fails over onto the restored instance, brings the other back passive and activates it, starts a round with `clusterctl request-snapshot`, restarts a follower's replica, and kills the cluster leader. Every restore must reach the state the log implies and no instance may diverge from a sequenced round. Needs `./gradlew uberJar compileTestJava` |
| `docker-failover-test.sh` | Multi-round containerized failover soak — the `docker/compose.yml` port of `failover-test.sh`. `ROUNDS` (15) kills under continuous `ProbeMarker` load, restoring the killed member between them, so each rejoin replays a Raft log that grew under the previous rounds. Asserts every round is a genuine leadership change, that a long-lived observer on each surviving node keeps delivering in order across all of them, and that a cold-start probe replays the whole multi-tenure history at the end. Needs Docker and `./gradlew operatorDist`; `ROUNDS=3` for a quick local run. CI runs it as `failover.yml` |
| `csharp-client-test.sh` | The C# client tier against a live 3-node cluster: UDP and IPC ingress, the fallback to UDP on a follower, `PendingSends` exactly-once across a leader kill, a cold start replayed from `globalSeqNo` 1, then the C# examples built from the packed package — `FollowStream`, `ColocatedApp`, and a `GatewayApp` pair handed over when its active instance is killed. Rerun on every Aeron upgrade (spec V-1); CI runs it in `chaos.yml` on every pull request. Needs the .NET SDK (`DOTNET` names one off the `PATH`) |
| `csharp-windows-test.sh` | The C# client on Windows, where it is deployed, against a member on Linux, on one host: the member in WSL1, which shares Windows' network stack, and on Windows a gateway host with the C# probe and examples beside it. UDP ingress through `PendingSends`, a cold start through the gateway host, and the C# gateway pair handed over. One member, because three cannot hold Raft's heartbeats under WSL1's system-call translation; the leader kill is `csharp-client-test.sh`'s. Runs in Git Bash; CI runs it in `windows.yml` |
| `replay-bench.sh <preload> [load-during]` | How fast a cold replica replays recorded history to caught-up; prints archive size, elapsed seconds and MB/s |

`failover.yml` also runs the faults none of these scripts can produce — partitions that leave clients
connected, one link cut and no other, lost NAKs or retransmits, delay and duplication, dropped ballots — and a
leader starved of CPU, against the same `docker/compose.yml` cluster. They are
[faulteron](https://github.com/FredrikJDahlberg/faulteron)'s tests, an eBPF injector in its own repository, run
from a pinned commit. To run them locally, on Linux or in a Docker VM, check it out beside this repository and run
its `test/seqeron-*.sh` after `./gradlew operatorDist`.

## Examples

`seqeron-examples` holds the smallest clients there are, one per language in `src/java`, `src/cpp` and
`src/csharp`, and the same flow in each: replay a node's history through that node's co-located Replayer,
switch to the live tap on catching up, and print every frame in `globalSeqNo` order. Each produces as well
as consumes — a `ConnectionOpened` announcing itself, then one ping payload a second whose echo it reads
back off its own tap — so both families are covered in both directions. Each is a **separate build**,
sharing that one source tree: the Java one resolves `org.limitless:seqeron` — the client tier alone, no
sequencer and no archive — the C++ one pulls `seqeron_core` in with `FetchContent`, and the C# one resolves
the `Org.Limitless.Seqeron` package from `build/nuget`, so what the artifacts fail to expose fails there
rather than passing on a source dependency. The C++ half also installs: `cmake --install` writes a CMake
package, and a consumer takes `seqeron::seqeron_core` off `find_package(seqeron)` instead, supplying its own
installed Aeron.

```bash
./seqeron-service/src/main/scripts/start-cluster.sh                              # in another shell

./gradlew publishToMavenLocal && ./gradlew -p seqeron-examples run  # Java

cmake -S seqeron-examples -B seqeron-examples/cmake-build-release \
      -DCMAKE_BUILD_TYPE=Release                                    # C++
cmake --build seqeron-examples/cmake-build-release --target follow_stream
./seqeron-examples/cmake-build-release/follow_stream

dotnet pack -c Release -o build/nuget seqeron-client/src/main/csharp/Seqeron.Client.csproj   # C#
dotnet run --project seqeron-examples/src/csharp/FollowStream
```

`ClusterProbe follow` does the same thing with three modes, latency stats and fault injection on top;
the examples are that one flow with nothing else in them. See
[`seqeron-examples/README.md`](../seqeron-examples/README.md).
