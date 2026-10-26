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
./gradlew compileTestJava                                       # TestGateway and TestApplication, for systemTest
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
cmake --build cmake-build-debug --target run_tests   # C++: 376 cases
./gradlew test                                       # Java: 677 cases
dotnet test --project seqeron-client/src/test/csharp/Seqeron.Client.Tests.csproj   # C#: 333 cases
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
mocks. Everything that needs an Aeron runtime is covered by `core_tests`, the integration suite and the
system tests below. Coverage is a JaCoCo report at `build/reports/jacoco/test/`, written by
`./gradlew test`.

## Integration tests

```bash
./gradlew :seqeron-service:integrationTest
```

The suite launches members in the test's own JVM through `SequencerServer.launch`, and a gateway host through
`ReplayerServer.launchGatewayHost`, on Aeron's `aeron-test-support`. `FailoverConfirmTest` is
`FailoverProcessTest`'s confirm producer in about two seconds: a producer on a follower streams through
`PendingSends`, the leader is stopped mid-stream, and its queue must show every frame exactly once, in order.
`FailoverPropertyTest` puts the producer on a gateway host and runs six seeded rounds, each stopping and restarting
a member or cutting its UDP for up to 1.5 s, the leader or a follower as the round draws. The producer's queue must
come out exact and every member must be up at the end. A failure prints its seed, and
`./gradlew :seqeron-service:integrationTest -Pseed=<n>` replays the rounds' draws, though not the threads'
timing. Both tests then read every queue recording on every member, and on the host, straight off its archive,
since a client's `ReplayerRecovery` would hide a repeated frame or replay over a gap: each must hold groupSeqNo 1
onwards once each, in order, and the same frame wherever two hold the same groupSeqNo. `InProcessCluster` activates
its one group before a test starts, since the cluster sequences nothing before it.

`CommittedEntryLossTest` scripts a fault sequence under which Aeron Cluster 1.53.2 loses a committed entry: a
follower restarts while the leader waits on a member that cannot copy the log, truncates the entries it
acknowledged, and the leader is cut before they are copied back. Each step waits on the members' election state
and commit position rather than on time, so it reproduces every run, and the stranded member stops advancing. It
fails on the pinned Aeron, so it runs only under `-PknownAeronFaults`, to check an Aeron upgrade against it. A second
test runs the same sequence by default and checks that the member that truncates the entry takes itself down, and
that, restarted, it rejoins with an active recording that is one log with the others'
([`fault-tolerance.md`](fault-tolerance.md) §1.4).

The UDP faults are `SequencerServer.launch`'s `udpFaults` overload, which drops each frame a member's driver sends
or receives that it names by stream id and direction: all of them, all but the consensus stream, or only the
consensus messages the member sends. `SystemTestWatcher` fails a test on any error in a driver's, archive's or
consensus module's error log, and copies a failed test's directories to `seqeron-service/build/test-output`. The
suite runs on JUnit 6, which `aeron-test-support` is built against, and on cluster port base 9500, clear of a
local cluster on 9300. A member stopped in-process is closed rather than killed, a cut drops frames rather than
delaying, reordering or duplicating them, and every member shares one JVM's CPU and GC, so the system tests below
remain the evidence for process death, the network faults faulteron injects, and the C# and C++ clients.

## System tests

```bash
./gradlew :seqeron-service:systemTest
```

The end-to-end harnesses, as JUnit 6 tests. `ClusterProcesses` launches the members, the probes and `clusterctl` as
child JVMs off the uber jar, so a kill is a process's death, and reads each member's state from the counters in its
CnC file rather than from its log: the consensus module's state and role, `REPLAYER_READY`, the replays served, the
leader the sequencer last recorded. A member that exits unasked fails the wait in progress, and each process's output
is a file of its own under `seqeron-service/build/system-test/<test>/`. The suite runs on cluster port base 9600,
clear of a local cluster and the integration suite, and `TIMEOUT_SCALE` stretches its deadlines. `TestGateway` and
`TestApplication` are test-source classes in no jar, so `systemTest` compiles the test classes and puts them on the
children's classpath. A test with knobs reads them from the environment, named below.

| test | what it asserts |
| --- | --- |
| `FailoverProcessTest` | A leader is `SIGKILL`ed (every run then takes the crash's path, [`fault-tolerance.md`](fault-tolerance.md) §1.2), and a fresh `ClusterProbe` follower cold-started on the new leader catches up on full history, each node's queue recording being one continuous run spanning both tenures. Two `confirm` producers stream across the kill: the one using `PendingSends` must see every frame exactly once, in order, and an untracked control reports what the kill lost |
| `GapRecoveryTest` | A live queue frame dropped on a caught-up consumer (`SIGUSR1` fault injection; `GAP_SIZE` frames) is recovered by resuming its recording at the hole, not by wedging |
| `PausedSubscriberTest` | A caught-up consumer `SIGSTOP`ped while more than two queue windows go by leaves its member up, and the resumed consumer heals the hole its eviction left |
| `SessionReplaceTest` | A `TestApplication` `SIGSTOP`ped past its session timeout, then deprived of quorum, replaces its cluster session both times, unfenced, without its duty cycle stopping while an attempt is pending; and the `TestGateway` pair paused past its session timeout has the standby take the role over rather than fence alongside the active instance |
| `ReplayerRestartTest` | A client's own node killed and restarted: the client fails fast and a fresh cold start is served from the node's new recording alone |
| `GatewayHostTest` | A `confirm` producer on a gateway host (node 3) streams while the member its relay reads, the leader, is killed: every frame comes back exactly once, in order, and the relay moves to another member. The host is then restarted, and a fresh `ClusterProbe` follower there catches up from its new recording |
| `SnapshotTest` | Application snapshots on a live three-member cluster: the `TestGateway` pair and a `TestApplication` replica per member take part in rounds every 2 s, and the test restarts the standby, fails over onto the restored instance, brings the other back as a hot standby that restores, starts a round with `clusterctl request-snapshot`, restarts a follower's replica, and kills the cluster leader. Every restore reaches the state the log implies and no instance diverges from a sequenced round |
| `ReplayColdStartTest` | A cold `ClusterProbe follow` on member 1 replays `REPLAY_PRELOAD` (20,000) preloaded frames to caught up within 120 s, on a quiet cluster and while four times as many more are sequenced, and has delivered them all in order. It prints the archive size and rate and asserts neither; `REPLAY_PRELOAD=400000` is the case that used to hang |
| `ChaosTest` | Randomized fault injection against a live three-member cluster, with a consumer replica on every member and the `TestGateway` pair (`GW-T-A`/`GW-T-B`, ports 9200/9201) taking load through its accept gate. Each round draws one of kill-leader, kill-follower, `SIGKILL` of a follower, pause, queue-drop and queue-stall, heals it, and checks the invariants; at the end every member's queue recording is gap-free and the same length. It prints its `SEED`, and `SEED=<n>` replays the draws; `ROUNDS`, `ONLY_FAULT` and `STEADY_STATE_SECS` tune the run. Tagged `chaos`, so `systemTest` leaves it out; `./gradlew :seqeron-service:systemTest -Pchaos` runs it alone, as `chaos.yml` does |

The tests tagged `docker` run the `docker/compose.yml` cluster, and `systemTest` leaves them out. They need Docker, and
the faulteron ones a checkout of [faulteron](https://github.com/FredrikJDahlberg/faulteron), an eBPF injector in its
own repository, which `failover.yml` pins (`FAULTERON_DIR`, default `../faulteron`):

```bash
./gradlew :seqeron-service:networkFaultTest   # builds the operator distribution first
```

`DockerCluster` drives the `docker` CLI rather than Testcontainers, whose compose support refuses the file's fixed
`container_name`, and reads a member's state with `docker exec node-N clusterctl counters`. Each test loads
`topology-test-groups.xml` once the cluster is up, and injects nothing until every member's queue answers a ping. The
two relay tests add a gateway host, `node-3` (the compose file's `gateway-host` profile), whose relay reads a follower
first, so that a rule aimed at that follower touches the relay's traffic and not the host's producer, which talks to
the leader. `COMPOSE_BUILD=0` skips `--build` when the image is already built. To run them locally, on Linux or in a
Docker VM, check faulteron out beside this repository and run them after `./gradlew operatorDist`.

| test | what it asserts |
| --- | --- |
| `DockerFailoverSoakTest` | Multi-round containerized failover soak. `ROUNDS` (15) kills under continuous `ProbeMarker` load, restoring the killed member between them, so each rejoin replays a Raft log that grew under the previous rounds. Every round is a genuine leadership change, a long-lived observer on each surviving node keeps delivering in order across all of them, and a cold-start probe replays the whole multi-tenure history at the end. `ROUNDS=3` for a quick local run; CI runs it in `failover.yml` |
| `PartitionLeaderTest` | the leader, cut off from its peers while a `ClusterProbe confirm` producer streams through it, dropped packets; the majority elected a new leader; the old one rejoined as a follower; the PendingSends producer saw every frame once |
| `AsymmetricPartitionTest` | with only the link between the leader and one follower cut (`--peer`), the cut-off follower, which stands for election, does not depose the leader; a producer on the other follower stays exact; the follower catches up once healed |
| `UnrepairedFollowerTest` | with 5% of one follower's log DATA dropped and its NAKs (`REPAIR=nak`, the default) or the leader's retransmissions to it (`REPAIR=retransmit`) gone, the follower falls behind with no election: a ping through its queue passes before the fault, times out under it, and passes after the heal; a producer on the other follower stays exact |
| `JitteryLeaderTest` | with the leader's cluster and ingress traffic delayed 20–50 ms and 10% duplicated, under the 200 ms leader heartbeat timeout, no election runs, every member's queue answers a ping, and a producer on a follower stays exact. `DELAY_MS=250 JITTER_MS=0` takes the delay past the timeout, a run expected to fail |
| `ThrottledLeaderTest` | with the leader's container capped at 0.05 CPUs (`docker update --cpus`), the cluster replaces it or the leader keeps leading, a producer on a follower stays exact, and the old leader rejoins and catches up once its CPU is back. A `follow` cold-started on it at the fault's start is caught up within 30 s of the producer's end, and its Replayer drops no control reply. The fault is the harness's, not faulteron's: `aeron:ipc` never reaches an interface. `CPUS=0.5` is a cap the leader keeps up under, a run expected to fail |
| `MuteVoterTest` | with one follower's RequestVote and Vote messages dropped and the leader then killed, the fault is invisible until the kill, the two survivors elect no leader while it holds and one as soon as it heals, the killed member rejoins, and a producer on the muted follower stays exact. The hold stays under `ClusterStreamSender`'s 5 s new-leader timeout, past which the producer's session closes |
| `RelayLinkTest` | with 5% of the DATA between the gateway host and the follower its relay reads dropped, and every NAK (`FAULT=loss`, the default), the relay leaves that follower and resumes on the next; with that follower's packets to the host delayed 20–50 ms and 10% duplicated (`FAULT=jitter`), the relay stays on it. Either way a producer on the host stays exact, and a fresh `follow` on the host catches up after the heal |
| `SlowArchiveControlTest` | with archive control messages between the gateway host and the relay's second member delayed `DELAY_MS` (requests on the host, `DIRECTION=request`, the default, or responses on the member, `DIRECTION=response`), and the host then cut from the first member, the relay gives the second up for the leader at 500 ms or more, since a connect is two exchanges under one 1 s timeout, and follows it below. A producer on the host stays exact, and the host logs no FATAL |

## Client and operator scripts

The harnesses for the C# and C++ clients, and the operator-facing local cluster, are still bash, under
`seqeron-service/src/test/scripts/`. Each brings a cluster up and tears it down again; run them from the repository
root, with `./gradlew uberJar` done first. The two C# harnesses want the .NET SDK beside the jar:
`csharp-client-test.sh` on Linux, and `csharp-windows-test.sh` on Windows, in Git Bash, with its member in a WSL1
distribution that has a JDK 21. `cpp-client-test.sh` drives the C++ client and wants CMake.

| Script | Purpose |
|--------|---------|
| `start-three-node-cluster.sh` | Start a local 3-node Raft cluster with a per-node `ClusterProbe` replica; blocks until Ctrl-C. `SEQERON_NO_CONSUMERS=1` leaves out the replicas, for a caller that runs its own |
| `csharp-client-test.sh` | The C# client tier against a live 3-node cluster: UDP and IPC ingress, the fallback to UDP on a follower, `PendingSends` exactly-once across a leader kill, a cold start replayed from `groupSeqNo` 1, then the C# examples built from the packed package — `FollowStream`, `ColocatedApp`, a `GatewayApp` pair handed over when its active instance is killed, and a `SnapshotApp` restart that restores from a round it confirmed; last, a member restarted at a cut, where a cold start with no snapshot file must fail from its poll. Rerun on every Aeron upgrade (spec V-1); CI runs it in `chaos.yml` on every pull request. Needs the .NET SDK (`DOTNET` names one off the `PATH`) |
| `csharp-windows-test.sh` | The C# client on Windows, where it is deployed, against a member on Linux, on one host: the member in WSL1, which shares Windows' network stack, and on Windows a gateway host with the C# probe and examples beside it. UDP ingress through `PendingSends`, a cold start through the gateway host, and the C# gateway pair handed over. One member, because three cannot hold Raft's heartbeats under WSL1's system-call translation; the leader kill is `csharp-client-test.sh`'s. Runs in Git Bash; CI runs it in `windows.yml` |
| `cpp-client-test.sh` | The C++ client tier against a live 3-node cluster: `cluster_probe` runs the same phases as the C# probe — UDP and IPC ingress, the fallback to UDP on a follower, `PendingSends` exactly-once across a leader kill, a cold start replayed from `groupSeqNo` 1 — then the C++ examples, built as an outside consumer builds them: `follow_stream`, `colocated_app`, a `gateway_app` pair handed over when its active instance is killed, and a `snapshot_app` restart that restores from a round it confirmed; last, a member restarted at a cut, whose recording `cluster_probe scan` replays with `ClusterStreamClient`, and where a cold start with no snapshot file must fail from its poll. Builds the probe and the examples in one tree, so Aeron once: `SEQERON_EXAMPLES_BUILD` (default `seqeron-examples/cmake-build-e2e`), configured with `SEQERON_BUILD_TESTS=ON`, and as Release only if it does not exist; CI runs it in `chaos.yml` on every pull request |

## Examples

`seqeron-examples` holds the smallest clients there are, one per language in `src/java`, `src/cpp` and
`src/csharp`, and the same flow in each: replay a node's history through that node's co-located Replayer,
switch to the live queue on catching up, and print every frame in `clusterSeqNo` order. Each produces as well
as consumes — a `ConnectionOpened` announcing itself, then one ping payload a second whose echo it reads
back off its own queue — so both families are covered in both directions. Each is a **separate build**,
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
