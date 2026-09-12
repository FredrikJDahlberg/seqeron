<picture>
  <source media="(prefers-color-scheme: dark)" srcset="doc/branding/seqeron-wordmark-dark.svg">
  <img src="doc/branding/seqeron-wordmark.svg" alt="seqeron" width="248" height="60">
</picture>

## Overview

**seqeron** assigns a single global, gap-free, replicated total order to messages arriving from
external producers, using an Aeron Cluster (Raft) replicated state machine as the sequencer.
Everything downstream of that point reads from one authoritative ordered stream instead of
coordinating directly with each other.

This repository is the **sequencing tier alone** — the sequencer, the replayer (both sides), the
client-side plumbing that follows the ordered stream, and the operator tooling. It carries **no FIX
code, no order flow and no reference data**: the edges that speak those protocols are their own
project, and the boundary is enforced rather than agreed. Nothing here decodes a `payloadId`, and the
build reaches for no application schema.

Each node republishes every sequenced frame onto a **node-local `aeron:ipc` tap** (stream 205) that
its own co-located Aeron Archive records. Leader and follower alike publish and record their own tap,
and since every node processes the same committed log in the same order the taps are byte-identical —
each node's archive independently holds a complete copy of sequenced history, with no cross-node
replication. Co-located applications read the tap *directly* and untethered for the live feed, and ask
a per-node **Replayer** to serve cold-start history and gaps off the recording. The sequencer therefore
has **zero live network subscribers**: a slow replica is dropped and heals by replay rather than
back-pressuring the cluster.

### Processes

- **`SequencerServer` / `SequencerService` / `Sequencer`** (Java) — the cluster node. `Sequencer` is
  the replicated state machine proper (no Aeron dependency, unit-tested directly): it stamps each
  ingress message with a monotone `globalSeqNo` plus the Raft consensus timestamp and synthesizes the
  frames the cluster itself owns (`ClusterHeartbeat`, `LeadershipChanged`, `GatewayActive`).
  `SequencerService` is its Aeron adapter and holds no replicated state of its own.
- **`ReplayerServer` / `ReplayerService`** (Java, `replayer/server`) — one per member, co-located in
  that member's Aeron directory. The only process that reads the archive: it serves an on-demand
  replay protocol to the co-located replicas, and sits off the live delivery path entirely. Its
  decisions live in pure seams — `Replayer`, `ReplaySlotAllocator`, `ReplayRecordings`,
  `ReplayClientIdCollisions` — with `AeronReplayer` the only part that touches Aeron.
- **`ClusterCtl`** (Java, `clusterctl`) — start/status/shutdown/activation tooling. Its `start` and
  `shutdown` publish `ClusterStarted`/`ClusterStopped` markers *through* the log, so the boundaries of
  a run are themselves sequenced. See [Operator tooling](#operator-tooling).
- **`ClusterProbe`** (Java) — the edge-neutral load generator and tap consumer this tier's own
  end-to-end scripts drive a cluster with. Three modes: `submit` (flood `ProbeMarker`s at ingress),
  `ping` (round-trip one through consensus and back off the tap) and `follow` (replay history through
  the co-located Replayer, then follow the tap live). It exists so core's e2e suite needs no product
  binary built.
- **`MetricsExporter` / `MetricsAggregator`** (Java) — the ops plane, orthogonal to the data flow: a
  node-local exporter serves `/metrics` off the Aeron CnC counters, and the aggregator pulls every
  node's exporter into one combined Prometheus endpoint (`doc/ops.md`).
- **`TestGateway`** (Java, `src/test/java`) — an elected active/standby producer used only by
  `chaos-runner.sh`. It speaks no application protocol and holds no session state, but it holds the
  same four fences a real gateway does, so the recovery-stall policy gets exercised inside this repo.
  It is in the test source set and therefore in no jar.

The **C++ half is a client library, not a set of executables**: `seqeron_core` is header-only, and
the only binary this build produces is `core_tests`. It gives an application written in C++ the
consumer side of everything above — `ClusterStreamSender` (the cluster client session state machine),
`ClusterStreamClient` / `ReplayerStreamReceiver` (replay history, then follow the tap live),
`SequencedFrame` (the envelope), `PortLayout`, and the pure policy classes. There is **no C++ replay
server**; the server side of the replay protocol is Java only.

### Load-bearing properties

- **The cluster parses no application payload.** Every ingress message is an `Unsequenced` frame
  (`sbe-frame.xml`, schema 210) whose body is one opaque payload named by `header.payloadId`;
  `Sequencer` decodes the frame header, stamps it, and copies the payload through byte-identical.
  Sequencing is copy-18/append-16, the body is never re-encoded, and `payloadId` 1 is retired and
  refused on ingress — what used to travel under it is now the **system family**, seqeron's own
  vocabulary, named by `header.systemEventType` at the same offset.
- **A consumer splits by family first, then dispatches on `(payloadId, templateId)` — never
  `templateId` alone.** Template ids are unique per schema, so two applications' templates can
  collide, and the uint16 at offset 16 is a `payloadId` on one family and a `systemEventType` on the
  other. `unwrapFrame` (C++, `SequencedFrame.hpp`) and `SequencedFrameDecoder` (Java) are the one
  place the envelope is stripped.
- **The log holds the authoritative state, and every decision consumers must agree on is emitted
  rather than inferred.** Connects, disconnects, promotions and the clock all round-trip through the
  sequencer, so a restarted or standby replica rebuilds by replaying rather than by asking anyone.
- **No snapshots — recovery is always full-log replay from `globalSeqNo` 1.** `SequencerService`
  refuses to take or restore one. That is what keeps every node's tap recording complete: a node
  restored from a snapshot would record only from wherever it resumed. The cost is recovery time and
  archive size growing with uptime — the 1 Hz heartbeat alone is ~86.4k frames/day.
- **A node that cannot record terminates itself.** `TapStallPolicy` watches the archive's
  `RecordingPos` counter, and a node whose recording has stopped or stopped advancing exits (70)
  rather than sequence history it cannot keep. Peers keep quorum, and the restart rebuilds its
  recording over the full-log replay it does anyway.

`doc/seqeron-protocol-spec.md` is the normative protocol specification; `doc/fault-tolerance.md`
covers what survives node loss, failover, a stuck archive and a lost frame.

## Build

Requires **JDK 21** and a **C++23** compiler. The C++ build needs Java on `PATH` too — the SBE tool
is a jar, downloaded once at configure time — and fetches Aeron 1.51.0 and GoogleTest from source.
Both halves generate independently from the same schemas under `src/main/sbe`, so their Aeron and SBE
versions are pinned to match (`build.gradle`'s `ext` block, `CMakeLists.txt`'s `FetchContent`).

### Java

```bash
./gradlew compileJava
./gradlew uberJar     # fat jar, run without Gradle: build/libs/seqeron-0.1.0-uber.jar
./gradlew test        # JUnit 5, ~1s
```

Every script resolves `build/libs/seqeron-0.1.0-uber.jar`, so `uberJar` is the prerequisite for all
of them; `SEQERON_JAR` overrides the path.

The codegen tasks run as part of `compileJava` and can be invoked on their own:

```bash
./gradlew generateFrameSbe generateReplaySbe generateProbeSbe   # Java codecs + IR
./gradlew generateClusterSbeIr                                  # IR only, no codecs
./gradlew compileTestJava                                       # TestGateway, for chaos-runner.sh
```

### C++

```bash
cmake -B cmake-build-debug -DCMAKE_BUILD_TYPE=Debug      # AddressSanitizer
cmake --build cmake-build-debug

cmake -B cmake-build-release -DCMAKE_BUILD_TYPE=Release
cmake --build cmake-build-release
```

`-DSEQERON_COVERAGE=ON` adds coverage instrumentation. The tree is developed on macOS/arm64 with
Apple clang; CI builds it on Ubuntu with both clang and gcc-14.

## Tests

```bash
cmake --build cmake-build-debug --target run_tests   # C++: 132 cases
./gradlew test                                       # Java: 286 cases
```

`run_tests` is `ctest --output-on-failure` with the build dependency wired up; plain `ctest` works
too, and the `..._NOT_BUILT` noise the old tree had to filter is gone with simdfix.

Run a single C++ suite by filter, or a single Java test class:

```bash
./cmake-build-debug/core_tests --gtest_filter='ReplayerRecovery*'
./gradlew test --tests '*SequencerTest'
```

The Java suite covers the deterministic decision-making — `Sequencer`, and `ReplayerService` through
its `Replayer` seam — and deliberately touches no Aeron runtime: no media driver, no cluster, no Aeron
mocks. Everything Aeron-shaped is covered by `core_tests` and by the end-to-end scripts below.
Coverage is a JaCoCo report at `build/reports/jacoco/test/`, written by `./gradlew test`.

## Scripts

Operator and cluster-lifecycle scripts live under `src/main/scripts/`; they start and stop things or
are standalone tools, and run no tests. `ports.sh` and `paths.sh` are sourced by every other script.

| Script | Purpose |
|--------|---------|
| `start-cluster.sh` | Start the single-node cluster — `SequencerServer`, `ReplayerServer` and a `ClusterProbe follow` replica — in the background; Ctrl-C stops all of them. `SEQERON_NO_CONSUMERS=1` leaves out the replica, for a caller that runs its own |
| `stop-cluster.sh` | Stop everything either start script launched, plus any `SEQERON_EXTRA_PROCESSES="label\|pattern;…"` a caller adds |
| `clusterctl.sh <command>` | Cluster life cycle: `start`, `shutdown`, `activate`, `load-topology`, `counters` — see [Operator tooling](#operator-tooling) |
| `sbe-log-printer.sh <archive-dir>` | Dump an Aeron Archive recording as JSON — see [Log printer](#log-printer) |
| `metrics-exporter.sh` / `metrics-aggregator.sh` | The Prometheus ops plane (`doc/ops.md`) |
| `purgelog.sh [--force]` | Delete archive/cluster directories under `$TMPDIR/seqeron-seq` and the `logs/` directory; the cluster must be stopped first |

The end-to-end harnesses live under `src/test/scripts/`. **All five are Java-only** — they drive the
cluster through `ClusterProbe`, which attaches to a member's own embedded media driver, so three of
them need no standalone `aeronmd` at all. Each brings a cluster up and tears it down again; run them
from the repository root, with `./gradlew uberJar` done first.

| Script | Purpose |
|--------|---------|
| `start-three-node-cluster.sh` | Start a local 3-node Raft cluster with a per-node `ReplayerServer` and `ClusterProbe` replica; blocks until Ctrl-C. `SEQERON_NO_CONSUMERS=1` leaves out the replicas, for a caller that runs its own |
| `failover-test.sh` | Force a failover, then cold-start a fresh `ClusterProbe` follower on the new leader and verify it catches up on full history — each node's tap recording is one continuous run spanning both tenures |
| `gap-recovery-test.sh` | Drop a live tap frame on a caught-up consumer (SIGUSR1 fault injection) and verify it re-walks its recording and heals rather than wedging |
| `replayer-restart-test.sh` | Kill and restart a node's `ReplayerServer` while a client is riding a replay from it, then kill and restart the client's own node and verify its cold-start walk crosses a real multi-recording chain |
| `chaos-runner.sh` | Randomized fault injection against a live 3-node cluster, with the `TestGateway` pair (`GW-T-A`/`GW-T-B`, ports 9200/9201) taking load through its accept gate; every run prints its `SEED` to replay the exact fault sequence. Needs `./gradlew uberJar compileTestJava` |
| `replay-bench.sh <preload> [load-during]` | How fast a cold replica replays recorded history to caught-up; prints archive size, elapsed seconds and MB/s |

## Sequencer

The sequencer runs as a 1- or 3-node Aeron Cluster. Each node is launched with `SequencerServer` and
configured entirely via system properties.

### Single-node (development)

```bash
./gradlew uberJar

java \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  -Dsequencer.memberId=0 \
  -jar build/libs/seqeron-0.1.0-uber.jar
# [SequencerServer] Starting member 0 | ingress=aeron:udp?endpoint=localhost:9302 | archive=aeron:udp?endpoint=localhost:9301 | baseDir=/tmp/seqeron-seq
# [SequencerServer/0] Running — Ctrl-C to stop
```

The node embeds its own MediaDriver and Archive — no separate `aeronmd` needed. Data is written to
`$TMPDIR/seqeron-seq/archive-0` and `$TMPDIR/seqeron-seq/cluster-0`.

`src/main/scripts/start-cluster.sh` does the same thing plus a co-located `ReplayerServer` and a
consumer replica, which is usually what you want:

```bash
./src/main/scripts/start-cluster.sh
# [cluster.sh] SequencerServer is running
# [ReplayerService/0] ready — tap recording 0 live, 1-recording chain verified from globalSeqNo 1; serving replay
# [ClusterProbe/0] Caught up — following live
```

### Three-node cluster

Run each command on its respective host (or in separate terminals on localhost for testing) — only
`-Dsequencer.memberId` differs between them:

```bash
java \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  -Dsequencer.memberId=0 \
  -Dsequencer.baseDir=/var/seqeron-seq \
  "-Dsequencer.clusterMembers=0,host0:9302,host0:9303,host0:9304,host0:9305,host0:9301|1,host1:9312,host1:9313,host1:9314,host1:9315,host1:9311|2,host2:9322,host2:9323,host2:9324,host2:9325,host2:9321" \
  -jar seqeron-0.1.0-uber.jar
```

`src/test/scripts/start-three-node-cluster.sh` builds that string with `ports.sh`'s
`cluster_members_string` and brings all three up on localhost.

### Port layout

Each member's ports are `9300 + memberId × 10 + offset` — the formula lives in
`sequencer/PortLayout.hpp`, `SequencerServer`'s Javadoc and `scripts/ports.sh`, and nowhere else:

| Offset | Purpose          | Member 0 | Member 1 | Member 2 |
|--------|------------------|----------|----------|----------|
| +1     | Archive control  | 9301     | 9311     | 9321     |
| +2     | Ingress          | 9302     | 9312     | 9322     |
| +3     | Consensus        | 9303     | 9313     | 9323     |
| +4     | Cluster log      | 9304     | 9314     | 9324     |
| +5     | File transfer    | 9305     | 9315     | 9325     |

This tier reserves **9300–9329** for those (three members of stride 10, wider than the 9301–9325 three
nodes actually bind), **9200–9209** for its own harness listeners, and `9400 + memberId` / 9500 for
the metrics plane. Every other block — an application's TCP listen port, each co-located client's
cluster egress port, the replay ports — belongs to the process that binds it, so this repo names none
of them. `doc/registries.md` §2 is the block table across all of them.

The sequenced stream itself has **no port**: it is a node-local `aeron:ipc` tap (stream 205) recorded
into each member's own archive.

### System properties

| Property                    | Default                          | Description                        |
|-----------------------------|----------------------------------|------------------------------------|
| `sequencer.memberId`        | `0`                              | Raft member ID for this node       |
| `sequencer.baseDir`         | `$TMPDIR/seqeron-seq`           | Root for archive and cluster dirs  |
| `sequencer.aeronDir`        | `$TMPDIR/seqeron-seq-aeron-<id>`| Aeron media driver directory       |
| `sequencer.clusterMembers`  | single-node localhost            | Full Aeron clusterMembers string   |
| `sequencer.idleStrategy`    | `backoff`                        | `backoff` or `yielding`            |

`ReplayerServer` takes `replayer.memberId` and `replayer.aeronDir` on the same defaults, so a
co-located pair needs only a matching `memberId`.

### Restart and failover

Archive and cluster directories are preserved on restart (`deleteArchiveOnStart=false`,
`deleteDirOnStart=false`). A node rejoins and replays the log in full — there are no snapshots, so
recovery always starts from `globalSeqNo` 1, which is what keeps every node's tap recording a
complete copy of history. `clusterctl shutdown` uses `ABORT` for the same reason. To wipe state for a
clean start, delete the `archive-<id>` and `cluster-<id>` subdirectories under `baseDir` — or run
`purgelog.sh`.

A leader failover is not a break in the tap: the tap publication is created once in `onStart` and
never re-created on a leadership change (`aeron:ipc` has no port to collide on), so a node's recording
is one continuous run spanning every leader tenure.

## Operator tooling

`clusterctl` is node-local — run it co-located with a `SequencerServer`, on any member:

```bash
./src/main/scripts/clusterctl.sh counters        # this node's operator counters; needs no cluster connection
./src/main/scripts/clusterctl.sh start           # record a "system started" marker (requires an elected leader)
./src/main/scripts/clusterctl.sh shutdown        # orderly stop; safe on every node, a no-op on followers
./src/main/scripts/clusterctl.sh activate <gatewayId>
./src/main/scripts/clusterctl.sh load-topology <file.xml>
```

`load-topology` publishes the deployment document — the gateway list, the co-located applications,
then the protocol registry — validated against the packaged `topology.xsd`. Run it once per cluster
lifetime, before any reference-data load. Only the gateway list is acted on: the sequencer synthesizes
the bootstrap `GatewayActive` per logical gateway behind the row whose `remaining` counts down to 0.
The application and protocol rows are labelling for `SbeLogPrinter`, decoded by nothing and gating
nothing.

Anything `clusterctl` does not recognize is passed through to `io.aeron.cluster.ClusterTool` against
this node's cluster dir (`describe`, `errors`, `list-members`, `recording-log`, …). `snapshot` is
refused. `CLUSTERCTL_*` environment variables map onto the `clusterctl.*` system properties; the full
runbook is `doc/clusterctl.md`.

## Log printer

`SbeLogPrinter` dumps an Archive recording (`archive.catalog` plus segment files under `archive-<id>`)
as JSON, decoded against the generated SBE IR. It works on a still-running cluster — an in-progress
recording is printed up to whatever has been written so far.

```bash
./src/main/scripts/sbe-log-printer.sh "${TMPDIR:-/tmp}/seqeron-seq/archive-0" --stream 205 --oneline
```

Or through Gradle, which takes the same options as `-P` properties:

```bash
./gradlew sbeLogPrinter -PlogDir="${TMPDIR:-/tmp}/seqeron-seq/archive-0" -Pstream=205 -Poneline
```

### Schemas

Four IR files ship inside the jar and **all of them are loaded by default** — `frame` (schema 210, the
envelope and the system family), `replay` (212, the node-local replay control plane), `probe` (214,
`ClusterProbe`'s own payload) and `cluster` (111, the Raft consensus log). Each frame is decoded
against the schema its own header names, so a single run reads an archive dir end to end whatever mix
of recordings it holds:

```
[Catalog] Recording ID: 0 | Stream ID: 205 | ...    → frames  (schema 210)
[Catalog] Recording ID: 1 | Stream ID: 100 | ...    → cluster (schema 111)
```

`--schema <name>` narrows the run to one; frames of the others are then labelled `<schema N not
loaded>` and skipped. `--list-schemas` prints the bundled names. `--spec <file.sbeir>` decodes against
an IR file outside the jar instead — the two are mutually exclusive, and it is how an application's
own schema gets in front of the tool.

`sbe-cluster.xml` is a trimmed mirror of `io.aeron.cluster.codecs`: the subset the C++ cluster client
needs in order to speak the wire protocol, plus a decode-only section covering what
`io.aeron.cluster.LogPublisher` appends to the Raft log — `TimerEvent`, `SessionOpenEvent`,
`SessionCloseEvent`, `ClusterActionRequest`, `NewLeadershipTermEvent`. A frame whose template the
schema does not define prints as `<not in schema>` with its template id rather than aborting the
scan, which is what a future Aeron version appending something new would look like.

### Selecting a recording

An archive dir holds more than one recording, so by default the printer dumps **all** of them. Stream
100 is the Raft cluster log; each recording on 205 is one generation of the sequenced tap, because a
node restart replays its whole cluster log and re-emits every message onto a *new* tap recording — so
a later recording starts again at `globalSeqNo` 1 and the earlier one is a strict prefix of it.

`--stream 205` dumps only the **newest** recording on that stream — one complete copy of sequenced
history, no repeats. Recording ids are not stable across restarts, which is why the selector is the
stream rather than the id. Omit it to get everything, stale tap generations included. The printer
exits non-zero if the requested stream matches no recording.

### Output format

Each message is preceded by a separator naming it — the JSON carries field values only, so a
header-only message such as `ClusterHeartbeat` would otherwise be indistinguishable from any other.
`--oneline` collapses each message onto a single line, which greps and diffs far better than the
default pretty print:

```
LeadershipChanged = { "header": { "sourceId": -1, "connectionId": -1, "sessionId": -1, "systemEventType": 5, "globalSeqNo": 1, "timestamp": 1788716861366 }, "newLeaderMemberId": 0 }
ClusterHeartbeat = { "header": { "sourceId": -1, "connectionId": -1, "sessionId": -1, "systemEventType": 16, "globalSeqNo": 2, "timestamp": 1788716862367 } }
```

The dump as a whole is not a JSON document either way — the `[Catalog]` and separator lines sit
between the objects — but with `--oneline` each individual message line parses on its own.

### Piping payloads to another decoder

`-o <payloadId>` writes that protocol's payloads to **stdout**, raw and back to back, for a decoder
that owns their schema (`doc/seqeron-protocol-spec.md` §13.1). This tier decodes no application
payload at all, so this is how one gets out to something that does:

```bash
./src/main/scripts/sbe-log-printer.sh "${TMPDIR:-/tmp}/seqeron-seq/archive-0" --stream 205 \
    -o 2 2>frames.log | order-decode
```

Stdout belongs to the payload stream for the whole run, so **every text line moves to stderr** — the
`[Catalog]` line, the dump itself, the errors. Redirect it as above to keep the frames beside the
payloads; the two are emitted in the same order, and the frame line is where `globalSeqNo` is. The
stream carries no framing of its own: an SBE payload declares its own block and var-data lengths, so
the decoder that holds the schema is what delimits it. It works on the Raft log (`--stream 100`) as
well as the tap, reading the ingress side of the same frames.

There is no `-P` property for this on the Gradle task — Gradle re-encodes a child process's stdout,
which corrupts the payload bytes. Use the script or the jar directly.

### Naming a payload it cannot decode

A payload whose schema is not loaded prints as its ids rather than being decoded — but it is
**labelled**, from the `PayloadIdRegistered` rows `clusterctl load-topology` put in the same recording
(`doc/seqeron-protocol-spec.md` §6.3):

```
<undecodable payload 2 (order v1): schema 220, templateId 1>
<undecodable payload 7: schema 900, templateId 3>
```

The second is an unregistered `payloadId`, which prints under its number. Registration is labelling
only: the sequencer never decodes those rows and they gate no frame.

## Example consumer

`examples/java` and `examples/cpp` are the smallest consumers there are, one per language and the same
flow in both: replay a node's history through that node's co-located Replayer, switch to the live tap on
catching up, and print every frame in `globalSeqNo` order. Each is a **separate build** — the Java one
resolves `org.limitless:seqeron` — the client tier alone, no sequencer and no archive — the C++ one pulls `seqeron_core` in with
`FetchContent` — so what the artifacts fail to expose fails there rather than passing on a source
dependency.

```bash
./src/main/scripts/start-cluster.sh                              # in another shell

./gradlew publishToMavenLocal && ./gradlew -p examples/java run  # Java

cmake -S examples/cpp -B examples/cpp/cmake-build-release \
      -DCMAKE_BUILD_TYPE=Release                                 # C++
cmake --build examples/cpp/cmake-build-release --target follow_stream
./examples/cpp/cmake-build-release/follow_stream
```

`ClusterProbe follow` does the same thing with three modes, latency stats and fault injection on top;
the examples are that one flow with nothing else in them. See
[examples/java/README.md](examples/java/README.md) and [examples/cpp/README.md](examples/cpp/README.md).

## Documentation

| Document | What it is |
|----------|------------|
| `doc/seqeron-protocol-spec.md` | The normative protocol specification — frames, families, the system vocabulary, the topology document |
| `doc/fault-tolerance.md` | Node loss, leader failover, a stuck archive, a lost frame: what survives each and how it recovers |
| `doc/registries.md` | The two shared namespaces — the producer `sourceId` space and the UDP port blocks |
| `doc/clusterctl.md` | The operator tool's runbook |
| `doc/ops.md` | The Prometheus/Grafana metrics stack |
| `doc/publishing.md` | What a consumer can resolve today, and the open items between that and a published coordinate |

Those six are the whole doc set, and every document reference in this tree resolves inside it.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the full text, and
<https://www.apache.org/licenses/LICENSE-2.0> for the canonical copy. Copyright is recorded in
[NOTICE](NOTICE); §4d obliges anyone redistributing seqeron to carry that file forward. Every
dependency the uber jar redistributes — Aeron, Agrona, sbe-tool — is under the same license, and
both files ship inside the jar under `META-INF/`.
