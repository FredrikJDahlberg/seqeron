![phixeron](doc/phixeron.png)

## Overview

phixeron assigns a single global, gap-free, replicated total order to inbound FIX messages
arriving over TCP, using an Aeron Cluster (Raft) replicated state machine as the sequencer.
Everything downstream of that point — order execution, position tracking, risk queries, FIX
resend/replay — reads from that one authoritative ordered stream instead of coordinating
directly with each other.

Each node republishes every sequenced frame onto a **node-local `aeron:ipc` tap** (stream 205)
that its own co-located Aeron Archive records. Leader and follower alike publish and record
their own tap, and since every node processes the same committed log in the same order the taps
are byte-identical — each node's archive independently holds a complete copy of sequenced
history, with no cross-node replication. Co-located C++ replicas read the tap *directly* and
untethered for the live feed, and ask a per-node **Replayer** to serve cold-start history and
gaps off the recording. The sequencer therefore has **zero live network subscribers**: a slow
replica is dropped and heals by replay rather than back-pressuring the cluster.

### Processes

- **`SequencerServer` / `SequencerService` / `Sequencer`** (Java) — the cluster node. `Sequencer`
  is the replicated state machine proper (no Aeron dependency, unit-tested directly): it stamps
  each ingress message with a monotone `globalSeqNo` plus the Raft consensus timestamp and
  synthesizes the frames the cluster itself owns (`ClusterHeartbeat`, `LeadershipChanged`, `GatewayActive`).
  `SequencerService` is its Aeron adapter and holds no replicated state of its own.
- **`ReplayerServer` / `ReplayerService`** (Java) — one per member, co-located in that member's
  Aeron directory. The only process that reads the archive: it serves an on-demand replay
  protocol to the co-located replicas, and sits off the live delivery path entirely.
- **`FixGateway`** (C++) — the FIX edge process, deployed as one active instance plus optional
  hot standbys of the same logical gateway. It bridges FIX TCP sessions to cluster ingress and
  frames execution reports arriving on the tap back to the originating connection. It holds no
  authoritative session state: a standby or restarted instance rebuilds every session by
  shadowing the tap, and serves clients only once a `GatewayActive` names it. Owns FIX resend
  recovery.
- **`ExchangeGateway`** (Java) — the **venue-facing** edge, the mirror of `FixGateway` pointing
  outward: an [Artio](https://github.com/real-logic/artio) initiator that logs on to an exchange,
  deployed as its own active/passive pair under a second `gatewaySourceId`. Artio owns the socket,
  the codecs and the session FSM, but every message it decides to send goes to cluster ingress first
  and reaches the venue only when it comes back on the tap — the same invariant the C++ edge holds,
  reached by a different mechanism. Session layer only: no order flow crosses it yet.
- **`OrderExecServer`** (C++) — a replica on *every* node, and the system's **execution venue**:
  it acknowledges each `NewOrderSingle` with an `ExecutionReport(New)` whose `ExecID` derives
  from the order's `globalSeqNo`, tracks per-account positions from fills, and answers
  `PortfolioQueryRequest` from an in-process `MockRiskEngine`. All replicas track state; only
  the replica on the current leader emits, with `OutstandingQueries` keeping query replies
  exactly-once across a failover.
- **`BasicDataServer`** (C++) — the reference-data gateway, a replica on every node and
  dual-role: on the leader it publishes the static reference data (FIX session comp-id pairs,
  gateway topology, trading-day calendar) into cluster ingress as ordinary messages; on every
  node it consumes them back off the tap into identical in-memory tables. This is what makes
  session identity and gateway topology properties of the log rather than of per-process config.
- **`ClusterCtl`** (Java, `clusterctl`) — start/status/shutdown tooling. Its `start` and
  `shutdown` publish `ClusterStarted`/`ClusterStopped` markers *through* the log, so the
  boundaries of a run are themselves sequenced.
- **`MetricsExporter` / `MetricsAggregator`** (Java) — the ops plane, orthogonal to the FIX data
  flow: a node-local exporter serves `/metrics` off the Aeron CnC counters, and the aggregator
  pulls every node's exporter into one combined Prometheus endpoint.

### Load-bearing properties

- **The cluster never parses FIX message bodies.** Every ingress message is an `Unsequenced` frame
  (`sbe-frame.xml`, schema 210) whose body is one opaque payload named by `header.payloadId`;
  `Sequencer` decodes the frame header, stamps it, and copies the payload through byte-identical.
  Only `payloadId` 1 — seqeron's own core payloads — is ever opened, and the one bounded exception
  inside it is the `GatewayRegistered` list row.
- **The log holds the authoritative state, and every decision consumers must agree on is
  emitted rather than inferred.** FIX session state is driven only by cluster-replicated
  callbacks, never straight off the TCP receive path; connects/disconnects, refusals, order
  acks, promotions and the clock all round-trip through the sequencer.
- **No snapshots — recovery is always full-log replay from `globalSeqNo` 1.** That is what
  keeps every node's tap recording complete, and all derived state is a pure function of the
  log. The cost is recovery time and archive size growing with uptime, bounded in practice by a
  one-trading-day log.

It depends on a sibling project, **simdfix**
(`git@github.com:FredrikJDahlberg/simdfix.git`, fetched via CMake `FetchContent`), which
provides the generic FIX wire-format codec, session state machine base classes, and the code
generator used to turn `fix-session.xml`/`fix-application.xml` into C++ FIX message headers.
phixeron generates its own copy of those headers rather than reusing simdfix's test fixtures.

`doc/design.md` is the full design description this summarizes; its *Known gaps* section and
`doc/todo.md` are the authority on what is not built yet — most notably there is no matching
engine (no fills, cancels or replaces past the `New` ack), no pre-trade risk gating, and no
edge authentication.

## Build

The source is split by module, and the directory a file is in is what owns it: `cluster/` is the
cluster tier (the sequencer, the replayer, the shared client classes and the tools, in both
languages), `gateways/` is the Java Artio FIX legs, and `src/main/cpp` is the C++ FIX edge. The
dependency runs one way, product to cluster, and both builds enforce it — Gradle through
`:gateways` depending on `:cluster`, CMake through the `phixeron_core` / `phixeron` target pair.
Each module owns its schemas under `<module>/src/main/sbe` (the C++ edge's stay in the root's
`src/main/resources`); the scripts under `src/{main,test}/scripts` are still shared at the root.
See `doc/future-arch.md` §11 for where this is going.

### C++

```bash
# Debug build (AddressSanitizer + coverage)
cmake -B cmake-build-debug -DCMAKE_BUILD_TYPE=Debug
cmake --build cmake-build-debug

# Release build
cmake -B cmake-build-release -DCMAKE_BUILD_TYPE=Release
cmake --build cmake-build-release
```

### Java

```bash
./gradlew compileJava

# Fat jar (run without Gradle)
./gradlew uberJar

# The artioSpike source set (MockExchange, MockOrderClient, FixTestClient) — neither
# compileJava nor uberJar builds it, and the two Artio gateway e2e scripts need it
./gradlew compileArtioSpikeJava
```

## Tests

```bash
cd cmake-build-debug && cmake --build . --target run_tests
```

`run_tests` runs both GoogleTest binaries: `core_tests` (the cluster tier) then `phixeron_tests`
(the C++ FIX edge). On the Java side `./gradlew test` runs both modules' suites.

Use `run_tests`, not plain `ctest`: simdfix's own test suite is registered here too, and since
its targets are `EXCLUDE_FROM_ALL` and never built in this project, `ctest` reports them as
spurious `..._NOT_BUILT` failures alongside phixeron's real results. If you do need `ctest`,
filter them out with `ctest --output-on-failure -E "_NOT_BUILT"`.

---

## Scripts

Cluster start/stop and utility scripts live under `cluster/src/main/scripts/` — these only start and
stop the cluster (or are standalone tools); they run no tests. `start-three-node-cluster.sh` is the
exception that moved: it stands up a cluster for the harnesses below and nothing deploys from it, so
it lives with them:

| Script | Purpose |
|--------|---------|
| `start-cluster.sh [debug\|release]` | Start the single-node cluster (`SequencerServer`, `aeronmd`, `FixGateway`, `ReplayerServer`, `OrderExecServer`) in the background; Ctrl-C stops all of them |
| `stop-cluster.sh` | Stop all cluster processes started by either start script |
| `sbe-log-printer.sh <archive-dir>` | Dump an Aeron Archive recording as JSON (see [Log printer](#log-printer)) |
| `purgelog.sh [--force]` | Delete archive/cluster directories under `$TMPDIR/phixeron-seq` and the `logs/` directory; cluster must be stopped first |

Test scripts sit under the module whose side they exercise (`doc/future-arch.md` §11 step 7a.2):
`cluster/src/test/scripts/` for the cluster tier's, `gateways/src/test/scripts/` for the Artio legs',
and the root's `src/test/scripts/` for the C++ edge's. Each brings the cluster up (via
`start-cluster.sh` above or `start-three-node-cluster.sh`) and tears it down via `stop-cluster.sh`
(run them from the repository root):

| Script | Purpose |
|--------|---------|
| `cluster/src/test/scripts/start-three-node-cluster.sh [debug\|release]` | Start a local 3-node Raft cluster with a `FixGateway` and a per-node `ReplayerServer` + `OrderExecServer` replica; blocks until Ctrl-C, then stops all of them |
| `src/test/scripts/three-node-e2e-test.sh [debug\|release]` | Start the 3-node cluster, run `fix_test_server` against it once, then tear everything down and exit with its pass/fail status (set `PHIXERON_FLOOD_ORDERS=<N>` for the delivery-latency-under-load run) |
| `src/test/scripts/fix-test-server.sh [debug\|release] [host [port]]` | Run a single FIX session (Logon → Heartbeat → NewOrderSingle → Logout) against a live `FixGateway` |
| `cluster/src/test/scripts/failover-test.sh` | Force a failover, then cold-start a fresh `OrderExecServer` on the new leader and verify it catches up on full history (each node's tap recording is one continuous run spanning both tenures) |
| `cluster/src/test/scripts/gap-recovery-test.sh` | Drop a live tap frame on a caught-up consumer (SIGUSR1 fault-injection) and verify it re-walks its recording and heals rather than wedging |
| `gateways/src/test/scripts/exchange-gateway-test.sh` | Bring up the venue leg — the `EGW-A`/`EGW-B` pair against a `MockExchange` — and verify nothing reaches the venue that has not round-tripped consensus, that a restart rebuilds session state from the log, and that failover works both automatically and via `clusterctl`. All-Java; no C++ build needed |
| `cluster/src/test/scripts/replayer-restart-test.sh` | Kill and restart a node's `ReplayerServer` while a client is riding a replay from it, then kill and restart the client's own node entirely and verify its fresh cold-start walk crosses a real multi-recording chain |

---

## Sequencer

The sequencer runs as a 1- or 3-node Aeron Cluster. Each node is launched with
`SequencerServer` and configured entirely via system properties.

### Single-node (development)

```bash
./gradlew uberJar

java \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  -Dsequencer.memberId=0 \
  -jar build/libs/phixeron-0.1.0-uber.jar
# [SequencerServer] Starting member 0 | ingress=aeron:udp?endpoint=localhost:9302 | archive=aeron:udp?endpoint=localhost:9301 | baseDir=/tmp/phixeron-seq
# [SequencerServer/0] Running — Ctrl-C to stop
```

The node embeds its own MediaDriver and Archive — no separate `aeronmd` needed.
Data is written to `/tmp/phixeron-seq/archive-0` and `/tmp/phixeron-seq/cluster-0`.

Each node publishes the sequenced stream onto a node-local `aeron:ipc` tap (stream 205) and records
it into its own co-located Archive. Co-located clients read the tap live **directly** and ask the
per-node Replayer to serve an archive replay on a gap or cold start; a remote client can replay the
recording directly from any member's archive. Every node records an identical continuous copy, so
there is no separate network global stream and no cross-node replication.

### Three-node cluster

Run each command on its respective host (or in separate terminals on localhost for testing):

**Member 0**
```bash
java \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  -Dsequencer.memberId=0 \
  -Dsequencer.baseDir=/var/phixeron-seq \
  "-Dsequencer.clusterMembers=0,host0:9302,host0:9303,host0:9304,host0:9305,host0:9301|1,host1:9312,host1:9313,host1:9314,host1:9315,host1:9311|2,host2:9322,host2:9323,host2:9324,host2:9325,host2:9321" \
  -jar phixeron-0.1.0-uber.jar
```

**Member 1**
```bash
java \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  -Dsequencer.memberId=1 \
  -Dsequencer.baseDir=/var/phixeron-seq \
  "-Dsequencer.clusterMembers=0,host0:9302,host0:9303,host0:9304,host0:9305,host0:9301|1,host1:9312,host1:9313,host1:9314,host1:9315,host1:9311|2,host2:9322,host2:9323,host2:9324,host2:9325,host2:9321" \
  -jar phixeron-0.1.0-uber.jar
```

**Member 2**
```bash
java \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  -Dsequencer.memberId=2 \
  -Dsequencer.baseDir=/var/phixeron-seq \
  "-Dsequencer.clusterMembers=0,host0:9302,host0:9303,host0:9304,host0:9305,host0:9301|1,host1:9312,host1:9313,host1:9314,host1:9315,host1:9311|2,host2:9322,host2:9323,host2:9324,host2:9325,host2:9321" \
  -jar phixeron-0.1.0-uber.jar
```

### Port layout

Each member's ports are `9300 + memberId × 10 + offset`:

| Offset | Purpose          | Member 0 | Member 1 | Member 2 |
|--------|------------------|----------|----------|----------|
| +1     | Archive control  | 9301     | 9311     | 9321     |
| +2     | Ingress          | 9302     | 9312     | 9322     |
| +3     | Consensus        | 9303     | 9313     | 9323     |
| +4     | Cluster log      | 9304     | 9314     | 9324     |
| +5     | File transfer    | 9305     | 9315     | 9325     |

Clients connect to archive control on port 9301 (member 0) to replay history, and to
ingress on port 9302 to send messages. The sequenced stream is a node-local `aeron:ipc` tap
(stream 205) recorded into each member's own archive — no network stream port. Cluster egress is a
fixed UDP port too — 9320 for `FixGateway`/`fix_test_server`, 9330 for `OrderExecServer` (see
[Order execution client](#order-execution-client)), 9360 for `ExchangeGateway` and 9380 for
`OrderGateway` — kept distinct because these sit on independent media driver processes that can't both
bind the same UDP port on `localhost`. The Artio-backed gateways each run their own Aeron Archive as
well, whose control channel needs a UDP port of its own: 9370 for `ExchangeGateway`, 9390 for
`OrderGateway`. On the FIX side, `FixGateway` accepts on 9000, `MockExchange` stands in for the venue
on 9010, and `OrderGateway` accepts on 9020.

### System properties

| Property                    | Default                          | Description                        |
|-----------------------------|----------------------------------|------------------------------------|
| `sequencer.memberId`        | `0`                              | Raft member ID for this node       |
| `sequencer.baseDir`         | `$TMPDIR/phixeron-seq`           | Root for archive and cluster dirs  |
| `sequencer.aeronDir`        | `$TMPDIR/phixeron-seq-aeron-<id>`| Aeron media driver directory       |
| `sequencer.clusterMembers`  | single-node localhost            | Full Aeron clusterMembers string   |

### Client startup

`SequencerClient` (abstract base) handles driver launch, archive connection, replay,
and cluster ingress. Extend it and implement `onSequencedMessage`:

```java
public class MyClient extends SequencerClient {
    @Override
    protected void onSequencedMessage(long globalSeqNo, long sourceSessionId,
                                      long appSeqNo, long timestamp,
                                      SequencedMessageDecoder decoder) {
        // process message
    }
}

// Drive the client
try (MyClient client = new MyClient()) {
    client.start();           // connects to single-node defaults (localhost:9301 / 9302)
    while (running) {
        idleStrategy.idle(client.poll());
    }
}
```

On startup the client replays the full history from the Archive and then follows
live data seamlessly on the same image. Override `replayStartPosition()` to return
the last-processed archive byte position to skip already-applied history on restart.

### Restart and failover

Archive and cluster directories are preserved on restart (`deleteArchiveOnStart=false`,
`deleteDirOnStart=false`). A node rejoins the cluster and replays the log in full — there are no
snapshots, and `SequencerService` refuses to take or restore one, so recovery always starts from
`globalSeqNo` 1 (which is what keeps every node's tap recording a complete copy of history). To wipe
state for a clean start, delete the `archive-<id>` and `cluster-<id>` subdirectories under `baseDir`.

### Log printer

`SbeLogPrinter` dumps an Archive recording (`archive.catalog` + segment files under
`archive-<id>`) as JSON, decoded against the generated SBE IR schema. It works on a
still-running cluster — an in-progress recording is printed up to whatever has been
written so far — so the cluster does not need to be stopped first.

```bash
./gradlew uberJar

./cluster/src/main/scripts/sbe-log-printer.sh "${TMPDIR:-/tmp}/phixeron-seq/archive-0" --stream 205
```

Or via Gradle directly — a `:cluster` task, so it runs on the cluster tier's classpath: core frames
print in full and an application payload is labelled from the recording's own `PayloadIdRegistered`
rows but not decoded. The wrapper above is the one that names payloads inline, because the uber jar
carries every module's IR; `-o` is the wrapper's too (Gradle re-encodes a child's stdout, which
corrupts raw payload bytes).
```bash
./gradlew sbeLogPrinter -PlogDir="${TMPDIR:-/tmp}/phixeron-seq/archive-0" -Pstream=205
```

#### Schemas

Every generated IR file ships inside the uber jar — `frame` (the envelope and core), `order`,
`session` and `basicdata` (the three application payloads), `unsequenced` (the node-local replay
control plane) and `cluster` (the Raft consensus log) — and **all of them are loaded by default**.
Each frame is decoded against the schema its own header names, and a payload inside an envelope the
same way, so a single run reads an archive dir end to end whatever mix of recordings it holds:

```
[Catalog] Recording ID: 0 | Stream ID: 205 | ...    → frames  (schema 210)
[Catalog] Recording ID: 1 | Stream ID: 100 | ...    → cluster (schema 111)
[Catalog] Recording ID: 2 | Stream ID: 205 | ...    → frames  (schema 210)
```

`--schema <name>` narrows the run to one schema; frames of the others are then labelled
`<schema N not loaded>` and skipped. `--list-schemas` prints the bundled names.
`--spec <file.sbeir>` decodes against an IR file outside the jar instead — the two are mutually
exclusive. The Gradle task takes the same as `-Pschema=` / `-Pspec=`.

`sbe-cluster.xml` is a trimmed mirror of `io.aeron.cluster.codecs`: the subset the C++ cluster
client needs to speak the wire protocol, plus a decode-only section covering what
`io.aeron.cluster.LogPublisher` appends to the Raft log — `TimerEvent`, `SessionOpenEvent`,
`SessionCloseEvent`, `ClusterActionRequest`, `NewLeadershipTermEvent`. Between those and
`SessionMessageHeader` (the envelope around every ingress message), a cluster-log recording
decodes end to end:

```
--- Log File Offset: 0 | NewLeadershipTermEvent (templateId 24) ---
{ "leadershipTermId": 0, "logPosition": 96, "timestamp": ..., "termBaseLogPosition": 0,
  "leaderMemberId": 0, "logSessionId": 1548081610, "timeUnit": "MILLIS", "appVersion": 1 }
--- Log File Offset: 224 | SessionOpenEvent (templateId 21) ---
{ "leadershipTermId": 0, "correlationId": 137, "clusterSessionId": 1, "timestamp": ...,
  "responseStreamId": 102, "responseChannel": "aeron:udp?...", "encodedPrincipal": "" }
```

A frame whose template the schema does not define prints as `<not in schema>` with its template
id rather than aborting the scan — which is what you would see if a future Aeron version appended
something new to the log.

#### Selecting a recording

An archive dir holds more than one recording, so by default the printer dumps **all** of them:

```
[Catalog] Recording ID: 0 | Stream ID: 205 | Start Pos: 0 | Stop Pos: 6336
[Catalog] Recording ID: 1 | Stream ID: 100 | Start Pos: 0 | Stop Pos: 8448
[Catalog] Recording ID: 2 | Stream ID: 205 | Start Pos: 0 | Stop Pos: 12480
```

Stream 100 is the Raft cluster log; the two on 205 are successive generations of the sequenced
tap, because a node restart replays its whole cluster log and re-emits every message onto a *new*
tap recording — so recording 2 starts again at `globalSeqNo` 1 and recording 0 is a strict prefix
of it.

`--stream 205` (or `-Pstream=205`) dumps only the **newest** recording on that stream — one
complete copy of sequenced history, no repeats. Recording ids are not stable across restarts,
which is why the selector is the stream rather than the id.

Omit the flag when you want everything, including stale tap generations. Exits non-zero if the
requested stream matches no recording.

#### Output format

Each message is preceded by a separator naming it — the JSON carries field values only, so a
header-only message such as `ClusterHeartbeat` is otherwise indistinguishable from any other:

```
--- Log File Offset: 96 | ClusterHeartbeat (templateId 16) ---
```

`--oneline` (or `-Poneline`) collapses each message onto a single line, which greps and diffs far
better than the default pretty print:

```
--- Log File Offset: 0 | LeadershipChanged (templateId 5) ---
{ "header": { "sourceId": -1, "connectionId": -1, "sessionId": -1, "globalSeqNo": 1, "timestamp": 1784483030632 }, "newLeaderMemberId": 0 }
```

Note the dump as a whole is not a JSON document either way — the `[Catalog]` and separator lines sit
between the objects — but with `--oneline` each individual message line parses on its own.

#### Piping payloads to another decoder

`-o <payloadId>` writes that protocol's payloads to **stdout**, raw and back to back, for a decoder that
owns their schema (`doc/seqeron-protocol-spec.md` §13.1). The printer decodes seqeron's own core
payloads (`payloadId` 1) unaided; everything else is somebody else's protocol, and this is how it gets
out:

```bash
./cluster/src/main/scripts/sbe-log-printer.sh "${TMPDIR:-/tmp}/phixeron-seq/archive-0" --stream 205 \
    -o 2 2>frames.log | order-decode
```

Stdout belongs to the payload stream for the whole run, so **every text line moves to stderr** — the
`[Catalog]` line, the dump itself, the errors. Redirect it as above to keep the frames beside the
payloads; the two are emitted in the same order, and the frame line is where `globalSeqNo` is.

The stream carries no framing of its own: an SBE payload declares its own block and var-data lengths, so
the decoder that holds the schema is what delimits it. It works on the Raft log (`--stream 100`) as well
as the tap, reading the ingress side of the same frames.

There is no `-P` property for this on the Gradle task — Gradle decorates its own stdout, which would
corrupt the stream. Use the script or the jar.

#### Naming a payload it cannot decode

A payload whose schema is not loaded prints as its ids rather than being decoded — but it is
**labelled**, from the `PayloadIdRegistered` rows `clusterctl load-topology` put in the same recording
(`doc/seqeron-protocol-spec.md` §6.3):

```
<undecodable payload 2 (phixeron-order v1): schema 220, templateId 1>
<undecodable payload 7: schema 900, templateId 3>
```

The second is an unregistered `payloadId`, which prints under its number. Registration is labelling
only: the sequencer never decodes those rows and they gate no frame. All four of this deployment's
schemas ship in the jar today, so the label is what a reader sees once an application's schema is no
longer seqeron's to bundle.

---

## Order execution client

`OrderExecServer` (C++, `src/main/cpp/.../order/OrderExecServer.cpp`) combines what used to
be two separate binaries — `application_stream_client` and the C++ `RiskEngineClient` — into one
cluster ingress client. It replays the cluster stream then follows it live, printing every
`NewOrderSingle`/`ExecutionReport` it sees, tracking each account's positions from those same
fills, answering `PortfolioQueryRequest`s with a risk assessment from a mocked external risk
engine, and submitting the `PortfolioQueryReply` back to cluster ingress. The mock engine is
synchronous, slow, and only services 5 requests at once (`MockRiskEngine`); queries beyond that
are throttled by leaving the `PortfolioQueryRequest` fragment unconsumed on the cluster stream
until a slot frees up, rather than blocking or dropping them.

Unlike `FixGateway` (which serves external, potentially remote TCP FIX clients over UDP),
`OrderExecServer` is deliberately deployed **co-located** with one `SequencerServer` member —
sharing that member's own embedded Aeron directory rather than the standalone `aeronmd` — so
archive access/replay, the live (post-catch-up) sequenced-stream tail (the co-located member's
`aeron:ipc` tap, read directly), and — while that member is leader — cluster ingress all go over `aeron:ipc`
instead of looping through two independent UDP media drivers. Cluster egress stays UDP regardless
(see `doc/design.md` §2.7 for the full rationale and fallback behavior when the co-located member
isn't currently leader).

```bash
cmake --build cmake-build-release --target OrderExecServer
PHIXERON_ORDER_EXEC_AERON_DIR="${TMPDIR:-/tmp}/phixeron-seq-aeron-0" ./cmake-build-release/OrderExecServer
# [OrderExecServer] Connected to co-located Aeron media driver at .../phixeron-seq-aeron-0
# [OrderExecServer] Connected to co-located Aeron Archive via IPC (holds the cluster stream recording)
# [OrderExecServer] Live from start
```

`PHIXERON_ORDER_EXEC_AERON_DIR` defaults to member 0's own `sequencer.aeronDir` default
(`$TMPDIR/phixeron-seq-aeron-0`, see [System properties](#system-properties)) — start the
sequencer node first (see [Sequencer](#sequencer)) and only override this if co-locating with a
different member.

---

## FIX TCP test client

`src/test/cpp/org/limitless/phixeron/session/FixTestServer.cpp` connects to the
`FixGateway` on TCP port 9000 and runs a minimal FIX session
using the simdfix `ClientSession` and generated message encoders:

1. **Logon** — negotiates the session (EncryptMethod=None, HeartbeatInterval=30 s)
2. **Heartbeat** — verifies the session is active
3. **NewOrderSingle** — sends a limit Buy order (Account=ACC1, ClOrdID=ORD-0001, AAPL, 100 @ 150.00)
4. **Logout** — tears the session down cleanly
5. **Risk engine query test** — since there is no downstream matching engine, submits a
   synthetic Trade `ExecutionReport` and a `PortfolioQueryRequest` directly to cluster
   ingress (bypassing the FIX/TCP gateway — see
   [order execution client](#order-execution-client)) and prints the resulting
   `PortfolioQueryReply`. Requires `aeronmd` (for `fix_test_server`'s own connection) and
   `OrderExecServer` to be running.

```
SenderCompID = CLIENT
TargetCompID = PHIXERON   (the gateway's identity)
```

### How to run

**1. Start the sequencer node** (single-node dev mode — see [Sequencer](#sequencer)):
```bash
./gradlew uberJar
java \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  -Dsequencer.memberId=0 \
  -jar build/libs/phixeron-0.1.0-uber.jar
```

**2. Start `aeronmd`** (separate terminal) — the C++ clients below need a media driver of
their own, since (unlike `SequencerServer`) they don't embed one:
```bash
source cluster/src/main/scripts/paths.sh   # aeron_default_dir: /dev/shm/aeron-<user> on Linux, $TMPDIR/aeron-<user> on macOS
AERON_DIR="$(aeron_default_dir)" ./cmake-build-release/_deps/aeron-build/binaries/aeronmd
```

**3. Start the FIX gateway** (separate terminal):
```bash
cmake --build cmake-build-release --target FixGateway
source cluster/src/main/scripts/paths.sh
AERON_DIR="$(aeron_default_dir)" ./cmake-build-release/FixGateway
# [TCP] Listening on port 9000
# [FixGateway] Caught up — following live stream
```

**4. Start `OrderExecServer`** (separate terminal — see
[Order execution client](#order-execution-client)), needed for step 5 below.

**5. Build and run the test client** (separate terminal):
```bash
cmake --build cmake-build-release --target fix_test_server
source cluster/src/main/scripts/paths.sh
AERON_DIR="$(aeron_default_dir)" ./cmake-build-release/fix_test_server
# [FixTestServer] Connecting to 127.0.0.1:9000
# [FixTestServer] Connected
# [FixTestServer] Sent  Logon          seq=1
# [FixTestServer] Recv  8=FIXT.1.1|9=...|35=A|49=PHIXERON|56=CLIENT|...
# [FixTestServer] Sent  Heartbeat      seq=2
# [FixTestServer] Sent  NewOrderSingle seq=3  Account=ACC1  ClOrdID=ORD-0001  AAPL Buy 100 @ 150.00
# [FixTestServer] Recv  8=FIXT.1.1|9=...|35=8|...                       (ExecutionReport ack)
# ...
# [FixTestServer] Sent  Logout         seq=6
# [FixTestServer] Recv  8=FIXT.1.1|9=...|35=5|...
# [FixTestServer] Starting risk engine query test
# [FixTestServer] Sent  ExecutionReport (Trade fill)  clOrdID=ORD-0001 [direct cluster ingress]
# [FixTestServer] Sent  PortfolioQueryRequest  account=ACC1 correlationId=777 [direct cluster ingress]
# [FixTestServer] Recv  PortfolioQueryReply  status=Ok riskScore=15 gross=1500000000000 net=1500000000000 positions=1
# [FixTestServer] Done.
```

Connect to a non-default host or port:
```bash
./cmake-build-release/fix_test_server 192.168.1.10 9000
```
