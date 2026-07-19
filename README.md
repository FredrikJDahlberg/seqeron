![phixeron](doc/phixeron.png)

## Overview

phixeron is an Aeron Cluster–based sequencer for a FIX gateway: a Java Raft cluster service
assigns a global total order to inbound FIX messages, and a C++ edge process bridges real FIX
TCP sessions to that cluster. It depends on a sibling project, **simdfix**
(`git@github.com:FredrikJDahlberg/simdfix.git`, fetched via CMake `FetchContent`), which provides
the generic FIX wire-format codec, session state machine base classes, and the code generator
used to turn `fix-session.xml`/`fix-application.xml` into C++ FIX message headers. phixeron
generates its own copy of those headers (see "SBE / FIX code generation" below) rather than
reusing simdfix's test fixtures.

## Build

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
```

## Tests

```bash
cd cmake-build-debug && ctest
```

---

## Scripts

Cluster start/stop and utility scripts live under `src/main/scripts/` — these only start and stop the
cluster (or are standalone tools); they run no tests:

| Script | Purpose |
|--------|---------|
| `start-cluster.sh [debug\|release]` | Start the single-node cluster (`SequencerNode`, `aeronmd`, `FixSessionClient`, `ReplayerNode`, `OrderExecClient`) in the background; Ctrl-C stops all of them |
| `start-three-node-cluster.sh [debug\|release]` | Start a local 3-node Raft cluster with a `FixSessionClient` and a per-node `ReplayerNode` + `OrderExecClient` replica; blocks until Ctrl-C, then stops all of them |
| `stop-cluster.sh` | Stop all cluster processes started by either start script |
| `sbe-log-printer.sh <spec.sbeir> <archive-dir>` | Dump an Aeron Archive recording as JSON (see [Log printer](#log-printer)) |
| `purgelog.sh [--force]` | Delete archive/cluster directories under `$TMPDIR/phixeron-seq` and the `logs/` directory; cluster must be stopped first |

Test scripts live under `src/test/scripts/` — each brings the cluster up via the start scripts above
and tears it down via `stop-cluster.sh` (run them from the repository root):

| Script | Purpose |
|--------|---------|
| `three-node-e2e-test.sh [debug\|release]` | Start the 3-node cluster, run `fix_test_server` against it once, then tear everything down and exit with its pass/fail status (set `PHIXERON_FLOOD_ORDERS=<N>` for the delivery-latency-under-load run) |
| `fix-test-server.sh [debug\|release] [host [port]]` | Run a single FIX session (Logon → Heartbeat → NewOrderSingle → Logout) against a live `FixSessionClient` gateway |
| `failover-test.sh` | Force a failover, then cold-start a fresh `OrderExecClient` on the new leader and verify it catches up on full history (each node's tap recording is one continuous run spanning both tenures) |
| `gap-recovery-test.sh` | Drop a live tap frame on a caught-up consumer (SIGUSR1 fault-injection) and verify it re-walks its recording and heals rather than wedging |

---

## Sequencer

The sequencer runs as a 1- or 3-node Aeron Cluster. Each node is launched with
`SequencerNode` and configured entirely via system properties.

### Single-node (development)

```bash
./gradlew uberJar

java \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  -Dsequencer.memberId=0 \
  -jar build/libs/phixeron-0.1.0-uber.jar
# [SequencerNode] Starting member 0 | ingress=aeron:udp?endpoint=localhost:9302 | archive=aeron:udp?endpoint=localhost:9301 | baseDir=/tmp/phixeron-seq
# [SequencerNode/0] Running — Ctrl-C to stop
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
fixed UDP port too — 9320 for `FixSessionClient`/`fix_test_server`, 9330 for `OrderExecClient` (see
[Order execution client](#order-execution-client)) — kept distinct because the two now sit on
independent media driver processes that can't both bind the same UDP port on `localhost`.

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
`deleteDirOnStart=false`). A node rejoins the cluster and replays from its last snapshot
automatically. To wipe state for a clean start, delete the `archive-<id>` and
`cluster-<id>` subdirectories under `baseDir`.

### Log printer

`SbeLogPrinter` dumps an Archive recording (`archive.catalog` + segment files under
`archive-<id>`) as JSON, decoded against the generated SBE IR schema. It works on a
still-running cluster — an in-progress recording is printed up to whatever has been
written so far — so the cluster does not need to be stopped first.

```bash
./gradlew uberJar
./gradlew generateSequencedSbe   # produces build/generated/sources/sbe/main/java/sbe-sequenced.sbeir

./src/main/scripts/sbe-log-printer.sh \
  build/generated/sources/sbe/main/java/sbe-sequenced.sbeir \
  "${TMPDIR}phixeron-seq/archive-0" --stream 205
```

Or via Gradle directly (defaults `-Pspec` to the sequenced IR above):
```bash
./gradlew sbeLogPrinter -PlogDir="${TMPDIR}phixeron-seq/archive-0" -Pstream=205
```

#### Selecting a recording

An archive dir holds more than one recording, so by default the printer dumps **all** of them:

```
[Catalog] Recording ID: 0 | Stream ID: 205 | Start Pos: 0 | Stop Pos: 6336
[Catalog] Recording ID: 1 | Stream ID: 100 | Start Pos: 0 | Stop Pos: 8448
Exception parsing segment .../1-0.rec: Required schema id 202 but was 111
[Catalog] Recording ID: 2 | Stream ID: 205 | Start Pos: 0 | Stop Pos: 12480
```

Stream 100 is the Raft cluster log (schema 111) — it cannot decode against the sequenced IR, so
it is reported on stderr and skipped. Streams 205 are two generations of the sequenced tap: a node
restart replays its whole cluster log and re-emits every message onto a *new* tap recording, so
recording 2 starts again at `globalSeqNo` 1 and recording 0 is a strict prefix of it.

`--stream 205` (or `-Pstream=205`) dumps only the **newest** recording on that stream — one
complete copy of sequenced history, no cluster-log error, no repeats. Recording ids are not stable
across restarts, which is why the selector is the stream rather than the id.

Omit the flag when you want everything, including stale tap generations. Exits non-zero if the
requested stream matches no recording.

#### Output format

Each message is preceded by a separator naming it — the JSON carries field values only, so a
header-only message such as `Tick` is otherwise indistinguishable from any other:

```
--- Log File Offset: 96 | Tick (templateId 16) ---
```

`--oneline` (or `-Poneline`) collapses each message onto a single line, which greps and diffs far
better than the default pretty print:

```
--- Log File Offset: 0 | LeadershipChanged (templateId 5) ---
{ "header": { "sourceId": -1, "connectionId": -1, "sessionId": -1, "globalSeqNo": 1, "timestamp": 1784483030632 }, "newLeaderMemberId": 0 }
```

Note the dump as a whole is not a JSON document either way — the `[Catalog]` and separator lines sit
between the objects — but with `--oneline` each individual message line parses on its own.

---

## Order execution client

`OrderExecClient` (C++, `src/main/cpp/.../sequencer/OrderExecClient.cpp`) combines what used to
be two separate binaries — `application_stream_client` and the C++ `RiskEngineClient` — into one
cluster ingress client. It replays the global stream then follows it live, printing every
`NewOrderSingle`/`ExecutionReport` it sees, tracking each account's positions from those same
fills, answering `PortfolioQueryRequest`s with a risk assessment from a mocked external risk
engine, and submitting the `PortfolioQueryReply` back to cluster ingress. The mock engine is
synchronous, slow, and only services 5 requests at once (`MockRiskEngine`); queries beyond that
are throttled by leaving the `PortfolioQueryRequest` fragment unconsumed on the global stream
until a slot frees up, rather than blocking or dropping them.

Unlike `FixSessionClient` (which serves external, potentially remote TCP FIX clients over UDP),
`OrderExecClient` is deliberately deployed **co-located** with one `SequencerNode` member —
sharing that member's own embedded Aeron directory rather than the standalone `aeronmd` — so
archive access/replay, the live (post-catch-up) sequenced-stream tail (the co-located member's
`aeron:ipc` tap, read directly), and — while that member is leader — cluster ingress all go over `aeron:ipc`
instead of looping through two independent UDP media drivers. Cluster egress stays UDP regardless
(see `doc/design.md` §2.7 for the full rationale and fallback behavior when the co-located member
isn't currently leader).

```bash
cmake --build cmake-build-release --target OrderExecClient
PHIXERON_ORDER_EXEC_AERON_DIR="${TMPDIR}phixeron-seq-aeron-0" ./cmake-build-release/OrderExecClient
# [OrderExecClient] Connected to co-located Aeron media driver at .../phixeron-seq-aeron-0
# [OrderExecClient] Connected to co-located Aeron Archive via IPC (holds the global stream recording)
# [OrderExecClient] Live from start
```

`PHIXERON_ORDER_EXEC_AERON_DIR` defaults to member 0's own `sequencer.aeronDir` default
(`$TMPDIR/phixeron-seq-aeron-0`, see [System properties](#system-properties)) — start the
sequencer node first (see [Sequencer](#sequencer)) and only override this if co-locating with a
different member.

---

## FIX TCP test client

`src/test/cpp/org/limitless/phixeron/session/FixTestServer.cpp` connects to the
`FixSessionClient` gateway on TCP port 9000 and runs a minimal FIX session
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
   `OrderExecClient` to be running.

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
their own, since (unlike `SequencerNode`) they don't embed one:
```bash
AERON_DIR="${TMPDIR}aeron-$(whoami)" ./cmake-build-release/_deps/aeron-build/binaries/aeronmd
```

**3. Start the FIX gateway** (separate terminal):
```bash
cmake --build cmake-build-release --target FixSessionClient
AERON_DIR="${TMPDIR}aeron-$(whoami)" ./cmake-build-release/FixSessionClient
# [TCP] Listening on port 9000
# [FixSessionClient] Caught up — following live stream
```

**4. Start `OrderExecClient`** (separate terminal — see
[Order execution client](#order-execution-client)), needed for step 5 below.

**5. Build and run the test client** (separate terminal):
```bash
cmake --build cmake-build-release --target fix_test_server
AERON_DIR="${TMPDIR}aeron-$(whoami)" ./cmake-build-release/fix_test_server
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
