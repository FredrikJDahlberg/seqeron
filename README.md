![phixeron](doc/phixeron.png)

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

## FIX Sequencer

The sequencer runs as a 1- or 3-node Aeron Cluster. Each node is launched with
`FixSequencerNode` and configured entirely via system properties.

### Single-node (development)

```bash
./gradlew uberJar

java \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  -Dphixeron.memberId=0 \
  -jar build/libs/phixeron-0.1.0-uber.jar
# [FixSequencerNode] Starting member 0 | ingress=aeron:udp?endpoint=localhost:9102 | baseDir=/tmp/phixeron
# [FixSequencerNode/0] Cluster node started — Ctrl-C to stop
```

The node embeds its own MediaDriver and Archive — no separate `aeronmd` needed.
Data is written to `/tmp/phixeron/archive-0` and `/tmp/phixeron/cluster-0`.

### Three-node cluster

Run each command on its respective host (or in separate terminals on localhost for testing):

**Member 0**
```bash
java \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  -Dphixeron.memberId=0 \
  -Dphixeron.baseDir=/var/phixeron \
  -Dphixeron.clusterMembers="0,host0:9102,host0:9103,host0:9104,host0:9105,host0:9101|1,host1:9112,host1:9113,host1:9114,host1:9115,host1:9111|2,host2:9122,host2:9123,host2:9124,host2:9125,host2:9121" \
  -jar phixeron-0.1.0-uber.jar
```

**Member 1**
```bash
java \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  -Dphixeron.memberId=1 \
  -Dphixeron.baseDir=/var/phixeron \
  -Dphixeron.clusterMembers="0,host0:9102,host0:9103,host0:9104,host0:9105,host0:9101|1,host1:9112,host1:9113,host1:9114,host1:9115,host1:9111|2,host2:9122,host2:9123,host2:9124,host2:9125,host2:9121" \
  -jar phixeron-0.1.0-uber.jar
```

**Member 2**
```bash
java \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  -Dphixeron.memberId=2 \
  -Dphixeron.baseDir=/var/phixeron \
  -Dphixeron.clusterMembers="0,host0:9102,host0:9103,host0:9104,host0:9105,host0:9101|1,host1:9112,host1:9113,host1:9114,host1:9115,host1:9111|2,host2:9122,host2:9123,host2:9124,host2:9125,host2:9121" \
  -jar phixeron-0.1.0-uber.jar
```

### Port layout

Each member's ports are `9100 + memberId × 10 + offset`:

| Offset | Purpose          | Member 0 | Member 1 | Member 2 |
|--------|------------------|----------|----------|----------|
| +1     | Archive control  | 9101     | 9111     | 9121     |
| +2     | Ingress          | 9102     | 9112     | 9122     |
| +3     | Consensus        | 9103     | 9113     | 9123     |
| +4     | Cluster log      | 9104     | 9114     | 9124     |
| +5     | File transfer    | 9105     | 9115     | 9125     |

### System properties

| Property                  | Default                        | Description                        |
|---------------------------|--------------------------------|------------------------------------|
| `phixeron.memberId`       | `0`                            | Raft member ID for this node       |
| `phixeron.baseDir`        | `$TMPDIR/phixeron`             | Root for archive and cluster dirs  |
| `phixeron.aeronDir`       | `$TMPDIR/phixeron-aeron-<id>`  | Aeron media driver directory       |
| `phixeron.clusterMembers` | single-node localhost          | Full Aeron clusterMembers string   |

### Restart and failover

Archive and cluster directories are preserved on restart (`deleteArchiveOnStart=false`,
`deleteDirOnStart=false`). A node rejoins the cluster and replays from its last snapshot
automatically. To wipe state for a clean start, delete the `archive-<id>` and
`cluster-<id>` subdirectories under `baseDir`.

---

## HelloWorld cluster examples

Two variants are provided. Both share the same C++ `HelloWorldClient`.

| Variant | Service | What it demonstrates |
|---------|---------|----------------------|
| **C++ mock** | `hello_world_service` (C++) | Wire-protocol handshake, no real cluster |
| **Java cluster** | `HelloWorldServiceNode` (Java) | Full single-node Aeron cluster with `ClusteredService` |

---

### Variant A — C++ mock service

The C++ service simulates a single-node cluster over the wire protocol and echoes messages unchanged.

**1. Start the Aeron media driver**
```bash
cmake-build-debug/_deps/aeron-build/binaries/aeronmd
```

**2. Start the C++ service** (separate terminal)
```bash
./cmake-build-debug/hello_world_service
# [Service] Listening on aeron:udp?endpoint=localhost:9010 stream 101
```

**3. Run the C++ client** (separate terminal)
```bash
./cmake-build-debug/hello_world_client
# [Client] Connected to media driver
# [Client] Sent SessionConnectRequest
# [Client] Session opened  sessionId=1  termId=0  leader=0
# [Client] Sent: Hello, World!
# [Client] Echo: Hello, World!
# [Client] Session closed
```

---

### Variant B — Java ClusteredService

`HelloWorldServiceNode` runs a real single-node Aeron cluster (MediaDriver + Archive + ConsensusModule + ClusteredServiceContainer). `HelloWorldClusteredService` transforms each message: upper-cases the payload and prepends the cluster timestamp.

**1. Build the fat jar**
```bash
./gradlew uberJar
```

**2. Start the Java cluster node** (separate terminal)
```bash
java \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
  -jar build/libs/phixeron-0.1.0-uber.jar
# [Node] Starting single-node cluster...
# [Node] Ingress: aeron:udp?endpoint=localhost:9010
# [Node] Cluster node started — waiting for clients (Ctrl-C to stop)
```

The node manages its own embedded media driver — no separate `aeronmd` needed.

**3. Run the C++ client** (separate terminal)
```bash
./cmake-build-debug/hello_world_client
# [Client] Connected to media driver
# [Client] Sent SessionConnectRequest
# [Client] Session opened  sessionId=1  termId=0  leader=0
# [Client] Sent: Hello, World!
# [Client] Echo: [<timestamp>] HELLO, WORLD!
# [Client] Session closed
```

> The Java node and the C++ client each need their own Aeron media driver.
> The node launches one internally; the client connects to the system default
> (`aeronmd` or the driver embedded in the node if they share the same Aeron dir).
> To run them as separate OS processes, start `aeronmd` first and let both attach to it.

---

### Channels and streams

| Direction             | Channel                             | Stream |
|-----------------------|-------------------------------------|--------|
| Client → Cluster      | `aeron:udp?endpoint=localhost:9010` | 101    |
| Cluster → Client      | `aeron:udp?endpoint=localhost:9020` | 102    |
| Archive control       | `aeron:udp?endpoint=localhost:9009` | —      |
| Consensus (internal)  | `aeron:udp?endpoint=localhost:9011` | —      |
