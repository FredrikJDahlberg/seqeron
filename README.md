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

Clients subscribe to the global sequenced stream on multicast
`aeron:udp?endpoint=224.0.1.1:9200|interface=localhost` stream 1, recorded by the
co-located Archive for replay on startup.

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
ingress on port 9302 to send messages. The global sequenced stream is published on
multicast `224.0.1.1:9200` (stream 1).

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

---

## FIX TCP test client

`src/test/cpp/org/limitless/phixeron/session/FixTestServer.cpp` connects to the
`fix_session_client` gateway on TCP port 9000 and runs a minimal FIX session
using the simdfix `ClientSession` and generated message encoders:

1. **Logon** — negotiates the session (EncryptMethod=None, HeartbeatInterval=30 s)
2. **Heartbeat** — verifies the session is active
3. **NewOrderSingle** — sends a limit Buy order (ClOrdID=ORD-0001, AAPL, 100 @ 150.00)
4. **Logout** — tears the session down cleanly

```
SenderCompID = CLIENT
TargetCompID = SEQUENCER   (the gateway's identity)
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

**2. Start the FIX gateway** (separate terminal):
```bash
cmake --build cmake-build-release --target fix_session_client
./cmake-build-release/fix_session_client
# [TCP] Listening on port 9000
# [FixSessionClient] Caught up — following live stream
```

**3. Build and run the test client** (separate terminal):
```bash
cmake --build cmake-build-release --target fix_test_server
./cmake-build-release/fix_test_server
# [FixTestServer] Connecting to 127.0.0.1:9000
# [FixTestServer] Connected
# [FixTestServer] Sent  Logon          seq=1
# [FixTestServer] Recv  8=FIXT.1.1|9=...|35=A|49=SEQUENCER|56=CLIENT|...
# [FixTestServer] Sent  Heartbeat      seq=2
# [FixTestServer] Sent  NewOrderSingle seq=3  ClOrdID=ORD-0001  AAPL Buy 100 @ 150.00
# [FixTestServer] Sent  Logout         seq=4
# [FixTestServer] Recv  8=FIXT.1.1|9=...|35=5|...
# [FixTestServer] Done.
```

Connect to a non-default host or port:
```bash
./cmake-build-release/fix_test_server 192.168.1.10 9000
```

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
