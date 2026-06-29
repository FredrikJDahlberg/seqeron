# phixeron
FIX Aeron Gateway

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
