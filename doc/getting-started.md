# Getting started

One node from a release, then a consumer and a producer against it, in Java — and the same two in C#
(section 5). Needs JDK 21, the .NET 10 SDK for the C#, and nothing from this repository's source.

## 1. Start a node

```bash
V=0.10.0  # the latest release: https://github.com/FredrikJDahlberg/seqeron/releases
curl -LO https://github.com/FredrikJDahlberg/seqeron/releases/download/v$V/seqeron-$V.zip
unzip seqeron-$V.zip && cd seqeron-$V
bin/start-cluster.sh
```

This starts a single-node cluster: `SequencerServer` as member 0, with its own media driver, archive
and Replayer; and a probe consumer. Logs go to `./logs`, state to
`$TMPDIR/seqeron-seq`. `bin/stop-cluster.sh` stops both, and `bin/purgelog.sh` then deletes the
state for a clean start.

**A consumer runs on a cluster node.** The sequenced stream and the replay protocol are `aeron:ipc` on
the node's media driver, whose directory is `$TMPDIR/seqeron-seq-aeron-<memberId>` (`java.io.tmpdir` in
Java). Submitting alone needs no node, since cluster ingress is UDP.

## 2. Add the dependency

```gradle
plugins {
    id 'application'
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
    maven { url 'https://jitpack.io' }
}

dependencies {
    implementation enforcedPlatform('com.github.FredrikJDahlberg.seqeron:seqeron-bom:v0.10.0')
    implementation 'com.github.FredrikJDahlberg.seqeron:seqeron'
}

application {
    mainClass = providers.gradleProperty('main').getOrElse('Follow')
    applicationDefaultJvmArgs = ['--add-opens=java.base/sun.nio.ch=ALL-UNNAMED',
                                 '--add-opens=java.base/java.lang=ALL-UNNAMED',
                                 '--add-opens=java.base/java.lang.reflect=ALL-UNNAMED',
                                 '--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED']
}
```

Use the node's release, with a `v` prefix. The BOM pins Aeron and Agrona to the versions the cluster
was built with. A different Aeron fails at
runtime, when frames do not decode, rather than at build time. Aeron needs the `--add-opens` flags.

## 3. Follow the stream

```java
import io.aeron.Aeron;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver;

public final class Follow {
    public static void main(final String[] args) {
        final String aeronDir = System.getProperty("java.io.tmpdir") + "/seqeron-seq-aeron-0";
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
             ReplayerStreamReceiver receiver = new ReplayerStreamReceiver(
                 23, // clientId: unique among the clients on this node
                 event -> System.out.printf("%d %s %d%n", event.globalSeqNo(),
                                            event.isSystem() ? "systemEventType" : "payloadId",
                                            event.isSystem() ? event.systemEventType() : event.payloadId()),
                 (leaderMemberId, leadershipTermId, globalSeqNo) ->
                     System.out.printf("%d leader is member %d%n", globalSeqNo, leaderMemberId),
                 () -> System.out.println("caught up"))) {
            receiver.start(aeron, 0); // member 0
            final BackoffIdleStrategy idle = new BackoffIdleStrategy();
            while (true) {
                idle.idle(receiver.poll());
            }
        }
    }
}
```

`gradle run` prints every frame the cluster has sequenced, once each, in `globalSeqNo` order: first the
node's recorded history, then the live stream. On a node started a few seconds earlier, for example:

```
1 leader is member 0
2 systemEventType 16
3 systemEventType 16
caught up
4 systemEventType 16
```

`systemEventType` 16 is `ClusterHeartbeat`, which the cluster sequences once a second. Frames with a
`payloadId` are application payloads, passed through unopened. `clientId` must be unique among the
clients on one node, since two sharing one cannot both follow the stream. Ids 1–22 and 31–36 are this
repository's own.
[`client-api.md`](client-api.md) covers the frame families and how to decode each.

## 4. Publish

```java
import io.aeron.Aeron;
import java.nio.charset.StandardCharsets;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.app.Application;
import org.limitless.seqeron.app.ClusterError;
import org.limitless.seqeron.app.Payload;

public final class Hello implements Application.Listener {
    private static final int SOURCE_ID = 100;  // this producer, spec §5
    private static final int PAYLOAD_ID = 100; // its payload encoding, spec §6.1

    private String fence;

    public static void main(final String[] args) {
        final String aeronDir = System.getProperty("java.io.tmpdir") + "/seqeron-seq-aeron-0";
        final UnsafeBuffer hello = new UnsafeBuffer("hello".getBytes(StandardCharsets.US_ASCII));
        final Hello listener = new Hello();
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
             Application app = Application.builder()
                 .sourceId(SOURCE_ID).clientId(24).memberId(0)
                 .egressChannel("aeron:udp?endpoint=localhost:0")
                 .listener(listener).build()) {
            app.start(aeron);
            final BackoffIdleStrategy idle = new BackoffIdleStrategy();
            long nextNs = 0;
            while (listener.fence == null) {
                final int work = app.doWork();
                if (app.canPublish() && System.nanoTime() >= nextNs) {
                    app.publish(PAYLOAD_ID, hello, hello.capacity()); // Declined: the next tick retries
                    nextNs = System.nanoTime() + 1_000_000_000L;
                }
                idle.idle(work);
            }
        }
        System.err.println(listener.fence);
    }

    @Override
    public void onSequenced(final Payload payload) {
        if (payload.sourceId() == SOURCE_ID && payload.payloadId() == PAYLOAD_ID) {
            System.out.printf("%d %s%n", payload.globalSeqNo(),
                              payload.buffer().getStringWithoutLengthAscii(payload.payloadOffset(),
                                                                           payload.payloadLength()));
        }
    }

    @Override
    public void onFenced(final ClusterError fence, final String detail) {
        this.fence = fence + ": " + detail; // stop; a restart re-reads the log
    }

    @Override
    public void onLeadershipChanged(final boolean leading) {
    }

    @Override
    public void onCaughtUp(final long globalSeqNo) {
    }

    @Override
    public void onClusterHeartbeat(final long clusterTimeNs, final long receiveTimeNs) {
    }
}
```

`gradle run -Pmain=Hello` submits `hello` once a second and prints each one as it comes back sequenced,
here between heartbeats. With `Follow` running too, the same frames appear there as `payloadId 100`.

```
9 hello
11 hello
13 hello
```

`Application` is one replica of a co-located application: one runs per node, and only the replica on
the leading node publishes, so `canPublish()` is false on every other node. Behind `doWork()` it holds the
cluster session, follows this node's stream, and resends what a leader failover lost. A `publish` that
returns `Published` has been placed at ingress, not yet sequenced. The payload is confirmed when it
arrives in `onSequenced`. `onFenced` means this replica may no longer act. Exit, and a restart re-reads
the log.

Take `sourceId` and `payloadId` values no other producer uses. [The spec](seqeron-protocol-spec.md)
holds both registries, §5 and §6.1.

## 5. The same in C#

```bash
dotnet new console -o Follow && cd Follow
dotnet add package Org.Limitless.Seqeron --version $V
```

The package is the node's release too, and it is on nuget.org from the release after 0.10.0. It names its
Aeron.NET and SBE runtime versions exactly. Aeron.NET trails Aeron by a version, which
[spec **V-1**](seqeron-protocol-spec.md) records as the one exception to running the same Aeron everywhere.
`Program.cs`:

```csharp
using Adaptive.Aeron;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Replayer.Client;

string aeronDir = Path.Combine(Path.GetTempPath(), "seqeron-seq-aeron-0");
using Aeron aeron = Aeron.Connect(new Aeron.Context().AeronDirectoryName(aeronDir));
using var receiver = new ReplayerStreamReceiver(
    25, // clientId: unique among the clients on this node
    e => Console.WriteLine($"{e.GlobalSeqNo} {(e.IsSystem ? "systemEventType" : "payloadId")} " +
                           $"{(e.IsSystem ? e.SystemEventType : e.PayloadId)}"),
    (leaderMemberId, leadershipTermId, globalSeqNo) =>
        Console.WriteLine($"{globalSeqNo} leader is member {leaderMemberId}"),
    () => Console.WriteLine("caught up"));
receiver.Start(aeron, 0); // member 0
var idle = new SleepingIdleStrategy(1); // ms
while (true)
{
    idle.Idle(receiver.Poll());
}
```

`dotnet run` prints what `Follow` does in Java. The producer is a second project, `Hello`, made the same way:

```csharp
using System.Diagnostics;
using System.Text;
using Adaptive.Aeron;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.App;

const int SourceId = 100;  // this producer, spec §5
const int PayloadId = 100; // its payload encoding, spec §6.1

string aeronDir = Path.Combine(Path.GetTempPath(), "seqeron-seq-aeron-0");
byte[] hello = Encoding.ASCII.GetBytes("hello");
var listener = new Hello(SourceId, PayloadId);
using Aeron aeron = Aeron.Connect(new Aeron.Context().AeronDirectoryName(aeronDir));
using var app = new Application(new ApplicationOptions {
    SourceId = SourceId, ClientId = 26, MemberId = 0,
    EgressChannel = "aeron:udp?endpoint=localhost:0", Listener = listener });
app.Start(aeron);
var idle = new SleepingIdleStrategy(1); // ms
long next = 0;
while (listener.Fence == null)
{
    int work = app.DoWork();
    if (app.CanPublish && Stopwatch.GetTimestamp() >= next)
    {
        app.Publish(PayloadId, hello); // Declined: the next tick retries
        next = Stopwatch.GetTimestamp() + Stopwatch.Frequency;
    }
    idle.Idle(work);
}
Console.Error.WriteLine(listener.Fence);

sealed class Hello(int sourceId, int payloadId) : IApplicationListener
{
    public string? Fence { get; private set; }

    public void OnSequenced(Payload payload)
    {
        if (payload.SourceId == sourceId && payload.PayloadId == payloadId)
        {
            string text = payload.Buffer.GetStringWithoutLengthAscii(payload.PayloadOffset, payload.PayloadLength);
            Console.WriteLine($"{payload.GlobalSeqNo} {text}");
        }
    }

    public void OnFenced(ClusterError fence, string detail) => Fence = fence + ": " + detail;

    public void OnLeadershipChanged(bool leading) { }

    public void OnCaughtUp(long globalSeqNo) { }

    public void OnClusterHeartbeat(long clusterTimeNs, long receiveTimeNs) { }
}
```

It does what `Hello` does in Java, through the same `Application`: an options object where Java has a
builder, and `Publish` takes the bytes as they are, or a span. `clientId`s 25 and 26 are not 23 and 24, so
both languages can follow one node at once.

## Next

- **Three nodes.** The README's [Three-node cluster](../README.md#three-node-cluster) section, and
  [`fault-tolerance.md`](fault-tolerance.md) for what survives a node or leader loss.
- **An elected gateway.** A producer at the edge of the system, deployed as an active/standby pair,
  uses `app.Gateway` instead of `Application` and needs a topology document. See
  [`client-api.md`](client-api.md#gateway) and [`clusterctl.md`](clusterctl.md).
- **C++.** The client library is header-only:

  ```cmake
  include(FetchContent)
  FetchContent_Declare(seqeron
      GIT_REPOSITORY https://github.com/FredrikJDahlberg/seqeron.git
      GIT_TAG        v0.10.0)
  FetchContent_MakeAvailable(seqeron)
  target_link_libraries(my_app PRIVATE seqeron::seqeron_core)
  ```

  [`seqeron-examples`](../seqeron-examples) has the same two programs in C++ and in C#: `FollowStream` and
  `ColocatedApp`.
