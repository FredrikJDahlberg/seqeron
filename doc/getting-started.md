# Getting started

The shortest path from nothing to a working seqeron deployment: a one-member cluster from a release, a
consumer that replays its history and follows it live, and a producer that submits to it and reads its own
messages back in order. Java first; section 5 does the same in C#. It needs JDK 21, the .NET 10 SDK for
the C#, and nothing from this repository's source. [`overview.md`](overview.md) explains what is running.

## 1. Start a node

```bash
V=0.11.1  # the latest release: https://github.com/FredrikJDahlberg/seqeron/releases
curl -LO https://github.com/FredrikJDahlberg/seqeron/releases/download/v$V/seqeron-$V.zip
unzip seqeron-$V.zip && cd seqeron-$V
bin/start-cluster.sh
```

That is a complete cluster of one member: `SequencerServer` as member 0, with its own media driver,
archive and Replayer, and a probe consumer beside it. Logs go to `./logs` and state to
`$TMPDIR/seqeron-seq`. `bin/stop-cluster.sh` stops both; `bin/purgelog.sh` then deletes the state for a
clean start.

A consumer runs on the member's host, because the sequenced stream and the replay protocol are `aeron:ipc`
on the member's media driver, whose directory is `$TMPDIR/seqeron-seq-aeron-<memberId>`
(`java.io.tmpdir` in Java). A producer that only submits can run anywhere, since cluster ingress is UDP.

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
    implementation enforcedPlatform('com.github.FredrikJDahlberg.seqeron:seqeron-bom:v0.11.1')
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

Take the client from the same release as the member, with a `v` prefix. The BOM pins Aeron and Agrona to
the versions the cluster was built with, which matters because a different Aeron fails at runtime, when
frames do not decode, not at build time. The `--add-opens` flags are Aeron's.

## 3. Follow the stream

A consumer needs one object, `ReplayerStreamReceiver`, and one call per duty cycle. It asks the member's
Replayer for the history, delivers it, then switches to the live stream when the two meet.

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

`gradle run` prints every frame the cluster has sequenced, once each, in `globalSeqNo` order: the
member's recorded history first, then the live stream. Against a member started a few seconds earlier:

```
1 leader is member 0
2 systemEventType 16
3 systemEventType 16
caught up
4 systemEventType 16
```

`globalSeqNo` 1 is always the first leader's election. `systemEventType` 16 is `ClusterHeartbeat`, the
cluster's clock, sequenced once a second. A frame with a `payloadId` instead is an application payload,
delivered exactly as its producer wrote it. The `clientId` must be unique among the clients on one member,
since two sharing one cannot both follow the stream; ids 1–22 and 31–43 are this repository's own.
[`client-api.md`](client-api.md) covers the frame families and how to decode each.

## 4. Publish

A producer that is also a consumer takes a façade. `Application` is the one for a co-located application:
it holds the cluster session, follows the stream, confirms its own messages and decides when this replica
may publish, so the program below is only its own logic.

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
between the heartbeats. With `Follow` running too, the same frames appear there as `payloadId 100`.

```
9 hello
11 hello
13 hello
```

Three things in it are worth knowing before going further:

- **Only one replica publishes.** A co-located application runs one replica per member, and only the
  replica beside the leader publishes, so `canPublish()` is false on every other member.
- **`Published` means placed, not sequenced.** The message reached the leader's ingress. It is sequenced
  when it arrives back in `onSequenced`, and if a leader failover loses it first, the façade resends it.
- **`onFenced` means stop.** The replica can no longer trust its view of the log. Exit; a restart re-reads
  the log.

Take `sourceId` and `payloadId` values no other producer uses; [the spec](seqeron-protocol-spec.md) holds
both registries, §5 and §6.1.

## 5. The same in C#

```bash
dotnet new console -o Follow && cd Follow
dotnet add package Org.Limitless.Seqeron --version $V
```

Take the package from the member's release too; it is on nuget.org. It names its Aeron.NET and SBE
runtime versions exactly. Aeron.NET is one release behind Aeron, the one exception to running the same
Aeron everywhere, which [spec **V-1**](seqeron-protocol-spec.md) records.
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
builder, and `Publish` takes the bytes as a buffer or a span. The `clientId`s, 25 and 26, differ from the
Java programs' 23 and 24, so both languages can follow one member at once.

## Next

- **Three members.** [`running-a-cluster.md`](running-a-cluster.md#three-nodes), and
  [`fault-tolerance.md`](fault-tolerance.md) for what survives the loss of a member or the leader.
- **An elected gateway.** A producer at the edge of the system, deployed as an active/standby pair, takes
  the `Gateway` façade instead of `Application`, and needs a topology document. See
  [`client-api.md`](client-api.md#gateway) and [`clusterctl.md`](clusterctl.md).
- **C++.** The client library is header-only:

  ```cmake
  include(FetchContent)
  FetchContent_Declare(seqeron
      GIT_REPOSITORY https://github.com/FredrikJDahlberg/seqeron.git
      GIT_TAG        v0.11.1)
  FetchContent_MakeAvailable(seqeron)
  target_link_libraries(my_app PRIVATE seqeron::seqeron_core)
  ```

  [`seqeron-examples`](../seqeron-examples) has the same two programs in C++ and in C#: `FollowStream` and
  `ColocatedApp`.
