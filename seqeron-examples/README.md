# Follow the ordered stream

The smallest client of seqeron there is, once per language and the same flow in each: it replays a node's
history through that node's co-located `ReplayerService`, switches to the live tap when it catches up, and
prints every frame in `globalSeqNo` order. Each also produces, in both families: one `ConnectionOpened`
system event announcing itself, then one `ping` payload a second at cluster ingress, whose echo comes back
through the same consumer — so the round trip is measured over the real path.

| | |
| --- | --- |
| `src/java/example/FollowStream.java` | built by `build.gradle`, which resolves `org.limitless:seqeron` |
| `src/cpp/FollowStream.cpp` | built by `CMakeLists.txt`, which pulls in `seqeron_core` |
| `src/java/example/ColocatedApp.java` | the same flow against the front door — see below |
| `src/cpp/ColocatedApp.cpp` | its C++ twin |
| `src/java/example/GatewayApp.java` | the other façade, an elected gateway pair — see below |
| `src/cpp/GatewayApp.cpp` | its C++ twin |
| `src/java/example/SnapshotApp.java` | a façade application whose state is restored from a snapshot — see below |
| `src/cpp/SnapshotApp.cpp` | its C++ twin |
| `src/csharp/FollowStream` | the low-level flow in C#, built against the `Org.Limitless.Seqeron` package — see below |
| `src/csharp/ColocatedApp` | its front-door twin in C# |
| `src/csharp/GatewayApp` | a C# gateway pair of its own |

## The same flow, against the front door

`ColocatedApp` does what `FollowStream` does and names none of the tiers under it. Its whole import list
from seqeron is `app` plus `protocol.Publish`: no receiver, no sender, no envelope, no `systemEventType`.
`Application` assembles the cluster session, the tap, confirmed ingress across a failover, the
fences and the leader gate, and hands it `Payload`s. The C++ twin adds `util/Env.hpp` and
`util/IdleStrategy.hpp`, which are support code beside the façade rather than a tier under it, and which
the Java half takes from its own standard library instead.

That is the claim these files exist to make, so compiling them is not enough — an import of
`sequencer.client` would compile too. **The imports are checked**: `./gradlew -p seqeron-examples check`
fails on one (`checkFacadeOnly`), and so does the C++ configure step, the way seqeron's own
`FacadeSurfaceTest` makes the same check from the inside.

It is a **co-located application** — the producer kind nothing elects, one replica per node, publishing
only while its own node leads — which is why it needs no topology document and no `clusterctl` step:
`LeadershipChanged` already picks the replica that submits. `FollowStream` stays as it is, deliberately:
it is what a consumer writing its own duty cycle programs against, in either language.

## An elected gateway

`GatewayApp` is the other producer kind: a **gateway**, one instance of an active/standby pair that the
cluster elects. It is written against `app.Gateway` alone, under the same import and include checks as
`ColocatedApp`. Where a real gateway would open a socket, it takes one simulated client connection when
designated and pings the cluster on that connection once a second.

A gateway needs what a co-located application does not: a topology row naming it. `topology.xml` is that
list, the pair `GW-EX-A`/`GW-EX-B` on `sourceId` 13. Load it once, into a cluster that has not loaded
another list. The cluster designates only the first list it sees; into one that has, designate an
instance with `clusterctl.sh activate 12` instead. The Java and C++ examples run the same pair, so either
instance may be either language.

## C#

`src/csharp` holds `FollowStream`, `ColocatedApp` and `GatewayApp`, one project each, with the same flows as
their twins and the C++ environment variables. They resolve `Org.Limitless.Seqeron` as a package from
`build/nuget` in the repo root (`NuGet.config`), where `dotnet pack` puts it, and their own
`Directory.Build.props` keeps the repo's build settings out — so anything the package fails to expose fails
here. `ColocatedApp` and `GatewayApp` fail their build if their source names seqeron anywhere but the `App`
namespace and the `Publish` alias (`Directory.Build.targets`); they take Agrona's backoff idle strategy rather
than `Util`'s.

`GatewayApp` runs its own pair, `GW-EX-CS-A`/`GW-EX-CS-B` on `sourceId` 18, listed in `topology-csharp.xml`
rather than `topology.xml`: a listed pair that nothing starts trades the role every 5 s for the life of the
cluster. `csharp-client-test.sh` in `seqeron-service/src/test/scripts` runs all three against a live
cluster, and the pair through a handover.

## Application state from a snapshot

`SnapshotApp` is a co-located application with state worth keeping: a ledger of eight accounts that open
with 1000 each. The leading replica submits a random transfer once a second (`payloadId` 7: `from` int32,
`to` int32, `amount` int64, no schema), and every replica applies each one in `globalSeqNo` order, rejecting
a transfer that would overdraw its source account. Whether a transfer applies depends on every transfer
before it, so a replica cannot rebuild its balances from part of the log: it needs all of it, or a snapshot
of the state at a known point in it and the frames after that point. Like `ColocatedApp`, it is written
against `app` alone and its imports and includes are checked the same way.

**How a snapshot is taken** (`doc/snapshot.md` in the seqeron repo is the full mechanism):

1. The sequencer starts a round by sequencing `SnapshotStarted`, every `interval` seconds of cluster time or
   on `clusterctl.sh request-snapshot`. Its `globalSeqNo`, `R`, is the round's cut.
2. Every replica dispatching that frame, live or replayed, serializes its state as of `R` before it
   dispatches `R + 1`. The façade writes its own header as record 0, then calls the listener's `onSnapshot`
   with a 65535-byte buffer and an increasing `recordIndex` until it returns 0. Each record goes straight into
   `<round>.snapshot` in the replica's own snapshot directory; the façade keeps only the count, the length
   and a CRC32C of the records.
3. The replica whose gate is open — the one on the leader — submits the round's `SnapshotEnd`: that count,
   length and CRC. The state itself never crosses the cluster.
4. Every replica compares its own count, length and CRC with the sequenced `SnapshotEnd`. On a match its file
   is confirmed and older ones are deleted. On a difference it is fenced with `SNAPSHOT_DIVERGED`: its state
   is not the leader's.

**How it is restored.** A replica given a `SnapshotListener` starts by asking its node's Replayer for the
`SnapshotEnd` of the newest round it holds a file for, and checks the file against it. On a match it calls
`onRestore` with each record in order, then resumes the tap at `R + 1`. A file the log does not confirm, or
that does not match its end, is passed over for an older one; with none left, it replays from `globalSeqNo` 1
as any other replica does. A file whose records are damaged, or whose `formatVersion` the build does not
support, fences it with `SNAPSHOT_UNRESTORABLE`.

**What the listener owes.** Step 4 holds only if every replica writes the same bytes for the same state:

- **Deterministic records.** The ledger is an array in account order. A hash map's iteration order, a local
  clock or the node's id in a record makes replicas disagree.
- **State from the log alone.** The leader's random numbers are in the transfer payload, never in the state;
  every replica sees the same payloads and so computes the same balances.
- **Records that fit.** Record 0 is the counters (applied and rejected transfers), then one record per
  account. A record is at most 65535 bytes, but there may be any number of them, so the state is bounded by
  the disk rather than by a buffer.
- **A `formatVersion`.** It is carried in `SnapshotEnd`, and a restore stops on a file of a version the build
  does not know. Raise it when the record layout changes.
- **`onRestore` at `recordIndex` 0 replaces the state.** A restore that falls back to an older file starts
  over at 0.

The application takes part in rounds only if its topology row says `snapshot="true"` and a `<snapshots>`
element enables them, so `topology.xml` lists `SnapshotApp` under `<applications>` (`sourceId` 14 in Java,
15 in C++) and starts a round every 10 s. Each replica's snapshot directory must survive its restarts and
belong to it alone.

**Both are separate builds, not subprojects of the repo they sit in.** The Java one resolves
`org.limitless:seqeron` as a published artifact and the C++ one pulls `seqeron_core` in with
`FetchContent`, which is the only way an example can show the artifacts are consumable at all — anything
they fail to expose fails here rather than passing on a source dependency. The C++ half also builds
against an installed tree: `SEQERON_SOURCE_DIR` defaults to the checkout this example ships in, an outside
consumer swaps it for `GIT_REPOSITORY`/`GIT_TAG` and changes nothing else, and `-DSEQERON_FIND_PACKAGE=ON`
takes `seqeron::seqeron_core` off `find_package(seqeron)` against a `cmake --install`ed prefix instead,
supplying its own installed Aeron:

    cmake -S seqeron-examples -B seqeron-examples/cmake-build-installed -DCMAKE_BUILD_TYPE=Release \
          -DSEQERON_FIND_PACKAGE=ON -DCMAKE_PREFIX_PATH=<prefix holding seqeron and Aeron>

## Run them

Every command runs in the seqeron repo root, and both examples can follow one node at the same time.
Start a node first, in another shell:

    ./seqeron-service/src/main/scripts/start-cluster.sh

Java, the low-level one and then the façade one — either may run alone, and both may run at once:

    ./gradlew publishToMavenLocal
    ./gradlew -p seqeron-examples run
    ./gradlew -p seqeron-examples runColocated
    ./gradlew -p seqeron-examples runSnapshot

C++:

    cmake -S seqeron-examples -B seqeron-examples/cmake-build-release -DCMAKE_BUILD_TYPE=Release
    cmake --build seqeron-examples/cmake-build-release --target follow_stream colocated_app gateway_app snapshot_app
    ./seqeron-examples/cmake-build-release/follow_stream
    ./seqeron-examples/cmake-build-release/colocated_app
    ./seqeron-examples/cmake-build-release/snapshot_app

C# — after re-packing an unchanged version, delete `~/.nuget/packages/org.limitless.seqeron`, which NuGet
would otherwise serve instead:

    dotnet pack -c Release -o build/nuget seqeron-client/src/main/csharp/Seqeron.Client.csproj
    dotnet run --project seqeron-examples/src/csharp/FollowStream
    dotnet run --project seqeron-examples/src/csharp/ColocatedApp

The gateway pair, after loading its list — stop the first and the second takes over:

    ./seqeron-service/src/main/scripts/clusterctl.sh load-topology seqeron-examples/topology.xml
    SEQERON_EXAMPLE_GATEWAY_NAME=GW-EX-A ./seqeron-examples/cmake-build-release/gateway_app
    SEQERON_EXAMPLE_GATEWAY_NAME=GW-EX-B ./seqeron-examples/cmake-build-release/gateway_app

or in Java, with `-Dgateway.name` in place of the variable:

    ./gradlew -p seqeron-examples runGateway -Dgateway.name=GW-EX-A
    ./gradlew -p seqeron-examples runGateway -Dgateway.name=GW-EX-B

and the C# pair, from its own list:

    ./seqeron-service/src/main/scripts/clusterctl.sh load-topology seqeron-examples/topology-csharp.xml
    SEQERON_EXAMPLE_GATEWAY_NAME=GW-EX-CS-A dotnet run --project seqeron-examples/src/csharp/GatewayApp
    SEQERON_EXAMPLE_GATEWAY_NAME=GW-EX-CS-B dotnet run --project seqeron-examples/src/csharp/GatewayApp

`SnapshotApp` needs the same list loaded, for its row and the round interval; it runs without it, but
takes no snapshots. A cluster that loaded an earlier copy of the list takes this one too: a row replaces the
row with its id, and the bootstrap designation is not repeated. Let it run past a round, which it reports as `# snapshot after N transfers`, stop it, and
start it again. The restart prints `# restored after N transfers` and the balances, then applies only the
transfers after the round's cut, and its total is still 8000. `clusterctl.sh request-snapshot` starts a round
at once instead of waiting for the interval.

The first CMake configure fetches and builds Aeron from source, which is what `add_subdirectory` of the
whole repo costs. GoogleTest is not fetched — that is seqeron's test dependency, not part of what it
exports.

Output is one line per frame — `globalSeqNo`, then the decoded system event, or the application
`payloadId` and template for a payload it has no decoder for:

    # following member 0 via /var/folders/…/seqeron-seq-aeron-0
    1 leader=member 0 term 0
    2 ClusterHeartbeat cluster clock 1789911942118284000ns
    3 ClusterHeartbeat cluster clock 1789911943118604000ns
    …
    # caught up — following the tap live
    231 ConnectionOpened connection=1 label=14 bytes
    232 ping echoed, round trip 3959us
    234 ping echoed, round trip 6415us
    …

`globalSeqNo` 1 is always a `LeadershipChanged`, and it reaches the leadership callback rather than
`onSequenced`. The heartbeat is the cluster clock at 1 Hz, so the lines between the pings are it. The C++
half prints its connection line from a `LifecycleEvent` — `ConnectionOpened connection=1 sourceId=10`,
identity without the payload (see the callback bullet below).

`start-cluster.sh` already runs a `ClusterProbe follow` replica of its own; these attach beside it with
client ids of their own, since two replicas sharing a Replayer client id supersede each other's replays
and neither catches up.

## Configuration

Java takes system properties and C++ takes environment variables — each reads what its language's
processes already do. The two client ids differ on purpose, so both examples can follow one node at once.

| Java | C++ | |
| --- | --- | --- |
| `-Dfollow.member` | `SEQERON_NODE_MEMBER_ID` | which node to attach to; default 0 |
| `-Dfollow.clientId` | `SEQERON_REPLAYER_CLIENT_ID` | this replica's Replayer client id; default 7 in Java, 8 in C++ |
| `-Dfollow.aeronDir` | `SEQERON_AERON_DIR` | that node's Aeron directory; default `{tmpdir}/seqeron-seq-aeron-{member}` |
| — | `SEQERON_IDLE_STRATEGY` | `backoff` (default), `yielding` or `busyspin` |
| — | `SEQERON_EXAMPLE_EGRESS_PORT` | UDP port the ping's cluster session takes egress on; default `9202 + member` |
| `-Dsnapshot.dir` | `SEQERON_EXAMPLE_SNAPSHOT_DIR` | `SnapshotApp`'s snapshot directory; default `{tmpdir}/seqeron-example-snapshots-{sourceId}-{member}` |

`GatewayApp` takes them under `gateway.` (`-Dgateway.name`, `-Dgateway.member`, `-Dgateway.clientId`,
`-Dgateway.aeronDir`), with client ids 15 and 16 for `GW-EX-A` and `GW-EX-B` in either language, and C++ egress
on `9205` and `9206`; the Java instance's egress is ephemeral. `SnapshotApp` takes the same settings under its own prefix (`-Dsnapshot.member`, `-Dsnapshot.clientId`,
`-Dsnapshot.aeronDir`), with client ids 17 in Java and 18 in C++, and C++ egress on `9207 + member`.

C# reads the C++ variables bar `SEQERON_EXAMPLE_EGRESS_PORT` — its egress is ephemeral — with client ids 19
(`FollowStream`), 20 (`ColocatedApp`) and 21, 22 (`GatewayApp`); only `FollowStream` reads
`SEQERON_IDLE_STRATEGY`.

## What they show

- **The receiver owns the history/live split.** There is no code here for requesting a replay, tracking
  the archive, or noticing a gap: `ReplayerStreamReceiver` does all of it and dispatches nothing out of
  order, which is why the gap check can be an assertion rather than a recovery path.
- **A consumer splits by family first.** `isSystem()` / `event.system`, then either a `systemEventType` or
  a `payloadId` — never a bare template id, which is unique only within one schema.
- **A payload is opaque to the tier.** The ping's payload is eight raw bytes — no SBE at all — under the
  examples' own `payloadId` 6, because the cluster decodes no `payloadId` and copies every payload through
  unopened. Java hands those bytes to `IngressPublisher.publishPayload`; C++'s `publishPayload` is
  templated on an SBE encoder, so a payload with no schema is framed there with the `Unsequenced` codec and
  offered through `offerFrame`, where `publishPayload` itself ends — the same three answers, and the same
  place an `IngressTracker` attaches. Neither needs anything from the consumer side.
- **`connectColocated` is the co-located producer's entry point.** Ingress goes over the member's own
  `aeron:ipc` — no endpoints to name, no ports to allocate — and falls back to the UDP endpoint set when
  that member is not the leader, which is the only member that subscribes to IPC ingress. Both languages
  carry the same call with the same semantics, so either example runs attached to any member, and nothing
  here configures a cluster: the member id it follows is the member id it produces to.
- **One session, kept alive.** The session is opened once and pinged every second, so the duty cycle calls
  `keepAlive()` every iteration — the cluster's `sessionTimeoutMs` is 1s, which one ping a second does not
  meet on its own, and the sender holds the 200ms interval and decides when one is actually due.
- **One publish, three answers.** `Publish` is `Published`, `Refused` (the payload is above
  `MAX_PAYLOAD_LENGTH` — local, permanent, nothing was offered) or `Declined` (the transport's answer, and
  the one worth retrying). The ping retries a `Declined` by simply sending the next second's.
- **The producer does not wait for its own frame.** `ping` submits and returns; the echo arrives in
  `onSequenced` in `globalSeqNo` order like everything else, which is what a real producer that is also a
  consumer looks like.
- **A fragment handler must not throw.** `Image::poll` advances the subscriber position regardless of what
  a handler raises, so the gap is recorded and the duty cycle raises it.
- **Both families, both directions.** The ping is an application payload; the connection each example
  announces is a system event, so it goes through `publishSystem` with an SBE payload and no `MessageHeader`
  of its own. Reading a system payload back is the same split in reverse: the decoder takes its own compiled
  `BLOCK_LENGTH` and `SCHEMA_VERSION`, submitted `ConnectionOpened` and synthesized `ClusterHeartbeat` alike.
  C++'s `decodeSystem` does that wrap in one call.
- **Three callbacks, not one — in C++.** `ConnectionOpened` and `ConnectionClosed` reach a C++ consumer
  through `onConnected`/`onDisconnected` rather than `onSequenced`, as a `LifecycleEvent`: the frame's
  identity and its payload, which `decodeSystem` reads `connectionData` from as it does in `onSequenced`.
  Java delivers both events to `onSequenced` like any other system frame, and so does C++ for a callback
  passed as `{}`.
- **`LeadershipChanged` has its own callback,** and `globalSeqNo` 1 is always one. With that callback
  `null` (Java) or `{}` (C++), it reaches `onSequenced` instead.
- **A send that succeeds is not a frame sequenced.** Nothing confirms ingress on egress, and a leader
  failover silently loses whatever the old leader had not committed. The ping lives with that — the next
  second's is its retry — which is what makes it a ping. A producer that cannot lose a frame passes an
  `sequencer/client/PendingSends` as the `IngressTracker` every `publish*` and `offerFrame` takes, hands the same one to
  the sender with `setIngressHold`, and resends what a term change lost; `ClusterProbe confirm` is that
  version of this example.

## What each build shows

- **C#: one dependency.** The `Org.Limitless.Seqeron` package brings Aeron.NET and the SBE runtime with it,
  as the dependencies it declares, at the versions seqeron was built against.

- **Java: one dependency.** `org.limitless:seqeron` brings Aeron and Agrona with it — seqeron declares
  them `api`, since they are in the signatures a consumer compiles against — and `seqeron-bom` holds all
  three at the versions seqeron was built against.
- **C++: header-only.** `seqeron_core` is an INTERFACE target carrying its own include roots and every
  Aeron target its headers reach for, so `target_link_libraries(follow_stream PRIVATE seqeron::seqeron_core)`
  is the whole link line.
- **C++: what a consumer does not inherit.** seqeron's own `-Wall -Wextra` and its Debug
  `-fsanitize=address` live on `seqeron_flags`, which only targets inside that repo link, and `core_tests`
  is not configured at all here — `SEQERON_BUILD_TESTS` defaults off when seqeron is added as a
  subdirectory, so neither the suite nor GoogleTest is fetched or built.
