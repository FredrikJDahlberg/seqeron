# Follow the ordered stream

The smallest client of seqeron there is, once per language and the same flow in both: it replays a node's
history through that node's co-located `ReplayerService`, switches to the live tap when it catches up, and
prints every frame in `globalSeqNo` order. Each also produces, in both families: one `ConnectionOpened`
system event announcing itself, then one `ping` payload a second at cluster ingress, whose echo comes back
through the same consumer — so the round trip is measured over the real path.

| | |
| --- | --- |
| `src/java/example/FollowStream.java` | built by `build.gradle`, which resolves `org.limitless:seqeron` |
| `src/cpp/FollowStream.cpp` | built by `CMakeLists.txt`, which pulls in `seqeron_core` |

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

Java:

    ./gradlew publishToMavenLocal
    ./gradlew -p seqeron-examples run

C++:

    cmake -S seqeron-examples -B seqeron-examples/cmake-build-release -DCMAKE_BUILD_TYPE=Release
    cmake --build seqeron-examples/cmake-build-release --target follow_stream
    ./seqeron-examples/cmake-build-release/follow_stream

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
identity without the body (see the callback bullet below).

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

## What they show

- **The receiver owns the history/live split.** There is no code here for requesting a replay, tracking
  the archive, or noticing a gap: `ReplayerStreamReceiver` does all of it and dispatches nothing out of
  order, which is why the gap check can be an assertion rather than a recovery path.
- **A consumer splits by family first.** `isSystem()` / `event.system`, then either a `systemEventType` or
  a `payloadId` — never a bare template id, which is unique only within one schema.
- **A payload is opaque to the tier.** The ping's body is eight raw bytes — no SBE at all — under the
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
- **One publish, three answers.** `Publish` is `Published`, `Refused` (the body is above
  `MAX_PAYLOAD_LENGTH` — local, permanent, nothing was offered) or `Declined` (the transport's answer, and
  the one worth retrying). The ping retries a `Declined` by simply sending the next second's.
- **The producer does not wait for its own frame.** `ping` submits and returns; the echo arrives in
  `onSequenced` in `globalSeqNo` order like everything else, which is what a real producer that is also a
  consumer looks like.
- **A fragment handler must not throw.** `Image::poll` advances the subscriber position regardless of what
  a handler raises, so the gap is recorded and the duty cycle raises it.
- **Both families, both directions.** The ping is an application payload; the connection each example
  announces is a system event, so it goes through `publishSystem` with an SBE body and no `MessageHeader`
  of its own. Reading a system body back is the same split in reverse — and it is where the two languages
  differ most. Java's `printSystem` holds both wrap rules: a submitted `ConnectionOpened` takes its
  decoder's compiled `BLOCK_LENGTH` and `SCHEMA_VERSION`, since the event's `blockLength()` is 0 for the
  nine submitted events, and a synthesized `ClusterHeartbeat` takes the event's own. C++ has one rule,
  because `decodeSystem` supplies the decoder's compiled constants for every system shape.
- **Three callbacks, not one — in C++.** `ConnectionOpened` and `ConnectionClosed` reach a C++ consumer
  through `onConnected`/`onDisconnected` and never through `onSequenced`, as a `LifecycleEvent`: the
  frame's identity with no body, so the `connectionData` the Java half prints off its own announcement is
  not reachable through that receiver. Java delivers both events to `onSequenced` like any other system
  frame. Passing `{}` for either one is a hole in `globalSeqNo` wherever a connection opens or closes,
  which is why the C++ example supplies all three and its `inOrder` check holds across the connections it
  announces itself.
- **`LeadershipChanged` has its own callback.** It reaches a consumer there and nowhere else, so a
  consumer that passes `null` (Java) or `{}` (C++) for it sees a hole in `globalSeqNo` at every leadership
  change — including `globalSeqNo` 1, which always is one.
- **A send that succeeds is not a frame sequenced.** Nothing confirms ingress on egress, and a leader
  failover silently loses whatever the old leader had not committed. The ping lives with that — the next
  second's is its retry — which is what makes it a ping. A producer that cannot lose a frame passes an
  `app/PendingSends` as the `IngressTracker` every `publish*` and `offerFrame` takes, hands the same one to
  the sender with `setIngressHold`, and resends what a term change lost; `ClusterProbe confirm` is that
  version of this example.

## What each build shows

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
