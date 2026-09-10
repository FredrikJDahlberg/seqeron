# Follow the ordered stream — C++

The C++ twin of [`examples/java`](../java): it replays a node's history through that node's co-located
`ReplayerService`, switches to the live tap when it catches up, and prints every frame in `globalSeqNo`
order. Once caught up it submits one ping frame a second at cluster ingress and reports the round trip
when each echo comes back. Same flow, same output.

This is a **separate build**, not part of the repo's CMake project. It pulls `seqeron_core` in with
`FetchContent`, which is the one way that target is consumable today: there is no `install()` or
`export()` for it, so `find_package(seqeron)` cannot work. `SEQERON_SOURCE_DIR` defaults to the checkout
this example ships in; an outside consumer swaps it for `GIT_REPOSITORY`/`GIT_TAG` and changes nothing
else.

## Run it

In the seqeron repo root, start a node:

    ./src/main/scripts/start-cluster.sh

Then, here:

    cmake -S . -B cmake-build-release -DCMAKE_BUILD_TYPE=Release
    cmake --build cmake-build-release --target follow_stream
    ./cmake-build-release/follow_stream

The first configure fetches and builds Aeron from source, which is what `add_subdirectory` of the whole
repo costs. GoogleTest is not fetched — that is seqeron's test dependency, not part of what it exports.

Output is one line per frame:

    # following member 0 via /var/folders/…/seqeron-seq-aeron-0
    1 leader=member 0
    2 system eventType=16
    …
    # caught up — following the tap live
    267 ping echoed, round trip 4932us
    269 ping echoed, round trip 6306us
    …

`start-cluster.sh` already runs a `ClusterProbe follow` replica of its own; this one attaches beside it
with a different client id, since two replicas sharing a Replayer client id supersede each other's
replays and neither catches up.

## Environment

| | |
| --- | --- |
| `SEQERON_NODE_MEMBER_ID` | which node to attach to; default 0 |
| `SEQERON_REPLAYER_CLIENT_ID` | this replica's Replayer client id; default 8 (the Java example uses 7) |
| `SEQERON_AERON_DIR` | that node's Aeron directory; default `{TMPDIR}/seqeron-seq-aeron-{member}` |
| `SEQERON_IDLE_STRATEGY` | `backoff` (default), `yielding` or `busyspin` |
| `SEQERON_EXAMPLE_EGRESS_PORT` | UDP port the ping's cluster session takes egress on; default `9202 + member` (`doc/registries.md` §2) |

## What it shows

- **Header-only.** `seqeron_core` is an INTERFACE target; there is nothing to link but Aeron's C client.
- **The receiver owns the history/live split.** No code here requests a replay, tracks the archive or
  notices a gap: `ReplayerStreamReceiver` does all of it and dispatches nothing out of order, which is
  why the check in `inOrder` can be an assertion rather than a recovery path.
- **A consumer splits by family first.** `event.system`, then either a `systemEventType` or a
  `payloadId` — never a bare `templateId`, which is unique only within one schema.
- **A payload is opaque to the tier.** The ping's body is eight raw bytes — no SBE at all — under the
  examples' own `payloadId` 6, because the cluster decodes no `payloadId` and copies every payload
  through unopened. Producing needs `ClusterStreamSender` and the `Unsequenced` codec, and nothing from
  the consumer side.
- **`connectColocated` is the co-located producer's entry point.** Ingress goes over the member's own
  `aeron:ipc` and falls back to its UDP endpoint — with a REDIRECT to the real leader — when that member
  is not leading, so this one works against a follower where the Java twin's plain IPC ingress does not.
- **One session, kept alive.** The session is opened once and pinged every second, so the duty cycle also
  calls `keepAlive()` every 200ms — the cluster's `sessionTimeoutMs` is 1s, which one ping a second does
  not meet on its own.
- **The producer does not wait for its own frame.** `ping` submits and returns; the echo arrives in
  `onSequenced` in `globalSeqNo` order like everything else, which is what a real producer that is also a
  consumer looks like.
- **A fragment handler must not throw.** `Image::poll` advances the subscriber position regardless of
  what a handler raises, so the gap is recorded and the duty cycle raises it.
- **`LeadershipChanged` has its own callback.** It reaches a consumer there and nowhere else, so a
  consumer that passes `{}` for it sees a hole in `globalSeqNo` at every leadership change — including
  `globalSeqNo` 1, which always is one.

## What a consumer does not inherit

`seqeron_core` carries the include roots and `aeron_client_wrapper`, and nothing else. seqeron's own
`-Wall -Wextra` and its Debug `-fsanitize=address` live on `seqeron_flags`, which only targets inside
that repo link, and `core_tests` is not configured at all here — `SEQERON_BUILD_TESTS` defaults off
when seqeron is added as a subdirectory, so neither the suite nor GoogleTest is fetched or built.
