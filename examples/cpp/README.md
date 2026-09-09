# Follow the ordered stream — C++

The C++ twin of [`examples/java`](../java): it replays a node's history through that node's co-located
`ReplayerService`, switches to the live tap when it catches up, and prints every frame in `globalSeqNo`
order. Same flow, same output.

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

The first configure fetches and builds Aeron and GoogleTest from source, which is what
`add_subdirectory` of the whole repo costs. Output is one line per frame:

    # following member 0 via /var/folders/…/seqeron-seq-aeron-0
    1 leader=member 0
    2 system eventType=16
    …
    # caught up — following the tap live

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

## What it shows

- **Header-only.** `seqeron_core` is an INTERFACE target; there is nothing to link but Aeron's C client.
- **The receiver owns the history/live split.** No code here requests a replay, tracks the archive or
  notices a gap: `ReplayerStreamReceiver` does all of it and dispatches nothing out of order, which is
  why the check in `inOrder` can be an assertion rather than a recovery path.
- **A consumer splits by family first.** `event.system`, then either a `systemEventType` or a
  `payloadId` — never a bare `templateId`, which is unique only within one schema.
- **A fragment handler must not throw.** `Image::poll` advances the subscriber position regardless of
  what a handler raises, so the gap is recorded and the duty cycle raises it.
- **`LeadershipChanged` has its own callback.** It reaches a consumer there and nowhere else, so a
  consumer that passes `{}` for it sees a hole in `globalSeqNo` at every leadership change — including
  `globalSeqNo` 1, which always is one.

## One thing to know

`seqeron_core` interface-links `seqeron_flags`, which carries `-Wall -Wextra` and, in a Debug build,
`-fsanitize=address`. A consumer inherits both. Configure Release unless you want the sanitizer.
