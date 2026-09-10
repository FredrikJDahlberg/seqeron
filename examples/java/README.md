# Follow the ordered stream — Java

The smallest consumer of seqeron there is: it replays a node's history through that node's co-located
`ReplayerService`, switches to the live tap when it catches up, and prints every frame in `globalSeqNo`
order. Once caught up it produces one frame a second of its own — `ping` submits it at cluster ingress,
and the echo comes back through the same consumer, so the round trip is measured over the real path.

This is a **separate build**, not a subproject. It resolves `org.limitless:seqeron` as a published
artifact, which is the only way an example can show the artifact is consumable at all — anything it
fails to expose fails here rather than passing on a source dependency.

## Run it

In the seqeron repo root, publish the artifact and start a node:

    ./gradlew publishToMavenLocal
    ./src/main/scripts/start-cluster.sh

Then, here:

    ../../gradlew -p . run

Output is one line per frame — `globalSeqNo`, then either the system event type or the application
`payloadId` and template:

    # following member 0 via /var/folders/…/seqeron-seq-aeron-0
    1 system eventType=13
    2 system eventType=16
    …
    # caught up — following the tap live
    233 ping echoed, round trip 6871us
    235 ping echoed, round trip 5086us
    …

`start-cluster.sh` already runs a `ClusterProbe follow` replica of its own; this one attaches beside it
with a different `follow.clientId`, since two replicas sharing a Replayer client id supersede each
other's replays and neither catches up.

## Properties

| | |
| --- | --- |
| `follow.member` | which node to attach to; default 0 |
| `follow.clientId` | this replica's Replayer client id; default 7 |
| `follow.aeronDir` | that node's Aeron directory; default `{tmpdir}/seqeron-seq-aeron-{member}` |

## What it shows

- **One dependency.** `org.limitless:seqeron` brings Aeron and Agrona with it — seqeron declares them
  `api`, since they are in the signatures a consumer compiles against.
- **The receiver owns the history/live split.** There is no code here for requesting a replay, tracking
  the archive, or noticing a gap: `ReplayerStreamReceiver` does all of it and dispatches nothing out of
  order, which is why the gap check in `onSequenced` can be an assertion rather than a recovery path.
- **A consumer splits by family first.** `isSystem()`, then either a `systemEventType` or a
  `payloadId` — never a bare template id, which is unique only within one schema.
- **A payload is opaque to the tier.** The ping's body is eight raw bytes — no SBE at all — under the
  examples' own `payloadId` 6, because the cluster decodes no `payloadId` and copies every payload
  through unopened. Producing needs `SystemFrame.wrapPayload` and an `AeronCluster` session, and nothing
  from the consumer side.
- **A co-located producer needs no endpoints.** Ingress is `aeron:ipc` on the member's own media driver:
  no `ingressEndpoints`, no ports. The leader is the only member that subscribes to it, which is the
  co-located kind's rule anyway — it publishes only while its own node leads. Against a three-node
  cluster, point `follow.member` at the leader.
- **One session, kept alive.** The session is opened once and pinged every second, so the duty cycle also
  sends a keep-alive every 200ms — the cluster's `sessionTimeoutMs` is 1s, which one ping a second does
  not meet on its own.
- **The producer does not wait for its own frame.** `ping` submits and returns; the echo arrives in
  `onSequenced` in `globalSeqNo` order like everything else, which is what a real producer that is also a
  consumer looks like.
- **A fragment handler must not throw.** `Image.poll` advances the subscriber position regardless of
  what a handler raises, so the gap is recorded and the duty cycle raises it.
- **`LeadershipChanged` has its own callback.** It reaches a consumer there and nowhere else, so a
  consumer that passes `null` for it sees a hole in `globalSeqNo` at every leadership change —
  including `globalSeqNo` 1, which always is one.
