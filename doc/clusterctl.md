# clusterctl — cluster life-cycle tool (Option A: node-local operator agent)

A small Java operations tool for opening and (primarily) cleanly closing the sequencer cluster.
Its central purpose is an **orderly shutdown** that leaves the recorded log fully
replayable/analysable afterwards.

## Shape

`clusterctl` is **node-local**: it runs co-located on a `SequencerNode` host, sharing that node's
Aeron directory and `clusterDir`. It combines two mechanisms that the original sketch conflated:

- **In-band marker messages** over cluster ingress — `ClusterStarted` / `ClusterStopped` records
  injected into the ordered log so the transitions are themselves sequenced, timestamped, auditable
  events. Ingress publish routes to the leader like any other client; the sequenced echo appears on
  the node-local tap (`aeron:ipc` stream 205) this tool already sits next to.
- **Out-of-band local control** via `io.aeron.cluster.ClusterTool` — role inspection and the
  cluster-terminating `ABORT` toggle. This is filesystem/counter based and **only works on the
  local node's `clusterDir`**, which is why the tool must be node-local (a remote UDP client could
  not do it). Aeron's `io.aeron.cluster.ClusterControl` toggles apply **only on the leader** (its
  javadoc), so the destructive commands must be run on the leader node.

`clusterctl` does **not** launch or restart cluster processes. Bringing nodes up is still
`start-cluster.sh` / launching `SequencerNode` JVMs. `start` only *records* that the system is up;
`shutdown` records the close marker and then terminates the running nodes.

### Implementation

Java — required, since `ClusterTool`, the Aeron cluster ingress client, and the SBE codecs are all
Java. There is no existing Java cluster-*client* today (the Java side is only the service + nodes),
so the ingress-connect/echo handshake is new Java code modelled on the C++ `ClusterIngressSender` +
tap-follow path, not shared with it.

The tool is one Java class, `org.limitless.phixeron.tools.ClusterCtl` (alongside `SbeLogPrinter`),
launched by `src/main/scripts/clusterctl.sh` — a thin wrapper matching the other scripts that sets
the classpath / `--add-opens` JVM options and forwards the subcommand and its arguments:

```
clusterctl.sh start          # record a "system started" marker
clusterctl.sh shutdown       # orderly stop
clusterctl.sh help
clusterctl.sh describe …     # → ClusterTool passthrough
```

(`ClusterCtl` is named to mirror the script and to avoid shadowing Aeron's own
`io.aeron.cluster.ClusterControl` toggle class that this tool drives via `ClusterTool`. Symlink
`clusterctl.sh` to `clusterctl` on an operator PATH if a bare name is wanted.)

## Commands

```
clusterctl.sh start        record a "system started" marker (precondition: elected leader)
clusterctl.sh shutdown     orderly stop; run on every node, no-op on followers
clusterctl.sh help         list commands and exit
clusterctl.sh <other...>   pass through to ClusterTool (describe / errors / list-members / …)
```

### start

Records/signals in the log that the system has started. It does **not** start the cluster.

1. **Precondition — an elected leader must exist.** Read the local node's cluster counters (or
   `ClusterTool` role/mark-file) to confirm the cluster has an elected leader. If none,
   `clusterctl.sh start` **exits non-zero and does nothing** — it never blocks waiting for an election.
2. Publish an **unsequenced `ClusterStarted`** (schema 200) with a `correlationId` via cluster
   ingress.
3. Wait (bounded timeout) for the **sequenced `ClusterStarted`** echo (schema 202) carrying the same
   `correlationId` on the local tap. On echo → print the assigned `globalSeqNo`/`timestamp`, exit 0.
   On timeout → exit non-zero.

Note: completion is the tool's **own `ClusterStarted` echo**, not `LeadershipChanged`.
`LeadershipChanged` (`sbe-sequenced.xml` template 5) is emitted by the sequencer on election,
independently of this marker; the "leader exists" check is the up-front precondition in step 1, and
is what makes `start` exit early when there is no leader.

### shutdown (the primary purpose; the last command issued)

One command for both routine end-of-day and an early stop — an "emergency" stop is just this same
shutdown run earlier than usual, so there is no separate command. It records the close marker and
terminates the cluster via `ClusterTool` `ABORT`, so the recorded log stays replayable/analysable
(`ABORT` + the wired termination hook close every node's archive cleanly — see below). Termination is
**consensus-coordinated**: the leader's single `ABORT` sets a common termination log position — taken
after `ClusterStopped` — and every node terminates at exactly that position, so `ClusterStopped` is
the last event in every node's log. Followers are brought down *by that `ABORT`*, not by an
independent local kill (which could stop a node before it applied `ClusterStopped` and break that
guarantee).

Safe to invoke on **every** node (e.g. a systemd unit fired cluster-wide); leader detection routes
the real work to the one leader, so the operator need not know which node leads.

1. **Leader gate.** Check role via `ClusterTool`/cluster counters. On a **follower**: **no-op, exit
   0** — publish nothing, terminate nothing; the leader's `ABORT` brings this node down.
2. On the **leader**: publish an **unsequenced `ClusterStopped`** (schema 200, `correlationId`) via
   ingress; wait (bounded timeout) for the **sequenced `ClusterStopped`** echo on the local tap.
3. **Durability barrier.** Confirm the local archive's tap recording position has advanced past the
   `ClusterStopped` frame (`RecordingPos`) — the marker is on disk, not just in flight.
4. **Terminate** via `ClusterTool` `ABORT` on the local (leader) `clusterDir`. Best-effort about the
   marker: if the echo (2) or barrier (3) does not clear within the timeout — an unhealthy cluster,
   which is often *why* you are stopping early — it aborts anyway, so the cluster still comes down
   cleanly (via `ABORT`, never `SIGKILL`, so the log is preserved). A log that ends *without*
   `ClusterStopped` is then the signal that the marker never landed before the abort.
5. **Verify** (the success criterion is a *readable log*, not "process exited"): after exit, run
   `SbeLogPrinter` against the archive dir and assert the tap recording (stream 205) has a valid
   stopPosition and dumps. If not, fall back to the alternatives below.

**Deployment note (systemd).** Let the leader's `ABORT` stop the follower `SequencerNode`s (they
exit on the coordinated termination); do not have systemd independently `SIGTERM` a follower's
`SequencerNode` in a way that races the `ABORT`, or a node could terminate before applying
`ClusterStopped`. (`SIGTERM` is itself clean now — it drives the same barrier teardown — it just
isn't ordered against the marker.)

### help / passthrough

`help` lists the commands and exits. Any other argument vector is forwarded verbatim to
`ClusterTool` (`describe`, `errors`, `list-members`, `recording-log`, …). These are node-local,
connection-less diagnostics — they work only where a `clusterDir` is present, i.e. on a node.

## Why `ABORT`, and the node fix that makes it safe

Two facts about Aeron 1.51.0 shutdown, both verified against the cluster sources, drive the design:

- **Use `ABORT`, not `SHUTDOWN`.** `SHUTDOWN` takes a **snapshot** before terminating
  (`ConsensusModuleAgent.java:2519`). This repo's invariant is *no snapshots — recovery is always
  full-log replay from gseq 1*, which is what makes the tap recording provably complete. A
  shutdown-snapshot would silently break that (next boot recovers from the snapshot instead of
  replaying). `ABORT` (`:2544`) terminates with no snapshot and coordinates all nodes at one log
  position — the right fit.

- **Termination hook wired in `SequencerNode` (done).** Aeron's default
  `ConsensusModule.Context.terminationHook` is a **no-op** `() -> {}` (`ConsensusModule.java:2089`);
  the documented pattern is `.terminationHook(barrier::signalAll)` (`:268`). `SequencerNode`
  previously used a `ShutdownSignalBarrier` but did **not** wire the hook, so a `ClusterTool`
  `ABORT`/`SHUTDOWN` terminated the consensus + service agents but never signalled the barrier —
  `SequencerNode.main` stayed blocked on `barrier.await()`, the `ClusteredMediaDriver`/`Archive`
  were **never closed through the clean try-with-resources path**, and the half-dead node
  (archive up, consensus dead) had to be `kill`ed, risking an unflushed catalog and an unreadable
  log. `SequencerNode` now wires `terminationHook(barrier::signalAll)`, so `ABORT` unwinds cleanly
  on every node → `Archive.close()` forces the catalog → `SbeLogPrinter` reads the tap. This is the
  change that makes `clusterctl` shutdown safe for log analysis.

Why the log is otherwise fine: `SequencerService.onTerminate` already closes `tapPub`, setting the
tap recording's stopPosition cleanly, and that callback fires on both `ABORT` and `SHUTDOWN`
(`ClusteredServiceAgent.java:1205`). `SbeLogPrinter` reads the archive catalog + segments **offline
from disk**, so "log analysable after shutdown" reduces to "was the archive closed cleanly" — which
the termination-hook fix guarantees.

## Alternatives if `ClusterTool` shutdown can't guarantee a readable log

If, even with the two changes above, a clean archive close cannot be relied on (e.g. `Archive.close`
does not force the catalog, or a node was mid-recording):

- **Alt A (strongest fallback) — local `SIGTERM`** to the `SequencerNode` PID instead of Aeron's
  `ClusterControl` toggle. That path is already clean today: it drives the same `ShutdownSignalBarrier` →
  try-with-resources → `Archive.close()` teardown that `stop-cluster.sh` relies on, and needs no
  termination-hook fix and no snapshot decision. Trade-off: not a consensus-coordinated quiesce, so
  correctness rests on the step-3 durability barrier (which already guarantees `ClusterStopped` is on
  disk on every node before any node is signalled).
- **Alt B — make "log is dumpable" the success criterion** (step 5): after exit, `SbeLogPrinter`
  the archive dir; if the tap recording has no valid stopPosition or fails to dump, escalate to
  Alt A and re-verify.
- **Alt C — belt-and-suspenders:** explicit `AeronArchive.stopRecording(tap)` before terminating,
  on top of `onTerminate`.

Post-shutdown analysis reads any single node's archive dir (every node's tap is byte-identical), via
`SbeLogPrinter` / `logprint.sh` with the sequenced SBE IR.

## Schema work (do both toolchains, in lockstep)

Add two messages to **both** `sbe-unsequenced.xml` (200) and `sbe-sequenced.xml` (202), kept
byte-identical past the shared `header` composite (same rule the existing messages follow), and
regenerate on the Java (`generateUnsequencedSbe`/`generateSequencedSbe`) and C++ (`Generate…SbeCodecs`)
sides:

- `ClusterStarted` — header + `correlationId` (and nothing else; empty business body).
- `ClusterStopped` — header + `correlationId`.

The sequencer needs no new logic to stamp these: `onSessionMessage` already copies any message type
through by template id.

## Non-goals / open items

- Does not launch or restart cluster processes.
- Destructive commands (`shutdown`, `ABORT` passthrough) run only on the leader node; no
  authentication is added — access control is "you have a shell on the node." If `shutdown` is ever
  exposed to remote invocation, it must gain an Aeron `Authenticator` first.
- No resident per-node daemon. `shutdown` may be fired on every node (leader does the work,
  followers no-op); `start` is a single leader-side invocation. Cross-node termination is
  consensus-coordinated by `ABORT`, not per-node kills.
