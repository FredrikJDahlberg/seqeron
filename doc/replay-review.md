# Replay review: the replay protocol, its server and its client as implemented

Reviewed 2026-10-24, at `9fa8feb` on `main`. "R-n" and "§n" refer to
[`seqeron-protocol-spec.md`](seqeron-protocol-spec.md).

**Scope.** Read: the server in Java (`ReplayerService`, `AeronReplayer`, `ReplaySlotAllocator`, `CutLookup`, the
`Replayer` seam), the client in Java (`ReplayerStreamReceiver`, `ReplayerRecovery`), the gateway host's relay
(`QueueRelay`, `AeronQueueRelay`), `sbe-replay.xml` and spec §10. The C++ and C# ports were grepped for the branches
the findings name, not read. Aeron 1.53.2's `Image.poll` and archive `ControlSession`, and Aeron.NET 1.52.2's
`Image.Poll`, were checked where a finding depends on them. Not run: no test was executed, and no finding was
reproduced.

**Verdict.** The protocol is sound: replies are correlated by `clientId` and `requestId`, each replay has a
subscription of its own, catch-up is detected by position, and recovery is bounded and falls back to a resume. One
defect should be fixed: the client's first-frame check fails silently in Java and C# (1). One path can plausibly
livelock the Replayer: an uncached cut search on its duty cycle, raced by the client's resends (2). The rest are
two inefficiencies, an optional protocol simplification, an undocumented consequence of purging, and stale
documents.

## Defects

**1. The first-frame check fails silently in Java and C#; C++ aborts.**

- `ReplayerRecovery.java:268-281` and `ReplayerRecovery.cs:283` throw from `onFrame` when the first frame observed is
  not `groupSeqNo` 1. `onFrame` runs inside `Image.poll`, which catches the exception, hands it to the Aeron error
  handler and moves past the frame (Aeron 1.53.2; Aeron.NET's `Image.Poll` has the same `catch (Exception)`).
- The client tier installs no error handler, so Aeron's default logs it. Every later frame throws again, nothing is
  dispatched, and the only signal is `RecoveryStalled` after 30 s.
- C++ (`detail/ReplayerRecovery.hpp:332-344`) calls `std::abort()`, so the three ports disagree.
- It breaks the rule that a client records a fault and raises it from its own duty cycle, which `fail()` does for
  every other fatal in the class.
- More reachable since purging: a walk from `FROM_START` without a snapshot file now begins mid-queue on a purged
  recording or on a member restarted at a cut.
- The suites miss it: `ReplayerRecoveryTest` calls `onFrame` directly, where the throw does surface.
- Fix: route it through `fail()` in all three, so `ReplayerStreamReceiver.poll` raises it, and change the three test
  cases to assert `fault()`.

**2. A restore's cut search can livelock the Replayer.**

- A restore whose cut the Replayer has not kept goes to `CutLookup.find` (`ReplayerService.java:660-675`), which
  runs `AeronReplayer.scanQueueRecording`: a synchronous archive replay read on the duty cycle, over the whole
  recording when the hint does not help. Its result is not kept: `serveReplay` does not call `cutLookup.keep` on
  `FOUND`.
- The client resends every 500 ms with a new `requestId` (R-1), and each resend supersedes the replay in flight.
- Once a scan takes longer than 500 ms, requests queue behind it, each scans again, and every reply carries a
  `requestId` the client has moved past. Nothing converges.
- Plausible route: a gateway host restarts after a member purged, so the host's positions no longer match the hints
  in its clients' files, and every restoring client scans from the recording's start, each separately. While a scan
  runs, the Replayer answers no one.
- Once the replay connects, the scan loop has no deadline: an archive that stops delivering hangs the Replayer for
  good. `ReplayerServer.java:83` says the duty cycle never blocks on a timeout of its own.
- It does not close the archive control session: the archive does that only after failing to send a ping for
  `connectTimeoutMs` (10 s by default), which takes a full response window.
- Not reproduced. A `ReplayerServiceTest` case with a scanner that advances the clock past 500 ms would show it.
- Fix: keep the cut on `FOUND`, then bound the scan with a deadline or make it incremental across duty cycles.

## Inefficiencies

**3. A stalled walk starts again from the recording's start.** `reRequestCurrent` (`ReplayerRecovery.java:673-681`)
re-requests `FROM_START` for a walk, on the 5 s stall watchdog and on an image closed short of `catchUpPosition`,
even after frames were dispatched. It is correct, since duplicates are dropped, but each stall replays the whole
recording. Once `lastGroupSeqNo != 0`, `requestResume()` at `lastFramePosition` is valid. All three ports.

**4. "Not now" is handled three ways.** No active recording or no tip yet is answered `ReplayPending` and added to
the pending list (`ReplayerService.java:646-658`). A recording that has not reached the resume position (685-692),
and a Replayer that is not ready or does not serve the queue (510-512), are answered `ReplayPending` and not added.
The client resends either way; the list matters only for fairness when every slot is taken. Dropping the other two
`addPending` calls stops replays being started later for requests the client has already replaced.

## Simplification

**5. Idempotent resends, as an option.** Today `requestId` changes on every resend, an unchanged one included (R-1),
and the server stops and restarts the replay each time. If a resend kept its `requestId` and the server answered a
repeat of an active request by sending the same `Replaying` again, the stale-reply problem, finding 2's class of
livelock and most of the churn would go; a deliberate re-request, as after a stall, would still take a new id. It
changes R-1 and three client ports, so it is worth doing only alongside another protocol change. Keeping the cut
(2) suffices on its own.

## Completeness

**6. Purging assumes every consumer of a queue takes part in snapshots.** `CLAUDE.md` states it. Two consequences
are written nowhere: a live consumer with no snapshot listener, such as a monitor or an audit tap, that falls
behind and whose resume point the purge passes is refused (R-2), which is fatal; and it cannot start again. A
sentence in `client-api.md` or `ops.md` covers it. Finding 1 decides whether the failure is loud.

## Documents that no longer match the code

- `sbe-replay.xml`, the `ReplayRequest` description: "Every recording a node holds starts at clusterSeqNo 1, so the
  active one alone is complete". The generated codecs embed no descriptions, so no regeneration is needed.
- Spec §10 (line 1007): "The cluster takes no snapshots…", and the sentence after it on an older recording being a
  prefix.
- Spec R-2 (line 977): "the recording changes only with the node". Purging moves its start while the node runs.
- The `ReplayerRecovery` class comment in all three ports ("from its start, which holds the whole queue"), and
  `ClusterStreamClient.hpp:275`.
- `AeronReplayer.java:29-32`: "every method here is a forward". The scan loop is decision logic, and it blocks.
- `ReplayerService.java:535-543`: `onQueueRequest`'s Javadoc sits above `logSharedSource`.

## What holds

- Correlation by `clientId` and `requestId` (R-1, R-4), and a replay subscription filtered to its own session, which
  avoids the tethered-subscriber trap on the shared replay stream.
- Completion by position rather than image close, and a resume's first frame checked against its anchor.
- The retained FIFO, bounded and falling back to a resume on overflow.
- Slots reclaimed from the driver's counters rather than timeouts (R-3).
- The relay's skip and republish rules, which keep the host's queue the member's frame for frame across member
  changes.
- The C++ and C# ports match Java everywhere checked, but for 1.

## Suggested order

1. Fail through `fault()` on the first-frame check (1), in all three ports.
2. Keep the cut on `FOUND` (2), then bound the scan.
3. Resume rather than walk again after a stall (3), in all three ports.
4. The pending-list cleanup (4) and the documents.
5. Decide on idempotent resends (5) when the protocol next changes.
