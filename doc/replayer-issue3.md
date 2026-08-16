# Replayer issue 3 — shared bootstrap replay

> **Status: REJECTED — will not be implemented** (decided 2026-08-09). Kept for the record; the
> reasoning that closed it is in `doc/design.md` §2.4 and `doc/todo.md`. In short: the only event
> this optimizes is a whole-node cold start, and at the morning start the log is near-empty (nothing
> to share) while a second member loss mid-day is quorum loss (game over, not a makespan problem) —
> leaving a single-member restart the surviving quorum masks. The mechanism below also has three
> defects it does not address: an archive replay image's positions **are** recording positions (what
> `catchUpPosition` and the resume anchor are counted in), so a relayed copy breaks the walk; a
> replica joining a replay already in flight starts mid-stream and hits its own `globalSeqNo`-1
> `abort()`; and relaying puts a full-log replay through the Replayer's single duty-cycle thread.

_Design, 2026-08-09. Addressed `doc/todo.md` "Replayer deferred pieces" / `doc/design.md` item 8:
"No shared bootstrap replay for many co-starting replicas (each app gets its own walk)"._

---

## 0. The problem

`ReplayerService.serveReplay` (`ReplayerService.java:579-648`) calls `archive.startReplay(...)`
once **per client**, even when several co-located apps cold-start together and walk the recording
chain in lockstep. Every cold-starting client's first request is `segmentIndex=0,
fromPosition=segment.startPosition()` (`serveReplay:618-622`), so N simultaneous cold-starters ask
for **byte-identical reads** of the same recording — and today each gets its own archive replay
session, its own disk I/O pass, and its own slot against `MAX_CONCURRENT_REPLAYS` (2). A third
simultaneous cold-starter queues behind two redundant reads of the same bytes.

`MAX_CONCURRENT_REPLAYS`'s own javadoc (`ReplayerService.java:148-153`) already names this exact
scenario as the one it exists to bound: *"the only multi-replay event that matters is node
start/restart"*. And the class's own javadoc states the intended shape — *"one reader per node
touches the archive on behalf of every co-located replica"* — which today is only true at the
process level, not at the I/O level: N clients still mean N archive reads.

## 1. Constraints

`doc/design.md` §3.4 (lines 1300-1313) records why a naive shared stream is dangerous here: the
*old* Replayer design held one long-lived, tethered replay stream across every replica's whole
lifetime, and Aeron flow-controls a tethered publication to its slowest subscriber — an idle
replica sitting at position 0 deadlocked every other replica's replay once the publisher outran one
term buffer. Any fix for issue 3 must not reintroduce that shape.

Two more constraints follow from the existing protocol:

- **No wire-format change.** `ReplayRequest` / `Replaying` / `ReplayPending` are used by every
  client (`FixGateway`, `OrderExecServer`, `BasicDataServer`) and must not change shape.
- **Correctness must never depend on sharing succeeding.** Coalescing is a pure efficiency layer;
  every path must have an always-safe fallback to today's per-client behavior.

## 2. Proposed mechanism: a short-lived internal relay, not direct multi-subscribe

Have clients attach to each other's replay indirectly, through ReplayerService's own duty-cycle
read, rather than directly to the archive's replay image.

1. **Grouping key**: `(recordingId, replayFrom)`. Cold-start walk steps always request a segment's
   own `startPosition()` (`serveReplay:618-622`), so simultaneous cold-starters collide on this key
   exactly — no fuzzy-matching needed.
2. **Grace window**: a request that matches an in-flight group's key *and arrives within a short
   window of the group's creation* (tens to a few hundred ms, matched to `ReplayerStreamReceiver`'s
   ~500 ms resend interval — `requestSub.poll` already drains up to `FRAGMENT_LIMIT` requests per
   duty cycle, so genuinely simultaneous cold-starts land in the same or adjacent poll calls) joins
   the group instead of starting a new archive replay. Outside the window, it falls back to exactly
   today's behavior — zero regression risk, purely opportunistic.
3. **One archive replay, one relay publication, per group.** ReplayerService starts a single
   `archive.startReplay` and a single internal `ExclusivePublication` on `REPLAY_STREAM_ID` (its
   own new session id — no new stream, no protocol change). It becomes the archive replay's
   **only, tethered** subscriber (bounded, reusing the existing stall handling —
   `onArchiveStalled` — that already guards `startReplayForClient`), and republishes each fragment
   verbatim onto the relay publication — the same opaque byte copy-through discipline
   `Sequencer.sequenceMessage` already uses elsewhere in this codebase.
4. **Clients subscribe to the relay exactly as they do today.** `sendReplaying` answers every group
   member with the *relay's* session id instead of the archive's. `ReplayerStreamReceiver` needs no
   change — it already just filters `REPLAY_STREAM_ID` by whatever `replaySessionId` the reply
   names.
5. **The relay publication is untethered**, mirroring the tap (205) and control (203) streams'
   existing pattern. A slow group member just falls behind and re-requests via the identical
   stall-recovery path that already exists (`REPLAY_STALL_TIMEOUT_MS`), so a straggler degrades to
   "one more request round trip," never blocks siblings, and cannot reintroduce the §3.4 deadlock —
   because unlike the retired design, this relay is per-group and bounded, not lifetime-held and
   unbounded.

## 3. Concrete touch points

- **`ReplaySlotAllocator`** — `ActiveSlot` gains a membership set (or refcount) instead of an
  implicit 1:1 client↔token mapping; `touch(clientId)` refreshes the shared slot on *any* member's
  heartbeat; the slot (and its `stopReplay`) is only released when the last member leaves or the
  idle-TTL fires with nobody left heartbeating. Additive to the existing TTL/heartbeat machinery,
  not a replacement — a bootstrap-length replay still needs it.
- **`ReplayerService`** — a small `Map<GroupKey, ActiveGroup>` in front of `serveReplay`, one new
  relay-read `Subscription.poll` added to the duty cycle (fragment-limited like `requestSub.poll`
  already is), and a plain byte-copy `FragmentHandler` for it.
- **Nothing else** — `ReplayRecordings`, the SBE schema, and `ReplayerStreamReceiver` are untouched.

## 4. How it improves the Replayer

- **Archive I/O drops from O(N) to O(1)** for the case the cap was explicitly sized for —
  simultaneous node/cluster restart, N co-located apps walking the same first segment.
- **`MAX_CONCURRENT_REPLAYS` becomes accurate.** Today it counts redundant reads of the same bytes
  as separate consumers; grouped, it bounds genuinely distinct reads, so a 3rd or 4th simultaneous
  cold-starter no longer queues behind two copies of the same data.
- **Faster convergence to caught-up** for the whole node at once, not just fewer disk passes — less
  queuing means less wall-clock time before every co-located replica reaches the live tap.
- **Matches the class's own documented intent** ("one reader... on behalf of every co-located
  replica") literally instead of only at the process level, closing this deferred item.

## 5. What this deliberately doesn't solve

Steady-state gap-resume requests (`segmentIndex < 0`) essentially never coincide byte-for-byte
across clients (each has its own last-received position), so grouping mostly helps cold start, not
ongoing gap recovery — that's fine, since cold start is where the contention actually lives. A
straggler outside the grace window gets no benefit, only the existing per-client path — by design,
since correctness must never depend on the coalescing landing.
