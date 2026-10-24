# Snapshot review: application and cluster snapshots as implemented

Reviewed 2026-10-24, at `70b4812` on `snapshot-plans`. "§n" refers to [`snapshot.md`](snapshot.md).

**Scope.** Read: the cluster side in Java (`SequencerService`, `Sequencer`'s round and snapshot code, `QueueStart`,
`SnapshotChoice`, `RestartSafeLog`, `RestartSafety`, `CutLookup`, the Replayer's cut tracking and purging,
`trim-log`), and the client side in Java (`SnapshotTaker`, `SnapshotStore`). Not run: no test was executed. The C++
and C# ports were grepped for the one finding that names them (1), not read.

**Verdict.** The design is built end to end: byte trigger, node-local signals, a queue that starts at a cut, the
cluster snapshot as an application round, restart-safe marking, purging on members and gateway hosts, `trim-log`.
The integration suite exercises the main paths. One defect stood in the way of relying on it: the confirmation that
licenses a purge was sent before the file it vouches for existed, and when it was never written (1). It is fixed.
The rest are a rebuild path the docs describe and the code defeats, stale documents, and test gaps.

## What changed since the last review

| earlier finding | now |
| --- | --- |
| Format change fences every instance | fixed: `supportsFormatVersion` and the file's version reach `onRestore` (§9) |
| Client restart bounded by the member's full replay | fixed by §10, with a condition: only for a member whose newest snapshot is restart-safe |
| Dispatch blocked while serializing | **open**; `SnapshotTaker.onSnapshotStarted` still serializes inline, and the session keepalive is the only mitigation |
| Damaged file needs a person | partly fixed: two files kept, fallback on damage. No inspect/verify tool, no peer-copy tool |
| One nondeterminism bug fences every replica | **open**: no determinism kit in any client tier |
| Observability | **open**: `seqeron_app_snapshot_round` is still the only snapshot metric |
| Catch-up costs a full serialization per round | **open** |
| Round policy global and time-based | fixed: `bytes` trigger |
| Archive growth | partly: queues purge automatically; the Raft log needs `trim-log` by hand |
| Replica down across a round is fenced | unchanged, and now the cost of purging (§10.8) |

## Defects

**1. `SnapshotConfirmed` is sent before the file is durable, and when the write failed.** Fixed: the store records
the newest round whose file is durable (`durableRound`), and `signal` sends `Confirmed`, and advances
`confirmedRound`, only once the pending round has reached it, in all three languages. `SnapshotTakerTest` covers a
rename that fails and a file that cannot be created, case for case. As found:

- `SnapshotTaker.onSnapshotEnd` calls `store.confirm(round)` and sets `pendingConfirmed` on the next line
  (`seqeron-client/src/main/java/org/limitless/seqeron/app/SnapshotTaker.java:145-147`). `confirm` only queues
  `confirmFile` on the writer thread (`replayer/client/SnapshotStore.java:190-196`). `signal()` then sends
  `SnapshotConfirmed` on the next duty cycle with no check that the rename happened.
- A write that fails while the file is written (`begin` or `append` → `failWrite` → `abandon`) clears
  `committedRound`, so `commit` and then `confirm` return without doing anything. `onSnapshotStarted` still sets
  `serialized`, and `onSnapshotEnd` still sets `pendingConfirmed`: the signal vouches for a file that was never
  written.
- `confirmFile` logs `SnapshotStoreFailed` and returns when the force or the rename fails (full disk, permissions).
  The signal is still sent.
- In both cases `confirmedRound`, and so `seqeron_app_snapshot_round`, advance too.
- §10.8 defines the signal as sent "when its file for the round is durable under its name". The Replayer takes it at
  that word: when every joined source has confirmed, it writes `safe`, and purges the queue below the cut.
- Failure: an application's disk fills while it writes round N. It compares the round, sends `Confirmed(N)`, and has
  no file. The member marks N safe and purges below it. The application restarts, finds a file of round N-1 or none,
  and is fenced; its history is gone. §10.8 lists "the files are the only copy" as a consequence, but this path loses
  the only copy without any file having been damaged.
- The same race, without a failure, loses a file to a power cut in the milliseconds between signal and `force`.
- C++ (`app/detail/SnapshotTaker.hpp:158`) and C# (`App/SnapshotTaker.cs:147`) set `pendingConfirmed` at the same
  point.
- Fix: send the signal from the writer's completion. `confirm` returns or reports the outcome, `signal` waits for it,
  and a failed or abandoned write sends nothing, so the round stays unsafe. `SnapshotStoreTest.undurableRoundDeletesNothing`
  already covers the store; a `SnapshotTakerTest` case in each language, sending no `Confirmed` after a failed write,
  pins the taker.

## Gaps against the spec

**2. Rebuilding a member from `ClusterBackup` does not restore at the cut.**

- §10.6 says a new member or lost disk takes the cluster snapshot and log from `ClusterBackup`, "restores at the cut
  and records its queues from there".
- `restart-safe.log` is the member's own and `ClusterBackup` does not copy it. `SnapshotChoice.invalidateUnsafe`
  invalidates every snapshot the file does not name, so a rebuilt member invalidates them all and replays the whole
  backed-up log. That works only because the backup never trims, and it takes the longest replay there is.
- The `trimmed` line that fences a whole-log replay lives in the same file. A rebuilt member has none, and a member
  whose `restart-safe.log` is lost loses the line with it. Either then invalidates every snapshot and attempts a
  whole-log replay of a log that no longer begins at its first term; `SnapshotChoice`'s "the Raft log has been
  trimmed" never fires.
- No runbook covers either case. `ops.md` and `running-a-cluster.md` do not mention `ClusterBackup`;
  `clusterctl.md` names it in one line and gives no procedure.
- Decide: a documented seeding step (copy a peer's `safe` lines for the snapshots the backup holds, which is sound only
  if the peer's cut was restart-safe on this member's clients), or state that a rebuild is a full replay. Either way,
  the trim fence needs a home that survives the loss of `restart-safe.log`.

**3. A source that joins late is not waited for.** `replayer.snapshotJoinMs` defaults to 5 s on a member and 10 s
on a host. `Joined` is placed by `SnapshotTaker.signal`, from the duty cycle, and the same duty cycle serializes
inline. An application that begins serializing when `SnapshotStarted` reaches it cannot place `Joined` until it is
done. So `joinMs` has to exceed the longest serialization, and the application finds out only when it is fenced.
`ops.md` says to keep it above that time; nothing measures the time (see 6). The failure is bounded: it hurts only
if that application restarts before it confirms the round it joined late.

**4. A replayed `SNAPSHOT` action on a member with an empty `restart-safe.log` is treated as live.**

- `onTakeSnapshot` decides live from `safeLog.isCut(logPosition)`. A member whose file is new or lost, replaying a log
  that holds many rounds, finds none of them and appends a `cut` line for each, hands each to the Replayer, which
  broadcasts a `SnapshotRound` for each.
- The Replayer tracks one round at a time, so each replaces the last. The last replayed round stays open, and if no
  source joins it within the window, as when no co-located application is up yet, it is marked safe. An older cut is
  a conservative restart point and the purge below it removes little, so the cost is a burst of `SnapshotRound`
  messages to clients that are far past those rounds, and nothing else.
- Only the first cut after the replay is real. It is the same case as 2.

**5. The genesis start does not purge the previous run's recordings.** `purgeOlderRecordings` runs in `restore()`
only (`SequencerService.java:237`). A member with no usable snapshot replays the whole log into a new recording
each start and leaves the old ones, which the Replayer ignores in favour of the newest. Only a disk-space cost. Not
new, but §10.3's "old recording: purged" covers the restore and says nothing of a genesis start.

## Operations and observability

**6. Nothing measures a round.** Missing: serialization time and bytes per instance, restore time and source, the
sequencer's latest round, the Replayer's current round and whether it is safe, the age of the newest `safe` cut, the
purge floor per queue, and the size of the Raft log. The runbook's own warnings ("an instance more than a round
behind blocks purging", "lag in `seqeron_app_snapshot_round`") need the first and last of these to be actionable.
The dispatch stall while serializing is unmeasured too.

**7. `trim-log` is manual and unscheduled.** The Raft log and the snapshot recordings grow until an operator runs it
on each member. Nothing alerts on their size. `trim-log` writes `trimmed` before it purges, which is right; it also
means a failed run leaves a member that can no longer replay its log whole, with nothing in the output saying so
beyond "run it again".

**8. No determinism kit, no inspect/verify, no peer-copy tool.** These were findings 4 and 5 of the last review and
remain. They matter more now: with queues purged below the cut, a file is the only copy (§10.8), and a fence needs a
peer's file copied in by hand. A replica that was down across one round is in that state.

## Documents that no longer match the code

- `snapshot.md` §10 intro (line 331) says "none of it is implemented"; §10.8 (line 551) repeats it. Its opening
  (lines 19-22) says "the cluster takes no snapshots of its own". All of §10 is implemented except the audit tool.
- `overview.md:260` says "a gateway host purges nothing". It does, since `2796724`.
- `snapshot.md` §10.6 "Order" and "Tests" describe steps as future work. Two plan documents,
  `snapshot-plan.md` and `snapshot-gateway-purge-plan.md`,
  are done; decide whether they stay as history or go.

## Test gaps

The integration suite covers: restart from a restart-safe snapshot, a queue at the cut, no safe snapshot and a
replayed action, another term length, the hold bounded by the state write, a trimmed log, a gateway that never
confirms and one that does, a relay's reports, a host's purge, and a relay left behind. What the spec asks for and
the code lacks:

- `FailoverPropertyTest` and `CommittedEntryLossTest` run with no `<snapshots>` policy. §10.6 asks for both with
  snapshots and purging on.
- A member killed between a round's cut and its mark, restarting from the previous set.
- A follower that was down past the trim horizon, catching up against a leader that has trimmed (§10.7 "not
  verified", still not).

## Suggested order

1. Decide 2, give the trim fence a home that survives a lost `restart-safe.log`, and write the rebuild runbook.
2. Metrics (6), then the documents.
3. Tooling (8), then scheduling or alerting for `trim-log` (7).
4. The two fault tests the spec names, and the horizon test.
