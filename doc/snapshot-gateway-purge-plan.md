# Gateway-host purging: implementation plan

How to implement the gateway host's half of [`snapshot.md`](snapshot.md) §10.6, which [`snapshot-plan.md`](snapshot-plan.md)
step 5 left out. A member's Replayer purges its queue recordings below the newest restart-safe cut. A gateway host's
Replayer purges nothing, so its recordings grow with uptime.

## What exists

- `ReplayerService.trackCuts` takes a `CutHandover` per round, keeps each queue's cut position (`keepCut`), offers a
  `SnapshotRound` to the applications, and feeds `RestartSafety`. A cut that every application that joined has
  confirmed raises `purgeFloors`, and `purgeBelowFloors` purges below it with `SegmentPurge.target`. A refusal keeps
  the floor and is retried after an interval.
- On a gateway host `cutIntake` is null, so `restartSafety` is null, `trackCuts` and `purgeBelowFloors` never run, and
  the `SnapshotJoined` and `SnapshotConfirmed` the host's instances send are dropped in `onRequest`.
- **Only `Application` answers a round.** `Gateway` ignores `SnapshotRound` and sends neither signal, in all three
  languages (`snapshot.md` §10.8, `client-api.md`). A host's tenants are mostly gateways, so with `RestartSafety` as it
  stands a round on a host has no joiners, is safe as soon as the join window closes, and the floor jumps to the newest
  cut whatever the gateways' files say.
- `RestartSafety` keys a joiner by `sourceId`. A gateway's `sourceId` is its logical gateway's, shared by both
  instances of a pair.
- A host runs one `AeronQueueRelay` per relayed group, all on one thread. `Sequencer.takeSnapshot` puts one marker,
  with one `clusterSeqNo`, on every queue, so each relay sees its own copy at its own time. On a member
  `SequencerService` fills every group's `markerPositions` in one handover.
- `QueueRelay.onFrame` publishes only the frame after the last one published, so within a run `publish` sees each
  marker at most once. Across a host restart that state is gone, and the relay's first frame is the member's recording
  start: a cut after a member restart, a segment boundary after a member purge.
- A relay is a replay session on the member's archive, and Aeron 1.53.2 refuses a purge below a segment a replay is
  reading (`ArchiveConductor.isValidDetach`). A connected relay, however far behind, holds the member's purge back.

## Decisions

- **Gateways join and confirm.** A gateway answers `SnapshotRound` and sends `SnapshotConfirmed` as an `Application`
  does. On a host the floor is then derived from every instance that takes part, with no fixed slack. A gateway that
  runs on a member then gates the member's restart-safe mark, as a co-located application does.
- **The two instances of a gateway pair never share a node**, member or host. `RestartSafety` keys a joiner by
  `sourceId`, which both share, so two on one node would count as one and the first confirmation would make the round
  safe.
- **The Replayer fails closed when the rule is broken.** `RestartSafety` also records each joiner's `clientId`, which
  the signals already carry. A second `clientId` for a `sourceId` in one round leaves that round unsafe, and the
  Replayer logs it. A node that breaks the rule never marks a cut safe and never purges, and fences no one. An instance restarted mid-round joins under a new `clientId` and leaves
  its round unsafe the same way.
- **One round per `clusterSeqNo`.** The first relay to report a cut opens the round. Every relay's report reaches
  `keepCut` for its own group. When the round is safe, each group's floor is the safe cut's position in that group,
  looked up in `CutLookup`. A group whose relay has not reached the marker gets no floor that round.
- **A restarted host purges its old recordings**, as `snapshot.md` §10.3 already says a member's restart does, before
  its Replayer is ready.
- **A relay left behind by member purging is a separate fix** (below). Member purging already ships, so it is a defect
  whether or not hosts purge.

## Order

Each step ships alone, with its tests.

### 1. The spec

- `snapshot.md` §10.6: a host's floor is derived as a member's is; drop "the cut before the newest one" and "the
  Replayer cannot see an instance's confirmation". Add the one-round-per-`clusterSeqNo` rule and the per-group floor
  lookup.
- `snapshot.md` §10.8: gateways answer `SnapshotRound`; the two instances of a pair never share a node, and a round
  in which two `clientId`s join for one `sourceId` stays unsafe.
- `client-api.md`, "Rounds and the Replayer": `Application` and `Gateway`.

### 2. Gateways join and confirm

- `Gateway` in Java, C++ and C#: `onSnapshotRound` hands the round to its `SnapshotTaker`, and the duty cycle calls
  `snapshots.signal` with its `sourceId`, as `Application` does. Replace the façades' "expects no answer" comments.
- `RestartSafety.onJoined` and `onConfirmed` take the signal's `clientId`; a second `clientId` for a `sourceId` in the
  round makes it unsafe. `ReplayerService.onRequest` passes it and logs the first such join of a round.
- Tests: `RestartSafetyTest`, two `clientId`s for one `sourceId` leave the round unsafe, whatever they confirm, and the
  next round is judged afresh. Integration: a gateway on a host joins and confirms a round, and a gateway on a member
  that has not confirmed keeps the member's cut unsafe.

### 3. The relay reports cuts

- In `AeronQueueRelay.publish`, a system `SNAPSHOT_STARTED` frame is reported once its offer has succeeded, with its
  round, its `clusterSeqNo`, its group and its start position in the local recording: the offer result minus
  `align(length + HEADER_LENGTH, FRAME_ALIGNMENT)`. A term-boundary padding frame precedes the frame start, so the
  subtraction holds; a frame larger than the MTU would not, and a marker is not.
- The report goes to the host's `ReplayerService` through a `ConcurrentLinkedQueue`, as `CutHandover` does on a member.
- **Live or historical.** A relay is live once the source position reaches the member's recorded position read in
  `startReplay`, and stays live across later member changes. Each report says which. Every report reaches `keepCut`;
  only a live one may open a round. A fresh relay's first frames are historical, so a restarted host never opens a
  round for a cut its instances may have confirmed in an earlier run.
- Tests: the reported position is the frame's start, including across a term boundary (integration; it needs an Aeron
  runtime); a back-pressured offer reports once; a marker before the relay is live is historical, one after is live,
  and a member change does not reset it.

### 4. The adapter and the host's `CutIntake`

- The adapter turns reports into `CutHandover`s on the Replayer's thread. The first live report of a `clusterSeqNo`
  opens a round: `round` from the marker, `clusterSeqNo` as `logPosition`. Later reports of the same `clusterSeqNo`,
  from other groups, only reach `keepCut`. `RestartSafety` returns `logPosition` opaquely and `trackCuts` compares it
  for equality, so it only needs to be unique and monotone; a host has no Raft log position.
- At safety, `trackCuts` sets each relayed group's floor from `CutLookup` by the cut's `clusterSeqNo` rather than from
  `inFlightCut.markerPositions`.
- A host has no cluster directory and nothing reads a safe log there (the launcher's `SnapshotChoice` and `trim-log`
  are a member's). `CutIntake.log` is null on a host and `appendSafe` is skipped.
- `ReplayerServer` builds the host's `CutIntake` with `replayer.snapshotJoinMs` from the host's own properties.
- Tests: `ReplayerService` on a host with `FakeReplayer`: a joined and confirmed cut raises every reported group's
  floor; a cut an instance joined and never confirmed does not; a second group's report of the same `clusterSeqNo`
  opens no second round and offers no second `SnapshotRound`; a group that reported nothing gets no floor; a burst of
  historical reports opens no round.

### 5. The Replayer purges on a host

- With `restartSafety` and `cutIntake` present, `trackCuts` and `purgeBelowFloors` run as on a member. `SegmentPurge.target`,
  the refusal retry and the cached-start refresh are unchanged.
- A host with no `<snapshots>` policy sees no markers and purges nothing.
- Tests: `QueuePurgeTest` on a host `ReplayerService`: the floor comes from the reported cuts, a refusal is retried and
  not counted, and a cut below the new start is refused.

### 6. A restarted host's old recordings

- A restarted relay creates a new local queue and recording from the member's current start. Once the new recording
  is active, `purgeRecording` removes every older recording of that stream on the host's archive, as `SequencerService`
  does on a member. This also removes an older recording an unclean shutdown left looking active.
- `checkReady` waits for it, as on a member (`snapshot.md` §10.5 step 4), so the Replayer never serves a recording
  being replaced.
- Tests: integration, a host restarted twice holds one recording per relayed queue.

### 7. Integration and docs

- A host runs through several rounds, its recording start moves up, and a restore at an older cut gets R-2.
- A gateway that has not confirmed holds the host's floor until it does, or until it misses the join window of a later
  round.
- A host restarted after a purge still serves, and `QueueRecordings` finds its recording contiguous from its start.
- A relay changes member across a purge; positions come from the local queue, so they do not depend on the source.
- A restarted host does not purge below a cut it re-reports.
- A host relaying two groups purges both at one round.
- `ops.md` and `running-a-cluster.md`: the host's join window, the host's purge, and the rule that a pair's instances
  never share a node.
- `seqeron-examples/README.md` already runs one instance of each pair on its single node, and `cpp-client-test.sh`
  and `csharp-client-test.sh` place the pair on two members, so neither breaks the rule.

## Separate fix: a relay left behind by member purging

A connected relay holds its member's purge back, so only a relay that is disconnected from every member while they purge
past it is left behind (`QueueRelayTest`, members purged past the last published frame). It then cycles through the
members with error logs and no fatal. That case is exact: every member's replay from its recording start opens on a frame
past the next one. The relay goes fatal there with no timeout, and the restart relays from the member's start into a new
recording (§10.3), which fences only instances whose newest file is below it. Bounding a member's floor by its relays
is not needed. A connected relay that lags grows its member's recording instead; document that in `ops.md`.

## Risks

- **A host's purge has no second check.** A member's unsafe cut is invalidated at start; a host's purge cannot be
  undone. `RestartSafety` does not wait for an instance that has not joined within the window, so an instance that is
  late or down at a round and restarts after the host has purged past its file is fenced until a peer's file is copied
  in. A member has the same trade-off with a milder consequence. Set the host's join window above a member's, and
  document it as the operator's fence-risk knob.
- **A confirmation that precedes its round is lost.** `SnapshotTaker` keeps only the latest announced round, and
  `RestartSafety` drops a join or confirmation for a round other than the current one. A join cannot precede its round,
  since it answers `SnapshotRound`. A confirmation is signalled once, so one that reaches the Replayer before `onCut` is
  dropped, the instance's later join stays unconfirmed, and the round is never safe. Nothing is purged for it. On a host
  the relay queues its report at the marker's offer, and the confirmation follows a `SnapshotEnd` that makes a round trip
  through the cluster, so only a badly delayed Replayer cycle loses the race. Add a test; if it is reachable, make
  `RestartSafety` keep confirmations for a round it has not yet seen, in one place and Java only. The alternative is
  `SnapshotTaker` answering `confirmed` again when a `SnapshotRound` names its confirmed round, in all three languages.
- **An instance more than a round behind blocks purging.** `RestartSafety` tracks one round and a later one replaces an
  unfinished one, so such an instance keeps every round unfinished. Nothing is fenced and the recording grows. A
  member has the same behaviour. `seqeron_app_snapshot_round` shows the instance; document it for hosts in `ops.md`.
