# Cluster snapshots: implementation plan

How to implement [`snapshot.md`](snapshot.md) §10, the cluster snapshot and the cut it shares with the
clients. Application snapshots (§1–9, §12–13) are built in every language: `SnapshotTaker`, `SnapshotStore`,
`SnapshotListener`, the restore in `ReplayerRecovery` and their tests. §11, the snapshot server, is out of
scope: the spec says its confirmation model is to be revised before anything is built.

## Order

Each step ships alone, with its tests.

### 1. Schema 210 and the byte trigger

- Add `bytes` to `SnapshotPolicyRegistered`.
- `TopologyDocument` requires `snapshot="true"` on every row when `<snapshots>` is present.
- `Sequencer` counts sequenced frame bytes, each frame once. The count is replicated state.
- Regenerate and commit the C++ and C# codecs. Ship on a purged archive (spec V-3), batched with any other
  schema 210 changes.
- Tests: `SequencerTest` (byte trigger beside interval), `TopologyDocumentTest`, `CheckSbeCodecsCurrent`,
  `checkCSharpSbeCurrent`.

### 2. Node-local signals

- `sbe-replay.xml`: `SnapshotRound`, `SnapshotJoined`, `SnapshotConfirmed`; `cutClusterSeqNo` on
  `ReplayRequest`. Regenerate in every language.
- The façades answer `SnapshotRound` with `SnapshotJoined`, and send `SnapshotConfirmed` once their file is
  durable under its final name.
- Keep the three `ReplayerRecoveryTest`s in the same case order. Update `client-api.md`.

### 3. A queue that does not start at `groupSeqNo` 1

- `ReplayerService.checkReady` serves a recording whatever its first frame.
- `QueueRelay` starts at the member's recording start.
- The Replayer's cut lookup: a kept-cut map; refuse below the recording (R-2); `ReplayPending` above the tip;
  otherwise read the recording for the marker.
- `ReplayerRecovery`, in all three languages, takes its anchor from the first frame actually read, not from
  the file's `cutPosition`.
- Harmless on its own; unblocks step 4.

### 4. The cluster snapshot

- **State.** `SequencerSnapshot`, a package-private interface `Sequencer` implements, with a schema of its
  own (Java only), maps written in key order. `SequencerSnapshotTest` snapshots at a random point of a seeded
  frame sequence, restores a second `Sequencer`, feeds both the rest, and compares every field and every
  encoded frame.
- **`onTakeSnapshot`.** Emit the marker `R` on every queue, write the state, return. It waits for no
  application.
- **Trigger.** The leader's service sets the `ClusterControl` toggle, found with
  `ClusterControl.findControlToggle`, only while `cluster.role()` is `LEADER`.
- **Restart-safe mark** (`snapshot.md` §10.8).
  1. The service hands each live cut to the Replayer: the round, `clusterSeqNo`, the marker's position and
     `Cluster.logPosition()`.
  2. A pure class (expected, joined, confirmed, join window) decides when the cut is restart-safe, and the
     Replayer then appends the log position to `restart-safe.log`, forced to disk.
  3. A replayed `SNAPSHOT` action is not handed over.
- **Start.** Before the consensus module starts, the launcher reads `recording.log` and `restart-safe.log`
  and calls `ClusterTool.invalidateLatestSnapshot(out, clusterDir)` until the newest snapshot is one the file
  names. It falls back to no snapshot only while `trim-log` has not run; otherwise the member does not start.
  `RecordingLog.createRecoveryPlan` can cross-check this before startup.
- **`onStart`.** Restore the state, then create each queue at its cut with
  `ChannelUriStringBuilder.initialPosition` when the term length and MTU match, else at position 0. A pure
  decision class covers genesis, restore at the marker's position, restore at 0.
- **Version.** `LOG_VERSION` is the major of `appVersion` on the consensus module's context and the service
  container's.

### 5. Purging, then `trim-log`

- The Replayer purges whole segments below the restart-safe floor with `purgeSegments`, rounded down to a
  segment boundary. A refusal (a replay reading below, another delete running) is retried, not a fault.
- `clusterctl trim-log` keeps the two newest restart-safe snapshots and the log from the older one, purges
  older snapshot recordings, and records in `restart-safe.log` that it ran.

## Aeron 1.53.2, checked against its sources

| claim | result |
| --- | --- |
| `Cluster.logPosition()` in `onTakeSnapshot` is the snapshot's log position | holds: `onServiceAction` sets it before `executeAction`, and `recording.log` entries carry the ack's value |
| `ClusterTool.invalidateLatestSnapshot(PrintStream, File)` is callable from the launcher | holds: public static, opens `RecordingLog` directly, invalidates every entry at the highest valid snapshot position |
| the toggle is leader-only, outside an election, compare-and-set from `NEUTRAL` | holds |
| in `State.SNAPSHOT` a leader polls no timers or ingress, so no `ClusterHeartbeat`; a follower polls neither log nor ingress | holds |
| `appVersion` majors are compared on snapshot load and on a leadership term replayed from the log | holds |
| recovery with a snapshot starts the log at the snapshot's position | holds; with none it starts at the recording's start, which a trim moves |
| `purgeSegments` | needs a segment-aligned position at or above the end of the first segment; refuses while a replay reads below it and while another delete runs |
| a queue created at `initialPosition` can be recorded | the API exists and Aeron uses it for its own replay publications; `RecordingWriter` takes its segment base from the join position |
| a replayed `SNAPSHOT` action | stronger than the spec first said: the consensus module enters `State.SNAPSHOT` and appends a real snapshot at the same log position |

## Not verified

- **Election catch-up against a trimmed leader log.** A follower down past the trim horizon may need log its
  leader has purged. Needs an integration test; such a member is rebuilt from `ClusterBackup`.
- **The archive's catalog `startPosition` for a queue created at `initialPosition`.** Test directly in
  `QueueRecordings`.
- **Recovery after `trim-log`** is Aeron's behavior, not a documented contract. Pin it with a test that trims
  a log, restarts the member and asserts it recovers.

## Tests beyond the unit tests

- A member killed between a cut and its mark restarts from the previous set.
- Across a round, no `ClusterHeartbeat` gap nears the shortest consumer queue-silence watchdog.
- A joined replica that never confirms, one that confirms late, and a replayed `SNAPSHOT` action.
- Positions after a restart with another IPC term length (`QueueRecordings`).
- `CommittedEntryLossTest` and `FailoverPropertyTest` with snapshots and purging on.

## Risks

- **The files become the only copy of an application's state** once step 4 is live, since the queue below the
  cut is gone. Before it reaches production: a scripted peer-copy procedure and an alert on
  `seqeron_app_snapshot_round` lag.
- **Invariants change.** "Queues byte-identical, recorded from 1" no longer holds. `QueueRecordings`,
  `FirstFrameNotOne` as a fence, `overview.md`, `fault-tolerance.md` and CLAUDE.md change with it.
- **A replica that was down across a round is fenced** until a peer's file is copied in.
- **A missed mark** costs replay from the older snapshot at each start until a later round is safe.
