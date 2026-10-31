# Application snapshots

A seqeron client recovers by replaying the log, and replaying from `clusterSeqNo` 1 takes longer the longer
the cluster has run. Application snapshots bound that. The state worth snapshotting lives in the clients,
where an Aeron Cluster snapshot cannot reach it, so the clients take their own, and the log makes them
safe: the sequencer marks a cut, every instance of a participating source serializes its state at that cut
into a file of its own, and the one instance that may publish submits a digest of it through the log. The
state never crosses the cluster. The log carries the cut and the digest; every instance checks its own file
against the digest; and a restart restores from its newest file the log confirms, then replays only what
follows:

```
S(n) at R, then the frames after R on the queue
```

Nothing before `R` is replayed, and frames keep their absolute `clusterSeqNo`; only the starting point moves.
seqeron never interprets the application's part of a snapshot.

Both façades, `Application` and `Gateway`, take part, in every language. With a `<snapshots>` policy the cluster
takes snapshots of its own (§10), at the cut its applications' rounds share (§10.8), and no member keeps its queues
below the newest restart-safe cut; without one a member recovers by replaying its full log
([`fault-tolerance.md`](fault-tolerance.md) §0). "spec §n" and "spec A-n" refer to [`seqeron-protocol-spec.md`](seqeron-protocol-spec.md).

## Overview

| role | does |
| --- | --- |
| the sequencer | starts rounds: synthesizes `SnapshotStarted` (§3) |
| every instance holding a participating source's state | serializes its state at `R` into its own file (§4) |
| the instance that may publish at `R` | submits the round's `SnapshotEnd` (§4) |
| every instance that serialized the round | compares its own serialization with the sequenced `SnapshotEnd`, and keeps the file it confirms (§4, §5, §8) |
| a restarting instance | checks on a replay that its newest confirmed file's cut is in the log, restores from it and resumes at the frame after `R` (§7) |

A round:

```
operator / heartbeat    sequencer (log)              source instances
SnapshotRequested ───►  R: SnapshotStarted ───────►  all: serialize at R into a file
or interval elapsed     sequences the end      ◄──── publisher: SnapshotEnd
                        SnapshotEnd ──────────────►  all: compare (A-7), confirm the file
```

A snapshot is a sequence of records: the header the façade writes (§6), then the records the application
encodes. `crc32c` covers their bytes in order. No instance holds a snapshot as one buffer: serialization
streams the records into the file, and a restore streams them out of it (§7).

## 1. Enabling

Snapshots are enabled by the topology document (spec §6.4), never by a member's configuration. The
sequencer acts on the setting, so it must come from the log (spec S-3).

```xml
<topology>
  <gateways>
    <gateway name="GW-A" id="1" sourceId="0" rank="0" snapshot="true"/>
    <gateway name="GW-B" id="2" sourceId="0" rank="1" snapshot="true"/>
  </gateways>
  <applications>
    <application name="RefDataServer" sourceId="3" snapshot="true"/>
  </applications>
  <snapshots interval="3600"/>
</topology>
```

- **`<snapshots>` turns rounds on.** `interval` is in seconds; 0 starts rounds only on an operator's
  request, and `bytes` (§10.6), optional, adds a trigger on sequenced bytes. Without the element no round
  starts. The loader requires `snapshot="true"` on every row with it (§10.6), and publishes it after every
  other row.
- **`snapshot="true"` makes a source take part.** The loader requires the same value on every row of one
  gateway `sourceId`. The façades read it from their own row; the sequencer does not.
- **A façade without a `SnapshotListener` takes no part**, whatever its row says.

## 2. Frames

All are in `sbe-frame.xml` (schema 210), so changing them is a schema 210 change, made on a purged archive
(spec V-3).

**Synthesized**, as a template of its own (spec §2):

| event | template | `systemEventType` | block | fields |
| --- | --- | --- | --- | --- |
| `SnapshotStarted` | 107 | 30 | 52 | `round` int64 |

**Submitted**, in `UnsequencedSystem`:

| event | `systemEventType` | block | fields | submitted by |
| --- | --- | --- | --- | --- |
| `SnapshotRequested` | 26 | 8 | `correlationId` int64 | `clusterctl request-snapshot` |
| `SnapshotEnd` | 28 | 28 | `round` int64, `recordCount` int32, `length` int64, `crc32c` uint32, `formatVersion` uint32 | a participating source |
| `SnapshotPolicyRegistered` | 29 | 12 | `intervalSeconds` uint32, `bytes` int64 (§10.6) | `clusterctl load-topology` |

`GatewayRegistered` and `ApplicationRegistered` each carry a `snapshot` uint8. `systemEventType` 27 is
retired and never reused.

- **`recordCount`, `length` and `crc32c`** describe the snapshot's records, the header included. The
  records themselves stay with the instances that serialized them (§4.1).
- **A record is at most 65,535 bytes**, what its length prefix in the file can say (§4.1). A snapshot holds
  any number of records, so the state may be any size, bounded by the instance's disk.
- **`formatVersion`** is the application's, opaque to seqeron.
- **A snapshot belongs to the frame header's `sourceId`**: one per application or logical gateway per round.
- **`clusterctl snapshot`** is Aeron's cluster snapshot and stays refused; `request-snapshot` is the
  application kind.

## 3. Rounds

A round is a cut and nothing more. The sequencer starts it and tracks nothing about it afterwards; its
replicated state, derived from the log alone, is the policy from the latest `SnapshotPolicyRegistered`, the
last round number, and the timestamp of the last `SnapshotStarted`.

**Start.** A round starts with a synthesized `SnapshotStarted`, whose `clusterSeqNo` is the round's cut, `R`.
Rounds are numbered from 1, and start on either of two triggers:

1. **Operator.** A `SnapshotRequested` is sequenced, and `SnapshotStarted` follows at the next
   `clusterSeqNo`, as a `GatewayActive` follows a `GatewayActivationRequested`.
2. **Interval.** A `ClusterHeartbeat` whose timestamp is at least `interval` past the last
   `SnapshotStarted` (or past the policy row, before any round). The check runs after gateway promotion
   (spec §7.2).

A new round supersedes the previous one: a publisher that has not yet placed the previous round's end
abandons it (§4).

The sequencer decodes `SnapshotPolicyRegistered` alone, and recognises `SnapshotRequested` by its
`systemEventType`. It checks a `SnapshotEnd` only for format (spec §9.2 conditions 8 and 9) and never
rejects one for its round: a rejected frame fences its producer (spec A-4), and a late or superseded end
is harmless.

## 4. Taking a snapshot

Every instance holding a participating source's state — each replica of an application, the active and
hot-standby instances of a gateway pair — handles `SnapshotStarted` the same way:

1. **Serialize.** On dispatching `SnapshotStarted`, live or replayed, the façade writes its header (§6) as
   record 0, then pulls the listener's records: it calls `onSnapshot(buffer, recordIndex)` with one
   65,535-byte buffer, from index 0, until it returns 0, and each call encodes the next record of the state.
   Each record goes straight into the round's file (§4.1), and the façade keeps only their count, length
   and running CRC. The file also records the cut: `R`, the position of its `SnapshotStarted` in the
   recording, and that frame's consensus timestamp. The state is the state after every frame up to `R`, and every record is pulled before
   the frame after `R` is dispatched, so the listener needs no frozen copy. The façade keeps its cluster session alive
   after each record, so the session timeout does not bound how long this takes.
2. **Publish the end.** The instance that may publish at `R` submits the round's `SnapshotEnd` through
   `PendingSends`, like any frame it places: an `Application` whose gate is open, or the active `Gateway`
   once it has placed its `GatewayStarted`, which binds its session to the source. No other instance submits
   for that round.
3. **Lose the role, lose the end.** A publisher that loses the role before its end is placed does not place
   it: for an `Application`, a `LeadershipChanged` naming another member; for a `Gateway`, a
   `GatewayActive` naming another instance. Its source then has no end for the round. A `LeadershipChanged`
   naming the same member closes the gate only until the replica is caught up again; it then submits, after
   `PendingSends` has resent what the election lost (spec A-5).
4. **Compare.** On its source's `SnapshotEnd` for the round, every instance that serialized the round
   compares `recordCount`, `length` and `crc32c` with its own (§8). On a match the round's file is
   confirmed (§5). On the next `SnapshotStarted`, a publisher that has not yet placed the previous round's end
   abandons it.

The snapshot frames are the façade's; the listener sees none of them. All of this depends on every instance
of a source serializing the same state to the same bytes (§8).

### 4.1 The snapshot directory

Each instance has a directory of its own, the snapshot directory on either façade, which a `SnapshotListener`
requires. A restart restores from what it holds, so it must outlive the process, and no other instance may
write to it, the other instance of a gateway pair included. §11 proposes a snapshot server per host in its
place.

A round is written as `<round>.tmp`: the records, each a little-endian uint16 length followed by its
bytes, then a trailer:

| offset | field | type | value |
| --- | --- | --- | --- |
| 0 | `round` | int64 | |
| 8 | `length` | int64 | the records' bytes, as `SnapshotEnd.length` |
| 16 | `recordCount` | int32 | |
| 20 | `crc32c` | uint32 | |
| 24 | `formatVersion` | uint32 | the listener's |
| 28 | `cutClusterSeqNo` | int64 | `R` |
| 36 | `cutPosition` | int64 | where the round's `SnapshotStarted` starts in the writer's recording; a hint, the cut being named by `cutClusterSeqNo` (§10.8) |
| 44 | `cutTimestamp` | int64 | that frame's consensus timestamp |
| 52 | magic | uint32 | `0x32504E53`, "SNP2" |

- **Named only once confirmed.** When the instance confirms the round (§5), a thread of its own forces the
  `.tmp` to disk, renames it `<round>.snapshot` and forces the directory, so dispatch never waits for the
  disk. A `.snapshot` file is therefore confirmed and durable. The next round's write waits for the previous
  confirmation to finish. Windows opens no directory to force, and .NET forces none on any system; there the
  file system's journal commits the rename before the deletions that follow it. A write that fails is logged
  as `SnapshotStoreFailed` and leaves no file; the instance still compares the round, and carries on without
  a copy of it.
- **Retention.** An instance keeps two confirmed files, so a damaged newest one has an older one to give way
  to (§7). Once a confirmed file is durable under its name, every older file but the newest confirmed one is
  deleted, the `.tmp` of a round whose end never came and one a crash left behind included; a file that never
  becomes durable deletes nothing.
- **Files are portable.** Every instance of a source writes the same bytes for a round (A-6), so a file is
  not tied to the instance that wrote it: a peer's file copied into a new host's directory spares that host
  a full replay.

**Gateway instances.** Both instances of a gateway pair hold state: the active one serializes, writes and
publishes the end; the hot standby serializes, writes and compares, and takes over at once when
`GatewayActive` names it. An instance that is not running is not part of the pair until it restarts, restores
(§7) and catches up.

## 5. Confirming a snapshot

An instance confirms its own file. The queue carries only committed frames, so the source's sequenced
`SnapshotEnd`, once dispatched, is in the log for good: an instance whose serialization of the round matches
it (§4 step 4) confirms that round's file there and then, and has no one to ask later. Its file is the
round's records as every instance of the source wrote them (A-6), so the confirmation holds for a copy of it
on another host too (§4.1).

What a confirmation cannot prove is that the log the instance later restores against is the one the file
was cut from: a purged archive starts the log, and its rounds, again at 1. The restore therefore checks the
cut against the recording before it reads a record (§7 step 3).

Each participating instance exports the round it last confirmed as a counter, `seqeron_app_snapshot_round`,
0 until its process confirms one, so an instance that misses rounds is visible ([`ops.md`](ops.md)).

## 6. The snapshot header

A restore replays nothing before `R`, so whatever the façade derived from earlier frames must travel in the
snapshot. Record 0 is that header, written by the façade; the records after it are the application's. Its
layout is seqeron's, little-endian, and versioned independently of `formatVersion`:

| offset | field | type | value |
| --- | --- | --- | --- |
| 0 | `headerVersion` | uint16 | 2 |
| 2 | `headerLength` | uint32 | the header's bytes |
| 6 | `leadershipTermId` | int64 | from the last `LeadershipChanged` before `R` |
| 14 | `leaderMemberId` | int32 | that frame's `newLeaderMemberId` |
| 18 | `groupCount` | uint8 | the active groups before `R` (spec §7.4) |
| 19 | groups | 33 bytes each | `groupId` uint8, `groupName` char[32], in ascending `groupId` |

A `Gateway`'s header continues at *G* = 19 + 33 × `groupCount`:

| offset | field | type | value |
| --- | --- | --- | --- |
| *G* | `gatewaySourceId` | int32 | the logical gateway |
| *G* + 4 | `activeGatewayId` | int32 | the instance the current `GatewayActive` names, or −1 |
| *G* + 8 | `highestConnectionId` | int32 | the highest `connectionId` in the source's history |
| *G* + 12 | `rowCount` | uint16 | the rows that follow |
| *G* + 14 | rows | 37 bytes each | `gatewayId` int32, `preferenceRank` uint8, `gatewayName` char[32] |

- **The rows are every instance of the gateway**, in list order. Every instance writes the same header, so
  none is marked as its own; an instance finds its row by name, as it does in the list.
- **The length tells the two apart.** An application's header is *G* bytes and a gateway's is *G* + 14 +
  37 × `rowCount`. The header is one record, so a gateway with snapshots has at most 1,769 instances,
  fewer with more groups.
- **The groups are the producer's group directory** (spec A-9): a restore skips the `GroupsRegistered`,
  so the façade resolves its group names from them. Every cut follows activation, because the snapshot policy
  row follows the groups (spec §6.4), so `groupCount` is never 0. A version 1 file was cut
  from a log that predates groups, which the change to schema 210 purged (spec V-3), so its cut check
  fails (§7).
- **The header is part of the snapshot's bytes**, so `crc32c` covers it and A-6 applies to it.
- **Everything else is the application's records**: its `OutstandingWork` set and the keys it drops
  duplicates by (spec A-2, A-3), and a gateway's open connections and what it derived from their
  `connectionData`.

## 7. Restore

A cold start of an instance with a `SnapshotListener` restores in `ReplayerRecovery`, in every language:

1. **Choose.** Take the newest confirmed file in the snapshot directory. One with no trailer gives way to the
   next older; with none, recover from the queue's first frame, `groupSeqNo` 1.
2. **Check the format.** If the listener's `supportsFormatVersion` refuses the file's `formatVersion`, stop.
   By default it accepts only the build's own (§9).
3. **Check the cut.** Request a replay of the recording from the file's `cutPosition`. If its first frame is
   the frame the file names, `clusterSeqNo` `cutClusterSeqNo` stamped `cutTimestamp`, the file is this log's:
   close the replay and read it. If it is any other frame, or the Replayer refuses the position (spec R-2),
   the file was cut from another log, and an older one is tried. A recording that has not reached the
   position yet, as while its member replays the log after a restart, is answered `ReplayPending`, and the
   check waits.
4. **Read the records** twice, at most 1,024 a duty cycle, dispatching nothing; frames arriving on the live
   queue meanwhile are held, as in any recovery. The first read checks them against the trailer and passes
   none on: records that do not match it are a damaged file, and an older one is tried, before the listener
   has seen any of it. The second reads them in. Record 0 is the header: if its `headerVersion` is one this
   build does not support, stop. Otherwise it stands in for the frames before `R`. Its leader becomes the
   receiver's current leader and its term confirmed ingress's, as the last `LeadershipChanged` before `R`
   would have made them; the source takes part in rounds, as its topology row before `R` said; its
   groups resolve the producer's group names, as the `GroupsRegistered` before `R`
   would have; and a `Gateway`'s election state is set from it. Every later record goes to the listener's
   `onRestore`, with the file's `formatVersion`. Records that no longer match the trailer stop the
   instance, since the listener holds part of the state.
5. **Resume** at `cutPosition` with `clusterSeqNo` `R` as the anchor, through the ordinary resume path (spec
   R-1, `requestResume`). The `SnapshotStarted` frame is the anchor and is dropped as a duplicate, so
   dispatch starts at the frame after it on the queue, and the anchor's `groupSeqNo` is where the
   receiver's continuity resumes (spec F-6). The instance serialized nothing for the restored round, so
   that round's `SnapshotEnd` changes nothing.
6. **Go live**, switching to the queue as any resume does.

An instance that stops is fenced with `SNAPSHOT_UNRESTORABLE` ([`ops.md`](ops.md)). A damaged file that gave
way stays until the instance confirms its round again on the way back to the tail, which replaces it. After a
restore, every fallback that would walk the recording from its start resumes at the restored snapshot's
`cutPosition` instead, where the first frame is `R`, and drops every frame up to the last one dispatched.

A process with no topology row has no snapshot of its own. It may restore from source *X*'s files only if it
runs *X*'s state machine.

## 8. Obligations

The scheme is sound only if every instance of a source agrees. Two of spec §16's obligations say so:

- **A-6. Snapshot records are deterministic.** Every instance of a source serializes the same state at `R`
  to the same records, byte for byte. The rule on state is equivalence (§10.8); equal bytes are what the
  comparison in A-7 can check.
- **A-7. A divergent instance stops.** An instance whose own `recordCount`, `length` or `crc32c` for a round
  differs from its source's sequenced `SnapshotEnd` stops, fenced with `SNAPSHOT_DIVERGED`. The sequenced
  end is the reference: a publisher that diverged has broken A-6, and nothing recovers from that. Its file of
  the round is never confirmed, so never restored (§5).

An application with no state to keep returns 0 from `onSnapshot` at index 0, which gives a file of the header
alone. Every replica agrees on it, so A-7 cannot tell it from a correct snapshot: an application that does hold
state and does this restores without it. Whether the state is really empty is the application's to prove
before deployment, by restoring a second instance from a snapshot taken at a random point and comparing it with
the first after the rest of the frames.

## 9. Versioning

Every instance of a source runs the same build, and so writes the same `formatVersion`. A format change is
therefore a coordinated upgrade: every instance moves to the new build between two rounds, since one that
writes another format than its source's publisher is fenced at the next round whenever its bytes differ
(A-7).

The restore reads what the listener says it reads (§7 step 2). A build that changes its format keeps the
formats it replaces readable: its `supportsFormatVersion` accepts them, and `onRestore`, which is given the
file's `formatVersion`, decodes them. Its instances then restore the files the previous build confirmed, and
write the new format from their first round. A format the listener refuses stops the restore, so the old
formats stay readable until no instance holds a file of them.

Protocol upgrades that change a source's format while it runs are not designed yet; they will build on
snapshots, and the field and the stop are already in place, so that design needs no change to schema 210
(spec V-3). [`upgrades.md`](upgrades.md) proposes one.

## 10. Cluster snapshots

Application snapshots shorten a client's restart; cluster snapshots shorten a member's, and exist only while a
`<snapshots>` policy is loaded. All of this section is implemented but the audit tool (§10.8). A cluster snapshot
is also an application round (§10.8). §10.1 to §10.5 bound a member's restart,
its queue starting again at the cut; §10.6 purges the queue while the member runs, which does change what a
client can walk; §10.7 trims the Raft log, which Aeron does not do; §10.8 is the cut they share.

**What the member's replay is for.** It rebuilds two things: the `Sequencer`'s state, and the member's queue
recording, which the Replayer serves. An Aeron cluster snapshot replaces the first. The second is history,
which a snapshot cannot regenerate; what lies below the cut is in the applications' own snapshots (§10.8), so
the recording starts again at the cut rather than being kept.

### 10.1 Rules

- **Equal state, not equal bytes.** As in Aeron, replicas that applied the same frames hold equivalent state:
  every instance of an application, both instances of a gateway, every member's `Sequencer`. A snapshot of one
  restores another. What the instances share is the cut's `clusterSeqNo`, which every queue and every member
  carries; a recording position is a node's own address for it. Nothing relies on two nodes' positions
  agreeing, and the queue need not be byte-identical for a file to be valid (§10.8).
- **A queue starts at a cut.** A member's queue recording starts at the cut of the set it restores, or at
  `groupSeqNo` 1 with none. It is created at the position the marker started at when the node's settings are
  the ones the snapshot names, so its positions then agree with the recording it replaces and with every
  peer's, and a lookup by position finds the frame at once; otherwise it starts at position 0 and the cut is
  found by `clusterSeqNo` (§10.3).
- **Replicated state holds no recording id.** It does carry each queue's marker position and the publication's
  term length and MTU, which are the node's own; they only decide whether a restored queue keeps its positions
  (§10.3), and nothing else depends on them.
- **Each snapshot stands alone.** Which was taken first never matters; the one ordering is at start (§10.5).
- **A recording is valid only with the Raft log it came from.** `purgelog.sh` removes both or neither.
- **Snapshots are seldom.** Each is taken once `X` bytes have accumulated since the last, the policy's
  `bytes` (§10.6), sized from the measured replay rate so that replaying `X` takes no longer
  than the restart a deployment accepts; at a minute, that is hours of traffic. A restart then replays at
  most `X`, or `2X` after a fallback (§10.2), however long the cluster has run, and a cluster whose log has
  not reached `X` takes none.

### 10.2 The `Sequencer`

| | |
| --- | --- |
| **contents** | Every field of `Sequencer` the log sets. Sequencing: `clusterSeqNo`. Groups: each active group's name, the active group mask, each queue's last `groupSeqNo`, each registered source's group, and the topology session. Leadership: the latest `LeadershipChanged`. Queues: for each, the position its marker started at, and the publication's term length and MTU (§10.8). Gateways: the gateway rows, each logical gateway's designated instance, the designations to publish behind the current frame, the pending activations and their deadlines, the bound sessions, and the two bootstrap flags. Counters the members must agree on: `rejectedFrameCount`, `gatewayPromotionCount` and `gatewayPromotionFailedCount`. Rounds: the policy, the last round with the timestamp of its start, the bytes sequenced since it (§10.6), and an unanswered `SnapshotRequested`. Aeron's own snapshot carries the sessions and the heartbeat timer. |
| **format** | A schema of its own, Java only: a begin message with `formatVersion` and the scalars, one message per entry, maps in key order, and an end message with the counts. Members' snapshots at one log position are equivalent, not byte-equal: the queue positions and settings they carry are each node's own. A restored hash map's capacity, and so its iteration order, need not be the original's, so no frame may depend on a map's iteration order. |
| **test** | `SequencerSnapshotTest` snapshots a `Sequencer` at a random point of a seeded frame sequence, restores a second from it, and feeds both the rest: every frame they encode, and every field, must match. The sequence includes deposed sessions, pending activations and requested rounds, so a field the snapshot misses fails it. |
| **contract** | `SequencerSnapshot`, a package-private interface in `sequencer` that `Sequencer` implements: `formatVersion`, `onSnapshot(buffer, recordIndex)` and `onRestore(buffer, length, recordIndex, formatVersion)`, the shape of `SnapshotListener`. `SequencerService` drives it from `onTakeSnapshot` and `onStart`. `onTakeSnapshot` emits the round's marker, then writes the state: the snapshot is the state after the marker (§10.8). |
| **when** | The `Sequencer` reports a round due (§10.6), from state every member holds, so a new leader carries on where the last one stopped, and the leader's service sets the `ClusterControl` toggle to `SNAPSHOT`. It asks only while `cluster.role()` is `LEADER`: the consensus module reads the toggle only on the leader and outside an election, so a request set elsewhere would wait and take an extra snapshot once that member leads. The toggle switches only from `NEUTRAL`, by compare-and-set, so a request that meets an operator's `clusterctl snapshot`, or a snapshot in progress, fails and is retried at the next check. `clusterctl snapshot` is allowed, and `clusterctl shutdown` uses `SHUTDOWN`. |
| **restore** | `onStart` reads the records through a fragment assembler, creates the queues at their cut (§10.3), and refuses a foreign schema, a newer version or a count that differs from the end's, as it refuses any snapshot today. |
| **version** | Aeron's `appVersion`, on the consensus module's context and the service container's, has `LOG_VERSION` as its major: a seqeron constant raised by every change spec V-3 makes a purge, and by a snapshot format the next release cannot read. Schema 210's own `version` stays 0 (spec §8), so it cannot serve. Aeron compares majors when it loads a snapshot and when it replays a leadership term from the log, so a member started on an archive V-3 says to purge stops with "incompatible version" rather than sequence another history. `LOG_VERSION` starts at 1, and seqeron's validator also accepts Aeron's default major 0, which every earlier log carries, until the first raise, so the release that introduces it needs no purge. |
| **the Raft log** | Holds the two newest restart-safe snapshots (§10.8) and the log from the older one, so the newer always has a fallback. The log below the older is trimmed by `clusterctl trim-log` (§10.7). |
| **fallback** | A snapshot `onStart` refuses stops the member's start, and Aeron loads no older one by itself. On each member it stops, the operator runs `clusterctl invalidate-latest-snapshot` (`ClusterTool`'s), and the member restores the older snapshot and replays the log from it: at most `2X`. A snapshot that is not restart-safe is invalidated the same way, by the launcher at start (§10.8). A release reads the snapshot format of the release before it; one that cannot raises `LOG_VERSION` and is deployed as a seqeron protocol change is, on a purged archive (spec V-3). |

### 10.3 The queue

A queue's recording is not one run from `groupSeqNo` 1 across every restart (§10.8). It starts at a cut, and a
restart starts it again at the cut of the set it restores. Nothing of the old recording is trusted or
extended, so its tail is not verified.

| | |
| --- | --- |
| **genesis** | With no snapshot, a new recording from `groupSeqNo` 1, as today. |
| **restore** | With the term length and `mtu` the snapshot names equal to the node's, each queue publication is created at the position its marker started at, which the `Sequencer`'s snapshot carries, with `ChannelUriStringBuilder.initialPosition(position, initialTermId, termLength)`. The archive records from there, so positions agree with the recording before and with the peers'. With either differing, the publication is created at position 0 and the member's positions are its own; nothing is refused, because a file names its cut by `clusterSeqNo` (§10.8). |
| **finding a cut** | The Replayer is asked for a cut by its `clusterSeqNo`, with the file's position as a hint (§10.8). It keeps each cut it has recorded, with its position (§10.6), and answers from that. A cut below the recording's first frame is refused (R-2), one above its tip is `ReplayPending`, and one between that is not kept, as after a Replayer restart, is found by reading the recording for the marker, which §10.6 bounds, and kept. |
| **first frame** | The marker, re-encoded from the round's state, in place of the `LeadershipChanged` that activation offers first today. |
| **old recording** | Purged with `purgeRecording` once the new one is recording, so the Replayer finds one recording per queue. |
| **emit** | The log replays from the set's position and every frame after the marker is offered, as live. Nothing is held back: the recording is new, and what it records again is what the old one held above the cut, frame for frame. |
| **fallback** | An older set, after `invalidate-latest-snapshot` (§10.2), restores the same way at its own cut and replays forward: at most `2X`. |
| **live** | As today: a recording that stops exits the member with 70 (`QueuePublisher`). |
| **faults** | A full disk or an archive error while the set is written terminates the member, as Aeron does for its own services (`ClusteredServiceAgent.onTakeSnapshot`, `ArchiveException.STORAGE_SPACE`). The restart restores the last good set. |
| **a gateway host** | Its relay starts at the member's recording start (§10.6) and records into a new local recording, as a member's restart does. |

The decision is a pure class over whether a snapshot exists and whether the term length and MTU it names are the
node's: genesis, restore at the marker's position, or restore at 0. It is unit-tested as `QueueRelay` is.

### 10.4 The Replayer

The Replayer holds no replicated state, so the cluster snapshot has nothing of it, and it keeps no index of
snapshots: the recording is bounded at the cut (§10.3) and purged (§10.6), so nothing of it grows with uptime.

### 10.5 A member's start

0. **Launcher.** Before the consensus module starts, invalidate the newest snapshot that is not restart-safe
   (§10.8).
1. **Sequencer.** `onStart` restores the snapshot, which gives the cut and each queue's marker position, term
   length and MTU; with none, the start is genesis.
2. **Queue.** Create each queue publication at its cut and purge the previous recording (§10.3); at genesis,
   record from `groupSeqNo` 1. Settings that differ from the snapshot's start the recording at position 0.
3. **Sequencer.** The log replays from the snapshot, every frame after the marker offered, and sequencing goes
   live.
4. **Replayer.** Ready once each queue is recording and the old recording is purged.

Step 4 waits for step 2 so that the Replayer never serves the recording being replaced.

### 10.6 Purging history

A restart bounds a recording at the cut (§10.3); between restarts it grows with traffic. Purging bounds it
while the member runs. A recording is a run of segment files from a start position: `purgeSegments` drops whole
files from the front, and every position above stays where it is, so a file's `cutPosition` (§4.1), a client's
resume and `QueueRelay`'s resume keep working. A client reads history from `groupSeqNo` 1 only when it walks,
and it walks only with no usable snapshot: in a young log, before its first confirmed round. Once every
instance holds a confirmed file, history below the oldest cut a file may name is never read.

**Policy.** `SnapshotPolicyRegistered` gains one field, set by `<snapshots>` (§1):

```xml
<snapshots interval="3600" bytes="1073741824"/>
```

- **`bytes`** makes a round due once that many bytes have been sequenced since the last `SnapshotStarted`, as
  `interval` does once that much time has passed; whichever comes first (§3). The count is every sequenced
  frame's length, each frame once, so it bounds every queue. It is replicated state, and the `Sequencer`'s
  snapshot carries it (§10.2). 0 turns the trigger off.
- A round is an Aeron snapshot (§10.8). The `Sequencer` reports one due on `interval`, on `bytes` or on an
  unanswered `SnapshotRequested`, and the leader's service sets the `ClusterControl` toggle (§10.2).

A recording then holds at most about `bytes` plus one round's wait, plus one segment, at any traffic rate.

**Purging.** Each node's Replayer purges its own queue recordings, on a member and on a gateway host alike:

- **It learns each cut as it is recorded**, with its position: on a member from `SequencerService`, which
  emits the marker, and on a gateway host from each relay, once it has published the marker. After a restart it starts again from the cut it
  restored at.
- **It purges whole segments** wholly below its floor, rounded down to a segment boundary
  (`purgeSegments` takes nothing else). The archive refuses a purge below a segment a replay session is
  reading, and a second delete on a recording while one runs; both are an error response, which the Replayer
  retries at its next duty cycle and does not treat as a fault. On a member and on a gateway host alike the
  floor is the newest restart-safe cut (§10.8): every instance on the node that joined the round has confirmed
  its file for it. A host's instances are the gateways and applications that read its recordings.
- **One round per `clusterSeqNo`.** A host has one relay per relayed group, and each sees the round's marker
  at its own time. The first relay to report the cut opens the round; the others' reports only record the
  cut's position in their group. When the round is restart-safe, each group's floor is that cut's position in
  its own recording, looked up by `clusterSeqNo` (§10.3). A group whose relay has not reached the marker gets
  no floor that round. A relay's report opens a round only when the relay is live: a marker it replays while
  catching up to the member's recorded position, as after a host restart, is only recorded, so the host never
  opens a round for a cut its instances may have confirmed in an earlier run.
- **A gateway host's relay** holds the purge of the member it reads, so only a relay disconnected from every member
  while they purged past it is left behind. It stops with a fatal rather than cycling through the members, and its
  restart relays from a member's recording start into a new recording (§10.3).
- **Nodes need not agree.** No client starts at the purge point: one restores at its file's cut, which is the
  same frame on every node, so each node may keep more than another.
- **The Raft log** below the older of the two cluster snapshots kept (§10.2) is trimmed by `clusterctl
  trim-log` (§10.7).

What changes for clients:

- **A file whose cut is still retained restores as §7 describes.** Nothing changes.
- **A file older than the retained history** names a `cutPosition` below the recording's start, which the
  Replayer refuses (spec R-2), and an older file is tried, as for a file from another log.
- **A walk on a truncated recording is fatal.** With no usable file the client walks, and the first frame
  is not `groupSeqNo` 1 (`FirstFrameNotOne`). The client is fenced until a peer's file is copied into its
  directory (§4.1), as it is today on a recording that does not reach the start of the queue.
- **Every application takes part.** With `<snapshots>` present, the loader requires `snapshot="true"` on
  every row, gateways and applications: a restart discards the queue below the cut. An application whose only
  output leaves the cluster is no exception: it has a row, and its instance that may publish submits the
  round's `SnapshotEnd`, the one frame it sends. A process with no row has no snapshot, and is fenced once its
  node's recording starts at a cut.

What changes on the nodes:

- **`ReplayerService.checkReady`** serves a recording whatever its first frame, since a member's restart
  starts its recording at a cut (§10.3) rather than at `groupSeqNo` 1.
- **A gateway host's relay** starts from the member's recording start rather than `groupSeqNo` 1, and
  replays from that start where it does today. Its own recording then starts where the member's does, and its
  Replayer purges it the same way.

A new member or a lost disk takes the cluster snapshot and the Raft log from Aeron's `ClusterBackup`, which
replicates both from a running cluster, restores at the cut and records its queues from there (§10.3). It
copies no queue, which is not in the cluster's recording log and needs none; the clients' snapshot files come
from a peer (§4.1).

**Tests.** `SequencerTest` covers the byte trigger beside the interval. `ReplayerRecoveryTest`, in every
language and the same order, covers a file whose cut is below the recording's start giving way to an older
one, and a file whose cut is at another position in this recording than its `cutPosition` restoring by
`cutClusterSeqNo` (§10.8). `QueueRelay` covers a start at a member's recording start above `groupSeqNo` 1, and a
resume on a member whose positions differ, which republishes no frame twice and misses none. The restart-safe
class (§10.8) covers a source that never joins, one that joins and never confirms, one that confirms late, and a
replayed action. The queue
decision class (§10.3) covers genesis, a restore at the marker's position and a restore at 0 on differing term
length or MTU. The Replayer's cut lookup covers a cut it has kept, one below the recording, one above its tip
and one it must read for. In the integration suite, `ClusterSnapshotTest` restarts a member from its restart-safe
snapshot with its queue at the cut, one with no safe snapshot from its whole log, one with another IPC term length at
position 0, and one whose log was trimmed; it asserts that no `ClusterHeartbeat` gap across a round nears the
shortest queue-silence watchdog, and covers a gateway's confirmations holding or releasing the cut, a gateway host's
relay, purge and restart, and a relay left behind by members purging past it. `QueueRecordings` reads each archive's
recordings directly, so a gap or repeat a client would hide fails the test. `FailoverPropertyTest` does not yet run
with snapshots and purging on, and no test kills a member between a round's cut and its mark.

### 10.7 Trimming the Raft log

Open-source Aeron trims nothing: a member's log recording grows from its first term, however many snapshots
it takes. Aeron's retention policy, `RetainEnoughDataForRecoveryPolicy`, belongs to the Data Retention
Regulator, a premium product, and `ClusterTool`'s `seed-recording-log-from-snapshot` rewrites
`recording.log` but deletes no data. seqeron therefore has a procedure of its own: `clusterctl trim-log`, run on
each member. It does what that policy does, with the count fixed at two (§10.2):

- **Find the floor.** Read `recording.log` (`RecordingLog`, read-only while the member runs) for its two
  newest valid snapshots that `restart-safe.log` names (§10.8), and take the older one's log position.
- **Purge the log below it.** `purgeSegments` on each log recording the recording log names, at the segment
  boundary at or below that position, through the member archive's local control. The archive refuses a
  position below the end of the recording's first segment, and one a replay is reading; a refusal leaves the
  log as it was, to be trimmed by the next run.
- **Purge older snapshots.** `purgeRecording` on every snapshot recording, the consensus module's and the
  service's, older than the two.
- **Per member, and safe to repeat.** Each member trims its own archive; a run that fails leaves more than
  it needs, never less.
- **The trim is in the archive's catalog.** A purged segment moves the first term's recording start above 0,
  and a start reads that instead of a record of its own, so a start never falls back to a replay of a log that no
  longer begins at its first term (§10.8), and a rebuilt member or a lost `restart-safe.log` cannot lose the fence.
- **`--dry-run`** prints each recording `trim-log` would purge, and for a log recording the position it would purge
  below, and purges nothing. It computes them as a run does. An operator who wants the history first copies it
  from the archive directory, whose rolled segment files do not change.
- **Recovery needs a snapshot.** With a snapshot, Aeron's recovery plan starts the log at the snapshot's
  position and reads nothing below it. With none, it starts at the recording's start, which a trim has moved,
  and would replay a partial log. A trimmed member therefore never runs without a snapshot (§10.8).
- **A lagging member can outlive the log it needs.** A follower that was down past the trim horizon needs log
  its leader may have purged, which neither the spec nor Aeron's sources settle; the integration suite
  covers it. Such a member is rebuilt from `ClusterBackup`, which keeps what the members trim.
- **Not the queue.** Its history is §10.6's.

### 10.8 One cut for the cluster and the clients

§10.3 and §10.6 are written to it. The cluster snapshot and an application round
are the same event, so that nothing on a queue needs saving. The state of the `Sequencer`, of every
application and of every gateway is snapshotted; the queues are not.

**The cut.** A round is Aeron's `SNAPSHOT` cluster action, not a frame the sequencer chooses a moment for.
Inside `SequencerService.onTakeSnapshot`:

1. The `Sequencer` synthesizes `SnapshotStarted` (`R`) on every queue.
2. The service writes the `Sequencer`'s state (§10.2) to the snapshot publication and returns, which acks.

Nothing waits for an application. While the consensus module is in `State.SNAPSHOT` it polls no ingress,
timers or service messages (`ConsensusModuleAgent.consensusWork`), so producers are held back for as long as the
`Sequencer` takes to write its state. The applications serialize as §4 says, each on dispatching `R`, while
the queue and its recording carry the frames after it: an application that falls behind is dropped from the
live feed and heals by replay (§4, §12).

**Restart-safe.** The cluster snapshot is in `recording.log` once the service acks, before any application
has a file for `R`. A member restarting from it starts its queue at `R` (§10.3), so a co-located application
whose newest file is older is below the recording's start and fenced (spec R-2, §10.6). A cut is therefore
restarted from only once it is *restart-safe* on that member: every co-located application that joined the
round has confirmed its file for it, durable under its name (§4.1). A file that is only written is not enough,
since a restore takes confirmed files alone (§7 step 1).

**Who is expected.** The sources with an instance on the node that join the round: the co-located
applications, one replica per member by definition, and the gateways running there. A node cannot see an
instance on another node, so it expects none of them; an instance with no file that reaches the cut is fenced
until a peer's file is copied in (§4.1). Only the instances running are expected (below).

**A gateway pair's instances never share a node**, member or host. The two instances of a gateway have one
`sourceId` (§1), so a node holding both would count them as one and the first confirmation would make the
round restart-safe. The signals carry each sender's `clientId`, and the Replayer records it: a second
`clientId` joining for one `sourceId` in a round leaves that round unsafe, and the Replayer logs it. A node
that breaks the rule never marks a cut restart-safe and never purges, and fences no one. An instance restarted
mid-round joins under a new `clientId` and leaves its round unsafe the same way; the next round is judged
afresh.

**The signals.** Three messages are added to `sbe-replay.xml`, generated in every language. That schema is
node-local and its two sides are built and restarted together, so adding them is compatible. Nothing blocks on
any of them:

| message | from → to | stream | sent |
| --- | --- | --- | --- |
| `SnapshotRound` (the round's `clusterSeqNo`) | `ReplayerService` → clients | control (203) | when `SequencerService` hands it a cut |
| `SnapshotJoined` (`clientId`, `sourceId`, the round) | an application or gateway → `ReplayerService` | request (202) | on receiving `SnapshotRound` |
| `SnapshotConfirmed` (the same fields) | an application or gateway → `ReplayerService` | request (202) | when its file for the round is durable under its name |

Presence is asked afresh each round. The expected sources are those that join within
`replayer.snapshotJoinMs`, 5 s by default on a member and 10 s on a gateway host. `SnapshotEnd` is not
the signal: it says the round's bytes agree, and the Replayer cannot see that this member's replica holds a
file (§5).

`SequencerService` hands the Replayer each cut it emits, in-process, which it already imports: the round, its
`clusterSeqNo`, the marker's position and the snapshot's log position. A pure class (the expected sources,
who has joined, who has confirmed, and the join window) decides when the cut is restart-safe, unit-tested as
`QueueRelay` is. It then appends the snapshot's log position to `restart-safe.log` in the member's cluster
directory, forced to disk, and the Replayer's purge floor moves to the cut (§10.6). A restart of the member or
of the Replayer drops a round in flight, which stays unsafe.

**At start.** Before the consensus module starts, the launcher reads `recording.log` and `restart-safe.log` and
invalidates the newest snapshot, as `clusterctl invalidate-latest-snapshot` does (`ClusterTool`'s, run on the
stopped member's cluster directory), until the newest is one the file names. The member then restores it and
replays the log from it (§10.5). It falls back to no snapshot, a replay of the whole log, only while
the log still begins at its first term, which the archive's catalog says: the first term's recording starts at
position 0 until `trim-log` purges a segment (§10.7). Otherwise the member does not start.

**A replica that is down.** It joins nothing and is not expected, so the cut is restart-safe without it and
the member's history is not held for it. The cost falls on the replica. The member's recording now starts at
the cut, so when the replica returns its newest file is cut below the recording's start, the Replayer refuses
it (spec R-2), the walk finds a first frame that is not `groupSeqNo` 1, and it is fenced until a peer's file is
copied in (§4.1, §10.6). That includes a replica that was down only across the round. The peers' replicas ran
the same state machine and confirmed the round, so a file exists; copying it is the operator's step until the
server of §11 fetches it.

**A replica that joined and does not confirm** leaves the round unsafe: a replica that is slow makes it safe
when it confirms, one that stops or misses the round (§12) never does. The member keeps its previous
restart-safe cut, the history from it and the sets it had, and the next round is tracked afresh. Producers are
not held either way.

**Replay.** A restart replays a `SNAPSHOT` action it meets in the log. The service container runs
`executeAction` for it without checking that the member is live (`ClusteredServiceAgent.onServiceAction`), so
`onTakeSnapshot` is called again. The marker is then a transition of the `Sequencer` like any other and is
numbered the same on every member; it is offered only above the tip (§10.3). The replay also takes a real
cluster snapshot: the consensus module enters `State.SNAPSHOT` on a replayed action
(`onReplayClusterAction`), waits for the service's ack and appends a snapshot to `recording.log` at the same
log position. The cut is not handed to the Replayer: whether it was restart-safe was decided in the run that
took it, and the next round is tracked afresh. So a cut that `restart-safe.log` names stays restart-safe
when it is replayed, and one it does not name is invalidated again at the next start, until a later round
is. Each such start replays from the older snapshot, so a missed mark costs replay until the next safe
round.

**What each part keeps**

| | |
| --- | --- |
| `Sequencer` | Aeron's snapshot, as §10.2. |
| applications, gateways | Their own files (§4.1), cut at `R`. Every source takes part, as in §10.6. |
| member | `restart-safe.log`: the log positions of the snapshots whose cut was restart-safe. Local to the member, in no replicated state. |
| Replayer | The round in flight, in memory. |
| queue | Nothing. A restored member's recording starts at `R`; running members purge below their floor (§10.6). |

**History.** The queue is bounded by the cut, so history is the Raft log: every ingress frame with its
consensus timestamp. `ClusterBackup` replicates it with the snapshots into the backup's own archive, which
keeps what the members trim (§10.7). A queue frame is a deterministic function of the log, so a frame-level
audit replays the log through a `Sequencer`; that tool does not exist, and its output depends on the
`Sequencer` version that read the log.

**Consequences**

- **The files are the only copy of an application's state.** A damaged or lost file with no peer copy
  (§4.1) cannot be rebuilt from the queue, which is not there to walk. A source with no file starts from a
  peer's, and only if it runs the same state machine.
- **Sequencing stops for the `Sequencer`'s write alone.** No frame, and no `ClusterHeartbeat`, which is a
  cluster timer and is not polled in `State.SNAPSHOT`. The write is bounded by the `Sequencer`'s state, not by
  any application, so the gap stays far below the shortest consumer queue-silence watchdog (`FrameLayer`);
  the integration suite asserts it across a round.
- **A crash between a cut and its mark costs the round.** The member restarts from the previous restart-safe
  set and replays the log since it: at most `2X` (§10.1).
- **Aeron tolerates the hold.** The leader keeps sending its position (`updateLeaderPosition` runs in every
  state), so a follower's `leaderHeartbeatTimeoutNs` (default 10 s) does not fire, and session and quorum
  checks run only in `ACTIVE` and end with `updateTimeOfLastActivity`, so a hold does not time a session out.
  Nothing in `onTakeSnapshot` depends on how Aeron treats a service that throws or runs long.

**Equivalent state.** The rule is Aeron's: instances that applied the same frames up to `R` hold equivalent
state, so any instance's file restores any other. It holds for seqeron's replicas under these conditions:

- **Inputs are the frames.** An instance is a function of the frames of its queues and the cluster time they
  carry. A multi-group application's state at `R` is every frame of every queue it reads below `R`'s
  `clusterSeqNo`, the marker being on all of them.
- **A file holds no role.** Whether the instance may publish, whether it is caught up, what `PendingSends` holds
  and a gateway's sockets belong to one instance and are in no file. A gateway's file holds what the frames
  derive (§6), which is why a standby's and an active's agree.
- **Divergence is detected by the CRC check, kept as it is.** A-6 and A-7 (§8) compare each instance's
  `recordCount`, `length` and `crc32c` with the sequenced `SnapshotEnd`. The serialization is written for the
  file anyway and the CRC accumulates as its records go out, so the check costs one hardware `crc32c` over
  bytes already in hand. An application-supplied digest would be a second function in every application and
  language, and buys only tolerance for a serialization that is not canonical, which A-6 rules out. Equal
  bytes are sufficient for equal state, not necessary; the check is stricter than the rule and fails safe.
- **Only a confirmed file is a peer's to copy** (§5), so an instance that diverged cannot pass its state on.

**Positions.** A file names its cut by `cutClusterSeqNo` (§4.1), which every queue and every member shares. Its
`cutPosition` is a position in the recording its writer had, a hint to the node that reads it:

- The `Sequencer` snapshot carries, per queue, the position the marker started at, and the queue publication's
  term length and MTU. They decide whether the restored recording can keep the positions it had. A frame's
  position follows from the frames before it, but only at equal term length and MTU (CLAUDE.md), so a node
  whose settings now differ is not refused; its recording starts at position 0.
- With equal settings each queue publication is created at the marker's position with
  `ChannelUriStringBuilder.initialPosition(position, initialTermId, termLength)`, as Aeron does for its own
  replay publications (`ArchiveConductor`), and a recording of it starts there: the catalog keeps a
  `startPosition` per recording, and a replay from the start position of one is ordinary. Positions then agree
  with the recording before and with the peers', which is what makes the hint right and the lookup free.
- `ReplayRequest` gains `cutClusterSeqNo`, set by a restore and absent otherwise. The Replayer finds the
  frame as §10.3 says and replays from it; `fromPosition` is the hint it tries first. The restoring client
  takes the position of the first frame it reads as its anchor, so §7 step 3 verifies what it received and
  step 5 and the paragraph after it resume at that position, not at the file's. It is the only change to §7.
- A gateway host's relay already resumes on another member at the same position and falls back to that
  recording's start when the frame there is not the next one (CLAUDE.md). Positions that differ make the
  fallback the common case: a longer replay, bounded by §10.6, and no republished or missed frame, since it
  republishes only the frame after the last one it published.
- Its first frame is the marker, re-encoded from the round's state. That replaces the `LeadershipChanged`
  that activation offers first today; a restoring client takes its leader and term from its file's header
  (§7 step 4) and gets the next `LeadershipChanged` from the replay.
- `ReplayerService.checkReady` serves a recording whatever its first frame, as in §10.6.

## 11. The snapshot server

Proposed, not implemented: a snapshot server per host in place of each instance's directory (§4.1), so that a damaged file falls back to an older round and a host with no file fetches one from a peer. The design is in [`snapshot-server.md`](snapshot-server.md). It was written for an earlier confirmation scheme and is to be revised before it is built.

## 12. Costs and limits

- **Log volume** is two frames per participating source per round, `SnapshotStarted` and `SnapshotEnd`,
  whatever the state's size.
- **Dispatch pauses** while serialization runs (§4 step 1): the listener's encoding, the CRC and the write
  into the page cache. A host that cannot hold the file's dirty pages writes at disk speed. The cluster
  session stays alive meanwhile, but the queue does not wait, so a pause long enough to drop the subscriber
  heals through replay.
- **Disk** holds up to four files per serializing instance: its two confirmed rounds, the round being
  written, and one whose end never came, until the next confirmation deletes it.
- **A crash costs at most a round.** A file is confirmed only when the instance dispatches its round's end, so
  one written by an instance that stopped before then is never restored, and the restart restores the round
  before it.
- **A round can be missed.** A source whose publisher changes before its end is placed has no end for that
  round, and its instances keep their previous confirmed file. So does one whose cut falls after a
  leadership change but before the new leader's replica has caught up and reopened its gate, or after a
  `GatewayActive` but before the instance it names has placed its `GatewayStarted`.
- **Catching up costs files.** A replica catching up through old rounds serializes and writes a file at
  each `SnapshotStarted` it passes.
- **No file means a full replay**: a new host, a lost disk, a purged log (§7 step 3).
  A peer's file spares it that (§4.1). The server proposed in §11 fetches one.
- **Durability is paid off the dispatch thread.** Forcing a file to disk takes milliseconds on local SSDs
  and up to about a hundred on network block storage, on the store's own thread. An OS crash before the
  newest file is durable leaves the previous confirmed one, which is deleted only after it.
- **A restore costs a replay round trip** before its first record, the check of the cut (§7 step 3), and
  reads the file twice (§7 step 4). Each record read counts as the duty cycle's work, so an idle strategy does
  not back off mid-file: the restore holds its thread until the last record.

Under §10.8 these change, and add:

- **Producers wait for the `Sequencer`'s write alone,** not for an application: no frame, and no
  `ClusterHeartbeat`, while it is written. The dispatch pause above remains per instance, with the queue
  absorbing what arrives behind it.
- **A round can be unsafe** (§10.8): a joined replica that never confirms leaves the member on its previous
  restart-safe cut, with the history from it and the sets it had. A crash between a cut and its mark costs the
  round the same way.
- **The queue is bounded by the cut:** a member's recording holds the frames since the last restart-safe set,
  not the uptime. The Raft log grows as before until `trim-log` (§10.7), and `ClusterBackup` keeps it for audit.
- **A member's restart is bounded** by the log since its last restart-safe set, and a client's by the same, since the
  recording starts at the cut instead of `groupSeqNo` 1.
- **The files are the only copy of an application's state,** and "no file" is a fence, not a full replay:
  there is no queue to walk. A replica that was down across a round needs a peer's file (§10.8).
- **Log volume is unchanged:** `SnapshotRound`, `SnapshotJoined` and `SnapshotConfirmed` are node-local and
  never sequenced.

## 13. Tests

Unit tests, in every language that has the code:

| what | tests |
| --- | --- |
| the header, the CRC | `SnapshotFormatTest` |
| rounds and their triggers | `SequencerTest`, `ConformanceTest` |
| the topology document's rules and publish order | `TopologyDocumentTest` |
| the file: its trailer and cut, confirmation, a torn or damaged one, retention; a golden trailer shared across the languages | `SnapshotStoreTest` |
| serialization, the end, abandoning, the comparison and confirmation | `SnapshotTakerTest` |
| a gateway's election state in the header | `GatewayLifecycleTest` |
| the restore, the check of its cut, its fallbacks to older files and to a walk | `ReplayerRecoveryTest`, case for case across the languages |
| restored state plus the tail equals a full replay, under random replay faults and a file of another log | `ReplayerRecoveryPropertyTest` |
| a restore against a recording whose positions differ from the file writer's: the same state as at equal positions | `ReplayerRecoveryPropertyTest`, its model archive drawing a position offset |

`SnapshotTest` runs the `TestGateway` pair and a `TestApplication` replica per member on a three-member
cluster, each instance with its own directory kept across its restarts. It drives the pair through
restores, a failover onto a restored instance, and the killed instance's return, restoring from the file it
confirmed while it served; then a round started by `clusterctl request-snapshot`, a follower's replica restarting, and
a leader kill, after which the new leader's replica publishes rounds and the killed member's clients
restore. It checks each restored state against the client traffic, and every later round against it (A-7).

Not covered: a publisher change mid-round, which the property test does not inject, and an OS crash tearing
a file, whose file side `SnapshotStoreTest` covers.
