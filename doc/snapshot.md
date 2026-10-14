# Application snapshots

A seqeron client recovers by replaying the log, and replaying from `globalSeqNo` 1 takes longer the longer
the cluster has run. Application snapshots bound that. The state worth snapshotting lives in the clients,
where an Aeron Cluster snapshot cannot reach it, so the clients take their own, and the log makes them
safe: the sequencer marks a cut, every instance of a participating source serializes its state at that cut
into a file of its own, and the one instance that may publish submits a digest of it through the log. The
state never crosses the cluster. The log carries the cut and the digest; every instance checks its own file
against the digest; and a restart restores from its newest file the log confirms, then replays only what
follows:

```
S(n) at R, then frames R+1, R+2, …
```

Nothing before `R` is replayed, and frames keep their absolute `globalSeqNo`; only the starting point moves.
seqeron never interprets the application's part of a snapshot.

Both façades, `Application` and `Gateway`, take part, in every language. The cluster takes no snapshots of
its own (§10): a member still recovers by replaying its full log ([`fault-tolerance.md`](fault-tolerance.md)
§0). "spec §n" and "spec A-n" refer to [`seqeron-protocol-spec.md`](seqeron-protocol-spec.md).

## Overview

| role | does |
| --- | --- |
| the sequencer | starts rounds: synthesizes `SnapshotStarted` (§3) |
| every instance holding a participating source's state | serializes its state at `R` into its own file (§4) |
| the instance that may publish at `R` | submits the round's `SnapshotEnd` (§4) |
| every instance that serialized the round | compares its own serialization with the sequenced `SnapshotEnd`, and keeps the file it confirms (§4, §5, §8) |
| a restarting instance | checks on a replay that its newest confirmed file's cut is in the log, restores from it and resumes at `R + 1` (§7) |

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
  request. Without the element no round starts. The loader requires at least one participating row with it,
  and publishes it after every other row.
- **`snapshot="true"` makes a source take part.** The loader requires the same value on every row of one
  gateway `sourceId`. The façades read it from their own row; the sequencer does not.
- **A façade without a `SnapshotListener` takes no part**, whatever its row says.

## 2. Frames

All are in `sbe-frame.xml` (schema 210), so changing them is a schema 210 change, made on a purged archive
(spec V-3).

**Synthesized**, as a template of its own (spec §2):

| event | template | `systemEventType` | block | fields |
| --- | --- | --- | --- | --- |
| `SnapshotStarted` | 107 | 30 | 42 | `round` int64 |

**Submitted**, in `UnsequencedSystem`:

| event | `systemEventType` | block | fields | submitted by |
| --- | --- | --- | --- | --- |
| `SnapshotRequested` | 26 | 8 | `correlationId` int64 | `clusterctl request-snapshot` |
| `SnapshotEnd` | 28 | 28 | `round` int64, `recordCount` int32, `length` int64, `crc32c` uint32, `formatVersion` uint32 | a participating source |
| `SnapshotPolicyRegistered` | 29 | 4 | `intervalSeconds` uint32 | `clusterctl load-topology` |

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

**Start.** A round starts with a synthesized `SnapshotStarted`, whose `globalSeqNo` is the round's cut, `R`.
Rounds are numbered from 1, and start on either of two triggers:

1. **Operator.** A `SnapshotRequested` is sequenced, and `SnapshotStarted` follows at the next
   `globalSeqNo`, as a `GatewayActive` follows a `GatewayActivationRequested`.
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
   `R + 1` is dispatched, so the listener needs no frozen copy. The façade keeps its cluster session alive
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

Each instance has a directory of its own, `snapshotDirectory` on either façade, which a `SnapshotListener`
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
| 28 | `cutGlobalSeqNo` | int64 | `R` |
| 36 | `cutPosition` | int64 | where the round's `SnapshotStarted` starts in the recording |
| 44 | `cutTimestamp` | int64 | that frame's consensus timestamp |
| 52 | magic | uint32 | `0x32504E53`, "SNP2" |

- **Named only once confirmed.** When the instance confirms the round (§5), a thread of its own forces the
  `.tmp` to disk, renames it `<round>.snapshot` and forces the directory, so dispatch never waits for the
  disk. A `.snapshot` file is therefore confirmed and durable. The next round's write waits for the previous
  confirmation to finish. Windows opens no directory to force, and .NET forces none on any system; there the
  file system's journal commits the rename before the deletions that follow it. A write that fails is logged
  as `SnapshotStoreFailed` and leaves no file; the instance still compares the round, and carries on without
  a copy of it.
- **Retention.** An instance keeps one confirmed file. Once a confirmed file is durable under its name, every
  older file is deleted, the `.tmp` of a round whose end never came and one a crash left behind included; a
  file that never becomes durable deletes nothing.
- **Files are portable.** Every instance of a source writes the same bytes for a round (A-6), so a file is
  not tied to the instance that wrote it: a peer's file copied into a new host's directory spares that host
  a full replay.

**Gateway instances.** Both instances of a gateway pair hold state: the active one serializes, writes and
publishes the end; the hot standby serializes, writes and compares, and takes over at once when
`GatewayActive` names it. An instance that is not running is not part of the pair until it restarts, restores
(§7) and catches up.

## 5. Confirming a snapshot

An instance confirms its own file. The tap carries only committed frames, so the source's sequenced
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
| 0 | `headerVersion` | uint16 | 1 |
| 2 | `headerLength` | uint32 | the header's bytes |
| 6 | `leadershipTermId` | int64 | from the last `LeadershipChanged` before `R` |
| 14 | `leaderMemberId` | int32 | that frame's `newLeaderMemberId` |

A `Gateway`'s header continues:

| offset | field | type | value |
| --- | --- | --- | --- |
| 18 | `gatewaySourceId` | int32 | the logical gateway |
| 22 | `activeGatewayId` | int32 | the instance the current `GatewayActive` names, or −1 |
| 26 | `highestConnectionId` | int32 | the highest `connectionId` in the source's history |
| 30 | `rowCount` | uint16 | the rows that follow |
| 32 | rows | 37 bytes each | `gatewayId` int32, `preferenceRank` uint8, `gatewayName` char[32] |

- **The rows are every instance of the gateway**, in list order. Every instance writes the same header, so
  none is marked as its own; an instance finds its row by name, as it does in the list.
- **The length tells the two apart.** An application's header is 18 bytes and a gateway's is 32 + 37 ×
  `rowCount`. The header is one record, so a gateway with snapshots has at most 1,770 instances.
- **The header is part of the snapshot's bytes**, so `crc32c` covers it and A-6 applies to it.
- **Everything else is the application's records**: its `OutstandingWork` set and the keys it drops
  duplicates by (spec A-2, A-3), and a gateway's open connections and what it derived from their
  `connectionData`.

## 7. Restore

A cold start of an instance with a `SnapshotListener` restores in `ReplayerRecovery`, in every language:

1. **Choose.** Take the newest confirmed file in the snapshot directory. One with no trailer gives way to the
   next older; with none, recover from `globalSeqNo` 1.
2. **Check the format.** If `formatVersion` is one this build does not support, stop.
3. **Check the cut.** Request a replay of the recording from the file's `cutPosition`. If its first frame is
   the frame the file names, `globalSeqNo` `cutGlobalSeqNo` stamped `cutTimestamp`, the file is this log's:
   close the replay and read it. If it is any other frame, or the Replayer refuses the position (spec R-2),
   the file was cut from another log, and an older one is tried. A recording that has not reached the
   position yet, as while its member replays the log after a restart, is answered `ReplayPending`, and the
   check waits.
4. **Read the records**, at most 1,024 a duty cycle, dispatching nothing; frames arriving on the live tap
   meanwhile are held, as in any recovery. Record 0 is the header: if its `headerVersion` is one this build
   does not support, stop. Otherwise it stands in for the frames before `R`. Its leader becomes the
   receiver's current leader and its term confirmed ingress's, as the last `LeadershipChanged` before `R`
   would have made them; the source takes part in rounds, as its topology row before `R` said; and a
   `Gateway`'s election state is set from it. Every later record goes to the listener's `onRestore`. Records
   that do not match the trailer are a damaged file, and the instance stops.
5. **Resume** at `cutPosition` with `globalSeqNo` `R` as the anchor, through the ordinary resume path (spec
   R-1, `requestResume`). The `SnapshotStarted` frame is the anchor and is dropped as a duplicate, so
   dispatch starts at `R + 1`. The instance serialized nothing for the restored round, so that round's
   `SnapshotEnd` changes nothing.
6. **Go live**, switching to the tap as any resume does.

An instance that stops is fenced with `SNAPSHOT_UNRESTORABLE`, and a damaged file stays for the operator to
remove ([`ops.md`](ops.md)). After a restore, every fallback that would walk the recording from its start
resumes at the restored snapshot's `cutPosition` instead, where the first frame is `R`, and drops every
frame up to the last one dispatched.

A consumer that publishes nothing has no snapshot of its own. It may restore from source *X*'s files only if
it runs *X*'s state machine.

## 8. Obligations

The scheme is sound only if every instance of a source agrees. Two of spec §16's obligations say so:

- **A-6. Snapshot records are deterministic.** Every instance of a source serializes the same state at `R`
  to the same records, byte for byte.
- **A-7. A divergent instance stops.** An instance whose own `recordCount`, `length` or `crc32c` for a round
  differs from its source's sequenced `SnapshotEnd` stops, fenced with `SNAPSHOT_DIVERGED`. The sequenced
  end is the reference: a publisher that diverged has broken A-6, and nothing recovers from that. Its file of
  the round is never confirmed, so never restored (§5).

## 9. Versioning

Every instance of a source runs the same build, and so writes the same `formatVersion`. A format change is
therefore a coordinated upgrade of every instance, and the restore stops on a format its build does not
support (§7 step 2). Protocol upgrades that change a source's format while it runs are not designed yet;
they will build on snapshots, and the field and the stop are already in place, so that design needs no
change to schema 210 (spec V-3). [`upgrades.md`](upgrades.md) proposes one.

## 10. Cluster snapshots

Application snapshots shorten a client's restart. A member still replays its full Raft log and re-records
its tap from `globalSeqNo` 1, and nothing is truncated. This section is the proposed design; none of it is
implemented. §10.1 to §10.5 bound a member's restart and keep every member's history complete, with no change
to any client; §10.6 truncates history, a separate step that does change them; §10.7 trims the Raft log,
which Aeron does not do.

**What the member's replay is for.** It rebuilds two things: the `Sequencer`'s state, and the member's tap
recording, which the Replayer serves. An Aeron cluster snapshot replaces the first. The second
is history, which a snapshot cannot regenerate, so the recording outlives the restart and is extended.

### 10.1 Rules

- **The recording is all the three share.** A member's tap recording is one run from `globalSeqNo` 1 across
  every restart. A frame's position is fixed once recorded, and nothing below the recording's verified end is
  rewritten.
- **Replicated state holds nothing member-local**: no recording id and no position.
- **Each snapshot stands alone.** Which was taken first never matters; the one ordering is at start (§10.5).
- **A recording is valid only with the Raft log it came from.** `purgelog.sh` removes both or neither.
- **Snapshots are seldom.** Each is taken once `X` bytes have accumulated since the last,
  `sequencer.snapshotLogBytes`, sized from the measured replay rate so that replaying `X` takes no longer
  than the restart a deployment accepts; at a minute, that is hours of traffic. A restart then replays at
  most `X`, or `2X` after a fallback (§10.2), however long the cluster has run, and a cluster whose log has
  not reached `X` takes none.

### 10.2 The `Sequencer`

| | |
| --- | --- |
| **contents** | Every field of `Sequencer` the log sets: `globalSeqNo`, `rejectedFrameCount`, the gateway rows, each logical gateway's designated instance, the activation queue, the pending activations and their deadlines, the bound sessions, the two bootstrap flags, and the policy, the last round with the timestamp of its start, and an unanswered `SnapshotRequested`. Aeron's own snapshot carries the sessions and the heartbeat timer. |
| **format** | A schema of its own, Java only: a begin message with `formatVersion` and the scalars, one message per entry, maps in key order, and an end message with the counts. Every member writes the same bytes at the same log position. A restored hash map's capacity, and so its iteration order, need not be the original's, so no frame may depend on a map's iteration order. |
| **test** | `SequencerSnapshotTest` snapshots a `Sequencer` at a random point of a seeded frame sequence, restores a second from it, and feeds both the rest: every frame they encode, and every field, must match. The sequence includes deposed sessions, pending activations and requested rounds, so a field the snapshot misses fails it. |
| **contract** | `SequencerSnapshot`, a package-private interface in `sequencer` that `Sequencer` implements: `formatVersion`, `onSnapshot(buffer, recordIndex)` and `onRestore(buffer, length, recordIndex)`, the shape of `SnapshotListener`. `SequencerService` drives it from `onTakeSnapshot` and `onStart`. The snapshot point is Aeron's, not a round's cut. |
| **when** | The leader's service sets the `ClusterControl` toggle to `SNAPSHOT` once the commit position is `X` past the last snapshot's log position in Aeron's recording log, so a new leader carries on where the last one stopped. It asks only while `cluster.role()` is `LEADER`: the consensus module reads the toggle only on the leader and outside an election, so a request set elsewhere would wait and take an extra snapshot once that member leads. The toggle switches only from `NEUTRAL`, by compare-and-set, so a request that meets an operator's `clusterctl snapshot`, or a snapshot in progress, fails and is retried at the next check. `clusterctl snapshot` is allowed, and `clusterctl shutdown` uses `SHUTDOWN`. |
| **restore** | `onStart` reads the records through a fragment assembler, and refuses a foreign schema, a newer version or a count that differs from the end's, as it refuses any snapshot today. |
| **version** | Aeron's `appVersion`, on the consensus module's context and the service container's, has `LOG_VERSION` as its major: a seqeron constant raised by every change spec V-3 makes a purge, and by a snapshot format the next release cannot read. Schema 210's own `version` stays 0 (spec §8), so it cannot serve. Aeron compares majors when it loads a snapshot and when it replays a leadership term from the log, so a member started on an archive V-3 says to purge stops with "incompatible version" rather than sequence another history. `LOG_VERSION` starts at 1, and seqeron's validator also accepts Aeron's default major 0, which every earlier log carries, until the first raise, so the release that introduces it needs no purge. |
| **the Raft log** | Holds the two newest snapshots and the log from the older one, so the newer always has a fallback. The log below the older is trimmed by `clusterctl trim-log` (§10.7). |
| **fallback** | A snapshot `onStart` refuses stops the member's start, and Aeron loads no older one by itself. On each member it stops, the operator runs `clusterctl invalidate-latest-snapshot` (`ClusterTool`'s), and the member restores the older snapshot and replays the log from it: at most `2X`. A release reads the snapshot format of the release before it; one that cannot raises `LOG_VERSION` and is deployed as a seqeron protocol change is, on a purged archive (spec V-3). |

### 10.3 The tap

| | |
| --- | --- |
| **find** | The member's newest local recording of the tap. |
| **verify the tail** | The archive does part of this itself: on start, it gives a recording a crash left active a stop position by scanning its last segment to the first empty frame, and refuses to start on a last fragment that straddles a page boundary and is not intact, which `ArchiveTool verify` repairs. It checks nothing before that fragment, and only the last segment. So, with `recordChecksum` (CRC32C) set on the member's archive, read the last two segments forward to the archive's stop position, to the first fragment whose checksum fails or frame whose `globalSeqNo` does not follow the one before, and truncate the recording there. An OS crash can lose what the archive had not written back, out of order; this cuts it off without forcing the archive to disk. The last frame left is the tip, `G_tip`. |
| **decide** | With `G_snap` the snapshot's `globalSeqNo`: with no snapshot, a new recording, as today. With `G_tip` ≥ `G_snap`, extend the recording at its end. With `G_tip` < `G_snap`, or no recording, copy the frames up to `G_snap` from a peer's tap, as `TapRelay` does, then extend. With no peer reachable, the member exits with 70. |
| **extend** | The tap publication takes the recording's own parameters from its descriptor, not the driver's defaults: `init-term-id`, `term-length` and `mtu`, and the `term-id` and `term-offset` of its stop position (`ChannelUriStringBuilder.initialPosition`). The archive extends it with `extendRecording`, so its positions run on unchanged even if the node's IPC term length or MTU has been changed since the recording began. |
| **emit** | While the log replays, nothing numbered at or below the tip is offered, synthesized frames included. |
| **live** | As today: a recording that stops exits the member with 70 (`TapPublisher`). |
| **a gateway host** | As today: its relay starts a new recording from `globalSeqNo` 1. |

The decision is a pure class over `G_tip`, `G_snap`, whether a recording exists and whether a peer answers,
unit-tested as `TapRelay` is.

### 10.4 The Replayer

The Replayer holds no replicated state, so the cluster snapshot has nothing of it, and it keeps no index of
snapshots (§5): with the recording kept, nothing of it grows with uptime.

### 10.5 A member's start

1. **Tap.** Find the recording, verify its tail and read `G_tip` (§10.3).
2. **Sequencer.** `onStart` restores the snapshot, which gives `G_snap`.
3. **Tap.** Extend the recording, copying from a peer first if it is behind, or exit with 70.
4. **Sequencer.** The log replays from the snapshot, emitting nothing up to the tip, and sequencing goes live.
5. **Replayer.** After step 3, the integrity check, and ready.

Step 5 waits for step 3: a replay served before the tail is verified could include what step 1 cuts off.

### 10.6 Truncating history

§10.1 to §10.5 bound a member's restart but not its archive. Truncation bounds the archive too, at the cost
of the walk from `globalSeqNo` 1.

- **The floor.** History is kept from the **floor** `F`: the cut of the oldest round a client may still
  restore. The `Sequencer` keeps those rounds' cuts and its snapshot carries them, so every member computes
  the same `F`. `F` is the cut of the round 255 before the latest, so an instance down for fewer than 256
  rounds, 256 × `interval` (§1), can still restore its file. With no round yet, `F` is `globalSeqNo` 1 and
  nothing is truncated.
- **Per member, anchored on `globalSeqNo`.** Each member finds `F`'s position in its own recording, where
  `F`'s `SnapshotStarted` is, rounds it down to a segment boundary and purges the segments below. The
  recording is the same one, so every position is unchanged: a file's `cutPosition` (§4.1), a client's resume
  and `TapRelay`'s resume keep working. Recordings left from earlier starts are deleted.
- **The integrity check** requires the recording's first frame to be at or before `F`. A gateway host's
  relay starts its recording at `F`.
- **The Raft log** below the older of the two cluster snapshots kept (§10.2) is trimmed by `clusterctl
  trim-log` (§10.7).

What changes for clients:

- A client with no confirmed file, or one whose newest confirmed round is older than `F`, needs history the
  member no longer has. The Replayer refuses its walk, and the client is fenced rather than started
  mid-history: today it takes whatever frame comes first. A consumer that holds no state, a follower of the
  live stream, may opt to start at `F` instead. A file older than `F` fails the cut check (§7 step 3), since
  its position is below the recording's start, and leads to the same walk.
- An instance with no file — a new host, a lost disk — restores from a peer's file (§4.1); with none, it is
  fenced.

A new member or a lost disk takes the cluster snapshot and the Raft log from Aeron's `ClusterBackup`, which
replicates both from a running cluster. It copies no tap, which is not in the cluster's recording log, so the
tap from `F` comes from a peer as §10.3 copies it, and the clients' snapshot files from a peer (§4.1).

### 10.7 Trimming the Raft log

Open-source Aeron trims nothing: a member's log recording grows from its first term, however many snapshots
it takes. Aeron's retention policy, `RetainEnoughDataForRecoveryPolicy`, belongs to the Data Retention
Regulator, a premium product, and `ClusterTool`'s `seed-recording-log-from-snapshot` rewrites
`recording.log` but deletes no data. seqeron therefore needs a procedure of its own: `clusterctl trim-log`,
to be implemented, run on each member. It does what that policy does, with the count fixed at two (§10.2):

- **Find the floor.** Read `recording.log` (`RecordingLog`, read-only while the member runs) for its two
  newest valid snapshots, and take the older one's log position.
- **Purge the log below it.** `purgeSegments` on each log recording the recording log names, at the segment
  boundary at or below that position, through the member archive's local control.
- **Purge older snapshots.** `purgeRecording` on every snapshot recording, the consensus module's and the
  service's, older than the two.
- **Per member, and safe to repeat.** Each member trims its own archive; a run that fails leaves more than
  it needs, never less.
- **Not the tap.** Its history is §10.6's.

## 11. The snapshot server

Each instance keeps its snapshots in a directory of its own (§4.1), and an instance with no file replays
from `globalSeqNo` 1. This section is the proposed design that replaces the directory
with a snapshot server per host; none of it is implemented. It was written when each Replayer confirmed
snapshots from an index of the log's ends: how the server learns that an instance confirmed a round (§5) is
open, and §11.2, §11.4 and §11.5 are to be revised for it before any of this is built.

What it changes: a damaged snapshot falls back to an older round rather than stopping the instance, and a host with no snapshot of a source fetches one from a
peer. Storage leaves the client tier. The façades stream records to the server and read them back, in
every language, and only the server, in Java, writes to disk.

### 11.1 The process

`SnapshotServer` (service tier) runs on every member host and every gateway host, in a process of its own.
It attaches to the host's media driver, the member's embedded driver or the gateway host's
`ReplayerServer` driver, as the host's clients do, and runs two things on it:

- **An Aeron Archive of its own**, beside the member's: its own directory (`snapshot.archiveDir`, default
  `<base>/snapshot-archive-<nodeId>`, best on a device of its own), its own `archiveId` (Aeron's default is
  unique per driver) and its own local control stream. Not the member's archive: that one also records the
  Raft log and the tap, a durability setting there would fsync the sequencing path, and a large snapshot
  would compete with the tap recording, whose stall terminates the member after 1 s (`TapPublisher`).
- **The agent**, `SnapshotService`: which recordings are confirmed, retention, restores and peer fetches,
  none of which the archive decides.

A process of its own because a member must never fail because of a snapshot. It ends when its driver goes
away, as every client on the host does, and is restarted with the member.

| archive setting | value | |
| --- | --- | --- |
| `fileSyncLevel` | 1 | data is forced after each block written, at most 1 MiB (`fileIoMaxLength`); segment files are preallocated, so data is all that changes |
| `catalogFileSyncLevel` | 1 | a recording's stop position survives a crash |
| `recordChecksum`, `replayChecksum` | `Checksums.crc32c()` | every fragment is checked when it is read back |
| `controlChannel` | member host: `aeron:udp?endpoint=<host>:<base + memberId*10 + 6>`; gateway host: `snapshot.endpoint`, or none | what peers replicate from (§11.5) |

`base + memberId*10 + 6` is unused in the member's stride, so the cluster block keeps its width
([`ops.md`](ops.md), "Ports").

### 11.2 Streams and messages

On `aeron:ipc`, on the host's driver:

| stream | carries |
| --- | --- |
| 208 | the snapshot archive's local control |
| 209 | requests, client → server |
| 210 | responses, server → client |
| 211 | one round's records: an exclusive publication per instance and round |
| 212 | one restore's records, replayed from the snapshot archive |

Three messages join the replay control protocol (`sbe-replay.xml`, schema 212, spec §10):

| message | id | direction | fields |
| --- | --- | --- | --- |
| `SnapshotOpen` | 25 | client → server | `clientId`, `requestId`, `sourceId`, `round`, `sessionId` |
| `SnapshotFetch` | 26 | client → server | `clientId`, `requestId`, `sourceId` |
| `SnapshotFetched` | 27 | server → client | `clientId`, `requestId`, `round`, `asOfGlobalSeqNo`, `asOfPosition`, `formatVersion`, `recordCount`, `length`, `crc32c`, `replaySessionId`; `round` = −1 for none |

`clientId` is the instance's Replayer client id, so one id names an instance to both servers. The server
asks the Replayer for ends with `SnapshotQuery` (§5), under a client id of its own; clients no longer do.

### 11.3 Taking a snapshot

§4 holds, except where the records go:

1. **Open.** On `SnapshotStarted`, the façade adds an exclusive publication on stream 211, sends
   `SnapshotOpen` with its session id, and waits for the publication to connect. The server records
   `aeron:ipc?alias=seqeron-snapshot.<sourceId>.<round>.|session-id=<sessionId>`, which only that
   publication matches, so a connected publication is one the archive records. The alias names the
   recording in the catalog. It ends in a dot because a catalog search matches a substring of the channel:
   `seqeron-snapshot.3.4.` finds round 4 of source 3 and not round 41.
2. **Stream.** Each record is one message, offered as the listener produces it and retried on
   back-pressure with the session kept alive, then the trailer of §4.1 as the last message, and the
   publication is closed. A record of at most 65,535 bytes is within the IPC message limit, an eighth of the
   term length. The façade still keeps the count, length and CRC, publishes the end if it may, and compares
   (A-7).
3. **Fail open.** A publication that does not connect within `SNAPSHOT_OPEN_TIMEOUT_MS` (proposed 1,000), or
   that loses its subscriber mid-round, stores nothing for the round. The instance still compares, the
   server keeps what it had, and a counter records it.

Dispatch pauses for the serialization and the IPC copy; the server forces the data behind it. A snapshot
larger than the publication window, half the term length, proceeds at the archive's write rate.

### 11.4 Confirmation and retention

The server confirms a recording when it has stopped, its last message is a trailer naming its round, and the
trailer's `recordCount`, `length` and `crc32c` equal the source's sequenced end. The trailer's figures are
the writing instance's, which stops if they differ from the end (A-7), and the fragment checksums cover the
bytes from the publication to the disk, so the server reads only the trailer: a bounded replay of the last
message. Data is forced block by block as it is recorded, so a confirmed recording is durable.

The server learns of a source's new rounds from the Replayer's per-source round counter (§5) on the shared
driver, and asks `SnapshotQuery` for each round it holds. It stores no confirmation: on start it confirms
what its catalog holds the same way. An answer of none is not final, since the Replayer may still be
indexing (§5), so the server asks again and deletes nothing on it.

Per source, the server keeps:

- the two newest confirmed rounds, so a newest one that fails its read at a restore (§11.6) has a fallback;
- any recording of a newer round, whose end may not be in the log yet.

It purges the rest (`purgeRecording`), and only once a newer round is confirmed, so a write that fails or a
crash that tears one never costs the last good snapshot. Two instances of a source on one host record the
same round twice; the first confirmed is kept.

### 11.5 Fetching from a peer

A server fetches a round it has no confirmed recording of, for a source a client has opened or fetched:

- **in the background**, when the Replayer indexes the source's newest round and no local recording of it
  is confirmed within `SNAPSHOT_FETCH_DELAY_MS` (proposed 5,000): a write that failed, an instance that was
  down at the cut;
- **on a `SnapshotFetch`** it has nothing confirmed for, before answering.

Its peers are the member hosts' servers, from `SEQERON_HOSTS` and the port above, and those named in
`snapshot.peers`. It lists a peer's catalog with `listRecordingsForUri` for the round's alias on stream
211, replicates the newest match into its own archive with `replicate`, and confirms the copy as its own
(§11.4); the copy keeps the original channel, so its alias finds it. It asks the peers in turn, and moves
on from one that has none or fails.

Replication is pulled by the destination archive, which must reach the source's control channel. A gateway
host's server is therefore a peer only with a `snapshot.endpoint`, and a source whose instances all run on
gateway hosts needs one there. The source archive replays to an ephemeral port on the fetching host, as a
member's archive does to a gateway host's relay.

### 11.6 Restore

§7 steps 1 to 3 become one request:

1. **Ask.** The client sends `SnapshotFetch` for its source, resent until answered. The server takes its
   newest confirmed round and reads the whole recording once, fragment checksums on and the records checked
   against the trailer. A recording that fails is purged and counted, and the next older confirmed round is
   tried. With none, after a fetch (§11.5), it answers `round` −1 and the client walks from `globalSeqNo` 1.
2. **Check the format**, as §7 step 2.
3. **Read the records** from the replay on stream 212 named by `replaySessionId`, as §7 step 4. A replay
   that ends early, because the server stopped, fences the instance with `SNAPSHOT_UNRESTORABLE`, since the
   listener holds part of the state; unlike a damaged file, a restart retries it with nothing to remove.
4. **Resume** and **go live**, as §7 steps 5 and 6.

A server that does not answer holds the restore under the recovery-stall timeout (spec §10.1), after which
the instance is fenced, as for an unavailable Replayer. A walk from `globalSeqNo` 1 would be correct, but
can take far longer than a gateway's activation deadline.

### 11.7 What changes elsewhere

| where | change |
| --- | --- |
| §4 step 1, §4.1 | records go to the server; `snapshotDirectory` and `SnapshotStore`'s files go, in every language |
| §5 | the server learns of a confirmed round (open, above) |
| §7 | steps 1 to 3 as §11.6 |
| §10.6 | an instance with no snapshot fetches one through its server (§11.5) |
| §12 | "No file means a full replay" goes; a process and an archive per host, and a UDP port per member host, are added |
| spec §10 | the three messages and streams 208 to 212 |
| [`ops.md`](ops.md) | the port, the server's counters, its process in the runbooks |
| scripts | `start-cluster.sh` and `start-three-node-cluster.sh` start a server per member after READY, `stop-cluster.sh` stops it, and `purgelog.sh` removes its archive with the log's, since a purged log's rounds start again at 1 |
| tests | the agent's unit tests, in Java, replace `SnapshotStoreTest`; the restore cases of `ReplayerRecoveryTest` change in every language; `snapshot-test.sh` adds a server restart, a round with no server, and a fetch from a peer |

## 12. Costs and limits

- **Log volume** is two frames per participating source per round, `SnapshotStarted` and `SnapshotEnd`,
  whatever the state's size.
- **Dispatch pauses** while serialization runs (§4 step 1): the listener's encoding, the CRC and the write
  into the page cache. A host that cannot hold the file's dirty pages writes at disk speed. The cluster
  session stays alive meanwhile, but the tap does not wait, so a pause long enough to drop the subscriber
  heals through replay.
- **Disk** holds up to three files per serializing instance: its confirmed round, the round being written,
  and one whose end never came, until the next confirmation deletes it.
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
- **A restore costs a replay round trip** before its first record: the check of the cut (§7 step 3).

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

`snapshot-test.sh` runs the `TestGateway` pair and a `TestApplication` replica per member on a three-member
cluster, each instance with its own directory kept across its restarts. It drives the pair through
restores, a failover onto a restored instance, and the killed instance's return, restoring from the file it
confirmed while it served; then a round started by `clusterctl request-snapshot`, a follower's replica restarting, and
a leader kill, after which the new leader's replica publishes rounds and the killed member's clients
restore. It checks each restored state against the client traffic, and every later round against it (A-7).

Not covered: a publisher change mid-round, which the property test does not inject, and an OS crash tearing
a file, whose file side `SnapshotStoreTest` covers.
