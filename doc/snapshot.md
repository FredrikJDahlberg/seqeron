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
| every instance that serialized the round | compares its own serialization with the sequenced `SnapshotEnd`, and keeps the file it confirms (§4, §8) |
| `ReplayerService` | indexes each source's `SnapshotEnd`s by round and answers `SnapshotQuery` (§5) |
| a restarting instance, or a passive gateway instance when activated | restores from its newest confirmed file and resumes at `R + 1` (§7) |

A round:

```
operator / heartbeat    sequencer (log)              source instances                     Replayer
SnapshotRequested ───►  R: SnapshotStarted ───────►  all: serialize at R into a file
or interval elapsed     sequences the end      ◄──── publisher: SnapshotEnd
                        SnapshotEnd ──────────────►  all: compare (A-7), keep the file
                                    ──────────────────────────────────────────────────►  index
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
   and running CRC. The state is the state after every frame up to `R`, and every record is pulled before
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
   confirmed, and the instance deletes its files of earlier rounds. On the next `SnapshotStarted`, a
   publisher that has not yet placed the previous round's end abandons it.

The snapshot frames are the façade's; the listener sees none of them. All of this depends on every instance
of a source serializing the same state to the same bytes (§8).

### 4.1 The snapshot directory

Each instance has a directory of its own, `snapshotDirectory` on either façade, which a `SnapshotListener`
requires. A restart restores from what it holds, so it must outlive the process, and no other instance may
write to it, the other instance of a gateway pair included.

`<round>.snapshot` holds the records, each a little-endian uint16 length followed by its bytes, then a
trailer:

| offset | field | type | value |
| --- | --- | --- | --- |
| 0 | `round` | int64 | |
| 8 | `length` | int64 | the records' bytes, as `SnapshotEnd.length` |
| 16 | `recordCount` | int32 | |
| 20 | `crc32c` | uint32 | |
| 24 | `formatVersion` | uint32 | the listener's |
| 28 | magic | uint32 | `0x50414E53`, "SNAP" |

- **Written then renamed.** A file is written as `<round>.tmp` and renamed once its trailer is written, with
  no fsync. A write that fails is logged as `SnapshotStoreFailed` and leaves no file; the instance still
  compares the round, and carries on without a copy of it.
- **Retention.** An instance keeps its newest confirmed file and any newer one, whose end may not be in the
  log yet. A `.tmp` a crash left behind goes when the next round is confirmed.
- **Files are portable.** Every instance of a source writes the same bytes for a round (A-6), so a file is
  not tied to the instance that wrote it: a peer's file copied into a new host's directory spares that host
  a full replay.

**Gateway instances.** An instance of a gateway pair is in one of three roles. The third is a setting of
its façade that the log never sees.

| role | holds state | at `SnapshotStarted` | when `GatewayActive` names it |
| --- | --- | --- | --- |
| active | yes | serializes, writes, publishes the end | — |
| hot standby | yes | serializes, writes, compares | takes over at once |
| passive | no | nothing | restores (§7), catches up, then takes over |

A passive instance writes no snapshot. It starts by reading the header (§6) of the newest confirmed file
its directory holds, left from when it last served, for its list row and the current `GatewayActive`, and
follows the tap from that snapshot's `asOfPosition` for the election alone; with no file it follows from
`globalSeqNo` 1. Its activation restores from the same file, or replays from `globalSeqNo` 1.

A failover to a passive instance costs that restore and catch-up, and races the activation deadline:
`GatewayStarted` follows only once the instance is caught up, and the sequencer promotes the next row after
`GATEWAY_ACTIVATION_TIMEOUT_MS` (5,000 ms, spec §7.2) of consensus time. A pair whose other instance is down
then alternates every 5 s until a restore finishes while the passive instance is named. The passive role
suits state that catches up well inside 5 s; larger state needs a hot standby.

## 5. Confirming a snapshot

A restarting instance needs to know which of its files the log confirms. Each `ReplayerService` keeps that
answer: it follows its member's active recording through an archive replay, as `TapRelay` does, and indexes
every source's `SnapshotEnd` by round, with where that round's `SnapshotStarted` is, both `R` and its
position. A tap subscription would not do, since it is untethered and a dropped one misses ends.

- **The first end wins.** A source's first end of a round is the one kept.
- **Bounded history.** An end whose round started more than four rounds before the latest
  `SnapshotStarted`, or outside this recording, is not indexed.
- **Rebuilt on restart**, since every run republishes the log from `globalSeqNo` 1. Every run's active
  recording starts at `globalSeqNo` 1 and its positions follow from the frames, so a position means the same
  on every member and in every recording that reaches it.

Two messages in the replay control protocol (`sbe-replay.xml`, schema 212, spec §10) carry the question and
the answer:

| message | id | direction | fields |
| --- | --- | --- | --- |
| `SnapshotQuery` | 23 | client → replayer | `clientId`, `requestId`, `sourceId`, `round` |
| `SnapshotLocation` | 24 | replayer → client | `clientId`, `requestId`, `round`, `asOfGlobalSeqNo`, `asOfPosition`, `formatVersion`, `recordCount`, `length`, `crc32c`; `round` = −1 for none |

A query names the round of a file the client holds, and the answer is that round's sequenced end, or none.
The Replayer answers from what it has indexed so far. A client that starts while its member is still
replaying the log after a restart finds no end for its newest rounds, and falls back to an older file or to
`globalSeqNo` 1; waiting for the member's replay to end would be an unbounded wait, which the
recovery-stall timeout (spec §10.1) does not allow.

Each `ReplayerService` exports, per source, its newest indexed round as a counter, so a source that misses
rounds is visible ([`ops.md`](ops.md)).

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

A cold start that names a `sourceId`, or a passive gateway instance's activation, restores in
`ReplayerRecovery`, in every language:

1. **Choose.** Take the newest round with a file in the snapshot directory. With none, recover from
   `globalSeqNo` 1.
2. **Ask.** Send `SnapshotQuery` for it, resent until answered. With no end for it, return to step 1 for an
   older round.
3. **Check the format.** If `formatVersion` is one this build does not support, stop.
4. **Check the file.** If it has no trailer, or its `recordCount`, `length` or `crc32c` differ from the
   end's, return to step 1 for an older round. A file an OS crash tore is one such; a file of a round this
   instance diverged in, and crashed before comparing, is another.
5. **Read the records**, at most 1,024 a duty cycle, dispatching nothing; frames arriving on the live tap
   meanwhile are held, as in any recovery. Record 0 is the header: if its `headerVersion` is one this build
   does not support, stop. Otherwise it stands in for the frames before `R`. Its leader becomes the
   receiver's current leader and its term confirmed ingress's, as the last `LeadershipChanged` before `R`
   would have made them; the source takes part in rounds, as its topology row before `R` said; and a
   `Gateway`'s election state is set from it. Every later record goes to the listener's `onRestore`. Records
   that do not match the trailer, and so the end, are a damaged file, and the instance stops.
6. **Resume** at `asOfPosition` with `globalSeqNo` `R` as the anchor, through the ordinary resume path (spec
   R-1, `requestResume`). The `SnapshotStarted` frame is the anchor and is dropped as a duplicate, so
   dispatch starts at `R + 1`. The instance serialized nothing for the restored round, so that round's
   `SnapshotEnd` changes nothing.
7. **Go live**, switching to the tap as any resume does.

An instance that stops is fenced with `SNAPSHOT_UNRESTORABLE`, and a damaged file stays for the operator to
remove ([`ops.md`](ops.md)). After a restore, every fallback that would walk the recording from its start
resumes at the restored snapshot's `asOfPosition` instead, where the first frame is `R`, and drops every
frame up to the last one dispatched. Only a passive gateway instance's activation repeats a completed
restore, starting over at step 1.

A consumer that publishes nothing has no snapshot of its own. It may restore from source *X*'s files only if
it runs *X*'s state machine.

## 8. Obligations

The scheme is sound only if every instance of a source agrees. Two of spec §16's obligations say so:

- **A-6. Snapshot records are deterministic.** Every instance of a source serializes the same state at `R`
  to the same records, byte for byte.
- **A-7. A divergent instance stops.** An instance whose own `recordCount`, `length` or `crc32c` for a round
  differs from its source's sequenced `SnapshotEnd` stops, fenced with `SNAPSHOT_DIVERGED`. The sequenced
  end is the reference: a publisher that diverged has broken A-6, and nothing recovers from that. Its file of
  the round is never restored, since it does not match the end (§7 step 4).

## 9. Versioning

Every instance of a source runs the same build, and so writes the same `formatVersion`. A format change is
therefore a coordinated upgrade of every instance, and the restore stops on a format its build does not
support (§7 step 3). Protocol upgrades that change a source's format while it runs are not designed yet;
they will build on snapshots, and the field and the stop are already in place, so that design needs no
change to schema 210 (spec V-3).

## 10. Cluster snapshots

Application snapshots shorten a client's restart. A member still replays its full Raft log and re-records
its tap from `globalSeqNo` 1, and nothing is truncated.

A round's cut is the natural point for the `Sequencer`'s own snapshot: its state is Java and small
(`globalSeqNo`, the heartbeat timer, the gateway list and election, the open connections, the snapshot policy
and round). History before the oldest `R` among the participating sources' newest confirmed rounds would
then be needed only by an instance with no file — a new host, a lost disk, a passive instance that never
served — which would need a peer's file first (§4.1).

But a member restored from a cluster snapshot would start its tap after the cut, so its positions would no
longer follow from `globalSeqNo` 1. The index (§5), `TapRelay`'s resume on another member and the Replayer's
integrity check all depend on that: they would need a base position per recording, and the index would
need each source's newest start and end carried over. This needs a prototype before it is specified.

## 11. Costs and limits

- **Log volume** is two frames per participating source per round, `SnapshotStarted` and `SnapshotEnd`,
  whatever the state's size.
- **Dispatch pauses** while serialization runs (§4 step 1): the listener's encoding, the CRC and the write
  into the page cache. A host that cannot hold the file's dirty pages writes at disk speed. The cluster
  session stays alive meanwhile, but the tap does not wait, so a pause long enough to drop the subscriber
  heals through replay.
- **Disk** holds up to two files per serializing instance: its newest confirmed round and any newer one.
- **A round can be missed.** A source whose publisher changes before its end is placed has no end for that
  round, and its instances keep their previous confirmed file. So does one whose cut falls after a
  leadership change but before the new leader's replica has caught up and reopened its gate, or after a
  `GatewayActive` but before the instance it names has placed its `GatewayStarted`.
- **Catching up costs files.** A replica catching up through old rounds serializes and writes a file at
  each `SnapshotStarted` it passes.
- **No file means a full replay**: a new host, a lost disk, a passive gateway instance that never served.
  A peer's file spares it that (§4.1).
- **No fsync.** An OS crash can lose the newest file; the restore falls back to an older one, or to
  `globalSeqNo` 1.
- **The Replayer's index** is rebuilt by reading the whole recording on restart, and keeps every round's end
  of every source.

## 12. Tests

Unit tests, in every language that has the code:

| what | tests |
| --- | --- |
| the header, the CRC | `SnapshotFormatTest` |
| rounds and their triggers | `SequencerTest`, `ConformanceTest` |
| the topology document's rules and publish order | `TopologyDocumentTest` |
| the index and `SnapshotQuery` | `SnapshotIndexTest`, `ReplayerServiceTest` |
| the file: its trailer, a torn or damaged one, retention; a golden trailer shared across the languages | `SnapshotStoreTest` |
| serialization, the end, abandoning, the comparison, retention | `SnapshotTakerTest` |
| a gateway's election state in the header | `GatewayLifecycleTest` |
| the restore, its fallbacks to older files and to a walk, and a restart | `ReplayerRecoveryTest`, case for case across the languages |
| restored state plus the tail equals a full replay, under random replay faults and unconfirmed files | `ReplayerRecoveryPropertyTest` |

`snapshot-test.sh` runs the `TestGateway` pair and a `TestApplication` replica per member on a three-member
cluster, each instance with its own directory kept across its restarts. It drives the pair through
restores, a failover onto a restored instance, and a passive activation from the file the instance kept
while it served; then a round started by `clusterctl request-snapshot`, a follower's replica restarting, and
a leader kill, after which the new leader's replica publishes rounds and the killed member's clients
restore. It checks each restored state against the client traffic, and every later round against it (A-7).

Not covered: a publisher change mid-round, which the property test does not inject, and an OS crash tearing
a file, whose file side `SnapshotStoreTest` covers.
