# Application snapshots

**Status: implemented** in both languages, for `Application` and `Gateway`. Cluster snapshots (§10) are
not; a node still recovers by full-log replay (`doc/fault-tolerance.md` §0). Section and rule references of
the form "spec §n" or "spec A-n" are to `doc/seqeron-protocol-spec.md`.

The state worth snapshotting lives in the clients, in Java and C++, where a `ClusteredService` snapshot
cannot reach it. The sequencer marks a point in the log, every instance of every participating application
serializes its state as of that point into a file of its own, and the one instance that may publish submits
the snapshot's digest, a `SnapshotEnd`, through the log. The bytes never cross the cluster: the log carries the
cut and the digest, every instance checks its own serialization against the digest, and a restore reads its
file. seqeron never interprets the application's part.

A restarting instance recovers from its newest snapshot that the log confirms, and the frames after it:

```
S(n) at R, then frames R+1, R+2, …
```

Nothing before `R` is replayed. Frames keep their absolute `globalSeqNo`; only the starting point moves.

## Overview

| role | does |
| --- | --- |
| sequencer | starts rounds: synthesizes `SnapshotStarted` (§3) |
| every instance holding a participating source's state | serializes its state at `R` into its own file (§4) |
| the instance that may publish at `R` | submits the round's `SnapshotEnd` (§4) |
| every instance that serialized the round | compares its own with the sequenced `SnapshotEnd` and keeps the file it confirms (§4, §8) |
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

Snapshots are enabled by the topology document (spec §6.4), never by node configuration: the sequencer
acts on them, so the setting must come from the log (spec S-3).

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

- `<snapshots>` enables rounds. `interval` is in seconds; 0 means rounds start only on operator request.
  The loader requires at least one participating row with it, and publishes its row after every other row.
- `snapshot="true"` makes a source participate. The loader requires the same value on every row of one
  gateway `sourceId`. The façades read it from their own row; the sequencer does not. A façade given no
  `SnapshotListener` takes part in no round, whatever its row says.
- Without `<snapshots>` no round starts.

## 2. Frames

All in `sbe-frame.xml` (schema 210). Adding them is a schema 210 change, so the release that introduces
snapshots starts from a purged archive (spec V-3).

**Synthesized** (a new template; spec §2):

| event | template | `systemEventType` | block | fields |
| --- | --- | --- | --- | --- |
| `SnapshotStarted` | 107 | 30 | 42 | `round` int64 |

**Submitted** (in `UnsequencedSystem`):

| event | `systemEventType` | block | fields | submitted by |
| --- | --- | --- | --- | --- |
| `SnapshotRequested` | 26 | 8 | `correlationId` int64 | `clusterctl request-snapshot` |
| `SnapshotEnd` | 28 | 28 | `round` int64, `recordCount` int32, `length` int64, `crc32c` uint32, `formatVersion` uint32 | a participating source |
| `SnapshotPolicyRegistered` | 29 | 4 | `intervalSeconds` uint32 | `clusterctl load-topology` |

`GatewayRegistered` and `ApplicationRegistered` each gain a `snapshot` uint8. `systemEventType` 27 is retired.

- `recordCount`, `length` and `crc32c` describe the snapshot's records, the header included. The records stay
  with the instances that serialized them (§4.1).
- A record is at most 65535 bytes, what its length prefix in the file (§4.1) can say. A snapshot holds any
  number of them, so the application's state may be any size; it is bounded by the instance's disk.
- `formatVersion` is the application's, opaque to seqeron.
- A snapshot belongs to the frame header's `sourceId`: one per application or logical gateway per round.
- `clusterctl snapshot` remains Aeron's cluster snapshot and stays refused; `request-snapshot` is the new
  command.

## 3. Rounds

A round is a cut and nothing more: the sequencer starts it and tracks nothing about it afterwards. Its
replicated state, derived from the log alone, is the policy from the latest `SnapshotPolicyRegistered`,
the last round number and the timestamp of the last `SnapshotStarted`.

**Start.** A round starts with a synthesized `SnapshotStarted` whose `globalSeqNo` is the round's cut, `R`.
Rounds are numbered from 1. Two triggers:

1. Operator: a `SnapshotRequested` is sequenced, and `SnapshotStarted` follows at the next `globalSeqNo`,
   as a `GatewayActive` follows a `GatewayActivationRequested`.
2. Interval: on a `ClusterHeartbeat` whose timestamp is at least `interval` past the last
   `SnapshotStarted` (or past the policy row, before any round). The check runs after gateway promotion
   (spec §7.2).

A new round supersedes the previous one: a publisher that has not yet placed the previous round's end
abandons it (§4).

The sequencer decodes `SnapshotPolicyRegistered` alone; it recognises `SnapshotRequested` by its
`systemEventType`. It validates a `SnapshotEnd` only for format (spec §9.2 conditions 8 and 9) and never
rejects one for its round: a rejected frame makes its producer fence (spec A-4), and a late or superseded
end is harmless.

## 4. Taking a snapshot

Every instance holding a participating source's state — each replica of an application, the active
and hot-standby instances of a gateway pair — handles `SnapshotStarted` the same way:

1. On dispatching `SnapshotStarted`, live or replayed, the façade writes its header (§6) as record 0, then
   pulls the listener's records: it calls `onSnapshot(buffer, recordIndex)` with one 65535-byte buffer, from
   index 0, until it returns 0, and the listener encodes the next record of its state into the buffer
   each time. Each record goes straight into the round's file (§4.1), and the façade keeps only their count,
   length and running CRC. The state is that after every frame up to `R`; every record is pulled before
   `R + 1` is dispatched, so the listener needs no frozen view of it. The façade keeps its cluster session
   alive after each record, so the session timeout does not bound how long this takes.
2. The instance that may publish at `R` — an `Application` whose gate is open, or the active `Gateway` once it
   has placed its `GatewayStarted`, which binds its session to the source — submits the round's
   `SnapshotEnd` through `PendingSends`, like any frame it places. No other instance submits for that round.
3. A publisher that loses the role before its end is placed does not place it: for an `Application`, a
   `LeadershipChanged` naming another member; for a `Gateway`, a `GatewayActive` naming another instance. Its
   source then has no end for the round. A `LeadershipChanged` naming the same member closes the gate only
   until it is caught up again; the publisher then submits, after `PendingSends` has resent what the election
   lost (spec A-5).
4. On its source's `SnapshotEnd` for the round, every instance that serialized the round compares
   `recordCount`, `length` and `crc32c` with its own (§8). On a match the round's file is confirmed, and the
   instance deletes its files of earlier rounds. On the next `SnapshotStarted`, a publisher that has not yet
   placed the previous round's end abandons it.

Snapshot frames are the façade's; the listener sees none of them. This requires every instance of a
source to serialize the same state to the same bytes (§8).

### 4.1 The snapshot directory

Each instance is given a directory of its own, `snapshotDirectory` on either façade, which a
`SnapshotListener` requires. A restart restores from what it holds, so it must outlive the process; no other
instance may write it, the other instance of a gateway pair included.

`<round>.snapshot` holds the records, each a little-endian uint16 length and its bytes, then a trailer:

| offset | field | type | value |
| --- | --- | --- | --- |
| 0 | `round` | int64 | |
| 8 | `length` | int64 | the records' bytes, as `SnapshotEnd.length` |
| 16 | `recordCount` | int32 | |
| 20 | `crc32c` | uint32 | |
| 24 | `formatVersion` | uint32 | the listener's |
| 28 | magic | uint32 | `0x50414E53`, "SNAP" |

A file is written as `<round>.tmp` and renamed once its trailer is, with no fsync. A write that fails is
logged as `SnapshotStoreFailed` and leaves no file; the instance still compares the round, and carries on
without a copy of it. An instance keeps its newest confirmed file and any newer one, whose end may not be in
the log yet; a `.tmp` a crash left behind goes when the next round is confirmed.

Every instance of a source writes the same bytes for a round (A-6), so a file is not tied to the instance that
wrote it: copying a peer's file into a new host's directory spares that host a full replay.

**Gateway instances.** An instance of a gateway pair is in one of three roles; the last is a setting of
its façade that the log never sees.

| role | holds state | at `SnapshotStarted` | when `GatewayActive` names it |
| --- | --- | --- | --- |
| active | yes | serializes, writes, publishes the end | — |
| hot standby | yes | serializes, writes, compares | takes over at once |
| passive | no | nothing | restores (§7) and catches up, then takes over |

A passive instance writes no snapshot. It starts by reading the header (§6) of the newest confirmed file
its directory holds, one left from when it last served, for its list row and the current `GatewayActive`,
and follows the tap from that snapshot's `asOfPosition` for the election alone; with none, it follows from
`globalSeqNo` 1. Its activation restores from the same file, or replays from `globalSeqNo` 1.

A failover to a passive instance costs that restore and catch-up, and it races the activation deadline:
`GatewayStarted` follows only once the instance is caught up, and the sequencer promotes the next row
after `GATEWAY_ACTIVATION_TIMEOUT_MS` (5000 ms, spec §7.2) of consensus time. A pair whose other instance
is down then alternates every 5 s until a restore finishes while the passive instance is named. The
passive role therefore suits state that catches up well inside 5 s; larger state needs a hot standby.

## 5. Confirming a snapshot

Each `ReplayerService` follows its node's active recording through an archive replay, as `TapRelay`
does, and indexes every source's `SnapshotEnd` by round, with where the round's `SnapshotStarted` is: `R`
and its position. A tap subscription would not do: it is untethered, and a dropped one misses ends. A
source's first end of a round is the one kept; an end whose round started more than four rounds before the
latest `SnapshotStarted`, or not in this recording, is not indexed. The index is rebuilt on restart, since
every run republishes the log from `globalSeqNo` 1.

Every run's active recording starts at `globalSeqNo` 1 and its positions follow from the frames, so a
position means the same on every node and in every recording that reaches it.

Two messages join the replay control protocol (`sbe-replay.xml`, schema 212, spec §10):

| message | id | direction | fields |
| --- | --- | --- | --- |
| `SnapshotQuery` | 23 | client → replayer | `clientId`, `requestId`, `sourceId`, `round` |
| `SnapshotLocation` | 24 | replayer → client | `clientId`, `requestId`, `round`, `asOfGlobalSeqNo`, `asOfPosition`, `formatVersion`, `recordCount`, `length`, `crc32c`; `round` = −1 for none |

A query names the round of a file the client holds, and the answer is that round's sequenced end, or none.
The Replayer answers with what it has indexed. A client that starts while its node is still replaying the
log after a restart finds no end for its newest rounds and falls back to an older file, or to `globalSeqNo`
1; holding the query until the node's replay ends would be an unbounded wait, which the recovery-stall
timeout (spec §10.1) does not allow.

Each `ReplayerService` exposes, per source, its newest indexed round as a counter, so a source that misses
rounds is visible.

## 6. The snapshot header

Record 0 of a snapshot is the header, which the façade writes; the records after it are the application's.
A restore replays nothing before `R`, so the header holds the state the façade derives from earlier
frames. Its layout is seqeron's, little-endian, and versioned independently of `formatVersion`:

| offset | field | type | value |
| --- | --- | --- | --- |
| 0 | `headerVersion` | uint16 | 1 |
| 2 | `headerLength` | uint32 | the header's bytes |
| 6 | `leadershipTermId` | int64 | from the last `LeadershipChanged` before `R` |
| 14 | `leaderMemberId` | int32 | that frame's `newLeaderMemberId` |

A `Gateway` header continues:

| offset | field | type | value |
| --- | --- | --- | --- |
| 18 | `gatewaySourceId` | int32 | the logical gateway |
| 22 | `activeGatewayId` | int32 | the instance the current `GatewayActive` names, or −1 |
| 26 | `highestConnectionId` | int32 | the highest `connectionId` in the source's history |
| 30 | `rowCount` | uint16 | rows that follow |
| 32 | rows | 37 bytes each | `gatewayId` int32, `preferenceRank` uint8, `gatewayName` char[32] |

The rows are every instance of the gateway, in list order. Every instance writes the same header, so none
is marked as its own; an instance finds its row by name, as it does in the list. An application's header
is 18 bytes and a gateway's is 32 + 37 × `rowCount`; `headerLength` tells them apart. The header is one
record, so a gateway snapshots with at most 1770 instances.

The header is part of the snapshot's bytes, so `crc32c` covers it and A-6 applies to it. The application's
records hold everything else, including its `OutstandingWork` set and the keys it drops duplicates by (spec
A-2, A-3), and a gateway's open connections and what it derived from their `connectionData`.

## 7. Restore

A cold start with a `sourceId`, or a passive gateway instance's activation, in `ReplayerRecovery` in both
languages:

1. Take the newest round with a file in the snapshot directory. With none, recover from `globalSeqNo` 1.
2. Send `SnapshotQuery` for it, resent until answered. With no end for it, return to step 1 for an older round.
3. If `formatVersion` is one this build does not support, stop.
4. If the file has no trailer, or its `recordCount`, `length` or `crc32c` differ from the end's, return to step 1
   for an older round. A file an OS crash tore is one such; a file of a round this instance diverged in and
   crashed before comparing is another.
5. Read the records, at most 1024 a duty cycle, dispatching nothing; frames off the live tap meanwhile are held
   as in any recovery. Record 0 is the header: if its `headerVersion` is one this build does not support,
   stop. Otherwise it stands in for the frames before `R`: its leader is the receiver's current leader and its
   term is confirmed ingress's, as the last `LeadershipChanged` before `R` would have made them, the source
   takes part in rounds, as its topology row before `R` said, and, for a `Gateway`, its election state is set
   from it. Every later record goes to the listener's `onRestore`. Records that do not match the trailer, and
   so the end, are a damaged file, and the instance stops.
6. Resume at `asOfPosition` with `globalSeqNo` `R` as the anchor — the existing resume path (spec R-1,
   `requestResume`). The `SnapshotStarted` frame is the anchor and is dropped as a duplicate; dispatch
   starts at `R + 1`. The instance serialized nothing for the restored round, so that round's `SnapshotEnd`
   changes nothing.
7. Switch to the live tap as a resume does.

An instance that stops is fenced with `SNAPSHOT_UNRESTORABLE`, and a damaged file stays for the operator to
remove (`doc/ops.md`). After a restore, each fallback that would walk the recording from its start
instead resumes at the restored snapshot's `asOfPosition`, where the first frame is `R`, and drops every
frame up to the last one dispatched. A completed restore is repeated only by a passive gateway instance's
activation, which starts over at step 1.

A consumer that publishes nothing has no snapshot of its own. It may restore from a file of source *X*'s only
if it runs *X*'s state machine.

## 8. Obligations

Two of spec §16's producer and replica obligations are snapshots':

- **A-6. Snapshot records are deterministic.** Every instance of a source serializes the same state at `R`
  to the same records, byte for byte.
- **A-7. A divergent instance stops.** An instance whose own `recordCount`, `length` or `crc32c` for a round
  differs from its source's sequenced `SnapshotEnd` stops, fenced with `SNAPSHOT_DIVERGED`. The sequenced
  end is the reference: a publisher that diverged has broken A-6, and nothing recovers from that. Its file of
  the round is never restored, as it does not match the end (§7 step 4).

## 9. Versioning

Deferred. Protocol upgrades will build on snapshots; snapshots do not depend on them. Until then every
instance of a source runs the same build, and so writes the same `formatVersion`. The field and the stop in
§7 step 3 stay, so the later design needs no change to schema 210 (spec V-3).

## 10. Cluster snapshots

Application snapshots shorten a client's restart; a node still replays the full Raft log and re-records
its tap from `globalSeqNo` 1, and nothing is truncated.

A round's cut is the natural point for the `Sequencer`'s own snapshot: its state is Java and small
(`globalSeqNo`, the heartbeat timer, the gateway list and election, open connections, the snapshot policy
and round). History before the oldest `R` among the participating sources' newest confirmed rounds is then
needed only by an instance with no file — a new host, a lost disk, a passive instance that never served —
which would need a peer's first (§4.1). A node restored from a cluster snapshot would, however, start its tap
after the cut, so positions no longer follow from `globalSeqNo` 1: the index (§5), `TapRelay`'s resume on
another member and the Replayer's integrity check all depend on that, and would need a base position per
recording, and the index would need each source's newest start and end carried over. This needs a prototype
before it is specified.

## 11. Costs and limits

- The log carries two frames per participating source per round, a `SnapshotStarted` and a `SnapshotEnd`,
  whatever the state's size.
- Dispatch stops for as long as serialization takes (§4 step 1): the listener's encoding, the CRC and the
  write into the page cache. A host that cannot hold the file's dirty pages writes it at disk speed. The
  cluster session stays alive meanwhile; the tap does not wait, so a pause long enough to drop the
  subscriber heals through replay.
- Every serializing instance keeps up to two files: its newest confirmed round and any newer one.
- A source whose publisher changes before its end is placed has no end for that round, and its instances
  keep their previous confirmed file, as does one whose cut falls after a leadership change but before the
  new leader's replica has caught up and reopened its gate, or after a `GatewayActive` but before the
  instance it names has placed its `GatewayStarted`.
- A replica catching up through old rounds serializes and writes a file at each `SnapshotStarted` it passes.
- An instance whose directory holds no file recovers from `globalSeqNo` 1: a new host, a lost disk, a passive
  gateway instance that never served. A peer's file spares it that (§4.1).
- Files are written without an fsync, so an OS crash can lose the newest; the restore falls back to an older
  file, or to `globalSeqNo` 1.
- The Replayer index is rebuilt by reading the whole recording on restart, and keeps every round's end of
  every source.

## 12. Tests

Unit tests, in both languages where the code is:

| what | tests |
| --- | --- |
| the header, the CRC | `SnapshotFormatTest` |
| rounds and their triggers | `SequencerTest`, `ConformanceTest` |
| the topology document's rules and publish order | `TopologyDocumentTest` |
| the index and `SnapshotQuery` | `SnapshotIndexTest`, `ReplayerServiceTest` |
| the file: its trailer, a torn or damaged one, retention; a golden trailer shared across the languages | `SnapshotStoreTest` |
| serialization, the end, abandoning, the comparison, retention | `SnapshotTakerTest` |
| a gateway's election state in the header | `GatewayLifecycleTest` |
| the restore, its fall-backs to older files and to a walk, and a restart | `ReplayerRecoveryTest`, case for case across the languages |
| restored state plus the tail equals full replay, under random replay faults and unconfirmed files | `ReplayerRecoveryPropertyTest` |

`snapshot-test.sh` runs the `TestGateway` pair and a `TestApplication` replica per member on a three-node
cluster, each instance with its own directory kept across its restarts. It drives the pair through restores,
a failover onto a restored instance and a passive activation from the file the instance kept while it
served; then a round started by `clusterctl request-snapshot`, a follower's replica restarting, and a cluster
leader kill, after which the new leader's replica publishes rounds and the killed member's clients restore.
It checks each restored state against the client traffic and every later round against it (A-7). Not
covered: publisher changes mid-round in the property test, and an OS crash tearing a file, whose file side
`SnapshotStoreTest` covers.
