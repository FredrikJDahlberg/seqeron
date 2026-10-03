# Application snapshots

**Status: implemented** in both languages, for `Application` and `Gateway`. Cluster snapshots (§10) are
not; a node still recovers by full-log replay (`doc/fault-tolerance.md` §0). Section and rule references of
the form "spec §n" or "spec A-n" are to `doc/seqeron-protocol-spec.md`.

The state worth snapshotting lives in the clients, in Java and C++, where a `ClusteredService` snapshot
cannot reach it. The sequencer marks a point in the log, every instance of every
participating application serializes its state as of that point, and the one instance that may publish
submits the bytes through the log. They are sequenced, replicated and recorded on every node's tap like any
other frame. seqeron stores and serves them; it never interprets the application's part.

A restarting replica recovers from its source's latest snapshot and the frames after it:

```
S(n) at R, then frames R+1, R+2, …
```

Nothing before `R` is replayed. Frames keep their absolute `globalSeqNo`; only the starting point moves.

## Overview

| role | does |
| --- | --- |
| sequencer | starts rounds: synthesizes `SnapshotStarted` (§3) |
| every instance holding a participating source's state | serializes its state at `R` (§4) |
| the instance that may publish at `R` | submits the bytes as `SnapshotChunk`s and a `SnapshotEnd` (§4) |
| `ReplayerService` | indexes each source's latest valid snapshot and answers `SnapshotQuery` (§5) |
| a restarting instance, or a passive gateway instance when activated | restores from that snapshot and resumes at `R + 1` (§7) |

A round:

```
operator / heartbeat    sequencer (log)              source instances               Replayer
SnapshotRequested ───►  R: SnapshotStarted ───────►  all: serialize at R
or interval elapsed                                  publisher: SnapshotChunk 0..n-1,
                        sequences chunks and end ◄── SnapshotEnd
                        SnapshotEnd ──────────────►  all: compare CRC (A-7), discard
                                    ─────────────────────────────────────────────► check, index
```

A snapshot is a sequence of records, each carried whole by one `SnapshotChunk`: the header the façade
writes (§6), then the records the application encodes. `crc32c` covers their bytes in order. No instance
ever holds a snapshot as one buffer. A restore queries the Replayer, streams the records to the façade and
the application as the chunks are replayed, and resumes the tap at `R` as the anchor (§7).

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
| `SnapshotChunk` | 27 | 12 | `round` int64, `chunkIndex` int32, `data` varData | a participating source |
| `SnapshotEnd` | 28 | 28 | `round` int64, `chunkCount` int32, `length` int64, `crc32c` uint32, `formatVersion` uint32 | a participating source |
| `SnapshotPolicyRegistered` | 29 | 4 | `intervalSeconds` uint32 | `clusterctl load-topology` |

`GatewayRegistered` and `ApplicationRegistered` each gain a `snapshot` uint8.

- A chunk carries one record, whole, of at most 1302 bytes: `MAX_PAYLOAD_LENGTH` (1316) less the 12-byte
  block and the 2-byte length prefix. A record never spans two chunks, so the application's state may be
  any size; it is bounded by the log and the publisher's memory, never by a buffer's capacity. Each chunk
  costs 76 bytes of framing on the tap, so an application with small records batches them itself.
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

A new round supersedes the previous one: a source still submitting the previous round abandons it (§4).

The sequencer decodes `SnapshotPolicyRegistered` alone; it recognises `SnapshotRequested` by its
`systemEventType`. It validates chunk and end frames only for shape (spec §9.2 conditions 8 and 9) and
never rejects one for its round: a rejected frame makes its producer fence (spec A-4), and a late or
superseded chunk is harmless.

## 4. Taking a snapshot

Every instance holding a participating source's state — each replica of an application, the active
and hot-standby instances of a gateway pair — handles `SnapshotStarted` the same way:

1. On dispatching `SnapshotStarted`, live or replayed, the façade writes its header (§6) as record 0, then
   pulls the listener's records: it calls `onSnapshot(buffer, recordIndex)` with one 1302-byte buffer, from
   index 0, until it returns 0, and the listener encodes the next record of its state into the buffer
   each time. The state is that after every frame up to `R`; every record is pulled before `R + 1` is
   dispatched, so the listener needs no frozen view of it. The instance that may publish at `R` — an
   `Application` whose gate is open, or the active `Gateway` — keeps the records, packed into fixed-size
   segments. Every other instance keeps only their count, length and running CRC.
2. The publisher submits the records as `SnapshotChunk`s, one each, paced across duty cycles within its
   `PendingSends` capacity, then `SnapshotEnd`. No other instance submits for that round.
3. A publisher that loses the role stops and does not resume: for an `Application`, a `LeadershipChanged`
   naming another member; for a `Gateway`, a `GatewayActive` naming another instance. Its source misses
   the round. A `LeadershipChanged` naming the same member closes the gate only until it is caught up
   again; the publisher then continues, after `PendingSends` has resent what the election lost (spec A-5).
4. On its source's `SnapshotEnd` for the round, every instance that serialized the round compares
   `chunkCount`, `length` and `crc32c` with its own (§8) and discards what it kept. On the next
   `SnapshotStarted` an instance discards anything it still holds, and a publisher still submitting
   abandons the round without a `SnapshotEnd`.

Snapshot frames are the façade's; the listener sees none of them. This requires every instance of a
source to serialize the same state to the same bytes (§8).

**Gateway instances.** An instance of a gateway pair is in one of three roles; the last is a setting of
its façade that the log never sees.

| role | holds state | at `SnapshotStarted` | when `GatewayActive` names it |
| --- | --- | --- | --- |
| active | yes | serializes, publishes | — |
| hot standby | yes | serializes, compares | takes over at once |
| passive | no | nothing | restores (§7) and catches up, then takes over |

A passive instance starts by reading the header of its source's latest snapshot (§6) for its list row and
the current `GatewayActive`, then follows the tap from that snapshot's `asOfPosition` for the election
alone. With no snapshot, it follows from `globalSeqNo` 1. A gateway whose source does not participate
has no snapshot, and its passive instance recovers from `globalSeqNo` 1.

A failover to a passive instance costs a restore, and it races the activation deadline:
`GatewayStarted` follows only once the instance is caught up, and the sequencer promotes the next row
after `GATEWAY_ACTIVATION_TIMEOUT_MS` (5000 ms, spec §7.2) of consensus time. A pair whose other instance
is down then alternates every 5 s until a restore finishes while the passive instance is named. The
passive role therefore suits state that restores and catches up well inside 5 s; larger state needs a
hot standby.

## 5. Locating a snapshot

Each `ReplayerService` follows its node's active recording through an archive replay, as `TapRelay`
does, and indexes, per source, its latest **valid** snapshot. A tap subscription would not do: it is
untethered, and a dropped one misses snapshots. A snapshot is valid when its chunks `0 … chunkCount − 1`
each appear once and in order, its `SnapshotEnd` follows them, and `length` and `crc32c` match. An invalid
one is never indexed, and the source's previous one stays. The index is rebuilt on restart, since every
run republishes the log from `globalSeqNo` 1.

Each entry holds `round`, `R`, `formatVersion`, the position of the `SnapshotStarted` frame and the
position just past the `SnapshotEnd`. Every run's active recording starts at `globalSeqNo` 1 and its positions
follow from the frames, so a position means the same on every node and in every recording that reaches
it.

Two messages join the replay control protocol (`sbe-replay.xml`, schema 212, spec §10):

| message | id | direction | fields |
| --- | --- | --- | --- |
| `SnapshotQuery` | 23 | client → replayer | `clientId`, `requestId`, `sourceId` |
| `SnapshotLocation` | 24 | replayer → client | `clientId`, `requestId`, `round`, `asOfGlobalSeqNo`, `asOfPosition`, `endPosition`, `formatVersion`; `round` = −1 for none |

The Replayer answers with what it has indexed. A client that starts while its node is still replaying
the log after a restart therefore restores from an older snapshot, or from `globalSeqNo` 1; holding the
query until the node's replay ends would be an unbounded wait, which the recovery-stall timeout (spec
§10.1) does not allow.

Each `ReplayerService` exposes, per source, the round of its latest indexed snapshot as a counter, so a
source that misses rounds is visible.

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
record, so a gateway snapshots with at most 34 instances.

The header is part of the snapshot's bytes, so `crc32c` covers it and A-6 applies to it. The application's
records hold everything else, including its `OutstandingWork` set and the keys it drops duplicates by (spec
A-2, A-3), and a gateway's open connections and what it derived from their `connectionData`.

## 7. Restore

A cold start with a `sourceId`, or a passive gateway instance's activation, in `ReplayerRecovery` in both
languages:

1. Send `SnapshotQuery`, resent until answered. With none, recover from `globalSeqNo` 1.
2. If `formatVersion` is one this build does not support, stop.
3. Replay the active recording from `asOfPosition`, with `R` as the anchor, dispatching nothing, and hand
   each of this source's chunks of that round on as it arrives, up to its `SnapshotEnd` (at
   `endPosition`). Record 0 is the header: if its `headerVersion` is one this build does not support,
   stop. Otherwise it stands in for the frames before `R`: its leader is the receiver's current leader
   and its term is confirmed ingress's, as the last `LeadershipChanged` before `R` would have made them,
   the source takes part in rounds, as its topology row before `R` said, and, for a `Gateway`, its
   election state is set from it. Every later record goes to the listener's `onRestore`. The chunks pass
   the same check as the index's (§5); the Replayer indexed this snapshot only once its recorded bytes
   passed it, so a failure here is a damaged recording, and the instance stops. Nothing is buffered: a
   replay lost before the end starts the restore over at the header, and `onRestore` over at record 0.
4. Resume at `asOfPosition` with `globalSeqNo` `R` as the anchor — the existing resume path (spec R-1,
   `requestResume`). The `SnapshotStarted` frame is the anchor and is dropped as a duplicate; dispatch
   starts at `R + 1`. The instance serialized nothing for the restored round, so that round's chunks and
   `SnapshotEnd` change nothing.
5. Switch to the live tap as a resume does.

An instance that stops is fenced with `SNAPSHOT_UNRESTORABLE`. After a restore, each fallback that would
walk the recording chain from segment 0 instead resumes at the restored snapshot's `asOfPosition`, where
the first frame is `R`, and drops every frame up to the last one dispatched. A completed restore is repeated
only by a passive gateway instance's activation, which starts over at step 1.

A consumer that publishes nothing has no snapshot of its own. It may restore from source *X*'s snapshot
only if it runs *X*'s state machine.

## 8. Obligations

Two of spec §16's producer and replica obligations are snapshots':

- **A-6. Snapshot records are deterministic.** Every instance of a source serializes the same state at `R`
  to the same records, byte for byte.
- **A-7. A divergent instance stops.** An instance whose own `chunkCount`, `length` or `crc32c` for a round
  differs from its source's sequenced `SnapshotEnd` stops, fenced with `SNAPSHOT_DIVERGED`. The sequenced
  snapshot is the reference: a publisher that diverged has broken A-6, and nothing recovers from that.

## 9. Versioning

Deferred. Protocol upgrades will build on snapshots; snapshots do not depend on them. Until then every
instance of a source runs the same build, and so writes the same `formatVersion`. The field and the stop in
§7 step 2 stay, so the later design needs no change to schema 210 (spec V-3).

## 10. Cluster snapshots

Application snapshots shorten a client's restart; a node still replays the full Raft log and re-records
its tap from `globalSeqNo` 1, and nothing is truncated.

A round's cut is the natural point for the `Sequencer`'s own snapshot: its state is Java and small
(`globalSeqNo`, the heartbeat timer, the gateway list and election, open connections, the snapshot policy
and round). History before the oldest `R` among the participating sources' latest snapshots is then
needed by no client. A node restored from a cluster snapshot would, however, start its tap after the cut,
so positions no longer follow from `globalSeqNo` 1: the index (§5), `TapRelay`'s resume on another member
and the Replayer's integrity check all depend on that, and would need a base position per recording. The
latest snapshot of each source would also have to be republished at the head of the new tap. This needs
a prototype before it is specified.

## 11. Costs and limits

- Snapshot bytes pass through Raft: 1 MB is about 800 chunks, 100 MB about 80,000, competing with live
  ingress. Larger state needs an out-of-band store.
- Dispatch stops for as long as serialization takes (§4 step 1).
- The publisher holds its serialized records in memory from `R` until its `SnapshotEnd` or the next
  round. Every other instance holds only a running count, length and CRC, and a restore streams.
- `interval` must cover the largest snapshot at the pace it is submitted; a source that overruns it never
  completes a round.
- A source whose publisher changes during a round misses that round and keeps its previous snapshot, as
  does one whose cut falls after a leadership change but before the new leader's replica has caught up
  and reopened its gate.
- A replica catching up through old rounds serializes at each `SnapshotStarted` it passes.
- The Replayer index is rebuilt by reading the whole recording on restart.

## 12. Tests

Unit tests, in both languages where the code is:

| what | tests |
| --- | --- |
| frames, the header, the validator | `SnapshotFormatTest` |
| rounds and their triggers | `SequencerTest`, `ConformanceTest` |
| the topology document's rules and publish order | `TopologyDocumentTest` |
| the index and `SnapshotQuery` | `SnapshotIndexTest`, `ReplayerServiceTest` |
| serialization, publishing, abandoning, the comparison | `SnapshotRecordsTest`, `SnapshotTakerTest` |
| a gateway's election state in the header | `GatewayLifecycleTest` |
| the restore, its fallbacks and a restart | `ReplayerRecoveryTest`, case for case across the languages |
| restored state plus the tail equals full replay, under random replay faults | `ReplayerRecoveryPropertyTest` |

`snapshot-test.sh` runs the `TestGateway` pair through restores, a failover onto a restored instance and a
passive activation on a three-node cluster, checking each restored state against the client traffic and
every later round against it (A-7). Not covered: member failures, and failovers or publisher changes
mid-round in the property test.
