# seqeron protocol specification

The normative definition of what crosses a seqeron cluster's boundary: the frames it sequences, the
system messages it accepts and emits, the replay protocol a client recovers through, and the obligations
of the producers and consumers on either side. Two implementations that follow it interoperate frame for
frame. [`overview.md`](overview.md) explains the design this specifies; this document is the reference an
implementation, a review or a test is held to.

Class and function names (`Sequencer`, `ReplayerRecovery`, …) say where a rule is enforced; they are not
part of the protocol.

**Conventions.** MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119. Rules carry stable
identifiers that code and tests cite: **F** frame, **T** transport, **S** sequencer, **P** payload,
**C** registration, **R** replay, **V** versioning, **E** encoding, **A** application, **H** hosted façade. Section
numbers are stable too, which is why §15 is unassigned. All SBE schemas here are little-endian.

---

## 1. Layers

The protocol is layered so that each layer has one owner. Aeron carries bytes; seqeron owns the frame
around a payload and its own system vocabulary; the application owns the payload.

| layer | contents | schema | owner |
| --- | --- | --- | --- |
| L0 transport | Aeron channels and stream ids; the Aeron Cluster ingress/egress session protocol | `sbe-cluster.xml` (111), a mirror of `io.aeron.cluster.codecs` | Aeron |
| L1 frame | two envelope pairs, `Unsequenced`/`Sequenced` (application) and `UnsequencedSystem`/`SequencedSystem` (system), plus five synthesized templates | `sbe-frame.xml` (210) | seqeron |
| L2 payload | one opaque, length-prefixed byte range per application frame, named by `payloadId` | the application's | the owner of that `payloadId` |
| system messages | seqeron's eighteen events, named by `systemEventType` | `sbe-frame.xml` (210) | seqeron |
| replay control | replayer ↔ client messages; node-local, never sequenced, never recorded | `sbe-replay.xml` (212) | seqeron |

The cluster tier never decodes an application payload.

> **P-0.** seqeron's obligation to a payload ends when the frame carrying it has been sequenced,
> recorded and delivered in order without a gap. seqeron never parses a payload and rejects one only
> when it is malformed as a frame (§9.2). Whether a payload is meaningful or acceptable is decided by
> the application after delivery.

An application that declines a payload does not change the stream: the frame is already sequenced,
and nothing is retracted or redelivered. **P-3** keeps the consumer's sequence tracking correct across
it.

## 2. Message classes

A frame is one of three classes, by where it came from.

| class | originates with | encoded by | appears on |
| --- | --- | --- | --- |
| unsequenced | an external producer (a gateway, an application, `clusterctl`) | the producer | cluster ingress only |
| sequenced | an external producer | the sequencer, from an unsequenced frame | the queues its `groupMask` names (§3.1), and replays of them |
| synthesized | the cluster | the sequencer | every queue, and replays of them |

Sequenced and synthesized frames draw `clusterSeqNo` from one counter (§9.1).

The template identifies a synthesized frame: `ClusterHeartbeat`, `LeadershipChanged`,
`GatewayActive`, `SnapshotStarted` and `SessionClosed` have templates of their own and no ingress form. Their header also carries
`sourceId`, `connectionId` and `sessionId` = −1, since there is no external producer, connection or
session to name. A consumer that needs to test provenance MUST test `sourceId` (**F-4**) and MUST NOT
depend on the other two fields.

Replay control messages (§10) belong to none of these classes: they are not frames and have no
header composite.

## 3. Transport bindings

Where each kind of message travels, and which of those streams is the durable record.

> **T-1.** A consumer MUST check `MessageHeader.schemaId` on every stream it reads.

| stream | channel / stream id | carries | recorded |
| --- | --- | --- | --- |
| cluster ingress | Aeron Cluster ingress, inside the L0 session envelope | `Unsequenced` (100) or `UnsequencedSystem` (102), schema 210 | in the Raft log |
| queue | `aeron:ipc`, stream `QUEUE_STREAM_ID_BASE` + `groupId` (2001–2016), one per group | `Sequenced` (101), `SequencedSystem` (103), or a synthesized template (104–108), schema 210: the frames of §3.1 | yes, by the co-located archive on every node |
| replay | stream 201 | archive bytes, identical to the queue replayed | no |
| replay request | stream 202 | schema 212, client → replayer | no |
| replay control | stream 203 | schema 212, replayer → client | no |
| hosted attach | streams 301 and 302, `aeron:ipc` | schema 218, client → host and host → client (§17.2) | no |
| hosted events and commands | streams 303 and 304, `aeron:ipc`, one image each per attachment | schema 218, host → client and client → host (§17.2) | no |

- **F-1. The queues are the record.** Without a `<snapshots>` policy the cluster takes no snapshots and recovers
  by replaying the full log from `clusterSeqNo` 1, so the queues' bytes are the only durable copy of a frame; with
  one, a member's queue starts again at the cut of the snapshot it restarts from (`doc/snapshot.md` §10.3). Every frame
  sequenced after activation (§7.4) is on at least one queue, and a frame on several queues is recorded once
  on each. An application snapshot (§7.3) is frames on a queue like any other.
- **F-2. Every node's queues carry the same frames in the same order.** Each frame is byte-identical on every
  queue that carries it, so a replay from any node's archive can stand in for any other's from where both begin.
  What is shared is the frames and their `clusterSeqNo` and `groupSeqNo`, not the recording's bytes. §9.3 keeps this true, `groupSeqNo` included (**F-6**). A
  recording begins at `groupSeqNo` 1, or at the cut its member restarted from, and its positions match another's
  only between nodes with the same IPC MTU and term length (§12).

### 3.1 Queues and groups

Every gateway and application belongs to one group, and each group has one queue: the part of the log
its members read. A queue has a single writer, the sequencer, and many readers: every member reads
every frame on it, and reading removes nothing, since the queue is recorded and replayed from its start.
No queue carries every frame. A deployment with one group has one queue, which carries every frame its
producers address. Groups are declared in the topology file (§6.4) and activated by the sequencer
(§7.4). A group's gateways and applications read its queue and recover from its recording; each
instance learns that queue's `groupId` from its node's replayer (**A-8**, **R-6**). A topology
declares at least one group, and no `groupId` is special: a deployment whose groups are 2 and 5 has
queues for 2 and 5 and no other. Every group is activated by the first topology load and none is added
later (**S-8**), so every queue carries every system frame from activation on, every topology row among
them. The **default queue** is that of the lowest active `groupId`. A tool, which is in no group, reads
the queue it is configured with, or else the default queue, or on a gateway host the lowest queue the
host relays (**R-6**).

> **F-5.** A queue carries, in `clusterSeqNo` order, its prologue (§7.4) and then every frame the
> sequencer emits after activation that is either of the system family or an application frame whose
> `groupMask` has the group's bit set. A frame is byte-identical on every queue that carries it but for
> its `groupSeqNo`.

> **F-6.** Every frame on a queue carries its `groupSeqNo` in its header (§4.1): 1 for the queue's first
> frame, increasing by exactly 1 per frame. `clusterSeqNo` increases along a queue but not by 1, so a
> consumer tracks a queue's continuity by `groupSeqNo` (**P-3**).

- A frame whose mask names several groups is on each of their queues, under each group's own `groupSeqNo`.
- A process that republishes a queue copies its frames unchanged. A gateway host relays the queue of
  each group it is configured with (`replayer.groups`, a list of `groupId`s, required since no group is
  special), each onto its own queue of the same stream id, and resumes a relay at the frame whose
  `groupSeqNo` follows the last one it published.
- The sequencer writes a queue reliably, and only once the node's archive is recording it, so a queue's
  recording starts at its first frame.
- The frames sequenced before activation are on no queue: the Raft log holds them. A cluster does nothing
  until its topology is loaded: before activation the sequencer refuses every ingress frame but the
  group table (§9.2 condition 13), and the prologue carries what a consumer needs of the rest.
- Until activation a node has no queue. No `clusterctl` command but `load-topology` is accepted
  (§6.4), and the replayer answers every request `ReplayPending` (**R-5**). A deployment is ready for
  its producers at activation, not at the cluster's first election.

## 4. The frame layer

Every message the cluster sequences is one of nine SBE templates. Four are envelope pairs, ingress and
sequenced, one pair per family; five are frames the sequencer synthesizes itself.

```
Unsequenced        (schema 210, template 100) { unsequencedHeader,       payload:varData }
Sequenced          (schema 210, template 101) { sequencedHeader,         payload:varData }
UnsequencedSystem  (schema 210, template 102) { unsequencedSystemHeader, body:varData }
SequencedSystem    (schema 210, template 103) { sequencedSystemHeader,   body:varData }
ClusterHeartbeat   (schema 210, template 104) { sequencedSystemHeader }
LeadershipChanged  (schema 210, template 105) { sequencedSystemHeader,   newLeaderMemberId, leadershipTermId }
GatewayActive      (schema 210, template 106) { sequencedSystemHeader,   gatewayId }
SnapshotStarted    (schema 210, template 107) { sequencedSystemHeader,   round }
SessionClosed      (schema 210, template 108) { sequencedSystemHeader,   clusterSessionId }
```

The members of each pair differ only in the fields sequencing adds. An application frame carries one
length-prefixed payload; a system frame carries one length-prefixed system payload. The five
synthesized templates carry their fields inline and have no payload.

### 4.1 Header layout

> **F-3.** Each unsequenced header composite is a byte prefix of its sequenced counterpart. The
> application and system composites are identical apart from the name of the uint16 at offset 16.

| offset | field | type | unsequenced | sequenced |
| --- | --- | --- | --- | --- |
| 0 | `sourceId` | int32 | ✓ | ✓ |
| 4 | `connectionId` | int32 | ✓ | ✓ |
| 8 | `sessionId` | int64 | ✓ | ✓ |
| 16 | `payloadId` / `systemEventType` | uint16 | ✓ | ✓ |
| 18 | `groupMask` | uint16 | ✓ | ✓ |
| 20 | `clusterSeqNo` | int64 | — | ✓ |
| 28 | `timestamp` | int64 | — | ✓ |
| 36 | `groupSeqNo` | int64 | — | ✓ |
| | encoded length | | **20** | **44** |

Consequences:

- Every header field is at the same offset in all nine templates. `clusterSeqNo` is at 20 and
  `groupSeqNo` at 36 in every sequenced frame, so a consumer can track continuity without branching on
  the template (**P-3**).
- Sequencing copies the first 20 bytes and appends 24, for both families.
- `blockLength` on an ingress frame is always 20 (§9.2 condition 4).
- Offset 16 is a `payloadId` in the application family and a `systemEventType` in the system family.
  The template says which; a consumer MUST NOT interpret one as the other.
- Offset 18 is the frame's group mask (§7.4): bit *g* − 1 names group *g*. The producer sets it on an
  application frame, never 0; it is 0 on a system frame (§9.2 conditions 12 and 13).

There is no padding: `clusterSeqNo` and `groupSeqNo` are unaligned. SBE's C++ codecs (`memcpy`) and
Agrona's `UnsafeBuffer` read each with a single unaligned load on x86-64 and ARM64. Use plain accessors only;
volatile or atomic access (`getLongVolatile`, `getAndAddLong`, C++ atomics) at an unaligned address
is undefined.

### 4.2 Frame sizes

| | ingress (100 / 102) | sequenced (101 / 103) |
| --- | --- | --- |
| `MessageHeader` | 8 | 8 |
| header composite (`blockLength`) | 20 | 44 |
| var-data length prefix | 2 | 2 |
| fixed overhead | **30** | **54** |

`MIN_INGRESS_LENGTH` is 30 for both ingress templates. A `ClusterHeartbeat` is 8 + 44 = 52 bytes: it
has no length prefix and no payload.

### 4.3 Provenance

> **F-4.** `sourceId` −1 is reserved for the cluster. Every other value names an external producer.
> An ingress frame MUST NOT carry −1 (§9.2 condition 6).

The template marks a frame as synthesized (§2); `sourceId` −1 is the header's copy of that fact.
There is no separate origin field. An application that needs to know a message's direction or role
carries that in its own payload.

## 5. Field authority

The ingress contract. **S-1:** the sequencer enforces it.

| field | on ingress | on the queue |
| --- | --- | --- |
| `sourceId` | set by the producer; MUST NOT be −1 (condition 6); checked against the gateway list (**S-6**) | copied; −1 on a synthesized frame |
| `connectionId` | set by the producer | copied |
| `sessionId` | set by the producer; advisory | overwritten with the Aeron Cluster session id the frame arrived on |
| `payloadId` | set by the producer; MUST NOT be 0 or 1 (condition 7) | copied |
| `systemEventType` | set by the producer; MUST be an allocated, ingress-legal value (condition 8) | copied |
| `groupMask` | set by the producer on an application frame: non-zero, only active groups' bits, its own group's included (condition 14, **A-9**); MUST be 0 on a system frame (condition 12) | copied; 0 on a synthesized frame |
| `clusterSeqNo` | absent | assigned by the sequencer (§9.1) |
| `timestamp` | absent | Raft consensus time at commit |
| `groupSeqNo` | absent | assigned by the sequencer for each queue the frame is on (**F-6**) |
| `payload` / `body` | set by the producer | copied byte for byte |

The outer `MessageHeader` is re-encoded, not copied:

| field | on ingress | on the queue |
| --- | --- | --- |
| `schemaId` | MUST be 210 (condition 2) | 210 |
| `templateId` | MUST be `Unsequenced` or `UnsequencedSystem` (condition 3) | `Sequenced` or `SequencedSystem` respectively |
| `blockLength` | MUST be 20 (condition 4) | 44 |
| `version` | MUST be 0 (condition 2) | 0 |

`version` is 0 for the life of schema 210 (§11).

**Producer kinds.** A *gateway* is a producer deployed as an active/standby pair. Its instances are
rows in the topology list (§6.4), and the cluster designates which instance is active
(`GatewayRegistered`, `GatewayStarted`, `GatewayActive`, `GatewayActivationRequested`; §7). A
*co-located application* runs one replica per cluster node, is not in the gateway list, and needs no
designation: only the replica on the leader submits, and `LeadershipChanged` says which that is.
Everything in this document about list rows, `gatewaySourceId` or promotion applies to gateways only.
`ConnectionOpened` and `ConnectionClosed` apply to either kind.

**`sourceId` allocation.** One id space for every producer. This table is the registry: a new
producer takes its id by adding a row.

| `sourceId` | owner | kind |
| --- | --- | --- |
| −1 | the cluster (synthesized frames only) | illegal on ingress |
| 0 | reserved for an external deployment | gateway |
| 2 | `clusterctl` | tool, never listed |
| 3 | reserved for an external deployment | application |
| 5, 6 | reserved for an external deployment | gateway |
| 7 | reserved for an external deployment | application |
| 8 | `ClusterProbe` (end-to-end tests) | tool, never listed |
| 9 | `TestGateway` pair (end-to-end tests) | gateway |
| 10 | `seqeron-examples` ping | tool, never listed |
| 11, 12 | `seqeron-examples` `ColocatedApp` (Java, C++) | application |
| 13 | `seqeron-examples` C++ gateway pair | gateway |
| 14, 15 | `seqeron-examples` `SnapshotApp` (Java, C++) | application |
| 16 | `TestApplication` replicas (end-to-end tests) | application |
| 17 | `seqeron-examples` `ColocatedApp` (C#) | application |
| 18 | `seqeron-examples` C# gateway pair | gateway |
| 19 | `seqeron-examples` `SnapshotApp` (C#) | application |

Gateways are listed by `GatewayRegistered.gatewaySourceId`, applications by
`ApplicationRegistered.applicationSourceId`. **S-6** checks gateways only. No list row may claim
`clusterctl`'s id.

> **S-6.** For an ingress frame (§9.2 condition 10):
>
> 1. `GatewayStarted` and `GatewayActivationRequested`: the `gatewayId` MUST name a list row. For
>    `GatewayStarted`, that row's `gatewaySourceId` MUST also equal `header.sourceId`, and the most
>    recent `GatewayActive` for that `gatewaySourceId` MUST name the `gatewayId`.
> 2. Any other system frame whose `sourceId` is a listed `gatewaySourceId`: it MUST arrive on a
>    cluster session already bound to a `gatewayId` with that `gatewaySourceId`.
> 3. A frame of either family on a session a `GatewayStarted` bound: the most recent `GatewayActive`
>    for that gateway MUST name the bound `gatewayId`, and no later `GatewayStarted` for that
>    `gatewayId` may have bound another session.
> 4. Everything else is not checked: application frames on a session no `GatewayStarted` bound (a
>    reply carries its requester's `sourceId`), and system frames with an unlisted `sourceId`.
>
> `GatewayStarted` creates the session binding; the closing of that cluster session removes it (the
> same event promotes a standby, §7.2). A later `GatewayStarted` for the same `gatewayId` moves the
> binding to its own session, and case 3 refuses the earlier session until it closes. These events and
> `GatewayActive` are in the log and the list is built from the log, so the check is deterministic
> (**S-3**). A binding MUST NOT be removed because of anything observed locally.

Case 1 rejects a `GatewayStarted` that arrives before `load-topology` has run, since the list is
empty.

Cases 1 and 3 make the log the judge of which instance acts. An operator request or an activation
timeout designates a sibling while the old instance's session stays open, and that instance serves until
it reads the `GatewayActive`; nothing it sends after that frame is sequenced. Its effects outside the
log, such as its external connections, are still its own to stop.

## 6. `payloadId`

A uint16 naming one application schema or encoding, never one message. A consumer maps it to a
decoder, which reads the payload's own framing.

### 6.1 Registry

| value | meaning |
| --- | --- |
| 0 | unset; invalid on the wire |
| 1 | retired (formerly seqeron's own messages, now the system family); refused on ingress (condition 7) so that a producer on an old build fails rather than having its frames passed through as an application payload |
| 2 and above | allocated by the deployment, one per application schema or encoding |

Current allocations:

| `payloadId` | protocol | shared across applications |
| --- | --- | --- |
| 2, 3 | reserved for an external deployment | no |
| 4 | reserved for an external deployment | yes |
| 5 | `ClusterProbe`'s `ProbeMarker` (`sbe-probe.xml`, schema 214) | no |
| 6 | `seqeron-examples` ping (8 raw bytes, no schema) | no |
| 7 | `seqeron-examples` `SnapshotApp` transfer (16 raw bytes, no schema) | no |

A protocol read by more than one application MUST be declared by a `PayloadIdRegistered` row (§6.3)
and MUST have a single owning repository whose codecs every consumer links. A `payloadId` that only
one application publishes and reads needs no row; the wire behaves the same either way (**P-1**,
**C-2**), and the only difference is the label `SbeLogPrinter` prints.

All `payloadId`s share one space. Applications on the same `payloadId` share its protocol. Two protocols
given the same number collide on a queue, and nothing here detects it; **P-4** detects a wrong decoder
only when the encoding describes itself.

### 6.2 Selective consumption

- **P-1.** A consumer MUST skip a `payloadId` it does not recognise, and MUST NOT treat it as an error.
- **P-2.** Within a `payloadId` it recognises, a consumer MUST skip templates it does not handle,
  without error. The sequencer does this for the system family (**S-2**).
- **P-3.** Continuity does not depend on understanding a payload. Gap detection uses `groupSeqNo`
  (**F-6**); de-duplication and recovery use `clusterSeqNo`. Both are read at their fixed offsets before
  any payload dispatch. A skipped frame still counts; skipping can neither create nor hide a gap.
- **P-4.** `payloadId` selects a decoder; the payload's own framing confirms the choice. Before
  decoding, a consumer checks what the encoding declares about itself: for SBE, that
  `MessageHeader.schemaId` matches the schema the decoder was generated from (§13.2 for other
  encodings). A consumer whose check fails skips the frame. This binds consumers only: the sequencer
  selects no decoder by `payloadId`.

### 6.3 Registration labels; it does not admit

```
PayloadIdRegistered  (systemEventType 23, block 36)
    { payloadId:uint16, protocolVersion:uint16, protocolName:char[32] }
```

One row per shared protocol (§6.1). `protocolName` is the label `SbeLogPrinter` prints for a payload
it cannot decode; `protocolVersion` is the revision the deployment runs, printed and not checked. It
is not the payload's `schemaId`: a `payloadId` may name an encoding that has no schema.

- **C-1.** `payloadId` 1 MUST NOT be registrable. The topology schema constrains `payloadId` to ≥ 2
  (§6.4).
- **C-2.** Registration does not gate. The sequencer MUST NOT reject a frame for an unregistered
  `payloadId` and MUST NOT decode `PayloadIdRegistered`. A consumer MUST NOT treat registration as
  permission; **P-1** applies regardless.
- **C-3.** Readers of the registry (only `SbeLogPrinter` today) de-duplicate on `payloadId`; a later
  row replaces the earlier name and version. Loading the topology twice is therefore safe.

A per-node, configuration-based allowlist is forbidden: a node whose configuration rejected a frame its
peers accepted would diverge (**S-3**). Any future admission rule MUST be derived from the log.

### 6.4 The topology file

`clusterctl load-topology <file>` publishes one operator file as system frames. Its `<gateways>`,
`<applications>` and `<protocols>` sections together describe every producer in the deployment; the
`<groups>` section before them names the groups they belong to, and an optional last element enables
snapshot rounds:

| section | publishes | describes |
| --- | --- | --- |
| `<groups>` | one `GroupsRegistered` for the section | the groups and their queues (§3.1, §7.4) |
| `<gateways>` | one `GatewayRegistered` per row | the gateway list (§7) |
| `<applications>` | one `ApplicationRegistered` per row | co-located applications |
| `<protocols>` | one `PayloadIdRegistered` per row | shared protocols (§6.3) |
| `<snapshots>` | one `SnapshotPolicyRegistered` | the snapshot round interval and byte threshold (§7.3) |

```xml
<topology>
  <groups>
    <group name="orders" id="1"/>
    <group name="refdata" id="2"/>
  </groups>
  <gateways>
    <gateway name="GW-A" id="1" sourceId="0" rank="0" group="orders"/>
    <gateway name="GW-B" id="2" sourceId="0" rank="1" group="orders"/>
  </gateways>
  <applications>
    <application name="RefDataServer" sourceId="3" group="refdata"/>
  </applications>
  <protocols>
    <protocol payloadId="4" version="1" name="refdata"/>
  </protocols>
</topology>
```

Attribute to field: a gateway's `name`/`id`/`sourceId`/`rank` are
`gatewayName`/`gatewayId`/`gatewaySourceId`/`preferenceRank`; an application's `name`/`sourceId` are
`applicationName`/`applicationSourceId`; a protocol's `payloadId`/`version`/`name` are
`payloadId`/`protocolVersion`/`protocolName`. A gateway's or application's optional `snapshot`
(default `false`) is `snapshot`, and `<snapshots interval>` and `<snapshots bytes>` (default 0) are `intervalSeconds` and `bytes`. A group's `name` is
the `groupNames` slot its `id` selects (§7.1), and a gateway's or application's `group` is the
`groupId` of the group it names. A gateway's `remaining` is computed by the loader from the row count of
its section. An application row has no `id` or `rank` because applications are not elected.

| constraint | enforced by |
| --- | --- |
| `name`: 1–32 printable US-ASCII characters | schema |
| `id` int32, `rank` uint8, `version` uint16 | schema |
| `sourceId` ≥ 0 (−1 is the cluster's, **F-4**) | schema |
| an application's `sourceId` is required | schema |
| `payloadId` ≥ 2 (**C-1**) | schema |
| unique within their section: gateway `id` and `name`, application `name` and `sourceId`, `payloadId`, group `id` and `name` | schema (identity constraints) |
| `<groups>` non-empty; `<gateways>`, `<applications>` and `<protocols>` optional | schema |
| group `id` 1–16 (`MAX_GROUPS`, §12) | schema |
| every gateway and application has a `group`, naming a declared group | schema (key/keyref) |
| exactly one `rank="0"` per gateway `sourceId` | loader |
| no `sourceId` is `clusterctl`'s, 2 (§5) | loader |
| no application `sourceId` equals a gateway `sourceId` | loader |
| every row of one gateway `sourceId` has the same `snapshot` and `group` | loader |
| `<snapshots>` has a row with `snapshot="true"`, and no row without | loader |

The loader MUST validate against the schema in its own artifact and MUST ignore any schema location
the document names. Otherwise a document could name a weaker schema and bypass **C-1**.

A gateway's or application's reference to a group is a name the schema resolves, so a document that names an
undeclared one fails validation before anything is published. What the schema cannot see, the groups a
producer addresses, is resolved against the log when the producer starts (**A-9**). Nor can it see the
log, and a row the sequencer refuses leaves its run partial: the bootstrap runs without it, or not at
all if it was the last. The loader therefore checks the file against the log before it publishes a row,
holding the topology session so that no other load changes the log in between.

> **S-9. One topology load at a time.** An accepted `GroupsRegistered` makes the cluster session it
> arrived on the topology session, until that session closes. The sequencer MUST refuse (§9.2 condition
> 11) a `GroupsRegistered` on another session while a topology session is open, and a
> `GatewayRegistered`, `ApplicationRegistered`, `PayloadIdRegistered` or `SnapshotPolicyRegistered` on
> any session but the topology session.

A loader that stops closes its session. One that dies holds the topology session until the cluster
times its session out, and the log records that close like any other.

**Publish order.** The loader MUST validate the whole file before publishing anything, and publishes it
on one cluster session:

1. On an archive whose groups are active, it compares the file's `<groups>` with the active
   `GroupsRegistered`, read from the default queue (§3.1), and publishes nothing if they differ
   (**S-8**).
2. It publishes the `<groups>` section as one `GroupsRegistered`, which activates the groups on a new
   archive and opens the topology session. It MUST see that frame on the queue of its file's lowest
   `groupId`, the default queue, before it publishes anything else, and stops if it does not: the file's
   groups differ from the active ones, or another load holds the topology session.
3. It reads that queue up to its `GroupsRegistered` and stops if a row's source was registered there
   with another `groupId` (**S-8**). Every queue carries every system frame from activation on, and no
   row precedes activation (§9.2 condition 13), so the queue holds every earlier row, and **S-9** keeps
   any other from following.
4. It publishes the gateway rows as one contiguous run in file order, with `remaining` counting down to
   0 on the last row; no other frame may come between them, because the sequencer synthesizes the
   bootstrap `GatewayActive` frames after that last row (§7.2). A file with no gateways publishes no
   run, and nothing is bootstrapped. Application rows and then protocol rows follow. They carry no
   countdown: the sequencer checks an application row's group (**S-8**) but builds nothing from the
   run. `SnapshotPolicyRegistered` comes last, so every participating row precedes the first round.
5. It waits for every frame it published to appear on that queue, in order, reports the first that does
   not as refused, and closes its session.

The groups come first because the sequencer refuses everything else until they are active (§9.2
condition 13), and because they open the topology session. The first `load-topology` of an archive is
therefore the first operator command: the sequencer refuses every other before it.

## 7. System messages

Eighteen events, defined in schema 210 beside the envelopes. Thirteen are submitted by producers and
travel in `UnsequencedSystem`/`SequencedSystem`. Five are synthesized by the sequencer and have templates
of their own.

| event | `systemEventType` | carried in | ingress-legal | the sequencer decodes it |
| --- | --- | --- | --- | --- |
| `ConnectionOpened` | 1 | payload | yes (producer) | no |
| `ConnectionClosed` | 2 | payload | yes (producer) | no |
| `LeadershipChanged` | 5 | template 105 | no | no (encodes only) |
| `ClusterStarted` | 10 | payload | yes (`clusterctl`) | no |
| `ClusterStopped` | 11 | payload | yes (`clusterctl`) | no |
| `ClusterHeartbeat` | 16 | template 104 | no | no (encodes only) |
| `GatewayRegistered` | 17 | payload | yes (`clusterctl load-topology`) | yes: gateway list and its `groupId` (**S-8**); bootstrap on `remaining == 0` |
| `GatewayActive` | 18 | template 106 | no | no (encodes only) |
| `GatewayStarted` | 19 | payload | yes (gateway) | yes: binds session to `gatewayId` |
| `PayloadIdRegistered` | 23 | payload | yes (`clusterctl load-topology`) | no (§6.3) |
| `GatewayActivationRequested` | 24 | payload | yes (`clusterctl activate`) | yes: validates `gatewayId`, then synthesizes `GatewayActive` |
| `ApplicationRegistered` | 25 | payload | yes (`clusterctl load-topology`) | yes: its `groupId` (**S-8**) |
| `SnapshotRequested` | 26 | payload | yes (`clusterctl request-snapshot`) | no: answered with `SnapshotStarted` (§7.3) |
| `SnapshotEnd` | 28 | payload | yes (participating source) | no |
| `SnapshotPolicyRegistered` | 29 | payload | yes (`clusterctl load-topology`) | yes: the round interval and byte threshold (§7.3) |
| `SnapshotStarted` | 30 | template 107 | no | no (encodes only) |
| `SessionClosed` | 31 | template 108 | no | no (encodes only) |
| `GroupsRegistered` | 32 | payload | yes (`clusterctl load-topology`) | yes: the group table; the first activates the groups (§7.4); opens the topology session (**S-9**) |

A submitted event's `systemEventType` equals its payload's SBE template id. The five synthesized frames
also carry their `systemEventType` (5, 16, 18, 30, 31), redundantly with the template id, so that offset 16
identifies every frame on a queue. Numbers are never reused: `GatewayActivationRequested` took 24
rather than a vacated number so that no recording can be misread.

**A system payload has no `MessageHeader`.** `header.systemEventType` identifies it. An encoder `wrap`s
the payload rather than calling `wrapAndApplyHeader`, and a decoder takes `BLOCK_LENGTH` and
`SCHEMA_VERSION` from its compiled constants. This is sound because of **V-3**: no seqeron decoder
reads bytes encoded by a different build.

**`GatewayActive` is produced only by the sequencer**, on four triggers (§7.2): bootstrap, the active
instance's session closing, an activation timeout, and a `GatewayActivationRequested`. For the last,
the sequencer validates the request against the list, sequences it, and synthesizes the
`GatewayActive` at the next `clusterSeqNo`. The operator's request is therefore on the log, and every
`SequencedSystem` frame has an `UnsequencedSystem` origin.

**`SessionClosed` bounds what a session got into the log.** The sequencer synthesizes one for every
cluster session close, gateway or not, after any `GatewayActive` that close causes (§7.2). The consensus
module appends nothing from a session once it begins closing it, and the close follows everything it did
append, so a frame from that session not on the queue before its `SessionClosed` never will be (**A-4**).

Header of a synthesized frame (§5 covers ingress only):

| field | value |
| --- | --- |
| `sourceId`, `connectionId`, `sessionId` | −1 (**F-4**) |
| `systemEventType` | the value for this template |
| `groupMask` | 0 |
| `clusterSeqNo` | the next value from the counter ingress also uses (§9.1) |
| `timestamp` | Raft consensus time, never a local clock (**S-3**) |
| `groupSeqNo` | the next value of each queue's own counter (**F-6**) |

> **S-2.** The sequencer decodes exactly the six payloads marked "yes" above, after reading
> `header.systemEventType` on every system-family ingress frame. It decodes no application payload.

An event that is not ingress-legal is rejected two ways: condition 3 rejects its template, and
condition 8 rejects its `systemEventType` inside an `UnsequencedSystem`. Condition 6 independently
rejects any ingress frame with `sourceId` −1.

These rules check well-formedness, not authorization. Aeron Cluster tolerates crash faults, not
Byzantine ones; the rules exist to catch misconfiguration and defects. Authentication is enforced at
the system's external edges and is outside this protocol.

### 7.1 Message fields

No system message has a `header` field; the frame's header composite (§4.1) serves. Every system
message except `ConnectionOpened` is fixed-length, with no var-data or repeating groups. The
synthesized templates' blocks include the 44-byte header composite.

| event | `systemEventType` | block | frame bytes | fields |
| --- | --- | --- | --- | --- |
| `ConnectionOpened` | 1 | 0 | 56 + *n* | `connectionData` varData |
| `ConnectionClosed` | 2 | 0 | 54 | none |
| `LeadershipChanged` | 5 | 56 | 64 | `newLeaderMemberId` int32, `leadershipTermId` int64 |
| `ClusterStarted` | 10 | 8 | 62 | `correlationId` int64 |
| `ClusterStopped` | 11 | 8 | 62 | `correlationId` int64 |
| `ClusterHeartbeat` | 16 | 44 | 52 | none |
| `GatewayRegistered` | 17 | 45 | 99 | `remaining` uint16, `gatewayId` int32, `gatewaySourceId` int32, `gatewayName` char[32], `preferenceRank` uint8, `snapshot` uint8, `groupId` uint8 |
| `GatewayActive` | 18 | 48 | 56 | `gatewayId` int32 |
| `GatewayStarted` | 19 | 8 | 62 | `gatewayId` int32, `firstConnectionId` int32 |
| `PayloadIdRegistered` | 23 | 36 | 90 | `payloadId` uint16, `protocolVersion` uint16, `protocolName` char[32] |
| `GatewayActivationRequested` | 24 | 4 | 58 | `gatewayId` int32 |
| `ApplicationRegistered` | 25 | 38 | 92 | `applicationSourceId` int32, `applicationName` char[32], `snapshot` uint8, `groupId` uint8 |
| `SnapshotRequested` | 26 | 8 | 62 | `correlationId` int64 |
| `SnapshotEnd` | 28 | 28 | 82 | `round` int64, `recordCount` int32, `length` int64, `crc32c` uint32, `formatVersion` uint32 |
| `SnapshotPolicyRegistered` | 29 | 12 | 66 | `intervalSeconds` uint32, `bytes` int64 |
| `SnapshotStarted` | 30 | 52 | 60 | `round` int64 |
| `SessionClosed` | 31 | 52 | 60 | `clusterSessionId` int64 |
| `GroupsRegistered` | 32 | 512 | 566 | `groupNames` char[512] |

Frame bytes are for the sequenced form: 54 + block for a submitted event, 8 + block for a synthesized
template.

Field semantics:

- `connectionData`: see below.
- `newLeaderMemberId`: the Aeron Cluster member that leads from this `clusterSeqNo` on.
- `leadershipTermId`: the term that starts here. One `LeadershipChanged` per term, including a term
  won again by the same member (that election also discarded ingress). Term ids are not contiguous; a
  failed election consumes one.
- `correlationId`: set by `clusterctl` so it can recognise its own frame on a queue.
- `remaining`: rows of the gateway run still to come after this one; 0 marks the last
  row.
- `gatewayId`: the list row's identity, unique in the list. `gatewaySourceId`: the logical gateway,
  shared by all its instances; **S-6** compares it with `header.sourceId`. `gatewayName`: US-ASCII,
  byte 0 = `0x00` means absent; an instance finds its own row by this name at start-up.
- `preferenceRank`: 0 is the designated primary; 1, 2, … are standbys of the same `gatewaySourceId`.
- `firstConnectionId`: the first `connectionId` this instance will allocate, chosen above the highest
  it saw during replay, so ids do not repeat across a restart.
- `protocolVersion`, `protocolName`: §6.3. `protocolName` is US-ASCII, padded with `0x00`.
- `snapshot`: 1 if the source takes part in snapshot rounds, else 0; the same on every row of one
  `gatewaySourceId`.
- `round`: a snapshot round's number, from 1; its cut is the `clusterSeqNo` of its `SnapshotStarted`.
- `clusterSessionId`: the Aeron Cluster session that closed. The header's `sessionId` stays −1 (**F-4**).
- `recordCount`, `length`, `crc32c`: the snapshot's records, its header included, their bytes, and the
  CRC-32C of those bytes in order. The records themselves never enter the log; they stay with the
  instances that serialized them.
- `systemEventType` 27 is retired, and ids are never reused.
- `formatVersion`: the application's, opaque to seqeron.
- `intervalSeconds`: seconds of consensus time between rounds; 0 means operator requests only.
- `groupId`: a group's identity, 1–16; bit `groupId` − 1 of `groupMask` names it. On
  `GatewayRegistered` and `ApplicationRegistered`, the group whose queue the instance reads (**A-8**), the
  same on every row of one `gatewaySourceId`.
- `groupNames`: the whole group table, sixteen 32-byte slots; slot *g* − 1 is group *g*'s name, as
  `gatewayName`, and a slot whose byte 0 is `0x00` declares no group. At least one slot declares a group
  (**S-8**). Producers name groups by these names (**A-9**).

The snapshot protocol these events carry is `doc/snapshot.md`.

**Connections.** The sequencer keeps no connection state. A consumer that tracks connections derives
its view from these frames, keyed on `(header.sourceId, header.connectionId)`. A crashed instance never
publishes the `ConnectionClosed` frames for its connections, so such a consumer treats a `GatewayStarted`
as closing every connection still open under that `gatewaySourceId`.

`ConnectionClosed` has no payload: `header.connectionId` identifies a connection every consumer has
already seen open. `ConnectionOpened` carries `connectionData`, because a producer allocates a new
`connectionId` for every connection, so a counterparty that reconnects and resumes the same session
arrives under a new id. A standby needs the mapping from id to application session before that
connection's first message, and this frame is the only place it can get it.

- The cluster tier MUST NOT decode `connectionData`. The sequencer forwards `ConnectionOpened` unopened.
- Its encoding is the producer's, identified by `header.sourceId`. A consumer that does not recognise
  the producer skips it, as **P-1** skips an unknown `payloadId`. **E-1** applies.
- It MAY be empty (length prefix 0). The sequencer MUST accept both empty and non-empty.
- It counts against `MAX_PAYLOAD_LENGTH` (§12).
- It adds no reject condition: conditions 8 and 9 already cover the event.

### 7.2 Promotion

Each synthesized `GatewayActive` names one `gatewayId`, never a `gatewaySourceId`. All instances of a
gateway share its `sourceId`, so naming the `sourceId` would designate every instance at once.

**Target.** When an instance loses the active role, the target is the list row with the lowest
`preferenceRank` among rows of the same `gatewaySourceId`, *excluding the instance that lost the
role*; ties go to the row that appears first in the log. If there is no such row, the sequencer
synthesizes nothing, logs, and does not advance `clusterSeqNo`; that gateway has no active instance
until one starts.

The rule excludes the losing instance rather than taking the next rank up. After rank 0 loses the role
to rank 1, a later loss by rank 1 returns it to rank 0. As a result, a gateway whose instances were all
down converges on whichever instance starts first.

**Triggers.**

1. Bootstrap: after the `GatewayRegistered` with `remaining` = 0, one `GatewayActive` per rank-0 row
   of a gateway no `GatewayActive` has designated yet, in list order. A later run designates only the
   gateways it adds.
2. Session close: the cluster session bound by a `GatewayStarted` (**S-6**) closes. The binding is
   removed first; then, if the most recent `GatewayActive` for that gateway names the instance the
   binding named, the target rule runs for it. A superseded instance's close promotes nothing. The
   close's own `SessionClosed` follows, promotion or not.
3. Activation timeout: a designated instance has not published a `GatewayStarted` within
   `GATEWAY_ACTIVATION_TIMEOUT_MS`. Without this trigger an instance that failed before registering
   could never be promoted past, since no session of its own would close.
4. Operator request: a `GatewayActivationRequested` naming a listed `gatewayId` designates that
   instance directly, without the target rule.

**Deadline.** `GATEWAY_ACTIVATION_TIMEOUT_MS` = 5 × `CLUSTER_HEARTBEAT_INTERVAL_MS` = 5000 ms (§12).
Every synthesized `GatewayActive`, from any trigger, arms a deadline at `timestamp` +
`GATEWAY_ACTIVATION_TIMEOUT_NS` on the consensus clock, whose unit is nanoseconds (§9.3). Repeated
failures therefore move down the list rather than stopping at the first.

At most one deadline is armed per `gatewaySourceId`; a new one replaces the old in place. Deadlines
are kept in arming order, so two gateways that bootstrap together keep separate deadlines and every
node evaluates them in the same order (**S-3**).

**Evaluation** runs on each `ClusterHeartbeat`, using its consensus timestamp and immediately after
the heartbeat frame, never on a local timer. The sequencer removes every expired deadline, in arming
order. For each, if no `GatewayStarted` has bound that `gatewayId` in the meantime, it synthesizes a
`GatewayActive` for the target, whose new deadline goes to the end of the order; otherwise it
discards the deadline. Each gateway is promoted at most once per heartbeat, and gateways whose
deadlines expire together are promoted behind the same heartbeat, in arming order.

Every input to promotion comes from the log: the list from `GatewayRegistered`, the bindings from
`GatewayStarted`, the time from the consensus timestamp. Every node therefore synthesizes the same
frame at the same `clusterSeqNo`.

### 7.3 Snapshot rounds

A round is a cut in the log: a synthesized `SnapshotStarted` whose `clusterSeqNo` is the point every
participating source serializes its state at. It is published by the cluster's snapshot action, which the leader
asks for when a round is due, so that a round is also a cluster snapshot (`doc/snapshot.md` §10). The sequencer tracks
nothing about a round once it has started; what the sources submit for it is `doc/snapshot.md`. Its state is the latest
`SnapshotPolicyRegistered`'s interval and byte threshold, the last round number, the timestamp the
interval counts from, and the bytes sequenced since the last `SnapshotStarted`: the length of every
frame published to the queues, each counted once however many queues it reaches.

**Triggers.** None fires before a `SnapshotPolicyRegistered` has been sequenced.

1. Operator request: a sequenced `SnapshotRequested` makes a round due. Without a policy, nothing follows it.
2. Interval: on each `ClusterHeartbeat`, immediately after promotion, when `intervalSeconds` > 0 and
   the heartbeat's timestamp is at least `intervalSeconds` past the last `SnapshotStarted`, or past the
   latest policy row before any round.
3. Bytes: on each `ClusterHeartbeat`, when `bytes` > 0 and the bytes sequenced, the heartbeat's own
   length included, have reached `bytes`. The count restarts at each `SnapshotStarted`, which counts
   toward the next round, and before any round at the latest policy row.

A round that is due stays due until the snapshot action publishes its `SnapshotStarted`, and a heartbeat that meets
both the interval and the byte trigger makes one round due. Rounds are numbered
from 1, and each `SnapshotStarted` supersedes the one before it. A policy row sequenced after the first
round changes the interval and the threshold but not the point either counts from.

### 7.4 Groups and producer-side routing

A group is a set of gateways and co-located applications that read one queue (§3.1). Routing is
producer-side: a producer addresses queues by setting their bits in `header.groupMask` (§4.1), where bit
*g* − 1 names group *g*. The topology names the groups and the producer chooses, frame by frame, which
of them a frame reaches; seqeron interprets no payload to decide it. The sequencer's state for groups
is the group table, each registered source's group, and one `groupSeqNo` counter per group, all derived
from the log (**S-3**).

**Why a queue per group.** An instance that reads the whole log dispatches every frame live and replays
every frame on a cold start, from `clusterSeqNo` 1 or its snapshot's cut, so its read cost and recovery
time grow with the deployment's traffic rather than its own. A queue per group bounds both by what the
group is addressed. Filtering on `groupMask` over one queue would not: a skipped frame (**P-1**) is
still replayed, relayed to a gateway host and read. Nor is a queue of every frame kept beside the
groups' queues: every frame after activation is on at least one of them (**F-1**) and the Raft log
holds the whole history, so no instance would read it.

**What it costs.** A frame is written to every queue that carries it: a system frame once per group, an
application frame once per bit in its mask, so a node's archive holds the sum of its queues. Each queue
is a recording, and a stalled one terminates its node (`doc/fault-tolerance.md` §1.3). A gateway host
runs one relay per group its instances read. Groups pay off when most application frames address one
group; a deployment whose frames mostly address every group pays the writes, saves no reads, and should
declare one group. Groups are not access control: the producer chooses the mask, and every queue
carries every system frame.

**Activation.** Until its groups are active, a log has no queue. The first `GroupsRegistered` activates
every group it declares: the sequencer builds the group table from it, then:

1. opens each group's queue and asks the node's archive to record each;
2. waits until the archive is recording all of them, for at most 5 s in all;
3. writes each queue's prologue at `groupSeqNo` 1, in ascending `groupId`: the most recent
   `LeadershipChanged`, as it was sequenced. The sequencer keeps that frame until then.

The wait holds up the node's processing of the log, so it is bounded. It is paid once, at the first
load, and again by each node's restart, whose replay reaches the same frame. A node whose archive is not
recording every queue by the deadline terminates itself, as one whose recording stops does
(`doc/fault-tolerance.md` §1.3): its peers keep quorum, and its restart replays to the same frame and
waits again. A queue is opened once, at activation, and never re-created, so a node's recording of it
is one continuous run across every leader tenure. (`SequencerService`)

It then emits the `GroupsRegistered` to every queue, as it emits every system frame from then on, so it
is the second frame of every queue. Between them, the two give a group's members what they need of the
frames before activation: the leadership state their leader gate needs (**A-1**) and the group table
they resolve names against (**A-9**). The only other frames before activation are heartbeats, earlier
`LeadershipChanged` frames and `SessionClosed` frames, none of which a consumer needs, since no ingress
frame but the table is sequenced before it (§9.2 condition 13). The table is one frame so that it is
sequenced whole or not at all: a failover cannot activate part of it.

**Selection.** An application frame goes to the queue of every group whose bit its `groupMask` sets, in
ascending `groupId`. The mask is the selection: the sequencer reads it and looks nothing up. A mask
that is 0 or names a group that is not active is refused (§9.2 condition 14), so every application
frame reaches a queue and every bit it carries names one. So is a mask, on a session a `GatewayStarted`
bound, that lacks that gateway's own group: its own queue would lack the frame (**A-9**).

**System frames are not routed.** Every queue carries every system frame (§9.2 condition 12), so each
group sees every leadership change, gateway designation, connection and snapshot frame, whichever
group its producer is in. A leader gate, a standby and a snapshot round each need these frames
complete, and their producers, the cluster among them, cannot know which groups that is. The cost is
that a gateway's `connectionData` reaches every group.

> **S-8. Groups are fixed at activation, and so is a source's group.** The sequencer MUST refuse (§9.2
> condition 11):
>
> 1. a `GroupsRegistered` that declares no group or two of whose slots carry the same name;
> 2. a `GroupsRegistered` after activation that differs from the active table in any slot. One that
>    equals it is sequenced and changes nothing, so a topology file loaded again is accepted;
> 3. a `GatewayRegistered` or `ApplicationRegistered` whose `groupId` is not an active group;
> 4. a `GatewayRegistered` or `ApplicationRegistered` whose `gatewaySourceId` or `applicationSourceId`
>    an earlier sequenced row registered with another `groupId`.

A bit therefore never changes meaning. A group cannot be added, removed or renamed on an existing
archive (§11), and a source cannot move to another group: its instances' state and snapshot files
derive from the queue they read (**A-8**). A producer's masks stay valid for its life. A restarting node
replays the log from `clusterSeqNo` 1 and activates the groups at the same frame as before, into new
recordings.

Adding groups to a running cluster is proposed, not implemented; the rules above hold until it is ([`upgrades.md`](upgrades.md) §7.4).

---

## 8. Schemas

seqeron owns three SBE schemas: the frames on the log, the replay control protocol beside it, and the hosted façade
protocol between a host and its client (§17).

| file | schema id | contains | change policy (§11) |
| --- | --- | --- | --- |
| `sbe-frame.xml` | 210 | the nine top-level templates, the four header composites, `messageHeader`, `varDataEncoding`, and the thirteen submitted system payloads | envelopes frozen; system events changeable under **V-3** |
| `sbe-replay.xml` | 212 | the eight replay control messages (§10) | changed in place, no version bump |
| `sbe-hosted.xml` | 218 | the hosted façade's messages (§17.3) | changed in place, `protocolVersion` raised (§17.7) |

The envelopes and system messages are one schema because they have one owner, ship in one artifact and
follow the same change rule. `version` stays 0 for the life of schema 210, including system message
changes: under **V-3** no decoder reads bytes from another build, so a version number would carry no
information. Schema 211 is unallocated. Schemas 111 (`sbe-cluster.xml`, Aeron's), 214
(`sbe-probe.xml`, an application payload) and 216 (`sbe-sequencer.xml`, the sequencer's own cluster-snapshot
state, written and read by one node's build alone) are not part of this protocol.

An application includes nothing from seqeron's schemas and generates nothing from them. It defines its
own payload encoding and reads frames through seqeron's compiled codecs (the jar in Java, the
installed headers in C++, the package in C#).

Both schemas' codecs ship together, one artifact per language: `ReplayerRecovery` decodes
`LeadershipChanged` (schema 210) while handling `Replaying` (schema 212).

## 9. Sequencer rules

What the sequencer does with each ingress frame: number it, validate it, decide deterministically, and
never fail silently.

### 9.1 `clusterSeqNo`

- **S-4.** `clusterSeqNo` is 1 for the first frame the cluster emits and increases by exactly 1 per
  emitted frame. It is never reused and has no gaps in the log, but a queue carries only some frames, so
  it jumps along a queue. A consumer detects loss by `groupSeqNo` (**F-6**): one that observes a gap in
  it has lost frames and MUST recover through the replayer; it MUST NOT skip.
- Forwarded and synthesized frames use the same counter.
- A rejected ingress frame does not advance the counter and leaves nothing in the log: neither the
  frame nor a record of its rejection (§9.6).
- **S-5.** Every reject condition in §9.2 is evaluated before the counter advances and before any state
  changes.
- The archive is addressed by recording position; the replayer needs no index from `clusterSeqNo` to
  position.
- A queue carries each frame's `clusterSeqNo` unchanged. Its own continuity is `groupSeqNo` (**F-6**).

### 9.2 Validation

The sequencer MUST reject an ingress frame on the first of these conditions that holds, evaluated in
order (each establishes what the next may read). It MUST NOT throw (§9.4).

| # | reject when |
| --- | --- |
| 1 | `length < MIN_INGRESS_LENGTH` (30) or `length > MAX_INGRESS_LENGTH` (§12) |
| 2 | `MessageHeader.schemaId != 210` or `MessageHeader.version != 0` |
| 3 | `MessageHeader.templateId` is neither `Unsequenced` nor `UnsequencedSystem` |
| 4 | `blockLength != 20` |
| 5 | the var-data length prefix is 65535 (`varDataEncoding`'s null value), or `MIN_INGRESS_LENGTH + bodyLength != length` |
| 6 | `sourceId == -1` (**F-4**) |
| 7 | `Unsequenced` and `payloadId` is 0 or 1 |
| 8 | `UnsequencedSystem` and `systemEventType` is not an allocated, ingress-legal value |
| 9 | `UnsequencedSystem` and the payload is shorter than that event's compiled `BLOCK_LENGTH` |
| 10 | the frame fails **S-6** |
| 11 | `UnsequencedSystem` carrying a `GroupsRegistered`, `GatewayRegistered`, `ApplicationRegistered`, `PayloadIdRegistered` or `SnapshotPolicyRegistered` that **S-8** or **S-9** refuses |
| 12 | `UnsequencedSystem` and `groupMask != 0` |
| 13 | no group is active, and the frame is not a `GroupsRegistered` (§7.4) |
| 14 | `Unsequenced` and `groupMask` is 0, sets a bit of a group that is not active, or, on a session a `GatewayStarted` bound, lacks the bit of that gateway's group (§7.4) |

A rejection is logged and counted (§9.6). Nothing is emitted and `clusterSeqNo` does not move.

Implementation notes:

- Conditions 1–6 are identical for both ingress templates; only 7–14 depend on the template.
- Conditions 4 and 5 are equalities, not bounds. A short `blockLength` would place the length prefix
  inside the header; a long one, or a length prefix short of the frame end, would silently drop bytes on
  every node. Compare condition 4 against the composite's generated `ENCODED_LENGTH`.
- Condition 3 rejects the five synthesized templates before any payload is read. Condition 8 rejects
  the same events named inside an `UnsequencedSystem`.
- The size limits are compiled-in protocol constants (§12), never read from a node's transport
  configuration per frame: a node configured differently from its peers would diverge (**S-3**). The
  MTU bounds no frame (**T-2**).
- Condition 1 is a backstop. A conforming producer's encode method refuses an oversized payload before
  sending (**T-3**).
- Conditions 6–9 and 12 depend only on the frame, so conforming encode methods (`SystemFrame`,
  `publishPayload`/`publishSystem`) refuse them too. Only conditions 10, 11, 13 and 14 can reject a
  frame from a conforming producer. §16 A-4 treats condition 10 as a fault; condition 11 means the file's
  groups change the log's or another load holds the topology session, either of which
  `clusterctl load-topology` reports when its `GroupsRegistered` does not appear (§6.4);
  condition 13 means a frame preceded the first `load-topology`; condition 14 means a producer
  addressed a group before resolving it from the log, or left out its own, which **A-9** forbids.
- Condition 9's minimum length is the decoder's compiled `BLOCK_LENGTH`, never the `blockLength` on the
  wire: an SBE decoder reads fixed fields at fixed offsets whatever block length it was given, so a
  sender-supplied length bounds nothing. It applies to every system event whose payload a later condition
  reads. Condition 8 needs no minimum; it reads only the header.

### 9.3 Determinism

> **S-3.** Every reject decision and every byte of every synthesized frame MUST be a function of the
> committed log alone: not of node configuration, wall-clock time, local state or hash-map iteration
> order.

Conditions 1–9 and 12 depend only on the frame's bytes. Conditions 10, 11, 13 and 14 read state, all
of it derived from the log (the list from `GatewayRegistered`, the bindings from `GatewayStarted`, the
designations from `GatewayActive`, the group table from `GroupsRegistered`, each source's group from
`GatewayRegistered` and `ApplicationRegistered`, and the topology session from `GroupsRegistered` and
the session closes).

- `timestamp` is the Raft consensus timestamp in nanoseconds since the epoch. Its precision and
  accuracy are those of the leader's clock.
- Synthesis over a collection (the bootstrap `GatewayActive` frames) MUST iterate in a deterministic
  order.
- Every synthesis deadline MUST be evaluated against the consensus clock, never a local timer.
- `ClusterHeartbeat` is driven by a cluster timer, not a local scheduler.
- Each group's `groupSeqNo` counter is sequencer state, rebuilt by the log's replay, from a snapshot or from the start,
  like the rest.

### 9.4 No callback may signal failure by throwing

When a cluster callback throws, `Image.boundedControlledPoll` has already advanced the log position
and `AgentRunner` keeps the agent running, so the frame is dropped and processing continues. With no
snapshots, that is a permanent hole. The same applies to clients: `Image.poll` passes the exception to
the error handler and advances the subscriber position. A client therefore records the fault and raises
it from its own duty cycle.

### 9.5 Encoding a frame

Sequencing an ingress frame: decode the `MessageHeader` and the 20-byte header; encode a new
`MessageHeader` (`Sequenced` or `SequencedSystem` to match the ingress template, `blockLength` 44,
schema 210, version 0, all constants); copy the 20-byte header prefix, offsets 16 and 18 included;
overwrite `sessionId`; append `clusterSeqNo`, `timestamp` and `groupSeqNo`; copy the length-prefixed
payload. The same code serves both families, because it never interprets offset 16, reads offset 18
only to select group queues (§7.4), and writes fields that are at the same offsets in both sequenced
composites.

A synthesized frame is one fixed-size encode: its own template, fields inline, no payload.

A frame is encoded once. Before each queue it is bound for receives it, the sequencer writes that
queue's next `groupSeqNo` into it (**F-6**); the other bytes are the same on every queue.

### 9.6 What a rejection leaves behind

> **S-7.** Every rejection MUST increment a rejected-frame counter and write a log line naming the
> failed condition and the cluster session. Nothing is emitted on the wire and nothing is written to
> the log.

- The counter is the node-local Aeron counter of type 5002
  (`SeqeronCounters.SEQUENCER_REJECTED_INGRESS_COUNT_TYPE_ID`). Every node rejects the same frames
  (**S-3**), so nodes that have applied the same log prefix MUST report the same count; a difference
  means the queues have diverged (**F-2**). A restarting node rebuilds the count during its full-log
  replay.
- The log line MUST be written and SHOULD be rate-limited. It carries the condition, the cluster
  session, the frame length, and whichever of `payloadId`, `sourceId` and `connectionId` the conditions
  before the failing one have validated. The counter stays exact while the log is rate-limited.
- The sequencer keeps no state derived from a rejection: no deny list, nothing rebuilt on replay.

A conforming producer is refused by its own encode method before sending (**T-3**), and MUST NOT
publish until it has resolved its own `{gatewayId, gatewaySourceId}` from the list on its queue (§5,
**S-6**), which keeps a `GatewayStarted` from preceding `load-topology`. The counter therefore
counts frames from non-conforming producers.

## 10. Replay control protocol

Eight messages between a node's replayer and its co-located clients, on streams 202 and 203. They carry
no header composite and are never sequenced or recorded.

| message (id) | direction | meaning |
| --- | --- | --- |
| `ReplayRequest` (6) | client → replayer | replay the active recording of the queue `queueId` names from its start (`fromPosition` = `FROM_START`), or resume it at `fromPosition` |
| `Replaying` (7) | replayer → client | attach to `replaySessionId` on stream 201 and follow it until `catchUpPosition` |
| `ReplayPending` (8) | replayer → client | not now: no slot is free, the replayer is not ready, the recording has not reached `fromPosition` yet, or the queue a `QueueRequest` asks for is not known yet; wait and resend |
| `QueueRequest` (9) | client → replayer | which queue to read: the group of the gateway instance `gatewayName` names, or of the application `sourceId` names, or, naming neither, the default queue (**R-6**) |
| `QueueResolved` (10) | replayer → client | read the queue `queueId` names |
| `SnapshotRound` (11) | replayer → every client | a snapshot round was cut on this node (`doc/snapshot.md` §10.8) |
| `SnapshotJoined` (12) | application → replayer | this application replica takes part in the round |
| `SnapshotConfirmed` (13) | application → replayer | this replica's file for the round is confirmed and durable under its name |

- **R-1.** `requestId` identifies the current request. The client increments it on every new request,
  an unchanged one included, and keeps it on a resend, and the replayer echoes it. A client MUST ignore a
  reply whose `requestId` is not its current one, and a repeat of a `Replaying` it has taken. The
  replayer answers a request whose `requestId` the client's running replay answers with that replay's
  `Replaying` again, and leaves the replay running.
- **R-2.** `NO_REPLAY_NEEDED` means nothing is to be replayed from where the request asked. For
  `FROM_START` the recording holds nothing yet and the client is at the tip. For a position, the replayer
  refuses it: it is before the recording's start or not a frame boundary in it. A client MUST treat that
  as fatal: positions follow from the frames, and neither a restart at a cut nor a purge moves a frame, so
  either the recording does not match the positions the client holds or its start has passed them
  (`doc/snapshot.md` §10.6). A position the recording has not reached yet is answered `ReplayPending`.
- **R-3.** A replay holds its slot until the replay's publication is gone: the archive has ended the
  replay and its subscriber has let it go, or the subscriber's client has died. A replay that moves has no
  time limit, but one whose subscriber reads nothing of it for `STALLED_REPLAY_TIMEOUT_MS` = 10 000 ms,
  twice the client's own replay-stall timeout, the replayer stops, freeing its slot. Its client then sees
  the image close short of `catchUpPosition` and requests again. A client releases its slot by closing its
  replay subscription.
- **R-4.** `clientId` is a correlation id the client draws once from its media driver
  (`Aeron.nextCorrelationId`), which every co-located client shares, so it is unique among the replayer's
  clients, two in one process included. A client MUST ignore a reply carrying another `clientId`.
- **R-5.** `queueId` names the queue a request is for, by its `groupId`. Every request a client sends names
  the queue it reads (**A-8**). A request for a queue with no active recording, because its group is not yet
  active or the gateway host does not relay it, is answered `ReplayPending`.
- **R-6.** A gateway or application instance learns its queue from its node's replayer, with a
  `QueueRequest`, before its first `ReplayRequest`. The replayer resolves it from the topology rows on the
  log: on a member from the sequencer's own state (**S-8**), on a gateway host from the frames its relay
  copies, since every queue carries every row (§3.1). Until the row is on the log, it answers
  `ReplayPending`. A source's group never changes (**S-8**), so a resolution holds for the life of the
  archive. A `QueueRequest` naming neither field resolves to the default queue (§3.1) on a member, and
  to the lowest queue the host relays on a gateway host. A `QueueRequest` holds no slot. The replayer
  logs, once per source, a `QueueRequest` it cannot answer with a queue it serves: the row is not on the
  log yet, or, on a gateway host, it names a group the host does not relay (`replayer.groups`). The
  instance waits unfenced, since the recovery-stall fence arms only after its first catch-up
  (`doc/fault-tolerance.md` §2.1), so alert on that log line.
- **R-7.** A request that names a cut (`cutClusterSeqNo`) asks for the active recording from the frame with that
  `clusterSeqNo`, wherever it is; `fromPosition` is only a hint at where, and need not be a frame boundary. The
  replayer answers it with a replay that starts at the frame, `ReplayPending` while the recording has not reached
  the cut, and `NO_REPLAY_NEEDED` (**R-2**) when the cut is before the recording's first frame or the recording
  passes it without a frame there, which means the cut was made in another log. The client checks the frame it
  receives, and anchors its resume at that frame's position.

The replayer serves only the active recording of each of its node's queues, whatever its first frame. With no
`<snapshots>` policy, a node's every start replays the log from `clusterSeqNo` 1 into a new recording, which
holds the whole queue. With one, a member restarted at a cut records from the cut, and a purge moves a
recording's start forward while the node runs (`doc/snapshot.md` §10.3, §10.6). A client replaying from
`FROM_START` checks that the first frame it dispatches is `groupSeqNo` 1, and stops if it is not; one restoring a
snapshot takes its first `groupSeqNo` from the frame at the cut (`doc/snapshot.md` §7).

The client detects completion by reaching `catchUpPosition`, not by the replay image closing: a
bounded replay of an active recording does not close its image at the bound. The first frame of a
resumed replay MUST be the frame the client resumed from; any other is fatal, as for R-2. One resume is
the exception: a replay from a snapshot file's cut on the queue the file was cut from (`doc/snapshot.md`
§7), whose first frame or refusal only tells the client whether the file was cut from this log.

Schema 212 changes in place without a version bump. Nothing records it, and a node's replayer and its
clients are built and restarted together.

### 10.1 Message fields

Each message is a standalone SBE message with its own 8-byte `MessageHeader` (schema 212), so its
encoded length is 8 + block. None has var-data or repeating groups.

| message | id | block | fields |
| --- | --- | --- | --- |
| `ReplayRequest` | 6 | 36 | `clientId` int64, `requestId` int64, `fromPosition` int64, `queueId` int32, `cutClusterSeqNo` int64 |
| `Replaying` | 7 | 32 | `clientId` int64, `requestId` int64, `replaySessionId` int64, `catchUpPosition` int64 |
| `ReplayPending` | 8 | 16 | `clientId` int64, `requestId` int64 |
| `QueueRequest` | 9 | 52 | `clientId` int64, `requestId` int64, `sourceId` int32, `gatewayName` char[32] |
| `QueueResolved` | 10 | 20 | `clientId` int64, `requestId` int64, `queueId` int32 |
| `SnapshotRound` | 11 | 16 | `round` int64, `clusterSeqNo` int64 |
| `SnapshotJoined` | 12 | 20 | `clientId` int64, `sourceId` int32, `round` int64 |
| `SnapshotConfirmed` | 13 | 20 | `clientId` int64, `sourceId` int32, `round` int64 |

- `clientId`: the client's id on its media driver (**R-4**). It addresses a reply on the shared control
  stream; `requestId` matches it to a request (**R-1**).
- `requestId`: echoed in every reply.
- `fromPosition`: the position to resume the active recording at, or `FROM_START` = −1 for its start.
- `queueId`: a `groupId`, 1–16 (**R-5**).
- `cutClusterSeqNo`: the `clusterSeqNo` of the snapshot cut a restore replays from, `fromPosition` then being
  only a hint at where it is; `NO_CUT` = −1 on every other request.
- `round`: a round's number, as `SnapshotStarted.round`. `SnapshotRound` is addressed to no client: every
  application replica and gateway instance on the stream answers it with a `SnapshotJoined`, and an instance
  that takes no part in snapshots answers nothing. `SnapshotRound` is not a reply, so **R-1** and **R-4**
  do not apply to it. A replica sends `SnapshotJoined` and `SnapshotConfirmed` again until the offer lands.
- `sourceId`: an application's `applicationSourceId`, a gateway's `sourceId` on `SnapshotJoined` and `SnapshotConfirmed`, or −1. `gatewayName`: a gateway instance's, as in
  `GatewayRegistered`, byte 0 = `0x00` meaning absent. A client MUST NOT name both; the replayer logs a
  request that does and does not answer it (**R-6**).
- `replaySessionId`: the Aeron replay session to attach to on stream 201, or `NO_REPLAY_NEEDED` = −1
  (`Aeron.NULL_VALUE`). There is no separate message for "nothing to replay".
- `catchUpPosition`: where the client stops following the replay and switches to the live queue,
  de-duplicating on `clusterSeqNo`.

**Slot limits.** A replayer serves at most `MAX_CONCURRENT_REPLAYS` = 4 replays at once, one while any of its
queue recordings trails its queue by more than a quarter of the publication window, and answers
`ReplayPending` beyond that. A slot frees when its replay's publication is gone (**R-3**): within a duty
cycle of the client closing its replay subscription, which it does at `catchUpPosition`, and within the
media driver's client liveness timeout (`aeron.client.liveness.timeout`, 10 s by default) of a client
dying. It frees within `STALLED_REPLAY_TIMEOUT_MS` of a client that is alive but has stopped reading. A
consumer's recovery-stall timeout MUST exceed the longer of the two by a margin. A client that receives
`ReplayPending` waits at its gap and delivers nothing, so a shorter timeout would fire against a
replayer that is working correctly. The client tier's default is 60 000 ms.

## 11. Versioning

This section covers seqeron's two schemas. Payload versioning belongs to the application (**V-2**).

| change | policy | affects |
| --- | --- | --- |
| schema 210 envelopes | frozen; any change is a new wire format | every producer and consumer |
| a submitted system event in schema 210 | allowed under **V-3**: a new `systemEventType` inside an unchanged envelope | the cluster tier and consumers of the system family |
| a synthesized system event in schema 210 | a frame-layer change: only the sequencer emits it, and it needs a template id | every producer and consumer |
| the `<groups>` section of the topology, and each source's group | fixed at the first topology load for the life of an archive, as is each source's group (**S-8**); adding groups is proposed ([`upgrades.md`](upgrades.md) §7.4) | every producer and consumer |
| schema 212 | in place, no version bump (§10) | one node's build |
| an application schema | the application's | the application |

System messages have no `sinceVersion` fields: under **V-3** no decoder reads another build's bytes,
so there is nothing to stay compatible with.

- **V-1. Library versions are part of the wire format.** Every producer, consumer and cluster node
  MUST use the Aeron, Agrona and SBE versions seqeron was built with; a mismatch corrupts frames rather
  than failing the build. seqeron publishes these versions: `seqeron-bom` for Java, for C++ the
  build fetches the pinned Aeron, and the installed package requires at least that version, and the C#
  package names its dependencies exactly. The versions an application uses for its own payload codecs
  are its own choice.

  **The one exception is the C# client**, on Aeron.NET, which trails Java: Aeron.NET 1.52.2 against
  Aeron 1.53.2, and the SBE C# runtime 1.23.1.1 under codecs generated by SBE 1.40.2 (`aeronDotnet` and
  `sbeDotnetRuntime` in `versions.properties`). The two Aeron versions agree on everything a client
  touches: the CnC version, the driver command codes, the log-buffer layout, the frame version and the
  cluster client protocol (0.3.0). Their cluster codecs differ only in `ServiceTerminationPosition`,
  which passes between the consensus module and the service container and never reaches a client.
  `csharp-client-test.sh` is the evidence, against a live cluster. It is rerun on every Aeron upgrade
  before the release is tagged, and `aeronDotnet` names the version it passed with.

  A hosted stub (§17) is held to its driver's CnC version alone, not to the Aeron version (§17.7).
- **V-2. Payload versioning is the application's.** A payload has exactly one encoder, its producer
  (**E-1**), so no two implementations need to agree; only an application's own old and new builds do.
  How it signals versions and what compatibility it guarantees are not specified here.
- **V-3. A change to schema 210 MUST NOT be rolled out across a running cluster, and no seqeron decoder
  may be required to read bytes encoded by an earlier build.** Two builds that encode the same
  synthesized frame differently break **F-2**; an archive that survives an upgrade is the same fault
  later. After such a change, every node's archive is purged. Schema 212 is exempt: it is not recorded
  and never leaves the node. A change to what §9.2 rejects is the same kind of change: a log replayed
  under another rule sequences another history. A node enforces this at start-up: `LogVersion`, the major of
  Aeron's `appVersion`, is raised by every such change, and a node started on a log or snapshot of another
  major stops with "incompatible version" rather than sequence a different history (`doc/ops.md`).

## 12. Limits

The protocol's fixed sizes and timings. They are compiled into every implementation, never configured.

| limit | value | reference |
| --- | --- | --- |
| `blockLength` | 20 on ingress templates, 44 on `Sequenced`/`SequencedSystem` (exact, §9.2); 44, 56, 48, 52 and 52 on templates 104, 105, 106, 107 and 108 | §4.2, §7.1 |
| per-message overhead | 94 bytes on ingress (32 Aeron data header + 32 Aeron Cluster session header + 30 frame); 86 on the queue (32 + 54) | §4.2 |
| `MAX_PAYLOAD_LENGTH` | 8874 bytes = 8960 (IPC MTU) − 86 | **T-2** |
| `MAX_INGRESS_LENGTH` | 30 + 8874 = 8904 bytes (condition 1) | §9.2 |
| `MIN_INGRESS_LENGTH` | 30 bytes (condition 1) | §4.2 |
| maximum sequenced frame | 54 + 8874 = 8928 bytes | §4.2 |
| consensus log | MUST carry a 32 + 8904 = 8936-byte message: a term length of at least 128 KiB | cluster configuration |
| `clusterSeqNo` | int64, starting at 1 | §9.1 |
| `groupSeqNo` | int64, starting at 1 on each group's queue | **F-6** |
| `timestamp` | int64, epoch nanoseconds (Aeron `NanosecondClusterClock`); a log recorded in another unit is refused | §9.3 |
| `CLUSTER_HEARTBEAT_INTERVAL_MS` | 1000 | §7, §9.3 |
| `GATEWAY_ACTIVATION_TIMEOUT_MS` | 5000 | §7.2 |
| `MAX_CONCURRENT_REPLAYS` | 4, across all of a replayer's queues | §10.1 |
| `groupMask` | uint16; bit *g* − 1 names group *g*; never 0 on an application frame | §4.1, §7.4 |
| `MAX_GROUPS` | 16, the width of `groupMask`; `groupId` 1–16 | §3.1, §7.4 |
| `QUEUE_STREAM_ID_BASE` | 2000; group `groupId`'s queue is stream 2000 + `groupId` | §3 |

These constants belong to the protocol, not to any one component. Each language compiles in a copy:
`protocol/FrameLayer.java`, the `Limits` block of `protocol/SequencedFrame.hpp`, and `Protocol/FrameLayer.cs`.
A change starts here, is made in all three, and is a wire change (**V-3**).

> **T-2.** A queue frame of `MAX_PAYLOAD_LENGTH` (8874) is 32 + 8928 = 8960 bytes with its Aeron data
> header: one fragment at an IPC MTU of 8960, the largest a 9000-byte jumbo frame carries. The constant
> is compiled in and is not configurable. The MTU is not part of the protocol: a frame longer than the
> MTU less 32 is fragmented. Every stream that carries frames MUST have a term length of at least
> 128 KiB, so the largest message, 8936 bytes on the consensus log, fits Aeron's term / 8 limit.

> **T-3.** The protocol's encode methods MUST refuse a payload longer than `MAX_PAYLOAD_LENGTH`;
> condition 1 is the backstop. The refusal is local and permanent: it is not back-pressure and MUST
> NOT be reported as retryable. What the producer does with the refused message is its own concern
> (**P-0**).

The encode methods are `SystemFrame.wrap`/`wrapPayload` (Java), `SystemFrame.Wrap`/`WrapPayload` (C#) and
`publishPayload`/`publishSystem` (C++). They report refusal as a return value, not an exception, because several callers run inside
Aeron poll callbacks, where an exception is swallowed (§9.4).

**A frame may be fragmented.** On the queue, its recording and a replay, a frame longer than the IPC MTU
(`aeron.ipc.mtu.length`) less 32 spans several fragments; on ingress and the consensus log the same holds
at the UDP MTU (`aeron.mtu.length`), where Aeron Cluster reassembles it. A consumer of the queue or a replay
MUST reassemble, as Aeron's `FragmentAssembler` does. Once frames fragment, a recording's positions
depend on its MTU, so every member and gateway host SHOULD use the same IPC MTU.

The MTU is a deployment's trade. At Aeron's default of 1408, payloads up to 1314 bytes never fragment
anywhere. An IPC MTU of 8960 keeps every frame whole on the queue, but an archive replay carries its
recording's MTU, so each UDP replay — a gateway host's relay, a follower on another host — then needs
jumbo frames on its path, small frames included, since Aeron batches them into datagrams up to the MTU.
Raising `MAX_PAYLOAD_LENGTH` is a protocol change (§11), not a deployment setting.

A payload larger than `MAX_PAYLOAD_LENGTH` must be split across frames or sent by another channel. A
split needs its own completeness rule; a `remaining` countdown, as in `GatewayRegistered`, is one.

## 13. Tooling and payload encodings

How a recording is read outside a client, and what an application may put in a payload.

### 13.1 `SbeLogPrinter`

`SbeLogPrinter` (`sbe-log-printer.sh`) decodes schema 210 in full: every system message, each payload
decoded through its `systemEventType`. It does not decode application payloads. It labels each payload
with the `protocolName` from any `PayloadIdRegistered` frames in the recording (§6.3), and otherwise
with its number.

`-o <payloadId>` writes that protocol's payloads to stdout, raw and back to back, with no framing
between them, and sends every text line (the catalog line, the frame dump, errors) to stderr. A decoder
that owns the payload schema reads stdout:

```
sbe-log-printer.sh <archive-dir> --stream 2001 -o <payloadId> | application-decoder
```

The payload's own schema delimits consecutive payloads. The frame dump on stderr is emitted in the same
order and serves for correlation.

It reads one queue's recording, `--stream` 2000 + `groupId`, labels each bit of a frame's `groupMask` with
the group's name from the `GroupsRegistered` frame, and shows each frame's `groupSeqNo`. No queue holds the
whole log; the Raft log recording does (`--schema cluster`).

### 13.2 Payload encodings

> **E-1.** An application or system payload that arrived through ingress is encoded once, by its producer, and
> is never re-encoded after sequencing. The exception is the five synthesized frames, which have no
> producer: every node encodes its own copy.

An application payload may use any encoding: SBE, protobuf, raw FIX, a proprietary binary. Every
replica receives the payload's bytes (through Raft, or from its own recording), so two encoders never
need to agree.

The frame and the system messages MUST stay fixed-layout SBE and MUST NOT become tag-encoded.
Synthesized frames are encoded independently on every node and must be byte-identical (**F-2**,
**S-3**).

The length prefix delimits a payload and `payloadId` names its type; no wrapper type such as
protobuf's `Any` is needed. What each encoding offers for **P-4** differs: SBE has
`MessageHeader.schemaId`; raw FIX has `BeginString` (tag 8, always first) and `MsgType` (tag 35);
protobuf has nothing, so `payloadId` alone identifies the protocol and the allocation rules of §6.1
are the only protection. Beyond that, the application's own validation catches a wrong decoder, except
between versions of the same protocol (**V-2**).

## 14. Conformance suite

The suite that holds the implementations to this document. `ConformanceTest` sits beside `SequencerTest`
in the Java suite, in `core_tests` for C++, and in the C# suite. It uses no Aeron runtime or media driver
and runs in under a second. The payload fixture is seqeron's own.

| # | asserts | rule |
| --- | --- | --- |
| 1 | **Copy fidelity.** A sequenced `Unsequenced` carries a byte-identical payload and an unchanged `payloadId`, for payloads of 0 bytes, 1 byte and `MAX_PAYLOAD_LENGTH` | §5 |
| 2 | **Prefix property.** Bytes 0–19 of each unsequenced composite equal those of its sequenced counterpart for the same values, offsets 16 and 18 included; the two families' composites are byte-identical; encoded lengths are 20 and 44 | **F-3** |
| 3 | **System frames round-trip.** Each of the thirteen submitted events, wrapped as `UnsequencedSystem` and sequenced, keeps a byte-identical payload and its `systemEventType`; `ConnectionOpened` with empty, short and maximum-length `connectionData`. Each synthesized template, encoded by the sequencer, has the right template id, inline fields and `systemEventType` | §7 |
| 4 | **Rejection table.** One case per §9.2 condition; each asserts the frame was not sequenced, nothing was emitted, `clusterSeqNo` did not move and the rejection counter rose by exactly 1. Cases: over `MAX_INGRESS_LENGTH` and under 30 (1); wrong `schemaId`, non-zero `version` (2); a non-ingress template, including each synthesized one (3); `blockLength` below and above 20 on both ingress templates (4); length prefix past the frame end, short of it, and 65535 (5); `sourceId` −1 (6); `payloadId` 0 and 1 (7); an unallocated `systemEventType`, the retired 27 and each synthesized one (8); a `GatewayStarted`, `SnapshotEnd`, `GroupsRegistered` or `ApplicationRegistered` payload one byte short of its block (9); **S-6**'s cases (10); **S-8**'s and **S-9**'s cases (11); a system frame with `groupMask` 1 (12); a `ClusterStarted`, a `SnapshotRequested`, a `ConnectionOpened` and an application frame before activation (13); an application frame with `groupMask` 0, one setting the bit of a group the table does not name, and one on a bound gateway session without that gateway's bit (14) | **S-4**, **S-5**, **S-7** |
| 4a | **Rejection denies nothing and repeats identically.** Two rejections under one `payloadId` each increment the counter and emit nothing; a well-formed frame with that `payloadId` afterwards is accepted unchanged | **S-7**, **C-2** |
| 4b | **The producer refuses before sending.** A payload of exactly `MAX_PAYLOAD_LENGTH` is published; one byte longer is refused with nothing offered to the transport, distinguishably from back-pressure, and the next well-formed publish succeeds. Uses an in-memory transport seam | **T-3**, §12 |
| 5 | **Synthesis is deterministic.** Two independent `Sequencer`s fed the same input emit byte-identical frames, heartbeats and snapshot rounds included | **S-3**, **F-2** |
| 6 | **Layout matches §4.** Every header field is at the §4.1 offset in all four composites; sizes match §4.2 (`MessageHeader` 8, composites 20 and 44, length prefix 2, overhead 30 and 54, `ClusterHeartbeat` 52, maximum payload frame 8928); payloads of 0 bytes, 1 byte and `MAX_PAYLOAD_LENGTH` round-trip | **F-2**, **F-3**, §12 |
| 7 | **Selective consumption.** A consumer given an unallocated `payloadId` and an unhandled `systemEventType` skips both without error, and its continuity tracking advances across them, for all six sequenced messages | **P-1**–**P-3** |
| 8 | **S-6.** A `GatewayStarted` matching its list row and naming the designated instance binds and is accepted. Rejected: the same frame with a different `gatewaySourceId`; an unlisted `gatewayId` claiming a listed `sourceId`; a `GatewayActivationRequested` for an unlisted `gatewayId`; a `GatewayStarted` for a listed instance not designated; another system frame with a listed `sourceId` on an unbound session; a system frame and an application payload on the bound session once a sibling is designated; an application payload on a session a later `GatewayStarted` for the same instance replaced. Accepted: an application payload with a listed `sourceId` on an unbound session, and a `clusterctl` marker (`sourceId` 2, `connectionId` −1) | **S-6** |
| 9 | **Promotion.** With ranks 0, 1, 2 under one `gatewaySourceId`: bootstrap activates rank 0 only; a `GatewayActivationRequested` is sequenced and answered at the next `clusterSeqNo`; a second run designates only the gateways the first did not; closing rank 0's session promotes rank 1; closing rank 1's promotes rank 0; an instance that publishes no `GatewayStarted` is replaced after exactly `GATEWAY_ACTIVATION_TIMEOUT_MS` of consensus time and not before; one that does arms nothing further; a `gatewaySourceId` with one row synthesizes nothing on close and `clusterSeqNo` does not move. Two gateways bootstrapped together keep separate deadlines | §7.2, **S-3** |
| 10 | **Group selection.** With three groups: each application frame reaches exactly the queues its `groupMask` names, for masks of one bit, two bits and every group's bit, byte-identical on each but for `groupSeqNo` and written in ascending `groupId`; every system frame, synthesized ones included, reaches every queue; frames before activation reach none; each queue's `groupSeqNo` runs from 1 without a gap. With one group, its queue carries every frame after activation | **F-5**, **F-6**, §7.4 |
| 11 | **Group activation.** Each queue opens with the most recent `LeadershipChanged` at `groupSeqNo` 1 and the `GroupsRegistered` at 2, byte-identical on every queue but for `groupSeqNo`. Refused: each **S-8** case, a table that adds a group among them. Accepted: another session's `GroupsRegistered` once the topology session has closed. Accepted and changing nothing: the same `GroupsRegistered` again after activation, and a gateway or application row registered again under its source's existing group. Two `Sequencer`s fed the same input write byte-identical queues with identical `groupSeqNo`s | **S-8**, **S-3**, **F-2** |
| 12 | **Queue continuity.** A consumer of a queue accepts its first frame only at `groupSeqNo` 1, tracks continuity by `groupSeqNo` across jumps in `clusterSeqNo`, reports a gap on a missing `groupSeqNo`, and drops a frame whose `clusterSeqNo` it has passed | **F-6**, **P-3** |

Rows 2, 3, 4b, 6, 7 and 12 run in every language; rows 1, 4, 4a, 5, 8, 9, 10 and 11 are Java only, because the
sequencer is Java only.

Each row builds its frames from this document's rules; there is no stored byte corpus. A failure names
the rule and field that broke rather than reporting that bytes changed. A layout property that a
corpus would catch and these rows do not belongs in §4.

The suite checks framing and copy fidelity only; it never decodes an application payload.

---

## 16. Producer and replica obligations

What an application must do so that the order of §9.1 reaches its own consumers intact. seqeron cannot
enforce these rules, since they concern behaviour after the queue. The client tier implements them in
`app`, in every language; the sender half of A-5 is in `sequencer.client`. A-6 and A-7
concern sources that take part in snapshot rounds (§7.3, `doc/snapshot.md`).

- **A-1. Leader-only work is gated.** A co-located replica MUST perform a leader-only side effect only
  while it is caught up and its own member is leader. Every applied `LeadershipChanged` closes the gate,
  including one that names the same member again, because a reply sent during that election may have
  been lost with it. (`LeaderGate`)
- **A-2. Leader-only work is tracked as outstanding.** A request is outstanding on every replica from
  its sequenced request until its sequenced reply. The leader MUST dispatch every outstanding request not
  yet dispatched since its gate opened, in `clusterSeqNo` order. A failed reply offer MUST release that
  dispatch. (`OutstandingWork`)
- **A-3. A re-emission is identical.** A-2 delivers at least once. The reply MUST be a pure function of
  the sequenced request and SHOULD be keyed on the request's `clusterSeqNo`, so that a duplicate is
  byte-identical. Consumers MUST drop duplicates by that key.
- **A-4. Ingress is confirmed on the queue, not at send.** A successful send means the frame reached the
  leader's ingress, not the log; egress confirms nothing. A producer that must not lose a frame MUST
  hold it as pending until its own queue shows it, matched by cluster session id, never by `sourceId`
  (which a gateway pair shares). A frame stamped with term T that has not appeared by the first
  `LeadershipChanged` with `leadershipTermId` > T is lost. The lost frames are exactly the newest
  frames stamped T, because a leader drops ingress stamped with any other term and every election
  discards unread ingress. A frame from a session that has not appeared by that session's
  `SessionClosed` is lost too (§7). A session whose open never committed has no `SessionClosed`, but was
  lost to an election, which the term bounds. If the producer sees an own frame that differs from its oldest pending one,
  per-session order no longer holds (usually because the sequencer rejected a frame, S-7), and the
  producer MUST fence. (`PendingSends`)
- **A-5. Lost frames are resent before anything new.** From the first sign of a newer term (egress
  `NewLeader` or the queue's `LeadershipChanged`, whichever comes first) until every older pending frame
  has been seen or resent, the producer MUST NOT place a new frame. This includes a send already
  retrying through the election: the sender learns of the new leader inside that retry, abandons the
  send and reports it not placed, with the session still open (`setIngressHold`, in both
  `ClusterStreamSender`s). The same holds from the replacement of a session until every frame pending
  on the old one has been seen or resent, since only the queue settles them. Lost frames MUST be resent
  oldest first, once the sender stamps a term the queue has reached, on a session the queue has not shown
  closed. A frame already in the log is never pending after it appears on the queue, so a resend
  creates no duplicate. (`PendingSends`)
- **A-6. Snapshot records are deterministic.** Every instance of a source MUST serialize the same state at
  a round's cut to the same records, byte for byte: no hash-map iteration order, no local time, no node
  identity. The header the façade writes is part of the records, and is identical on every instance by
  construction. (`SnapshotListener`)
- **A-7. A divergent instance stops.** An instance whose own `recordCount`, `length` or `crc32c` for a round
  differs from its source's sequenced `SnapshotEnd` no longer holds the state the log implies, and MUST
  stop. The sequenced end is the reference: a publisher that diverged has broken A-6. The façades
  fence with `SNAPSHOT_DIVERGED`. (`SnapshotTaker`)
- **A-8. An instance reads its group's queue.** Every instance of a gateway or an application MUST read
  the queue its `GatewayRegistered.groupId` or `ApplicationRegistered.groupId` names, as its node's
  replayer resolves it (**R-6**), since instances of one source on different queues would apply different
  frames. Its snapshot files are cut from that queue (`doc/snapshot.md`). A consumer or a
  tool has no row and reads the queue it is configured with; a tool's default is its replayer's default
  queue (**R-6**), whose group is then its own (§3.1). (`Application`, `Gateway`)
- **A-9. A producer names the groups it addresses, and resolves them from the log before its first
  frame.** A producer is configured with group names, never bits. It resolves them once, when it has caught
  up: from the latest `GroupsRegistered` on the queue it reads, or from its snapshot header when none
  follows the cut (`doc/snapshot.md`). It MUST stop on a name the table does not carry (the façades
  fence with `GROUP_UNKNOWN`), and publishes nothing before it has resolved them. An active group never
  changes (**S-8**), so one resolution holds for the producer's life and the hot path carries a mask
  resolved once. The publish path MUST
  itself add the producer's own group's bit to every frame: a frame missing from its own queue is
  counted lost and resent (**A-4**, **A-5**), and the resend duplicates it on every other queue it
  reached under a new `clusterSeqNo`, which **A-3** cannot drop. A reply carries its request's `groupMask`, which already
  names the requester's group and the replier's, so every group that saw the request, and holds it
  outstanding, sees its reply (**A-2**). The façades resolve the names they are built with, and hand
  the table to the producer's listener once, before the publish gate opens, as an immutable
  `GroupDirectory`: names to masks and `groupId` to name. (`Application`, `Gateway`)

A-4 and A-5 apply within one producer process. A restarted producer, or a standby promoted in its
place, starts with nothing pending; and a frame lost with neither a leader change nor a session close
(an ingress image that drops and rejoins within the session timeout) has no boundary to be counted
against.

---

## 17. Hosted façade protocol

Its rules carry the prefix **H**. Its ids are schema 218 (§8), streams 301–304 (§3) and counter type ids
5300–5303 and 5297 (`ops.md`, "Counter type ids"). [`hosted-facade.md`](hosted-facade.md) is its design record: what
it adds to and retires from the tree, its open questions, its measurements and the prototype's results.

A **hosted façade** splits an `Application`, `Gateway` or `Consumer` across two processes on one host. The
**host** is a Java process that runs the real façade (`app`) and owns everything of §16 that concerns the
cluster: the session, confirmed ingress (**A-4**, **A-5**), the leader gate (**A-1**), group resolution
(**A-8**, **A-9**), fencing, the replay and recovery path, and the snapshot files and rounds (**A-7**). The
**client** is the application, in any language, whose only seqeron code is a thin stub with the façade's
operation names. The host calls the client's listener over a stream rather than in-process; the client
calls `publish`, `reply` and the connection operations over a second.

**The aim is that no client re-implements an Aeron API or ports a seqeron state machine.** Today the C++
client hand-writes the Aeron Cluster client protocol (`ClusterStreamSender`; Aeron's C++ client has no
cluster API), and both the C++ and C# clients port `PendingSends`, `ReplayerRecovery`, `SnapshotStore` and
the façades from Java, case for case. Hosted, they carry none of it, at the cost of one IPC hop per message.
The motivation is a smaller code base, and with it less maintenance and complexity;
[`hosted-facade.md`](hosted-facade.md) §1 sets what is added against what is retired.
**The first stage is C++ alone**: the direct C++ façades are retired and the hosted stub is the only C++ client.
The C# client stays direct. Java's direct façades stay, since the host is built on them.

The client keeps what is the application's: its state, a deterministic reply (**A-3**), deterministic
snapshot records (**A-6**) and `OutstandingWork` (**A-2**). The host's façade is built with no tracker.

**The protocol reuses Aeron's and the replayer's patterns rather than inventing its own.** The host and client
stand in the relation of Aeron Cluster's consensus module and its clustered service, which Aeron also runs as
two processes over IPC:

| here | precedent |
| --- | --- |
| stream 303: the façade's callbacks, in order | the log stream a clustered service follows |
| stream 304: commands and snapshot records, always read, commands queued on the host (**H-5**) | the service-to-consensus-module stream, read in full, service messages queued in `PendingServiceMessageTracker` |
| a command carries the gate epoch and is dropped once it has passed (**H-3**) | ingress carries `leadershipTermId`, and `ConsensusModuleAgent.onIngressMessage` drops a message of another term |
| `Published` is an offer, which an epoch change can lose without notice (**H-4**) | `MOCKED_OFFER`: a clustered service's offer on a follower succeeds and sends nothing |
| liveness and lag in counters, not messages (§17.2.1) | the driver's client heartbeat-timestamp counter, Aeron's position counters |
| `Attach` with `clientId` and `requestId`, a resend answered again, `AttachPending` (**H-8**) | the replayer's `ReplayRequest`: **R-1**, **R-4**, `ReplayPending` |
| shared streams to attach, then a session's own image (**H-15**) | streams 202/203, then `Replaying.replaySessionId` on stream 201 |
| `SnapshotCut` answered by records and `SnapshotEnd` (**H-6**) | a snapshot in log order answered by `ServiceAck` |

### 17.1 Scope

- Host and client share a host and a media driver: the member's, or a gateway host's. The streams are
  `aeron:ipc`; there is no UDP binding. A client on a host with no driver and no JVM is out of scope.
- Trust is the Aeron directory's: streams 301 and 304 are writable and streams 302 and 303 readable by any process
  that can open the host's directory, so the first process to attach wins (**H-8**). That is the boundary which
  guards the queue and cluster ingress today. Restrict the directory to the application's user; `clientId`
  (**H-15**) separates clients and is not a secret.
- One client per host instance. A host runs one façade, for one `sourceId` (or gateway pair instance, or
  consumer), configured through the host's own properties, and is named by a `hostName` unique on its driver; the
  protocol carries no façade configuration. One process may run several hosts, each on its own thread with its own
  Aeron client, and on a gateway host `ReplayerServer` runs them beside its Replayer (§17.6).
- A hosted `Consumer` is the same protocol with fewer messages, as the direct `Consumer` has fewer callbacks: it
  receives `Payload`, `CaughtUp`, `ClusterTime` and `Fenced`, and sends only `Attach` and `Detach`. It has no
  stream 304, no `Gate`, `Role`, `GroupsResolved`, restore or `SnapshotCut`, and the host opens no cluster session
  for it. Its read path (`ReplayerRecovery`, `ReplayerStreamReceiver`) is most of what a client would otherwise
  port, so a consumer that is not hosted is the exception.

### 17.2 Transport

| stream | channel / stream id | direction | publication | carries |
| --- | --- | --- | --- | --- |
| attach requests | `aeron:ipc?term-length=64k`, stream 301 | client → host | shared by every client on the driver | `Attach`, `Detach` |
| attach replies | `aeron:ipc?term-length=64k`, stream 302 | host → client | shared by every host on the driver | `Attached`, `AttachPending` |
| events | `aeron:ipc`, stream 303 | host → client | one exclusive publication per attachment | the façade's callbacks |
| commands | `aeron:ipc?term-length=1m`, stream 304 | client → host | one exclusive publication per attachment | commands, `Activated`, snapshot records |

All four are unrecorded, carry schema 218, and never leave the node. Streams 303 and 304 are reliable. A host
subscribes to 301, and a client to 302, untethered (`tether=false`), as a client subscribes to the replayer's 203: a
reader that stops polling is left behind rather than holding back every other attach on the driver, and attach is
resend-driven (**H-8**), so an `Attach` or reply it missed is sent again. A `Detach` is not resent: a host that
misses one ends the attachment by **H-9** instead. **T-1** applies:
a reader MUST check `MessageHeader.schemaId` is 218. A message above one MTU is fragmented by Aeron, up to an
eighth of its stream's term. Stream 303 takes the driver's 16 MiB IPC default (`ops.md`), since its window is what
absorbs a client that falls behind. Stream 304 takes 1 MiB: the host reads it every duty cycle (**H-5**), so it
needs no depth, and its 128 KiB message limit holds a snapshot record (at most 65,535 bytes) and the largest
command; at the default it would hold 48 MiB of the driver's shared memory per attachment. Streams 301 and 302
carry a few small messages per attachment, so they take Aeron's minimum term, 64 KiB, whose 8 KiB message limit an
`Attach` is far below; at the default they would hold 96 MiB between them.

Streams 301 and 302 are the replayer's 202 and 203 for attachment: every host reads 301, and a client reads 302
only until it is attached; untethered, neither back-pressures another's attach. Streams 303 and 304 are
an attachment's own, as a replay is on stream 201: each side subscribes with the other's session id in the
channel (`aeron:ipc?session-id=`), so a reader sees only its attachment's image and a reliable publication is
held back only by its own peer.

Neither end batches beyond what the transport does: one `Payload` per queue frame, so a cut, a `Gate` edge or a
`Connection` falls between messages and needs no rule about a batch boundary, and each end polls with a fragment
limit per duty cycle.

#### 17.2.1 Counters

Liveness and lag travel outside the streams, in the driver's counters, as the driver tracks each Aeron client by a
heartbeat-timestamp counter (`DRIVER_HEARTBEAT_TYPE_ID`). Nothing a stream holds or back-pressures can delay them.

| counter | written by | value | read by |
| --- | --- | --- | --- |
| client heartbeat | client | its monotonic clock in ms, every duty cycle, every spin of a retry (**H-5**) and every `keepAlive()` | host: `CLIENT_LOST` (**H-9**) |
| client applied | client | `position` of the last `Payload` applied | host: lag (**H-14**) |
| host heartbeat | host | its monotonic clock in ms, every duty cycle, every spin of a wait and every spin of the façade's ingress send (**H-10**) | client: `HOST_LOST` (§17.6) |
| host retired | host | total SBE length of the windowed commands that have left the host's queue, placed or dropped | stub: the command window (**H-5**) |

Each side allocates its own two counters with Aeron's `addCounter`, labelled with its `clientId`, before it
attaches; `Attach` and `Attached` carry their ids and registration ids. **A heartbeat is stale when its value has
not changed for the timeout, measured on the reader's own monotonic clock**, so no clock is compared across
processes, and a reader that was itself blocked finds the counter changed and judges the writer alive. A counter its
owner's driver has freed is stale. The driver may reuse a freed id, so a reader also takes a counter as stale once
its state is no longer allocated or its registration id differs from the one it was given.

### 17.3 Messages

Attach, streams 301 and 302:

| message | stream | fields | meaning |
| --- | --- | --- | --- |
| `Attach` | 301 | `clientId`, `requestId`, `hostName`, `kind` (Gateway, Application, Consumer), `protocolVersion`, `aeronVersion`, `commandSessionId`, `heartbeatCounterId` and `appliedCounterId` with their registration ids, `formatVersion`, repeating `supportedFormat` | starts a façade (**H-8**); `commandSessionId` is the client's stream 304 publication, absent for a Consumer |
| `Detach` | 301 | `clientId` | orderly shutdown (**H-9**) |
| `Attached` | 302 | `clientId`, `requestId`, `eventSessionId`, `heartbeatCounterId` and `retiredCounterId` with their registration ids, `commandWindow`, `sourceId` | accepts an `Attach`, or refuses it with `eventSessionId` `NO_ATTACH` (−1) and a `reason` (`BUSY`, `VERSION`, `KIND`), as `Replaying` refuses with `NO_REPLAY_NEEDED` (**R-2**) |
| `AttachPending` | 302 | `clientId`, `requestId` | not now: the host is still closing a previous attachment's façade; resend, as for `ReplayPending` |

Host → client, stream 303:

| message | fields | meaning |
| --- | --- | --- |
| `GroupsResolved` | repeating `name`, `groupId`, `mask` | the `GroupDirectory`, once, before the first `Gate` or `Role` |
| `RestoreRecord` | `recordIndex`, `formatVersion`, `data` | one `onRestore` call, in call order; `recordIndex` 0 means clear state. No records means start empty (**H-7**) |
| `Payload` | `clusterSeqNo`, `groupSeqNo`, `sourceId`, `connectionId`, `groupMask`, `sourceSessionId`, `clusterTimestampNs`, `receiveTimeNs`, `position`, `payloadId`, then the payload bytes with their own `MessageHeader` | `onSequenced`: one application payload, with what the façade's `Payload` exposes |
| `Connection` | `connectionId`, `opened`, `data` | Gateway only: `onConnectionOpened` / `onConnectionClosed` |
| `ClusterTime` | `clusterTimeNs`, `receiveTimeNs` | `onClusterHeartbeat` |
| `Gate` | `epoch`, `leading`, `canPublish`, `caughtUp` | every change of the façade's gate state, in event order: `leading` is `isLeading()` (Application) or `isServing()` (Gateway) and drives `onLeadershipChanged` and `OutstandingWork.onNotLeader()`; `canPublish` is `canPublish()` (Application) or `canAccept()` (Gateway), `leading` and ingress not held (**A-5**), and is what the stub's `canPublish()` returns (**H-16**); `epoch` is the host's (**H-3**) |
| `Role` | `active`, `firstConnectionId` | Gateway only: `onActivated` / `onStandby`, sent before the `Gate` that follows it; the stub allocates connection ids from `firstConnectionId` (**H-4**) |
| `CaughtUp` | `clusterSeqNo` | `onCaughtUp` |
| `SnapshotCut` | `roundId` | write the records for this cut (**H-6**); `roundId` only pairs the cut with its records and end, numbered by the host within the attachment, and the cluster's round is the host's `seqeron_app_snapshot_round` (`ops.md`) |
| `Dropped` | `firstSeq`, `lastSeq`, `reason` (`EPOCH`, `REFUSED`) | the host did not place the `Publish` commands `firstSeq` to `lastSeq` (**H-17**) |
| `Resolved` | `through` | every `Publish` up to `commandSeq` `through` is decided; those not placed were named by a `Dropped` before it (**H-17**) |
| `Fenced` | `error`, `detail` | `onFenced`: terminal |

Client → host, stream 304. The host queues a command and places it later (**H-3**, **H-5**); it handles the
rest as it reads them:

| message | fields | handled | meaning |
| --- | --- | --- | --- |
| `Publish` | `epoch`, `commandSeq`, `connectionId`, `groupMask`, `payloadId`, `payload` | queued | `publish` (**H-4**) |
| `Reply` | `epoch`, `sourceId`, `connectionId`, `groupMask`, `payloadId`, `payload` | queued | `reply`; the three ids are the request's `ReplyAddress` (**H-11**) |
| `OpenConnection` | `epoch`, `connectionId`, `data` | queued | Gateway only: `openConnection`, with the id the stub allocated (**H-4**) |
| `CloseConnection` | `epoch`, `connectionId` | queued | Gateway only: `closeConnection` |
| `GateClosed` | `epoch` | queued | Gateway only: the edge closed on its own (`gateClosed()`) |
| `Activated` | `opened` | on read | Gateway only: the result of `onActivated`, answering `Role` (§17.5) |
| `SnapshotRecord` | `roundId`, `recordIndex`, `data` | on read | one `onSnapshot` result (**H-6**) |
| `SnapshotEnd` | `roundId`, `recordCount` | on read | no more records; `recordCount` −1 abandons the round |

The façade consumes seqeron's system frames on the host (topology rows, leadership, snapshot rounds,
`SessionClosed`, the connection lifecycle) and calls its listener with what a direct listener receives, so no
system frame crosses to the client and the client needs no frame decoder (`unwrapFrame`,
`SequencedFrameDecoder`). A payload is not re-encoded: the host copies its bytes, and the stub supplies the
`Payload` accessors over the received message. The delivery rules of `client-api.md` ("Message delivery") that
concern the listener apply unchanged.

### 17.4 Rules

- **H-1. The host runs the real façade.** Every rule of §16 that the façade enforces holds for the hosted
  pair as for a direct one, except **A-2**, **A-3** and **A-6**, which are the client's. The host adds two of its
  own: it queues the client's commands (**H-5**), and it drops a command whose epoch has passed (**H-3**), which a
  direct `Gateway` would place, since `Gateway.publish` checks no gate.
- **H-2. Events are the façade's callbacks, in order, once, applied in stream order.** `Payload`, `Connection`,
  `ClusterTime`, `Gate`, `Role` and `CaughtUp` share stream 303 and are the façade's callbacks in the order it made
  them. A client that sees `Gate` has applied every `Payload` before it, which is what **A-1**'s "caught up"
  requires of state the host cannot see.
- **H-3. A command is placed only in the epoch it was decided in.** The host's epoch rises at every edge of
  `leading`, opening or closing, the close and reopen of a new term with the same leader included; a change of
  `canPublish` or `caughtUp` alone leaves it. `Gate` carries it, and the stub stamps every queued command with the
  epoch of the last `Gate` it applied, or the one above it once it has answered `Activated(true)` (§17.5). The host
  places the command at the head of its queue when that epoch is current and `leading` is true, and drops it,
  counted, otherwise, as the consensus module acts on ingress only in its current term and drops the rest without a
  reply. A command is therefore never placed under a gate other than the one its client decided it under: one in
  flight over an edge is dropped, never placed late. What a client does not get placed at an edge is exactly what it stamped with
  an earlier epoch that the host had not yet placed, and the host reports each such `Publish` (**H-17**). The client runs `OutstandingWork.onNotLeader()` when it applies
  `Gate` with `leading=false`, before anything else it does on that event, and **A-2** redispatches the tracked work
  at the next open.
- **H-4. A command is an offer, as in a direct façade.** `publish` and `reply` return at once with the result of an
  offer (`protocol.Publish`), `openConnection` with an id or `NO_CONNECTION`, and the host sends nothing back.
  `Published` means the stub offered the command to stream 304, which the host places if its epoch is still current
  when it reaches the head of the queue (**H-3**); the host's confirmed ingress (**A-4**, **A-5**) carries it on
  from there. The stub retains the command until the host reports its fate and resends one that was dropped at an edge (**H-17**), so a `Published` `Publish` is lost only with the client or the host process.
  `Declined` is retryable: the stub's mirror says the gate is shut or ingress held (**H-16**), the command window is
  full (**H-5**), or stream 304 is back-pressured or not yet connected. `Refused` is final and is the stub's own
  check, the direct façade's local ones: a payload above `MAX_PAYLOAD_LENGTH`, a retired `payloadId`, a `groupMask`
  naming a group `GroupsResolved` lacks. The caller retries as against a direct façade, and a `Declined` `Reply`
  releases its dispatch (**A-2**) as a failed reply offer does there.
  `openConnection` takes its id from the stub: ids start at the last `Role`'s `firstConnectionId` and count up, as
  the façade allocates them. The stub returns `NO_CONNECTION` when its mirror says it is not serving, the command
  window is full, or stream 304 is back-pressured or not yet connected, and takes the id only once the offer lands,
  so a `NO_CONNECTION` uses none up. While `canAccept()` is true, `NO_CONNECTION` is retryable, as `Declined` is.
  The host opens the connection through the façade when it places the `OpenConnection`, and fences if the façade
  allocates a different id.
- **H-5. The host reads stream 304 every duty cycle, and queues the commands.** As the consensus module reads a
  service's messages and acks from one stream and appends the messages from its own tracker, the host never stops
  reading at a command it cannot place yet. It moves each queued message (§17.3) into a FIFO of its own and handles
  the others as it reads them, including while it waits in `onActivated` or in a cut. The one stretch it reads
  neither 301 nor 304 is a façade send that spins through an election, up to the send's 10 s stall bound (**H-10**).
  Each duty cycle it places commands from the head of the queue by **H-3**, up to a per-cycle limit, until the queue
  is empty or the façade declines one (ingress held, its tracker full, the transport back-pressured, or a `Publish`
  whose connection's `ConnectionOpened` is not yet placed); that one stays at the head for the next cycle. The host
  places commands only from its own duty cycle, never inside a façade callback, so a `reply` made inside
  `onSequenced` is placed on a later cycle, in the order the commands were offered.
  **The command window bounds the queue.** It covers `Publish`, `Reply` and `OpenConnection`, the three calls the
  stub can decline (`Declined`, or `NO_CONNECTION` from `openConnection`, **H-4**), and counts each by its SBE
  length: its `MessageHeader` and body, the length the stub encodes and the host reads, not the Aeron frame. The
  host retired counter rises by that length as each leaves the queue, placed or dropped. The stub keeps the same sum
  for the windowed commands it has offered and declines one that would put more than `commandWindow` bytes (from
  `Attached`) between the two. `commandWindow` is at least the length of a `Reply` carrying a payload of
  `MAX_PAYLOAD_LENGTH`, so every command fits it, and at most stream 304's publication window, half its 1 MiB term
  or the driver's `aeron.ipc.publication.term.window.length` if that is smaller, so the window rather than the
  stream's back-pressure declines a command. The window bounds the host's queue and the time a command waits in it,
  and the host fences a client that exceeds it.
  **What cannot be declined is outside the window.** `closeConnection()` and `gateClosed()` return nothing, and
  `Activated`, `SnapshotRecord` and `SnapshotEnd` answer the host, so the stub retries each offer within the call
  until it lands, writing the client heartbeat on every spin, as a direct `closeConnection` puts its work in a
  backlog rather than fail. The retry is bounded by the host's duty cycle, or by the send spin when the host is in
  one, and the heartbeat keeps a host that returns from that spin from judging the client lost. The host's queue
  holds at most the window, one `CloseConnection` per open connection and one `GateClosed`.
- **H-6. A snapshot cut is a barrier.** The host sends `SnapshotCut` at the point the façade calls `onSnapshot` for
  the round: after the last `Payload` the façade dispatched before the cut and before any later one, and sends
  nothing more on stream 303 until the round's `SnapshotEnd`. The client serializes its state as of the cut
  (**A-6**), one `SnapshotRecord` per `onSnapshot` call, then sends `SnapshotEnd`. The host ends its poll of stream
  304 after each `SnapshotRecord`, so it holds at most one, and returns it from the `onSnapshot` call it waits in,
  0 for `SnapshotEnd` and −1 for one that abandons; commands it reads meanwhile are queued, not placed. **The host
  keeps the cluster session alive while it waits**, by calling the façade's `keepAlive()` on every spin, so the
  round is not bounded by `sequencer.sessionTimeoutMs`. It is bounded by the client: when the client heartbeat
  counter is stale for `clientTimeoutMs`, the host returns −1 from `onSnapshot` (the façade abandons the round,
  deleting its partial file) and closes the façade (`CLIENT_LOST`). A client that serializes for longer than
  `clientTimeoutMs` calls `keepAlive()` from the callback (**H-14**). The host writes the file, computes the length
  and `crc32c`, and compares them with the sequenced `SnapshotEnd` (**A-7**); a mismatch fences `SNAPSHOT_DIVERGED`,
  and the client's records are what diverged.
- **H-7. Restore precedes every `Payload`, by stream order alone.** The façade restores before it dispatches
  anything, calling `onRestore` once per record with no begin, end or count. The host forwards each call as a
  `RestoreRecord` on stream 303 in call order, so the records precede every `Payload` and no handshake is needed. A
  client that receives no `RestoreRecord` starts empty; one that receives them resets its state at `recordIndex` 0.
  The host's `supportsFormatVersion` answers from the client's `supportedFormat` list, which the C++ stub takes from
  its listener's `supportedFormatVersions()`; a format it lacks fences `SNAPSHOT_UNRESTORABLE`. A client with no
  listener lists none, and the host's façade then has none either: it takes part in no round and restores nothing,
  as a direct façade given no listener.
- **H-8. An attach starts a façade.** `Attach` follows the replayer's request rules: `clientId` is drawn once from
  the client's driver (`Aeron.nextCorrelationId`, **R-4**), and `requestId` rises with every new `Attach` and is
  kept on a resend (**R-1**). Before it attaches, the client allocates its counters and, unless it is a Consumer,
  creates its exclusive publication on stream 304. A host answers only an `Attach` naming its own `hostName`. It
  refuses a `protocolVersion` other than its own with `VERSION` (§17.7), and a `kind` other than its own with `KIND`,
  and answers `AttachPending` while it is still closing a previous attachment's façade.
  Otherwise it creates its stream 303 publication, subscribes to the client's stream 304 image and answers
  `Attached` at once. It builds the façade only once that publication is connected, which only the attached client's
  subscription makes it, so every callback has a reader, restore included, which the façade runs in its constructor;
  it loses a client whose subscription does not connect within `clientTimeoutMs`. An attachment begins at the
  `Attached` that accepts it; until it ends, the host answers a resend of that request with the same `Attached`, any
  other `Attach` with `Attached(NO_ATTACH, BUSY)`, and acts on a `Detach` only from that `clientId`. A client
  resends `Attach` until it hears `Attached` or a refusal, then closes its stream 302 subscription and subscribes to
  stream 303; it retries on `BUSY`, exits 78 on `VERSION` or `KIND` (§17.6), and exits 70 after its attach timeout,
  or when its stream 303 image has not connected within `hostTimeoutMs` of the `Attached` (`HOST_LOST`), since the
  host has then closed the attachment.
- **H-9. A loss ends it.** The host closes the façade, releasing the cluster session (so a standby takes over): on
  `Detach`, discarding the commands it has not placed, as a direct `close()` discards what it has not placed; when
  the client heartbeat counter is stale for `clientTimeoutMs` or the client's stream 304 image goes away
  (`CLIENT_LOST`; a Consumer, which has no stream 304, by its counter alone); and when an offer to stream 303 stays
  back-pressured while the publication's limit has not advanced for half the queue-stall timeout (`CLIENT_STALLED`).
  The limit advances with whatever the client reads, so a client that is slow but reading is not stalled, as **R-3**
  judges a replay by its publication's limit (`AeronReplayer.replayProgress`). Half the timeout puts
  `CLIENT_STALLED` ahead of the façade's own `QUEUE_STALLED`: the façade checks that fence only in `doWork()`, which
  cannot run during the spin, and counts from a `ClusterHeartbeat` dispatched at most a heartbeat period before the
  spin began. The host then waits for a new `Attach`, which restores and replays from scratch: a restarted client
  has no state, so the façade never resumes mid-stream.
- **H-10. `clientTimeoutMs` MUST NOT exceed `sequencer.sessionTimeoutMs`** (default 5 s), so a hung client costs a
  gateway's pair no longer to hand over than a hung direct gateway does. In return the host's duty cycle keeps the
  cluster session alive while a client callback runs, and so does its wait in a snapshot cut (**H-6**); a slow
  callback costs the cluster no more than a slow direct one. Liveness is read from a counter, so time the host
  spends blocked in the façade, in a send that spins through an election, costs the client nothing. **Nor does it
  cost the host**: the façade's builder takes the `IdleStrategy` that send and the cluster connect spin on
  (`hosted-facade.md` §1), and the host's writes the host heartbeat on every spin. The client's `hostTimeoutMs`, the counterpart of `clientTimeoutMs`, is therefore not tied to the
  send's 10 s stall bound. A hung host's session times out within `sequencer.sessionTimeoutMs` whatever the client
  does, so `hostTimeoutMs` decides only how soon the client restarts the pair.
- **H-11. `Reply` names its request by value.** `Payload` carries the request's `sourceId`, `connectionId` and
  `groupMask`, which are its `ReplyAddress`, and `Reply` carries the same three, as the direct `reply` takes a
  `ReplyAddress` the application kept. The host retains nothing per request.
- **H-12. After `Fenced` the host sends nothing.** The host releases the cluster session before sending it,
  as the façade does, and the client exits with 70.
- **H-13. A payload is copied once.** The host never decodes a payload: it copies the queue's payload bytes into
  `Payload`, and the stub reads them in place (§17.3).
- **H-14. Liveness and lag are counters** (§17.2.1). A callback that spins calls `keepAlive()`, as for the direct
  façades. The client writes its applied counter after each `Payload` it applies, and the host exports the
  difference between its latest dispatched `position` and that counter, labelled with the client's `clientId`
  (`ops.md`).
- **H-15. An attachment is a pair of images.** Events and commands travel on exclusive publications made for the
  attachment and read by session id, so a message a dead attachment left in a stream is on an image the next
  attachment never reads, and no message on streams 303 and 304 carries an attachment id. On streams 301 and 302 a
  client ignores a reply carrying another `clientId` (**R-4**) or a `requestId` other than its current one
  (**R-1**).
- **H-16. The stub's queries are mirrors.** `canPublish()`, `isLeading()`, `isServing()`, `canAccept()`,
  `isCaughtUp()` and `lastClusterSeqNo()` return what the stub last applied from `Gate`, `CaughtUp` and `Payload`,
  and from its own `Activated(true)` (§17.5). They lag the host by a hop and the client's polling, so a `publish`
  that passes the stub's check can reach the host after the gate has closed, where its epoch has passed and it is
  dropped (**H-3**). The `Gate` that closed it reaches the client in stream order, the host reports the drop and the
  stub resends the `Publish` (**H-17**); `OutstandingWork` redispatches a `Reply` (**A-2**) as it does for work the
  cluster lost at a failover.
- **H-17. A `Publish` the host does not place is reported, and the stub resends it.** An Application stub numbers its
  `Publish` commands `commandSeq` 1, 2, … within an attachment, stamps each with it, and keeps the encoded command
  until the host has decided its fate; the retained bytes are bounded by twice `commandWindow` plus one command, and a `publish` that finds no room declines. The
  host decides a `Publish` when it leaves the queue: placed, or not placed. For each run of consecutive `Publish`
  commands not placed it sends `Dropped(firstSeq, lastSeq, reason)` on stream 303 and, once it has placed or
  dropped what it takes in a duty cycle, `Resolved(through)` for the highest `commandSeq` decided. Stream order puts
  every `Dropped` below `through` before the `Resolved`, so a stub that applies the events in order has the
  complete fate of every `Publish` up to `through`: one that no `Dropped` names was placed, and the stub frees it.
  `EPOCH` is a command whose epoch had passed (**H-3**). After the `Gate` that opens the new epoch, and once
  `Resolved` covers every `Publish` stamped with an earlier epoch, the stub resends the `EPOCH` ones in
  their original order and before any new `Publish`, each under a fresh `commandSeq` and the new epoch; until then
  `publish` returns `Declined`. A resend the window or stream 304 declines is retried on the next duty cycle. The
  `Publish` commands keep their order among themselves across any number of edges.
  `REFUSED` is a command the host's façade refused (an unaddressable `groupMask`, a frame the sequencer would
  reject), which the stub's own checks (**H-4**) should have caught. It is final: the stub does not resend it, and
  raises the fault from its duty cycle as `ClusterError.CLIENT_PROTOCOL`, as for any breach of this protocol. A
  group change cannot cause it, since the stub and the host resolve the same table, which is fixed (**S-8**). A
  `Publish` the façade declines stays at the head of the queue (**H-5**) and is neither. A Gateway stub's `Publish`,
  `Reply`, `OpenConnection`, `CloseConnection` and `GateClosed` carry `commandSeq` 0 (or none) and are not retained: a `Reply` is
  tracked by **A-2**, and the others belong to a connection or an edge that the same edge ends. Nothing is retained
  across an attachment: a client or host that is lost loses what it had not placed, as a direct producer that
  dies does.

### 17.5 Where a hosted façade differs from a direct one

What `client-api.md` states for the hosted column. Every callback maps one to one: `onGroupsResolved` to
`GroupsResolved`, `onLeadershipChanged` to `Gate`, `onActivated` and `onStandby` to `Role`, `onSequenced` to
`Payload`, `onConnectionOpened` and `onConnectionClosed` to `Connection`, `onCaughtUp` to `CaughtUp`,
`onClusterHeartbeat` to `ClusterTime`, `onFenced` to `Fenced`; and the calls to `Publish`, `Reply`,
`OpenConnection`, `CloseConnection` and `GateClosed`. The pair, the election and `GatewayStarted`/`GatewayActive`
are the cluster's and are unchanged: each gateway instance is a host and a client, and the cluster designates one.
Where they differ:

- **`Published` survives a gate edge (H-17).** Where the direct call that would have reached a shut gate returns
  `Declined`, the hosted `publish` returns `Published` and the stub resends the command after the edge, so an
  Application tracks its publishes in `OutstandingWork` (A-2) exactly as it does against a direct façade: a reply by
  the request it answers, and nothing more. The one new rule is for a `Publish` the host refuses, which is a
  protocol fault and not a retry (**H-17**). A Gateway's commands lost at an edge belong to connections the same
  edge dropped.
- **`onActivated` returns a boolean.** The host's listener sends `Role(active, firstConnectionId)` and waits for
  the client's `Activated(opened)`, calling `keepAlive()` as it spins, then returns that value; `false` is retried
  on the next `doWork()`, as in a direct gateway. It blocks the host's duty cycle for as long as the client's dial
  takes, as a direct gateway's `onActivated` blocks its own. A direct gateway serves as soon as `onActivated`
  returns true, so a stub that answers `Activated(true)` mirrors `leading` and `canPublish` as true at once and
  stamps with the epoch one above its last `Gate`'s, the one the host's open edge takes (**H-3**); the `Gate` that
  follows confirms it. An initiator can therefore open the connection it dialled without waiting for that `Gate`.
- **`openConnection` returns an id the stub allocated** (**H-4**). `canAccept()` is the last `Gate`'s `canPublish`,
  or true from the stub's own `Activated(true)`, so a client that checks it before accepting, as the contract asks,
  rarely sees `NO_CONNECTION`. A direct gateway returns it only when not serving; the stub also returns it for a
  full window or a back-pressured stream 304, which the caller retries (**H-4**). A connection accepted while an
  edge is in flight is dropped by the host by its epoch and followed by the `Role` or `Gate` that closes the edge,
  which drops every connection. `closeConnection` returns nothing.
- **`publish` on a connection whose `ConnectionOpened` is unplaced is `Published`**, where the direct call returns
  `Declined`: the command waits in the host's queue behind its `OpenConnection` (**H-5**).
- **`onStandby` runs after the façade has stood down,** not before it moves on. A connection accepted in that
  window is dropped by the standby, and its commands by their epoch.
- **The queries lag** by a hop (**H-16**).
- **A host fault ends the client.** A client that loses its host exits (§17.6), so a host crash drops a gateway's
  edge where a direct gateway's equivalent was the process; a client crash costs the same as a direct one, and the
  cluster hands over within `clientTimeoutMs`, which is no longer than the session timeout.

### 17.6 Failure model and supervision

| failure | effect |
| --- | --- |
| client crashes or hangs | its heartbeat counter is stale for `clientTimeoutMs` or its stream 304 image goes away: host closes the façade (`CLIENT_LOST`), session released; standby takes over. A hung client that resumes finds its stream 303 image gone, or never connected within `hostTimeoutMs` of `Attached`, and exits 70 (`HOST_LOST`) |
| host crashes or hangs | its heartbeat counter is stale for `hostTimeoutMs` or its stream 303 image goes away: the client exits 70 (`HOST_LOST`); the cluster times out the host's session within `sequencer.sessionTimeoutMs` regardless |
| member leader changes | host emits `Gate(leading=false)` with a new epoch in stream order and drops every command of an earlier epoch (**H-3**); the client's unanswered leader work is redispatched at the next open (**A-2**) |
| client slower than the queue | host back-pressured and the client reads nothing of stream 303 for half the queue-stall timeout: `CLIENT_STALLED`; a slow client that keeps reading is not fenced |
| host's cluster session lost | the façade's own rules (`client-api.md`, "Guarantees"); `Fenced` is forwarded |
| façade cannot reach the cluster | its connect times out or is refused three times, 10 s each, so a connect that met an election is tried again; before each retry the host watches the client's heartbeat for `clientTimeoutMs`, since its own wait makes the counter look changed, and drops a client silent through it (`CLIENT_LOST`). Then the host logs `CLUSTER_UNREACHABLE` and exits 70, and the client exits 70 (`HOST_LOST`) |
| media driver lost | raises from the host's `doWork()` and from the client's, as for a direct façade |

A hosted pair ends when either process does, so its availability is that of both; the cluster sees either loss
as a direct façade's fence, within the same `sequencer.sessionTimeoutMs`. The one failure a direct client does not
have is the loss of the host process itself, which costs the client a restart. Nothing in the cluster tier
changes, and no frame of schema 210 differs.

**Supervision is the deployment's**, as for every other seqeron process: its supervisor (systemd or similar)
starts and restarts them, and the repository ships no unit files for any. The host and the client are two units,
and the protocol makes their order and their restarts independent of each other, so no unit names the other. The
exit codes are the repository's: 70 for "fenced or lost, restart me", 0 for an orderly stop (`ops.md`). The client
adds 78 for an attach the host refused with `VERSION` or `KIND`: a configuration error a restart does not cure, so
its unit does not restart on 78 (systemd's `RestartPreventExitStatus=78`).

- **Either may start first.** A client resends `Attach` until it is answered (**H-8**); a host waits for one.
- **A host fault restarts the client too, without the supervisor linking them.** A host that dies or goes silent
  makes the client exit 70 (`HOST_LOST`), so its own unit restarts it. A host that fences sends `Fenced` and
  exits 70, and so does one whose façade cannot reach the cluster (`CLUSTER_UNREACHABLE`), without sending anything.
- **A process that runs several hosts rebuilds one rather than exit** (`proxy.hosts`). A host that would exit 70 is
  closed, which frees its counters and closes its images, so its client exits 70 as it would for a host process, and
  is built afresh a second later; the process and its other hosts carry on. A host that throws is the process's fault:
  a standalone process exits 70, and `ReplayerServer` takes its fatal path, so on a gateway host a host fault takes
  down the Replayer and every consumer it serves.
- **A client that crashes or hangs does not restart the host.** On `CLIENT_LOST` the host closes the façade,
  releasing the cluster session, and waits for a new `Attach` (**H-9**); the restarted client attaches to the
  running host. A client that breaks the protocol, by exceeding the command window (**H-5**) or opening a
  connection under an id the façade did not allocate (**H-4**), or whose records diverge (**H-6**), fences the
  façade, and the host exits 70 with it.
- **A restart is the usual one.** Whatever restarts replays or restores from the newest confirmed snapshot, and a
  gateway returns as a standby, active only when the cluster names it.

What this adds to an operator's list is a second unit per hosted instance and the host's counters (**H-14**); the
two units' restart rates need alerts, since a loop of `HOST_LOST` is a host fault.

### 17.7 Versioning

Schema 218 is exempt from **V-3**, as 212 is: it is unrecorded and never leaves the node. It changes in place with
no version bump, as 212 does: `version` stays 0 and the schema has no `sinceVersion` fields. `Attach` carries
`protocolVersion`, a constant of `HostedProtocol` raised with every change to schema 218 or to a rule of §17, and a
host refuses a client of another with `Attached(NO_ATTACH, VERSION)`. A host and its client therefore agree on the
protocol, not on the release, so a release that leaves §17 alone does not make them upgrade together. That is the
one check beyond **T-1**: the replayer's clients are restarted with it and need none, but a mismatch here would
otherwise surface as a decode failure at runtime. `Attach` also carries `aeronVersion`, which the host logs and does
not check. A stub and its host share only the media driver, whose formats (the log buffers, the driver's commands, the
counters) are versioned by its CnC version, and Aeron refuses a client of another CnC major version when it connects,
before it can attach. A stub may therefore run another Aeron than its host: Aeron.NET, which trails Java (**V-1**), or
an installed Aeron above the one the C++ build pins.
