# seqeron protocol specification

Normative. This document defines the frames the seqeron cluster sequences, the system messages it
accepts and emits, the replay control protocol, and the obligations of producers and consumers.
Class and function names (`Sequencer`, `ReplayerRecovery`, …) say where a rule is enforced; they are
not part of the protocol.

**Conventions.** MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119. Rules carry stable
identifiers that code and tests cite: **F** frame, **T** transport, **S** sequencer, **P** payload,
**C** registration, **R** replay, **V** versioning, **E** encoding, **A** application. Section numbers
are stable too, which is why §15 is unassigned. All SBE schemas here are little-endian.

---

## 1. Layers

| layer | contents | schema | owner |
| --- | --- | --- | --- |
| L0 transport | Aeron channels and stream ids; the Aeron Cluster ingress/egress session protocol | `sbe-cluster.xml` (111), a mirror of `io.aeron.cluster.codecs` | Aeron |
| L1 frame | two envelope pairs, `Unsequenced`/`Sequenced` (application) and `UnsequencedSystem`/`SequencedSystem` (system), plus three synthesized templates | `sbe-frame.xml` (210) | seqeron |
| L2 payload | one opaque, length-prefixed byte range per application frame, named by `payloadId` | the application's | the owner of that `payloadId` |
| system messages | seqeron's twelve events, named by `systemEventType` | `sbe-frame.xml` (210) | seqeron |
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

| class | originates with | encoded by | appears on |
| --- | --- | --- | --- |
| unsequenced | an external producer (a gateway, an application, `clusterctl`) | the producer | cluster ingress only |
| sequenced | an external producer | the sequencer, from an unsequenced frame | the tap and replays of it |
| synthesized | the cluster | the sequencer | the tap and replays of it |

Sequenced and synthesized frames draw `globalSeqNo` from one counter (§9.1).

The template identifies a synthesized frame: `ClusterHeartbeat`, `LeadershipChanged` and
`GatewayActive` have templates of their own and no ingress form. Their header also carries
`sourceId`, `connectionId` and `sessionId` = −1, since there is no external producer, connection or
session to name. A consumer that needs to test provenance MUST test `sourceId` (**F-4**) and MUST NOT
depend on the other two fields.

Replay control messages (§10) belong to none of these classes: they are not frames and have no
header composite.

## 3. Transport bindings

> **T-1.** A consumer MUST check `MessageHeader.schemaId` on every stream it reads.

| stream | channel / stream id | carries | recorded |
| --- | --- | --- | --- |
| cluster ingress | Aeron Cluster ingress, inside the L0 session envelope | `Unsequenced` (100) or `UnsequencedSystem` (102), schema 210 | in the Raft log |
| tap | `aeron:ipc`, stream 205 | `Sequenced` (101), `SequencedSystem` (103), or a synthesized template (104–106), schema 210 | yes, by the co-located archive on every node |
| replay | stream 201 | archive bytes, identical to the tap | no |
| replay request | stream 202 | schema 212, client → replayer | no |
| replay control | stream 203 | schema 212, replayer → client | no |

- **F-1. The tap is the record.** The cluster takes no snapshots and always recovers by replaying the
  full log from `globalSeqNo` 1, so the tap's bytes are the only durable copy of a frame.
- **F-2. Every node's tap is byte-identical.** A replay from any node's archive can stand in for any
  other's. §9.3 keeps this true.

## 4. The frame layer

```
Unsequenced        (schema 210, template 100) { unsequencedHeader,       payload:varData }
Sequenced          (schema 210, template 101) { sequencedHeader,         payload:varData }
UnsequencedSystem  (schema 210, template 102) { unsequencedSystemHeader, body:varData }
SequencedSystem    (schema 210, template 103) { sequencedSystemHeader,   body:varData }
ClusterHeartbeat   (schema 210, template 104) { sequencedSystemHeader }
LeadershipChanged  (schema 210, template 105) { sequencedSystemHeader,   newLeaderMemberId, leadershipTermId }
GatewayActive      (schema 210, template 106) { sequencedSystemHeader,   gatewayId }
```

The members of each pair differ only in the fields sequencing adds. An application frame carries one
length-prefixed payload; a system frame carries one length-prefixed system payload. The three
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
| 18 | `globalSeqNo` | int64 | — | ✓ |
| 26 | `timestamp` | int64 | — | ✓ |
| | encoded length | | **18** | **34** |

Consequences:

- Every header field is at the same offset in all seven templates. `globalSeqNo` is at 18 in every
  sequenced frame, so a consumer can track continuity without branching on the template (**P-3**).
- Sequencing copies the first 18 bytes and appends 16, for both families.
- `blockLength` on an ingress frame is always 18 (§9.2 condition 4).
- Offset 16 is a `payloadId` in the application family and a `systemEventType` in the system family.
  The template says which; a consumer MUST NOT interpret one as the other.

There is no padding: `globalSeqNo` is unaligned. SBE's C++ codecs (`memcpy`) and Agrona's
`UnsafeBuffer` read it with a single unaligned load on x86-64 and ARM64. Use plain accessors only;
volatile or atomic access (`getLongVolatile`, `getAndAddLong`, C++ atomics) at an unaligned address
is undefined.

### 4.2 Frame sizes

| | ingress (100 / 102) | sequenced (101 / 103) |
| --- | --- | --- |
| `MessageHeader` | 8 | 8 |
| header composite (`blockLength`) | 18 | 34 |
| var-data length prefix | 2 | 2 |
| fixed overhead | **28** | **44** |

`MIN_INGRESS_LENGTH` is 28 for both ingress templates. A `ClusterHeartbeat` is 8 + 34 = 42 bytes: it
has no length prefix and no payload.

### 4.3 Provenance

> **F-4.** `sourceId` −1 is reserved for the cluster. Every other value names an external producer.
> An ingress frame MUST NOT carry −1 (§9.2 condition 6).

The template marks a frame as synthesized (§2); `sourceId` −1 is the header's copy of that fact.
There is no separate origin field. An application that needs to know a message's direction or role
carries that in its own payload.

## 5. Field authority

The ingress contract. **S-1:** the sequencer enforces it.

| field | on ingress | on the tap |
| --- | --- | --- |
| `sourceId` | set by the producer; MUST NOT be −1 (condition 6); checked against the gateway list (**S-6**) | copied; −1 on a synthesized frame |
| `connectionId` | set by the producer | copied |
| `sessionId` | set by the producer; advisory | overwritten with the Aeron Cluster session id the frame arrived on |
| `payloadId` | set by the producer; MUST NOT be 0 or 1 (condition 7) | copied |
| `systemEventType` | set by the producer; MUST be an allocated, ingress-legal value (condition 8) | copied |
| `globalSeqNo` | absent | assigned by the sequencer (§9.1) |
| `timestamp` | absent | Raft consensus time at commit |
| `payload` / `body` | set by the producer | copied byte for byte |

The outer `MessageHeader` is re-encoded, not copied:

| field | on ingress | on the tap |
| --- | --- | --- |
| `schemaId` | MUST be 210 (condition 2) | 210 |
| `templateId` | MUST be `Unsequenced` or `UnsequencedSystem` (condition 3) | `Sequenced` or `SequencedSystem` respectively |
| `blockLength` | MUST be 18 (condition 4) | 34 |
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

Gateways are listed by `GatewayRegistered.gatewaySourceId`, applications by
`ApplicationRegistered.applicationSourceId`. **S-6** checks gateways only. No list row may claim
`clusterctl`'s id.

> **S-6.** For an `UnsequencedSystem` frame (§9.2 condition 10):
>
> 1. `GatewayStarted` and `GatewayActivationRequested`: the `gatewayId` MUST name a list row. For
>    `GatewayStarted`, that row's `gatewaySourceId` MUST also equal `header.sourceId`.
> 2. Any other system frame whose `sourceId` is a listed `gatewaySourceId`: it MUST arrive on a
>    cluster session already bound to a `gatewayId` with that `gatewaySourceId`.
> 3. Everything else is not checked: all application frames, and system frames with an unlisted
>    `sourceId`.
>
> `GatewayStarted` creates the session binding; the closing of that cluster session removes it (the
> same event promotes a standby, §7.2). There is one binding per live gateway session. Both events are
> in the log and the list is built from the log, so the check is deterministic (**S-3**). A binding
> MUST NOT be removed because of anything observed locally.

Case 1 rejects a `GatewayStarted` that arrives before `load-topology` has run, since the list is
empty.

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

A protocol read by more than one application MUST be declared by a `PayloadIdRegistered` row (§6.3)
and MUST have a single owning repository whose codecs every consumer links. A `payloadId` that only
one application publishes and reads needs no row; the wire behaves the same either way (**P-1**,
**C-2**), and the only difference is the label `SbeLogPrinter` prints.

All `payloadId`s share one space. Two applications that pick the same number collide on the tap, and
nothing here detects it; **P-4** detects a wrong decoder only when the encoding describes itself.

### 6.2 Selective consumption

- **P-1.** A consumer MUST skip a `payloadId` it does not recognise, and MUST NOT treat it as an error.
- **P-2.** Within a `payloadId` it recognises, a consumer MUST skip templates it does not handle,
  without error. The sequencer does this for the system family (**S-2**).
- **P-3.** Continuity does not depend on understanding a payload. Gap detection, de-duplication and
  recovery use `globalSeqNo`, read at its fixed offset before any payload dispatch. A skipped frame
  still counts; skipping can neither create nor hide a gap.
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

`clusterctl load-topology <file>` publishes one operator file as system frames. Its three sections
together describe every producer in the deployment:

| section | publishes | describes |
| --- | --- | --- |
| `<gateways>` | one `GatewayRegistered` per row | the gateway list (§7) |
| `<applications>` | one `ApplicationRegistered` per row | co-located applications |
| `<protocols>` | one `PayloadIdRegistered` per row | shared protocols (§6.3) |

```xml
<topology>
  <gateways>
    <gateway name="GW-A" id="1" sourceId="0" rank="0"/>
    <gateway name="GW-B" id="2" sourceId="0" rank="1"/>
  </gateways>
  <applications>
    <application name="RefDataServer" sourceId="3"/>
  </applications>
  <protocols>
    <protocol payloadId="4" version="1" name="refdata"/>
  </protocols>
</topology>
```

Attribute to field: a gateway's `name`/`id`/`sourceId`/`rank` are
`gatewayName`/`gatewayId`/`gatewaySourceId`/`preferenceRank`; an application's `name`/`sourceId` are
`applicationName`/`applicationSourceId`; a protocol's `payloadId`/`version`/`name` are
`payloadId`/`protocolVersion`/`protocolName`. `remaining` is computed by the loader from the row
count. An application row has no `id` or `rank` because applications are not elected.

| constraint | enforced by |
| --- | --- |
| `name`: 1–32 printable US-ASCII characters | schema |
| `id` int32, `rank` uint8, `version` uint16 | schema |
| `sourceId` ≥ 0 (−1 is the cluster's, **F-4**) | schema |
| an application's `sourceId` is required | schema |
| `payloadId` ≥ 2 (**C-1**) | schema |
| unique within their section: gateway `id` and `name`, application `name` and `sourceId`, `payloadId` | schema (identity constraints) |
| `<gateways>` non-empty; `<applications>` and `<protocols>` optional | schema |
| exactly one `rank="0"` per gateway `sourceId` | loader |
| no `sourceId` is a reserved id (§5) | loader |
| no application `sourceId` equals a gateway `sourceId` | loader |

The loader MUST validate against the schema in its own artifact and MUST ignore any schema location
the document names. Otherwise a document could name a weaker schema and bypass **C-1**.

**Publish order.** The loader MUST validate the whole file before publishing anything. It then
publishes the gateway rows as one contiguous run in file order, with `remaining` counting down to 0 on
the last row; no other frame may come between them, because the sequencer synthesizes the bootstrap
`GatewayActive` frames after that last row (§7.2). Application rows and then protocol rows follow.
They carry no countdown, since the sequencer does not act on them.

## 7. System messages

Twelve events, defined in schema 210 beside the envelopes. Nine are submitted by producers and travel
in `UnsequencedSystem`/`SequencedSystem`. Three are synthesized by the sequencer and have templates of
their own.

| event | `systemEventType` | carried in | ingress-legal | the sequencer decodes it |
| --- | --- | --- | --- | --- |
| `ConnectionOpened` | 1 | payload | yes (producer) | yes: open-connection set |
| `ConnectionClosed` | 2 | payload | yes (producer) | yes: open-connection set |
| `LeadershipChanged` | 5 | template 105 | no | no (encodes only) |
| `ClusterStarted` | 10 | payload | yes (`clusterctl`) | no |
| `ClusterStopped` | 11 | payload | yes (`clusterctl`) | no |
| `ClusterHeartbeat` | 16 | template 104 | no | no (encodes only) |
| `GatewayRegistered` | 17 | payload | yes (`clusterctl load-topology`) | yes: gateway list; bootstrap on `remaining == 0` |
| `GatewayActive` | 18 | template 106 | no | no (encodes only) |
| `GatewayStarted` | 19 | payload | yes (gateway) | yes: binds session to `gatewayId`; releases stale connections |
| `PayloadIdRegistered` | 23 | payload | yes (`clusterctl load-topology`) | no (§6.3) |
| `GatewayActivationRequested` | 24 | payload | yes (`clusterctl activate`) | yes: validates `gatewayId`, then synthesizes `GatewayActive` |
| `ApplicationRegistered` | 25 | payload | yes (`clusterctl load-topology`) | no |

A submitted event's `systemEventType` equals its payload's SBE template id. The three synthesized frames
also carry their `systemEventType` (5, 16, 18), redundantly with the template id, so that offset 16
identifies every frame on the tap. Numbers are never reused: `GatewayActivationRequested` took 24
rather than a vacated number so that no recording can be misread. `ClusterHeartbeat` is unrelated to
`ReplayHeartbeat` (§10).

**A system payload has no `MessageHeader`.** `header.systemEventType` identifies it. An encoder `wrap`s
the payload rather than calling `wrapAndApplyHeader`, and a decoder takes `BLOCK_LENGTH` and
`SCHEMA_VERSION` from its compiled constants. This is sound because of **V-3**: no seqeron decoder
reads bytes encoded by a different build.

**`GatewayActive` is produced only by the sequencer**, on four triggers (§7.2): bootstrap, the active
instance's session closing, an activation timeout, and a `GatewayActivationRequested`. For the last,
the sequencer validates the request against the list, sequences it, and synthesizes the
`GatewayActive` at the next `globalSeqNo`. The operator's request is therefore on the log, and every
`SequencedSystem` frame has an `UnsequencedSystem` origin.

Header of a synthesized frame (§5 covers ingress only):

| field | value |
| --- | --- |
| `sourceId`, `connectionId`, `sessionId` | −1 (**F-4**) |
| `systemEventType` | the value for this template |
| `globalSeqNo` | the next value from the counter ingress also uses (§9.1) |
| `timestamp` | Raft consensus time, never a local clock (**S-3**) |

> **S-2.** The sequencer decodes exactly the five payloads marked "yes" above, after reading
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
synthesized templates' blocks include the 34-byte header composite.

| event | `systemEventType` | block | frame bytes | fields |
| --- | --- | --- | --- | --- |
| `ConnectionOpened` | 1 | 0 | 46 + *n* | `connectionData` varData |
| `ConnectionClosed` | 2 | 0 | 44 | none |
| `LeadershipChanged` | 5 | 46 | 54 | `newLeaderMemberId` int32, `leadershipTermId` int64 |
| `ClusterStarted` | 10 | 8 | 52 | `correlationId` int64 |
| `ClusterStopped` | 11 | 8 | 52 | `correlationId` int64 |
| `ClusterHeartbeat` | 16 | 34 | 42 | none |
| `GatewayRegistered` | 17 | 43 | 87 | `remaining` uint16, `gatewayId` int32, `gatewaySourceId` int32, `gatewayName` char[32], `preferenceRank` uint8 |
| `GatewayActive` | 18 | 38 | 46 | `gatewayId` int32 |
| `GatewayStarted` | 19 | 8 | 52 | `gatewayId` int32, `firstConnectionId` int32 |
| `PayloadIdRegistered` | 23 | 36 | 80 | `payloadId` uint16, `protocolVersion` uint16, `protocolName` char[32] |
| `GatewayActivationRequested` | 24 | 4 | 48 | `gatewayId` int32 |
| `ApplicationRegistered` | 25 | 36 | 80 | `applicationSourceId` int32, `applicationName` char[32] |

Frame bytes are for the sequenced form: 44 + block for a submitted event, 8 + block for a synthesized
template.

Field semantics:

- `connectionData`: see below.
- `newLeaderMemberId`: the Aeron Cluster member that leads from this `globalSeqNo` on.
- `leadershipTermId`: the term that starts here. One `LeadershipChanged` per term, including a term
  won again by the same member (that election also discarded ingress). Term ids are not contiguous; a
  failed election consumes one.
- `correlationId`: set by `clusterctl` so it can recognise its own frame on the tap.
- `remaining`: gateway rows still to come after this one; 0 marks the last row.
- `gatewayId`: the list row's identity, unique in the list. `gatewaySourceId`: the logical gateway,
  shared by all its instances; **S-6** compares it with `header.sourceId`. `gatewayName`: US-ASCII,
  byte 0 = `0x00` means absent; an instance finds its own row by this name at start-up.
- `preferenceRank`: 0 is the designated primary; 1, 2, … are standbys of the same `gatewaySourceId`.
- `firstConnectionId`: the first `connectionId` this instance will allocate, chosen above the highest
  it saw during replay, so ids do not repeat across a restart.
- `protocolVersion`, `protocolName`: §6.3. `protocolName` is US-ASCII, padded with `0x00`.

**Connections.** The sequencer keeps a set of open connections keyed on `(header.sourceId,
header.connectionId)`. When a logical gateway publishes `GatewayStarted`, the sequencer releases every
connection still open under that `gatewaySourceId`, because a crashed instance never publishes the
`ConnectionClosed` frames for its connections. The set is node-local state derived from the log
(**S-3**), and the release emits no frame. A consumer that tracks connections derives its view from the
same frames.

`ConnectionClosed` has no payload: `header.connectionId` identifies a connection every consumer has
already seen open. `ConnectionOpened` carries `connectionData`, because a producer allocates a new
`connectionId` for every connection, so a counterparty that reconnects and resumes the same session
arrives under a new id. A standby needs the mapping from id to application session before that
connection's first message, and this frame is the only place it can get it.

- The cluster tier MUST NOT decode `connectionData`. The sequencer dispatches `ConnectionOpened` on
  `systemEventType` and reads nothing past the block.
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
synthesizes nothing, logs, and does not advance `globalSeqNo`; that gateway has no active instance
until one starts.

The rule excludes the losing instance rather than taking the next rank up. After rank 0 loses the role
to rank 1, a later loss by rank 1 returns it to rank 0. As a result, a gateway whose instances were all
down converges on whichever instance starts first.

**Triggers.**

1. Bootstrap: after the `GatewayRegistered` with `remaining` = 0, one `GatewayActive` per rank-0 row,
   in list order.
2. Session close: the cluster session bound by a `GatewayStarted` (**S-6**) closes. The binding is
   removed first; then the target rule runs for the instance it named.
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
the heartbeat frame, never on a local timer. The sequencer takes the first expired deadline in arming
order and removes it. If no `GatewayStarted` has bound that `gatewayId` in the meantime, it
synthesizes a `GatewayActive` for the target; otherwise it discards the deadline. At most one
promotion happens per heartbeat.

Every input to promotion comes from the log: the list from `GatewayRegistered`, the bindings from
`GatewayStarted`, the time from the consensus timestamp. Every node therefore synthesizes the same
frame at the same `globalSeqNo`.

---

## 8. Schemas

| file | schema id | contains | change policy (§11) |
| --- | --- | --- | --- |
| `sbe-frame.xml` | 210 | the seven top-level templates, the four header composites, `messageHeader`, `varDataEncoding`, and the nine submitted system payloads | envelopes frozen; system events changeable under **V-3** |
| `sbe-replay.xml` | 212 | the six replay control messages (§10) | changed in place, no version bump |

The envelopes and system messages are one schema because they have one owner, ship in one artifact and
follow the same change rule. `version` stays 0 for the life of schema 210, including system message
changes: under **V-3** no decoder reads bytes from another build, so a version number would carry no
information. Schema 211 is unallocated. Schemas 111 (`sbe-cluster.xml`, Aeron's) and 214
(`sbe-probe.xml`, an application payload) are not part of this protocol.

An application includes nothing from seqeron's schemas and generates nothing from them. It defines its
own payload encoding and reads frames through seqeron's compiled codecs (the jar in Java, the
installed headers in C++).

Both schemas' codecs ship together, one artifact per language: `ReplayerRecovery` decodes
`LeadershipChanged` (schema 210) while handling `Replaying` (schema 212).

## 9. Sequencer rules

### 9.1 `globalSeqNo`

- **S-4.** `globalSeqNo` is 1 for the first frame the cluster emits and increases by exactly 1 per
  emitted frame. It is never reused and has no gaps. A consumer that observes a gap has lost frames and
  MUST recover through the replayer; it MUST NOT skip.
- Forwarded and synthesized frames use the same counter.
- A rejected ingress frame does not advance the counter and leaves nothing in the log: neither the
  frame nor a record of its rejection (§9.6).
- **S-5.** Every reject condition in §9.2 is evaluated before the counter advances and before any state
  changes.
- The archive is addressed by recording position; the replayer needs no index from `globalSeqNo` to
  position.

### 9.2 Validation

The sequencer MUST reject an ingress frame on the first of these conditions that holds, evaluated in
order (each establishes what the next may read). It MUST NOT throw (§9.4).

| # | reject when |
| --- | --- |
| 1 | `length < MIN_INGRESS_LENGTH` (28) or `length > MAX_INGRESS_LENGTH` (§12) |
| 2 | `MessageHeader.schemaId != 210` or `MessageHeader.version != 0` |
| 3 | `MessageHeader.templateId` is neither `Unsequenced` nor `UnsequencedSystem` |
| 4 | `blockLength != 18` |
| 5 | the var-data length prefix is 65535 (`varDataEncoding`'s null value), or `MIN_INGRESS_LENGTH + bodyLength != length` |
| 6 | `sourceId == -1` (**F-4**) |
| 7 | `Unsequenced` and `payloadId` is 0 or 1 |
| 8 | `UnsequencedSystem` and `systemEventType` is not an allocated, ingress-legal value |
| 9 | `UnsequencedSystem` and the payload is shorter than that event's compiled `BLOCK_LENGTH` |
| 10 | `UnsequencedSystem` and the frame fails **S-6** |

A rejection is logged and counted (§9.6). Nothing is emitted and `globalSeqNo` does not move.

Implementation notes:

- Conditions 1–6 are identical for both ingress templates; only 7–10 depend on the template.
- Conditions 4 and 5 are equalities, not bounds. A short `blockLength` would place the length prefix
  inside the header; a long one, or a length prefix short of the frame end, would silently drop bytes on
  every node. Compare condition 4 against the composite's generated `ENCODED_LENGTH`.
- Condition 3 rejects the three synthesized templates before any payload is read. Condition 8 rejects
  the same events named inside an `UnsequencedSystem`.
- The size limits are compiled-in protocol constants (§12), never read from a node's transport
  configuration per frame: a node configured differently from its peers would diverge (**S-3**). MTU
  is checked once at start-up (**T-2**).
- Condition 1 is a backstop. A conforming producer's encode method refuses an oversized payload before
  sending (**T-3**).
- Conditions 6–9 depend only on the frame, so conforming encode methods (`SystemFrame`,
  `publishPayload`/`publishSystem`) refuse them too. Only condition 10 can reject a frame from a
  conforming producer, and §16 A-4 treats that as a fault.
- Condition 9's minimum length is the decoder's compiled `BLOCK_LENGTH`, never the `blockLength` on the
  wire: an SBE decoder reads fixed fields at fixed offsets whatever block length it was given, so a
  sender-supplied length bounds nothing. It applies to every system event whose payload a later condition
  reads. Condition 8 needs no minimum; it reads only the header.

### 9.3 Determinism

> **S-3.** Every reject decision and every byte of every synthesized frame MUST be a function of the
> committed log alone: not of node configuration, wall-clock time, local state or hash-map iteration
> order.

Conditions 1–9 depend only on the frame's bytes. Condition 10 reads state, all of it derived from the
log (the list from `GatewayRegistered`, the bindings from `GatewayStarted`).

- `timestamp` is the Raft consensus timestamp in nanoseconds since the epoch. Its precision and
  accuracy are those of the leader's clock.
- Synthesis over a collection (the bootstrap `GatewayActive` frames) MUST iterate in a deterministic
  order.
- Every synthesis deadline MUST be evaluated against the consensus clock, never a local timer.
- `ClusterHeartbeat` is driven by a cluster timer, not a local scheduler.

### 9.4 No callback may signal failure by throwing

When a cluster callback throws, `Image.boundedControlledPoll` has already advanced the log position
and `AgentRunner` keeps the agent running, so the frame is dropped and processing continues. With no
snapshots, that is a permanent hole. The same applies to clients: `Image.poll` passes the exception to
the error handler and advances the subscriber position. A client therefore records the fault and raises
it from its own duty cycle.

### 9.5 Encoding a frame

Sequencing an ingress frame: decode the `MessageHeader` and the 18-byte header; encode a new
`MessageHeader` (`Sequenced` or `SequencedSystem` to match the ingress template, `blockLength` 34,
schema 210, version 0, all constants); copy the 18-byte header prefix, offset 16 included; overwrite
`sessionId`; append `globalSeqNo` and `timestamp`; copy the length-prefixed payload. The same code serves
both families, because it never interprets offset 16 and the fields it writes are at the same offsets
in both sequenced composites.

A synthesized frame is one fixed-size encode: its own template, fields inline, no payload.

### 9.6 What a rejection leaves behind

> **S-7.** Every rejection MUST increment a rejected-frame counter and write a log line naming the
> failed condition and the cluster session. Nothing is emitted on the wire and nothing is written to
> the log.

- The counter is the node-local Aeron counter of type 5002
  (`SeqeronCounters.SEQUENCER_REJECTED_INGRESS_COUNT_TYPE_ID`). Every node rejects the same frames
  (**S-3**), so nodes that have applied the same log prefix MUST report the same count; a difference
  means the taps have diverged (**F-2**). A restarting node rebuilds the count during its full-log
  replay.
- The log line MUST be written and SHOULD be rate-limited. It carries the condition, the cluster
  session, the frame length, and whichever of `payloadId`, `sourceId` and `connectionId` the conditions
  before the failing one have validated. The counter stays exact while the log is rate-limited.
- The sequencer keeps no state derived from a rejection: no deny list, nothing rebuilt on replay.

A conforming producer is refused by its own encode method before sending (**T-3**), and MUST NOT
publish until it has resolved its own `{gatewayId, gatewaySourceId}` from the list on the tap (§5,
**S-6**), which keeps a `GatewayStarted` from preceding `load-topology`. The counter therefore
counts frames from non-conforming producers.

## 10. Replay control protocol

Six messages between a node's replayer and its co-located clients, on streams 202 and 203. They carry
no header composite and are never sequenced or recorded.

| message (id) | direction | meaning |
| --- | --- | --- |
| `ReplayRequest` (6) | client → replayer | replay history. `segmentIndex >= 0`: replay that segment of the recording chain (one recording per leader tenure) from position 0. `segmentIndex < 0`: resume the active recording at `fromPosition` |
| `Replaying` (7) | replayer → client | attach to `replaySessionId` on stream 201 and follow it until `catchUpPosition` |
| `ReplayPending` (8) | replayer → client | all replay slots are busy; wait at the gap and do not advance past it |
| `ReplayComplete` (9) | client → replayer | caught up; free the slot now instead of at the idle timeout |
| `ReplayHeartbeat` (20) | client → replayer | still replaying; sent about every 500 ms to keep the slot |
| `ReplayUnavailable` (21) | replayer → client | this node cannot serve history for the replayer's lifetime (unlike `ReplayPending`, not transient) |

- **R-1.** `requestId` identifies the current request. The client increments it on every send,
  including an unchanged resend, and the replayer echoes it. A client MUST ignore a reply whose
  `requestId` is not its current one.
- **R-2.** `NO_REPLAY_NEEDED` ends a walk only when `recordingId == -1`. With any other `recordingId` it
  means only that the segment is empty; the client requests the next one.
- **R-3.** `recordingId` detects a change in the recording chain. The replayer resolves the chain on
  every request and may drop a stale span. A client MUST remember the `recordingId` it last received for
  its current index and, on a mismatch, restart the walk from segment 0.
- **R-4.** The slot timeout is an idle timeout, reset by `ReplayHeartbeat`. A replay has no time limit.

The client detects completion by reaching `catchUpPosition`, not by the replay image closing: a
bounded replay of an active recording does not close its image at the bound.

Schema 212 changes in place without a version bump. Nothing records it, and a node's replayer and its
clients are built and restarted together.

### 10.1 Message fields

Each message is a standalone SBE message with its own 8-byte `MessageHeader` (schema 212), so its
encoded length is 8 + block. None has var-data or repeating groups.

| message | id | block | fields |
| --- | --- | --- | --- |
| `ReplayRequest` | 6 | 24 | `clientId` int32, `requestId` int64, `fromPosition` int64, `segmentIndex` int32 |
| `Replaying` | 7 | 36 | `clientId` int32, `requestId` int64, `replaySessionId` int64, `catchUpPosition` int64, `recordingId` int64 |
| `ReplayPending` | 8 | 12 | `clientId` int32, `requestId` int64 |
| `ReplayComplete` | 9 | 4 | `clientId` int32 |
| `ReplayHeartbeat` | 20 | 4 | `clientId` int32 |
| `ReplayUnavailable` | 21 | 12 | `clientId` int32, `requestId` int64 |

- `clientId`: identifies the client on the shared control stream. It does not match a reply to a
  request; `requestId` does (**R-1**).
- `requestId`: echoed in `Replaying`, `ReplayPending` and `ReplayUnavailable`. `ReplayComplete` and
  `ReplayHeartbeat` have none, because nothing answers them.
- `segmentIndex`, `fromPosition`: as in `ReplayRequest` above; `fromPosition` is read only when
  `segmentIndex < 0`.
- `replaySessionId`: the Aeron replay session to attach to on stream 201, or `NO_REPLAY_NEEDED` = −1
  (`Aeron.NULL_VALUE`). There is no separate message for "nothing to replay".
- `catchUpPosition`: where the client stops following the replay and switches to the live tap,
  de-duplicating on `globalSeqNo`.
- `recordingId`: the recording the requested segment resolved to, or −1 when the chain is exhausted.

The two sentinels are independent. `replaySessionId == NO_REPLAY_NEEDED` with `recordingId >= 0`
means that segment is empty: request the next. With `recordingId == -1` it means the chain is
exhausted: the walk ends and the client is caught up. A client that confuses the two stops replaying
with history still ahead of it.

**Slot limits.** A replayer serves at most `MAX_CONCURRENT_REPLAYS` = 4 replays at once and answers
`ReplayPending` beyond that. It reclaims a slot idle for `REPLAY_SLOT_TTL_MS` = 5000 ms, ten missed
heartbeats. A client sends `ReplayHeartbeat` about every 500 ms while its replay image is running.
`ReplayComplete` frees a slot immediately; a cold-start walk does not send it, because each segment
request supersedes the previous one and frees its slot.

> **Maximum pending wait = `MAX_CONCURRENT_REPLAYS` × `REPLAY_SLOT_TTL_MS` = 20 000 ms**: the worst
> case, in which every slot is held by a dead client and the slots are reclaimed one timeout apart. A
> consumer's recovery-stall timeout MUST exceed it by a margin. A client that receives `ReplayPending`
> waits at its gap and delivers nothing, so a shorter timeout would fire against a replayer that is
> working correctly. The client tier's default is 60 000 ms.

## 11. Versioning

This section covers seqeron's two schemas. Payload versioning belongs to the application (**V-2**).

| change | policy | affects |
| --- | --- | --- |
| schema 210 envelopes | frozen; any change is a new wire format | every producer and consumer |
| a submitted system event in schema 210 | allowed under **V-3**: a new `systemEventType` inside an unchanged envelope | the cluster tier and consumers of the system family |
| a synthesized system event in schema 210 | a frame-layer change: only the sequencer emits it, and it needs a template id | every producer and consumer |
| schema 212 | in place, no version bump (§10) | one node's build |
| an application schema | the application's | the application |

System messages have no `sinceVersion` fields: under **V-3** no decoder reads another build's bytes,
so there is nothing to stay compatible with.

- **V-1. Library versions are part of the wire format.** Every producer, consumer and cluster node
  MUST use the Aeron, Agrona and SBE versions seqeron was built with; a mismatch corrupts frames rather
  than failing the build. seqeron publishes these versions: `seqeron-bom` for Java, and for C++ the
  build fetches the pinned Aeron, and the installed package requires at least that version. The
  versions an application uses for its own payload codecs are its own choice.
- **V-2. Payload versioning is the application's.** A payload has exactly one encoder, its producer
  (**E-1**), so no two implementations need to agree; only an application's own old and new builds do.
  How it signals versions and what compatibility it guarantees are not specified here.
- **V-3. A change to schema 210 MUST NOT be rolled out across a running cluster, and no seqeron decoder
  may be required to read bytes encoded by an earlier build.** Two builds that encode the same
  synthesized frame differently break **F-2**; an archive that survives an upgrade is the same fault
  later. After such a change, every node's archive is purged. Schema 212 is exempt: it is not recorded
  and never leaves the node.

## 12. Limits

| limit | value | reference |
| --- | --- | --- |
| `blockLength` | 18 on ingress templates, 34 on `Sequenced`/`SequencedSystem` (exact, §9.2); 34, 46 and 38 on templates 104, 105 and 106 | §4.2, §7.1 |
| per-message overhead | 92 bytes on ingress (32 Aeron data header + 32 Aeron Cluster session header + 28 frame); 76 on the tap (32 + 44) | §4.2 |
| `MAX_PAYLOAD_LENGTH` | 1316 bytes = 1408 (MTU) − 92 | **T-2** |
| `MAX_INGRESS_LENGTH` | 28 + 1316 = 1344 bytes (condition 1) | §9.2 |
| `MIN_INGRESS_LENGTH` | 28 bytes (condition 1) | §4.2 |
| maximum sequenced frame | 44 + 1316 = 1360 bytes | §4.2 |
| consensus log | MUST carry a 32 + 1344 = 1376-byte message in one packet; MTU 1408 gives `maxPayloadLength` 1376 | cluster configuration |
| `globalSeqNo` | int64, starting at 1 | §9.1 |
| `timestamp` | int64, epoch nanoseconds (Aeron `NanosecondClusterClock`); a log recorded in another unit is refused | §9.3 |
| `CLUSTER_HEARTBEAT_INTERVAL_MS` | 1000 | §7, §9.3 |
| `GATEWAY_ACTIVATION_TIMEOUT_MS` | 5000 | §7.2 |
| `MAX_CONCURRENT_REPLAYS` | 4 | §10.1 |
| `REPLAY_SLOT_TTL_MS` | 5000 | §10.1 |
| maximum pending wait | 20 000 ms | §10.1 |

These constants belong to the protocol, not to any one component. Each language compiles in a copy:
`protocol/FrameLayer.java` and the `Limits` block of `protocol/SequencedFrame.hpp`. A change starts
here, is made in both, and is a wire change (**V-3**).

> **T-2.** A message fits in one Aeron MTU of 1408 bytes: headers plus `MAX_PAYLOAD_LENGTH` (1316).
> On the consensus log a message is 32 + 32 + 1344 = 1408 bytes; on the tap, 32 + 1360 = 1392. The
> constant is compiled in and is not configurable. At start-up a node MUST check that its ingress and
> tap MTUs are each at least 1408, and MUST refuse to start otherwise.

> **T-3.** The protocol's encode methods MUST refuse a payload longer than `MAX_PAYLOAD_LENGTH`;
> condition 1 is the backstop. The refusal is local and permanent: it is not back-pressure and MUST
> NOT be reported as retryable. What the producer does with the refused message is its own concern
> (**P-0**).

The encode methods are `SystemFrame.wrap`/`wrapPayload` (Java) and `publishPayload`/`publishSystem`
(C++). They report refusal as a return value, not an exception, because several callers run inside
Aeron poll callbacks, where an exception is swallowed (§9.4).

**No frame is ever fragmented.** **T-3** keeps oversized frames from reaching a transport and **T-2**
keeps the maximum within one MTU, so neither the recording nor a replay ever holds part of a frame. A
consumer MUST NOT rely on fragment reassembly on ingress, the tap or a replay.

Rationale for using the MTU rather than the term length: Aeron caps a publication's `maxPayloadLength`
at MTU − 32, while the term length caps a message at `min(termLength / 8, 16 MB)`. Only the MTU
guarantees one packet per message. The two MTUs are `aeron.mtu.length` (ingress over UDP) and
`aeron.ipc.mtu.length` (the tap); both MUST be at least 1408, Aeron's default. Raising the limit is a
protocol change (§11), not a deployment setting.

A payload larger than `MAX_PAYLOAD_LENGTH` must be split across frames or sent by another channel. A
split needs its own completeness rule; a `remaining` countdown, as in `GatewayRegistered`, is one.

## 13. Tooling and payload encodings

### 13.1 `SbeLogPrinter`

`SbeLogPrinter` (`sbe-log-printer.sh`) decodes schema 210 in full: every system message, each payload
decoded through its `systemEventType`. It does not decode application payloads. It labels each payload
with the `protocolName` from any `PayloadIdRegistered` frames in the recording (§6.3), and otherwise
with its number.

`-o <payloadId>` writes that protocol's payloads to stdout, raw and back to back, with no framing
between them, and sends every text line (the catalog line, the frame dump, errors) to stderr. A decoder
that owns the payload schema reads stdout:

```
sbe-log-printer.sh <archive-dir> --stream 205 -o <payloadId> | application-decoder
```

The payload's own schema delimits consecutive payloads. The frame dump on stderr is emitted in the same
order and serves for correlation.

### 13.2 Payload encodings

> **E-1.** An application or system payload that arrived through ingress is encoded once, by its producer, and
> is never re-encoded after sequencing. The exception is the three synthesized frames, which have no
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

`ConformanceTest`, beside `SequencerTest` in the Java suite and in `core_tests` for C++. It uses no
Aeron runtime or media driver and runs in under a second. The payload fixture is seqeron's own.

| # | asserts | rule |
| --- | --- | --- |
| 1 | **Copy fidelity.** A sequenced `Unsequenced` carries a byte-identical payload and an unchanged `payloadId`, for payloads of 0 bytes, 1 byte and `MAX_PAYLOAD_LENGTH` | §5 |
| 2 | **Prefix property.** Bytes 0–17 of each unsequenced composite equal those of its sequenced counterpart for the same values, offset 16 included; the two families' composites are byte-identical; encoded lengths are 18 and 34 | **F-3** |
| 3 | **System frames round-trip.** Each of the nine submitted events, wrapped as `UnsequencedSystem` and sequenced, keeps a byte-identical payload and its `systemEventType`; `ConnectionOpened` with empty, short and maximum-length `connectionData`. Each synthesized template, encoded by the sequencer, has the right template id, inline fields and `systemEventType` | §7 |
| 4 | **Rejection table.** One case per §9.2 condition; each asserts the frame was not sequenced, nothing was emitted, `globalSeqNo` did not move and the rejection counter rose by exactly 1. Cases: over `MAX_INGRESS_LENGTH` and under 28 (1); wrong `schemaId`, non-zero `version` (2); a non-ingress template, including each synthesized one (3); `blockLength` below and above 18 on both ingress templates (4); length prefix past the frame end, short of it, and 65535 (5); `sourceId` −1 (6); `payloadId` 0 and 1 (7); an unallocated `systemEventType` and each synthesized one (8); a `GatewayStarted` payload shorter than 8 bytes (9); **S-6**'s cases (10) | **S-4**, **S-5**, **S-7** |
| 4a | **Rejection denies nothing and repeats identically.** Two rejections under one `payloadId` each increment the counter and emit nothing; a well-formed frame with that `payloadId` afterwards is accepted unchanged | **S-7**, **C-2** |
| 4b | **The producer refuses before sending.** A payload of exactly `MAX_PAYLOAD_LENGTH` is published; one byte longer is refused with nothing offered to the transport, distinguishably from back-pressure, and the next well-formed publish succeeds. Uses an in-memory transport seam | **T-3**, §12 |
| 5 | **Synthesis is deterministic.** Two independent `Sequencer`s fed the same input emit byte-identical frames, heartbeats included | **S-3**, **F-2** |
| 6 | **Layout matches §4.** Every header field is at the §4.1 offset in all four composites; sizes match §4.2 (`MessageHeader` 8, composites 18 and 34, length prefix 2, overhead 28 and 44, `ClusterHeartbeat` 42, maximum payload frame 1360); payloads of 0 bytes, 1 byte and `MAX_PAYLOAD_LENGTH` round-trip | **F-2**, **F-3**, §12 |
| 7 | **Selective consumption.** A consumer given an unallocated `payloadId` and an unhandled `systemEventType` skips both without error, and its continuity tracking advances across them, for all five sequenced messages | **P-1**–**P-3** |
| 8 | **S-6.** A `GatewayStarted` matching its list row binds and is accepted. Rejected: the same frame with a different `gatewaySourceId`; an unlisted `gatewayId` claiming a listed `sourceId`; a `GatewayActivationRequested` for an unlisted `gatewayId`; another system frame with a listed `sourceId` on an unbound session. Accepted: an application payload with a listed `sourceId`, and a `clusterctl` marker (`sourceId` 2, `connectionId` −1) | **S-6** |
| 9 | **Promotion.** With ranks 0, 1, 2 under one `gatewaySourceId`: bootstrap activates rank 0 only; a `GatewayActivationRequested` is sequenced and answered at the next `globalSeqNo`; closing rank 0's session promotes rank 1; closing rank 1's promotes rank 0; an instance that publishes no `GatewayStarted` is replaced after exactly `GATEWAY_ACTIVATION_TIMEOUT_MS` of consensus time and not before; one that does arms nothing further; a `gatewaySourceId` with one row synthesizes nothing on close and `globalSeqNo` does not move. Two gateways bootstrapped together keep separate deadlines | §7.2, **S-3** |

Rows 2, 3, 4b, 6 and 7 run in both languages; rows 1, 4, 4a, 5, 8 and 9 are Java only, because the
sequencer is Java only.

Each row builds its frames from this document's rules; there is no stored byte corpus. A failure names
the rule and field that broke rather than reporting that bytes changed. A layout property that a
corpus would catch and these rows do not belongs in §4.

The suite checks framing and copy fidelity only; it never decodes an application payload.

---

## 16. Producer and replica obligations

What an application must do so that the order of §9.1 reaches its own consumers intact. seqeron cannot
enforce these rules, since they concern behaviour after the tap. The client tier implements them in
`org.limitless.seqeron.app` (Java and C++); the sender half of A-5 is in `sequencer.client`.

- **A-1. Leader-only work is gated.** A co-located replica MUST perform a leader-only side effect only
  while it is caught up and its own member is leader. Every applied `LeadershipChanged` closes the gate,
  including one that names the same member again, because a reply sent during that election may have
  been lost with it. (`LeaderGate`)
- **A-2. Leader-only work is tracked as outstanding.** A request is outstanding on every replica from
  its sequenced request until its sequenced reply. The leader MUST dispatch every outstanding request not
  yet dispatched since its gate opened, in `globalSeqNo` order. A failed reply offer MUST release that
  dispatch. (`OutstandingWork`)
- **A-3. A re-emission is identical.** A-2 delivers at least once. The reply MUST be a pure function of
  the sequenced request and SHOULD be keyed on the request's `globalSeqNo`, so that a duplicate is
  byte-identical. Consumers MUST drop duplicates by that key.
- **A-4. Ingress is confirmed on the tap, not at send.** A successful send means the frame reached the
  leader's ingress, not the log; egress confirms nothing. A producer that must not lose a frame MUST
  hold it as pending until its own tap shows it, matched by cluster session id, never by `sourceId`
  (which a gateway pair shares). A frame stamped with term T that has not appeared by the first
  `LeadershipChanged` with `leadershipTermId` > T is lost. The lost frames are exactly the newest
  frames stamped T, because a leader drops ingress stamped with any other term and every election
  discards unread ingress. If the producer sees an own frame that differs from its oldest pending one,
  per-session order no longer holds (usually because the sequencer rejected a frame, S-7), and the
  producer MUST fence. (`PendingSends`)
- **A-5. Lost frames are resent before anything new.** From the first sign of a newer term (egress
  `NewLeader` or the tap's `LeadershipChanged`, whichever comes first) until every older pending frame
  has been seen or resent, the producer MUST NOT place a new frame. This includes a send already
  retrying through the election: the sender learns of the new leader inside that retry, abandons the
  send and reports it not placed, with the session still open (`setIngressHold`, in both
  `ClusterStreamSender`s). Lost frames MUST be resent oldest first, once the sender stamps a term the
  tap has reached. A frame already in the log is never pending after it appears on the tap, so a resend
  creates no duplicate. (`PendingSends`)

A-4 and A-5 apply within one producer process. A restarted producer, or a standby promoted in its
place, starts with nothing pending; a lost cluster session stays lost; and a frame lost without a
leader change (an ingress image that drops and rejoins within the session timeout) has no term boundary
to be counted against.
