# seqeron protocol — specification

_Normative specification, 2026-09-02. The **what** and **how** only; the design record, the forces
behind each decision and the alternatives weighed are in `doc/seqeron-protocol.md`, which this
condenses and does not supersede. Rule identifiers are that document's, unchanged._

> **Status: specification of the target, not of the tree.** The tree is mid-§15: every message family
> is on a payload now (`sbe-frame.xml` 210 with core, plus `sbe-order.xml` 220, `sbe-session.xml` 230
> and `sbe-basicdata.xml` 240), the old pair is gone, and the replay control protocol stands alone in
> `sbe-replay.xml` (212). What is left is the registry work of steps 2 and 8 and §14's conformance
> suite. This states what `doc/future-arch.md` §11 step 3 lands; §15 is the migration and records what
> has landed so far. Statements about today's code are marked _(today)_.
>
> **Normative language.** MUST / MUST NOT / SHOULD as usual. Rules carry identifiers — **F-n** frame,
> **T-n** transport, **S-n** sequencer, **P-n** payload, **C-n** registration, **R-n** replay, **V-n**
> versioning, **E-1** encoding. §14 lists the ones a conformance test exists for.

---

## 1. Layers

seqeron owns the frame; the application owns the payload. **The cluster tier decodes `payloadId` 1 and
nothing else.**

| layer | what it is | schema | owner |
| --- | --- | --- | --- |
| **L0 transport** | Aeron channels, stream ids, the cluster ingress/egress session protocol | `sbe-cluster.xml` (mirror of `io.aeron.cluster.codecs`) | Aeron, mirrored by seqeron |
| **L1 frame** | the `Unsequenced`/`Sequenced` envelope pair — the only two templates on the recorded path | `seqeron-frame.xml` (210) | seqeron |
| **L2 payload** | one opaque, length-prefixed byte range per frame, named by `payloadId` | core: `seqeron-frame.xml` (210), beside the envelope; applications: their own | whoever `payloadId` names |
| **control plane** | replayer↔client replay control; off-frame, node-local, never sequenced, never recorded | `seqeron-replay.xml` (212) | seqeron |

> **P-0. seqeron's obligation to a payload is discharged in full when the frame carrying it has been
> sequenced, recorded and delivered in order with no gap.** seqeron never parses a payload and can
> reject one only for being malformed as a *frame* (§9.2). Meaning, consistency and acceptance are
> decided after delivery, by the application.

A declined payload is not a protocol event: the frame is already sequenced and delivered, nothing is
retracted or redelivered, and **P-3** keeps the consumer in sequence across it. Two consumers of the
same `payloadId` on different build vintages may disagree about whether a payload parses and both
conform.

## 2. Message classes

| class | originates with | authored by | appears on |
| --- | --- | --- | --- |
| **unsequenced** | an external producer | a gateway, an application, `clusterctl` | cluster ingress only |
| **sequenced** | an external producer, admitted and stamped | the sequencer, from an unsequenced frame | the tap, and replays of it |
| **synthesized** | the cluster itself | the sequencer, on its own initiative | the tap, and replays of it |

Synthesis and forwarding draw from one `globalSeqNo` counter (§9.1). Sequenced and synthesized frames
share the `Sequenced` template; the class is marked on the wire by the header — `sourceId`,
`connectionId` and `sessionId` all −1, because each names an external producer, its connection and its
session, and a cluster-originated message has none (§4.3, §7). **`sourceId == −1` is the mark proper**
(**F-4**) — it alone is reserved and it alone is refused on ingress, and the other two follow it; a
consumer MUST test `sourceId` and MUST NOT require the other two. All three are wire bytes under
**F-2**, not defaults. The mark is only as good as ingress: §9.2 conditions 6 and 9 are what make it a
statement of provenance rather than an unchecked assertion.

The classes partition frames, not payloads — `GatewayActive` is synthesized at bootstrap and forwarded
from `clusterctl` on promotion. The replay control messages (§10) are outside all three: not frames,
no header composite, never sequenced, never recorded.

---

## 3. Transport bindings

A consumer MUST validate `MessageHeader.schemaId` on every stream it reads (**T-1**).

| stream | channel / id | carries | recorded? |
| --- | --- | --- | --- |
| cluster ingress | Aeron Cluster ingress, inside the L0 session envelope | `Unsequenced` — schema 210, template 100 | in the Raft log |
| **the tap (Feeder)** | `aeron:ipc`, stream **205** | `Sequenced` — schema 210, template 101 | **yes**, by the co-located archive, on every node |
| replayer replay | stream **201** | `Sequenced` — replayed archive bytes, byte-identical to the tap | no |
| replayer request | stream **202** | schema 212, client → replayer | no |
| replayer control | stream **203** | schema 212, replayer → client | no |

- **F-1. The tap is the record.** No snapshots; recovery is always full-log replay from `globalSeqNo` 1,
  so a frame's bytes on the tap are the only durable statement of it that exists.
- **F-2. Every node's tap is byte-identical.** A replay served from any node's archive is substitutable
  for any other's. §9.3 exists to keep this true.

## 4. The frame layer

```
Unsequenced  (schema 210, template 100) { unsequencedHeader, payload:varData }
Sequenced    (schema 210, template 101) { sequencedHeader,   payload:varData }
```

The two differ only by the stamp; each body is one length-prefixed payload and nothing else.

### 4.1 Header layout

**F-3. `unsequencedHeader` is a byte-prefix of `sequencedHeader`.**

| offset | field | type | `unsequencedHeader` | `sequencedHeader` |
| --- | --- | --- | --- | --- |
| 0 | `sourceId` | int32 | ✓ | ✓ |
| 4 | `connectionId` | int32 | ✓ | ✓ |
| 8 | `sessionId` | int64 | ✓ | ✓ |
| 16 | `payloadId` | uint16 | ✓ | ✓ |
| 18 | `globalSeqNo` | int64 | — | ✓ |
| 26 | `timestamp` | int64 | — | ✓ |
| | **ENCODED_LENGTH** | | **18** | **34** |

Three changes from today: `globalSeqNo`/`timestamp` move to the **end**, `payloadId` becomes a header
field rather than a message field, and `origin` is deleted (§4.3). Consequences that other rules depend
on: every frame-layer field sits at a fixed offset on both sides, so one decoder covers both and a
consumer routes on `payloadId` without knowing which template it holds; sequencing is **copy-18,
append-16**; and `blockLength` is exactly the composite's `ENCODED_LENGTH`, which is what §9.2
condition 4 compares against.

**No pad.** `payloadId` is 2-byte aligned and `globalSeqNo` sits unaligned at 18. Both generators read
fixed-width fields through `memcpy` (SBE C++) and `UnsafeBuffer` (Agrona), which compile to single
unaligned loads on x86-64 and ARM64. **Plain accessors only** — `getLongVolatile`/`getAndAddLong` and
C++ atomics on an unaligned address are undefined.

**Both composites are renamed.** Both are `header` today, which is a collision once they share one file.
`unsequencedHeader`/`sequencedHeader` splits the generated `HeaderDecoder`/`HeaderEncoder` into two
names across **17 files** in Java and C++.

### 4.2 Frame sizes

| | `Unsequenced` | `Sequenced` |
| --- | --- | --- |
| `MessageHeader` | 8 | 8 |
| `blockLength` (the header composite, `payloadId` included) | **18** | **34** |
| var-data length prefix | 2 | 2 |
| **fixed overhead** | **28** | **44** |

`MIN_INGRESS_LENGTH` is 28 _(today: 25)_. A `ClusterHeartbeat` is 44 + 8 (its payload's own
`MessageHeader`, zero body) = **52**.

### 4.3 Provenance

There is no `origin` field _(today: `None`, `Client`, `Gateway`, `Application`)_. Provenance is
`sourceId`:

> **F-4. `sourceId == −1` is reserved for the cluster itself and marks the synthesized class.** Every
> other value names an external producer. An ingress frame MUST NOT carry it (§9.2, condition 6).

The direction an application needs (`Client` vs `Gateway`) was one application's vocabulary and moves
to the payload: a field of its own on the FIX session families, in the schemas that already define
them. `clusterctl` gets a reserved `sourceId` of its own (§5) so it no longer stamps −1.

## 5. Field authority

The ingress contract; **S-1** is that the sequencer enforces it.

| field | on `Unsequenced` | on `Sequenced` |
| --- | --- | --- |
| `sourceId` | producer-set, MUST NOT be −1 (§9.2, condition 6), **checked against the roster** (**S-6**) | copied verbatim; −1 marks synthesized (**F-4**) |
| `connectionId` | producer-set | copied verbatim |
| `sessionId` | producer-set, **advisory** | **overwritten** with the true Aeron Cluster session id the frame arrived on |
| `payloadId` | producer-set, MUST be non-zero | copied verbatim |
| `globalSeqNo` | **no such field** on the unsequenced header | sequencer-assigned (§9.1) |
| `timestamp` | **likewise** | Raft consensus time at commit |
| `payload` | producer-set | copied **byte-identical** |

The outer `MessageHeader` is the sequencer's on both sides, and is re-encoded rather than copied:

| field | on `Unsequenced` | on `Sequenced` |
| --- | --- | --- |
| `schemaId` | producer-set, MUST be 210 (condition 2) | 210 |
| `templateId` | producer-set, MUST be `Unsequenced` (condition 3) | **`Sequenced`** |
| `blockLength` | producer-set, MUST be 18 (condition 4) | **34** |
| `version` | producer-set, MUST be 0 | 0 |

`version` is 0 for the life of the frame schema, which is frozen (§11), and is not an extension point a
producer may write into _(today the sequencer copies the ingress `version` onto the tap)_.

**`sourceId` is one id space.** Two values are reserved; the rest is the roster's.

| `sourceId` | who | rostered? |
| --- | --- | --- |
| **−1** | the cluster itself — the synthesized class, and nothing else (**F-4**) | never; **illegal on ingress** |
| **2** | `clusterctl`, on every marker it submits | never |
| 0, 3, 5, 6 … | gateways and node-local publishers _(today: the C++ gateway pair 0, an unrostered node-local publisher 3, the venue leg 5, the order-entry leg 6)_ | gateways by `GatewayRegistered.gatewaySourceId`; the rest not |

`clusterctl`'s id MUST NOT be a `gatewaySourceId` any roster row claims. The roster check is scoped to
`payloadId` 1 (**C-1**):

> **S-6.** For a frame with `payloadId == 1` (§9.2, condition 10):
>
> 1. **`GatewayStarted`** — its `gatewayId` MUST name a roster row, and that row MUST carry
>    `gatewaySourceId == header.sourceId`. Both halves are required, and a `gatewayId` the roster does
>    not name is rejected whatever `sourceId` it carries.
> 2. **Any other core frame whose `sourceId` the roster claims** — it MUST arrive on a cluster session
>    already bound to a `gatewayId` of that `sourceId`.
> 3. **Everything else is unchecked** — every non-core payload, and every core frame other than
>    `GatewayStarted` carrying an unrostered `sourceId`.
>
> The binding is created by `GatewayStarted` and removed when that cluster session closes — the same
> event that promotes a standby (§7) — so the set holds one entry per live gateway session. Both edges
> are log events and the roster is log-derived, so the check is **S-3**-safe. A binding MUST NOT be
> removed on anything a node observes locally.

Case 1 is total, so **a `GatewayStarted` arriving before `load-topology` is rejected**: the roster it
is checked against is empty. That is the existing start-up order as a wire rule.

## 6. `payloadId`

A `uint16` naming a **decoder namespace and encoding** — one application schema, never one message. The
consumer maps it to a decoder module; that module reads its own framing.

### 6.1 Registry

The registry lives in the log: `clusterctl load-topology <file>` publishes one `PayloadIdRegistered`
core frame per row of the file's `<protocols>` section (§6.4).

| value | names | in scope here? |
| --- | --- | --- |
| 0 | unset — **invalid on the wire** | fixed by this document |
| 1 | **seqeron core** (`seqeron-frame.xml`, schema 210) | fixed by this document; **implicit** — never registered (**C-1**) |
| 2… | one per application schema or encoding | the deployment's to allocate; **registered only where the protocol is shared** |

_(This deployment today, all four allocated by §15 steps 3 and 4: **2** = the order family
(`sbe-order.xml` 220, the order flow and the portfolio query over it), **3** = the FIX session family
(`sbe-session.xml` 230, both edges' session layer), **4** = reference data (`sbe-basicdata.xml` 240).
**4** is the only one that crosses application boundaries and so the only one §6.4's example declares;
2 and 3 are private between the processes that speak them and need no row.)_

**The registry's subject is the shared protocol.** A `payloadId` that one application publishes and
that same application alone reads is **out of scope**: seqeron neither allocates it nor requires it
declared, and **P-1** and **C-2** mean the wire behaves identically either way — the only thing a
missing row costs is the label §13.1 would have printed. What this document fixes is 0 and 1, and the
requirement that a protocol crossing application boundaries be declared.

A `payloadId` whose protocol does cross those boundaries needs a **single owning repo**, and every
consumer links its codecs and tracks its version like any other dependency — a schema two repos may
edit re-creates one layer up the byte-identity problem the envelope deletes. Its publisher is an
application like any other, never the cluster tier. seqeron's involvement ends at the number.

It is nonetheless **one id space**: two applications that privately pick the same number collide on the
tap, and no rule here detects it — **P-4** catches a wrongly selected decoder only where the encoding
self-describes. Allocating across applications is the deployment's, and declaring the crossing ones is
the part this document requires.

### 6.2 Selective consumption

- **P-1. An unrecognised `payloadId` is skipped.** A consumer MUST NOT treat one as an error.
- **P-2. Within a `payloadId` it recognises, a consumer skips inner templates it does not handle**, and
  MUST NOT treat those as an error either. The sequencer is an instance of this (**S-2**).
- **P-3. Frame continuity does not depend on payload comprehension.** Gap detection, de-duplication and
  the recovery walk are anchored on `header.globalSeqNo`, read at a fixed offset **before any payload
  dispatch** (`ReplayerRecovery`). A consumer counts every frame it ignores; a skip can neither
  manufacture nor mask a gap.
- **P-4. `payloadId` selects a decoder; the payload's own framing verifies it.** Before decoding a
  field, a consumer confirms against whatever the payload's encoding self-describes — for SBE,
  `MessageHeader.schemaId` against the schema the decoder was generated from (§13.2 for the others).
  **Two dispositions:** a *consumer* whose check fails **skips** the frame, as P-1 skips an unrecognised
  `payloadId`; the *sequencer at ingress* **rejects** (§9.2, condition 8), because it is deciding
  admission rather than consumption.

### 6.3 Registration is labeling, not admission

```
PayloadIdRegistered  (core, template 23, block 36)
    { payloadId:uint16, protocolVersion:uint16, protocolName:char[32] }
```

One row per **shared** protocol (§6.1); an application's private `payloadId` has none.
`protocolName` is the label `SbeLogPrinter` puts on a payload it cannot decode (§13.1); `protocolVersion`
is which revision the deployment runs, printed beside it and checked by nothing. It is **not** the
payload's `schemaId` — a `payloadId` may name an encoding that has none. No `description`, no
`remaining` countdown.

- **C-1. `payloadId` 1 is core, is never registered, and MUST NOT be registrable.** Enforced where the
  row is written: the topology file's schema constrains `payloadId` to 2 or above (§6.4).
- **C-2. Registration does not gate.** The sequencer MUST NOT reject a frame for carrying an
  unregistered `payloadId`, and MUST NOT decode `PayloadIdRegistered` at all. A consumer MUST NOT read
  registration as permission: **P-1** is unchanged and unconditional.
- **C-3. De-duplicated on `payloadId` by whoever reads the set** — `SbeLogPrinter`, and nothing else
  today — so re-running the load is safe; a later row supersedes the name and version.

A per-node *configured* allowlist is forbidden: a node whose config rejected a frame its peers accepted
would fork `globalSeqNo` (**S-3**). Any future gate must be log-derived.

Runbook order is unchanged and carries no new constraint: `clusterctl start` → `load-topology` →
application data loads. No frame depends on the `<protocols>` rows.

### 6.4 The topology file

One operator file, two sections, two core payloads: `<gateways>` produces the `GatewayRegistered`
roster (§7), `<protocols>` the `PayloadIdRegistered` rows above — one per **shared** protocol, and
nothing for an application's private `payloadId` (§6.1). It is XML with a schema shipped
beside it _(today: CSV, roster only)_ — **C-1** is a constraint on what may be *written*, and the
schema is where a constraint on writing belongs.

```xml
<topology>
  <gateways>
    <gateway name="GW-A" id="1" sourceId="0" rank="0"/>
    <gateway name="GW-B" id="2" sourceId="0" rank="1"/>
  </gateways>
  <protocols>
    <protocol payloadId="4" version="1" name="phixeron-basicdata"/>
  </protocols>
</topology>
```

Attributes are the payload fields under short names: `name`/`id`/`sourceId`/`rank` are
`gatewayName`/`gatewayId`/`gatewaySourceId`/`preferenceRank` (§7.1), and
`payloadId`/`version`/`name` are `payloadId`/`protocolVersion`/`protocolName` (§6.3). `remaining`
appears in neither section — it is the publisher's, counted off the row count.

| constraint | enforced by | why there |
| --- | --- | --- |
| `name` 1..32 printable US-ASCII | schema | the `char[32]` it encodes into, and a launch-time join key |
| `id` int32, `rank` uint8, `version` uint16 | schema | the field widths |
| `sourceId >= 0` | schema | −1 is the cluster's own (**F-4**) and never a roster row's |
| `payloadId >= 2` | schema | **C-1** — 0 is invalid on the wire, 1 is core |
| `id`, `name` and `payloadId` each unique | schema, as identity constraints | a duplicate `gatewayId` silently drops an instance; a duplicate `payloadId` is an operator slip, not **C-3**'s supersede |
| exactly one `rank="0"` per `sourceId` | the loader | not expressible per row; a second rank-0 leaves a logical gateway an arbitrary primary |
| `sourceId` is none of the reserved ids (§5) | the loader | likewise — it is a check against a constant, not a field range |
| `<gateways>` non-empty; `<protocols>` MAY be absent or empty | schema | an empty roster elects nobody; a deployment whose applications share no protocol declares none |

**The schema is the loader's, not the document's.** A loader MUST resolve the schema from its own
artifact and MUST NOT honour a schema location the document names: a file that names its own schema
can name a lax one, and **C-1** would be advisory. A document MAY carry the location as an editor
affordance.

**Publish order.** The loader MUST validate the whole file before publishing any row — a file that
fails half-way leaves a roster the log has already closed. The roster rows are then published as one
contiguous run in file order, `remaining` counting down to 0 on the last; **nothing may fall between
them**, because that last row is the completeness edge §7.2 synthesizes the bootstrap `GatewayActive`s
behind. The protocol rows follow it, and carry no countdown of their own.

## 7. Core payloads — `payloadId` 1

Ten messages in `seqeron-frame.xml` (schema 210), **defined once each**, beside the envelope pair they
ride in — same owner, same artifact, same change rule, so they are one schema. Core is an application
of its own, read by the sequencer the way an application's payloads are read by that application.

**Template ids are unique per schema, so the envelope pair moved rather than core.** `Unsequenced` and
`Sequenced` are **100** and **101**; `ClientConnected` and `ClientDisconnected` keep 1 and 2. The
envelope is new and nothing has ever encoded it, while core's ids are already in recordings — which is
the same argument that fixes the rest of the ids below.

| payload | id | ingress-legal? | synthesized? | sequencer **decodes** it? |
| --- | --- | --- | --- | --- |
| `ClientConnected` | 1 | ✓ gateway | — | ✓ open-connection set |
| `ClientDisconnected` | 2 | ✓ gateway | — | ✓ open-connection set |
| `LeadershipChanged` | 5 | ✗ | ✓ per term, de-duplicated | — (encode only) |
| `ClusterStarted` | 10 | ✓ clusterctl | — | — |
| `ClusterStopped` | 11 | ✓ clusterctl | — | — |
| `ClusterHeartbeat` | 16 | ✗ | ✓ 1 Hz | — (encode only) |
| `GatewayRegistered` | 17 | ✓ clusterctl `load-topology` | — | ✓ roster + `remaining == 0` election edge |
| `GatewayActive` | 18 | ✓ clusterctl `activate` | ✓ bootstrap, and on every promotion | — (encode only) |
| `GatewayStarted` | 19 | ✓ gateway | — | ✓ binds cluster session → gatewayId; releases stale connections |
| `PayloadIdRegistered` | 23 | ✓ clusterctl `load-topology` | — | — (encode only; §6.3) |

Ids are the ones they hold today. `PayloadIdRegistered` is new and takes **23**, clear of the retired
ids 12–15, so a mixed-vintage recording can never read as this frame. `ClusterHeartbeat` keeps id 16 under a new name (`Tick` until 2026-08-30) and is
unrelated to `ReplayHeartbeat` (§10).

**`GatewayActive` has four producers, three of them the cluster.** The sequencer synthesizes it at
bootstrap (one per rank-0 roster row, behind the `GatewayRegistered` whose `remaining` reaches 0) and on
promotion by two paths — the active instance's cluster session closing (`Sequencer.sessionClosed`), and
a designated instance failing to answer with a `GatewayStarted` within `GATEWAY_ACTIVATION_TIMEOUT_MS`
(`Sequencer.pendingGatewayActivationTimeout`). `clusterctl activate` is the fourth and the only ingress
one.

**The header a synthesized frame carries** (§5's table is the ingress contract and does not cover it):

| field | value |
| --- | --- |
| `sourceId`, `connectionId`, `sessionId` | **−1** _(today: `Sequencer.NO_SOURCE_ID`)_ — reserved for exactly this and refused on ingress (**F-4**) |
| `globalSeqNo` | the next value off the counter ingress shares (§9.1) |
| `timestamp` | the Raft consensus timestamp — never `System.currentTimeMillis()` (**S-3**) |

These are wire bytes under **F-2**, not defaults.

> **S-2. The sequencer's entire decode surface is core's inner framing plus the four rows marked ✓
> above.** §9.2 conditions 8 and 9 read the inner `MessageHeader` — schema id and template id — of every
> `payloadId` 1 frame; only those four templates are opened past it. Every application payload and
> core's own encode-only frames are opaque to it.

**Ingress-legal ✗ means the sequencer MUST reject it on ingress** (`ClusterHeartbeat`,
`LeadershipChanged`). Condition 6 (reserved `sourceId`) and condition 9 (synthesis-only template) are
independent: one catches an ingress-legal template submitted at −1, the other a `ClusterHeartbeat`
submitted under a producer's own id.

**Ingress-legality is a well-formedness rule, not authorization.** Aeron Cluster is Raft —
crash-fault tolerant, not Byzantine — so a dishonest producer is out of scope, and every rule here
exists against misconfiguration and software defect. The trust boundary is the perimeter: network
topology and authentication at the external FIX edges (`doc/todo.md`), which touches no rule here.

### 7.1 Payload fields

**A core payload carries no `header` field.** The header composite is the frame's (§4.1); a payload
that repeated it would nest one envelope inside another. This is the level of nesting the envelope
collapses, and it is why `GatewayStarted`'s floor in §9.2 condition 10 is 8 + 8 rather than 8 + 26.

**Nine of the ten have no var-data and no repeating group**, so their encoded length is exactly their
own 8-byte `MessageHeader` plus their block, and that block is the sum of their fields with no padding.
`ClientConnected` is the one exception and carries a single opaque var-data field (below).

| payload | id | block | payload bytes | fields |
| --- | --- | --- | --- | --- |
| `ClientConnected` | 1 | **0** | 10 + *n* | `connectionData` varData — **opaque to core** |
| `ClientDisconnected` | 2 | **0** | 8 | none — `header.connectionId` is the whole message |
| `LeadershipChanged` | 5 | 4 | 12 | `newLeaderMemberId` int32 |
| `ClusterStarted` | 10 | 8 | 16 | `correlationId` int64 |
| `ClusterStopped` | 11 | 8 | 16 | `correlationId` int64 |
| `ClusterHeartbeat` | 16 | **0** | 8 | none — the frame's `timestamp` is the whole message |
| `GatewayRegistered` | 17 | 43 | 51 | `remaining` uint16, `gatewayId` int32, `gatewaySourceId` int32, `gatewayName` char[32], `preferenceRank` uint8 |
| `GatewayActive` | 18 | 4 | 12 | `gatewayId` int32 |
| `GatewayStarted` | 19 | 8 | 16 | `gatewayId` int32, `firstConnectionId` int32 |
| `PayloadIdRegistered` | 23 | 36 | 44 | `payloadId` uint16, `protocolVersion` uint16, `protocolName` char[32] |

Field semantics that other rules depend on:

- **`connectionData`** — see below.
- **`newLeaderMemberId`** — the Aeron Cluster `memberId` that is leader from this `globalSeqNo` onward.
  Synthesized once per term, de-duplicated on this value.
- **`correlationId`** — assigned by `clusterctl` and echoed in the frame, so the tool can match its own
  marker coming back off the tap.
- **`remaining`** — roster rows still to come after this one; **0 marks the last row**, which is the
  completeness edge the sequencer synthesizes the bootstrap `GatewayActive`s behind.
- **`gatewayId`** — the roster row's own identity, unique across the roster. **`gatewaySourceId`** — the
  logical gateway the row belongs to, shared by every instance of it, and the value **S-6** matches
  `header.sourceId` against. **`gatewayName`** char[32], US-ASCII, `0x00` in byte 0 signalling absent —
  the launch-time identity an instance resolves its own `{gatewayId, gatewaySourceId}` by.
- **`preferenceRank`** uint8 — **0 is the designated primary**, 1, 2 … are standbys of the same
  `gatewaySourceId`. This is what "one per rank-0 roster row" in the bootstrap synthesis means.
- **`firstConnectionId`** — the `header.connectionId` this gateway instance allocates from, chosen past
  the highest its own replay held, so connection ids do not collide across a restart.
- **`protocolVersion` / `protocolName`** — §6.3; `protocolName` is `char[32]`, US-ASCII, trailing
  `0x00`, and neither field is read by the sequencer (**C-2**).

`ClusterHeartbeat`'s empty block is deliberate and is what makes it the cheapest frame in the system:
its content is entirely the frame's consensus `timestamp` and `globalSeqNo`, so the payload is its
`MessageHeader` alone (§4.2's 52-byte frame).

**`ClientConnected` carries an opaque tail; `ClientDisconnected` does not, and the asymmetry is
deliberate.** Both *frames* are core because connection lifecycle is core functionality: the sequencer
keys its open-connection set on `header.sourceId` and `header.connectionId`, and releases every
connection still open under a `gatewaySourceId` behind that logical gateway's next `GatewayStarted` — a
crashed instance publishes none of the `ClientDisconnected`s that would have closed them out. **That set
is node-local sequencer state and the release synthesizes no frame**: `ClientDisconnected` has one
producer, the gateway, and a promoted instance therefore inherits no open connection. A consumer keeping
a view of its own derives it from the same replayed frames, and **S-3** keeps every node's set identical.
For a **disconnect that is the whole message** — `header.connectionId` names a connection every
consumer already saw connect, so there is nothing to add and the payload is its `MessageHeader` alone.

A **connect** is different, and `header.connectionId` alone is not enough for it. The id is minted by
the gateway, one per connection, so a counterparty that reconnects arrives under a *new* id while
resuming the *same* session — which means the id cannot carry identity, and a standby that has never
held the socket learns the id→session mapping from this frame or not at all. It needs that mapping at
connect, before the session's first message. But *what* the identity is — a FIX comp-id pair for one
gateway, something else for the next — is the producing application's, and the cluster tier has no use
for it. So it rides in `connectionData`, and the following hold:

- **Core MUST NOT decode `connectionData`.** The sequencer dispatches this template on template id alone
  and reads nothing below `blockLength`; **S-2** is unchanged, because opening a core template is not the
  same as opening this field.
- **Its encoding is the producing gateway's**, identified by `header.sourceId` — the same value that
  routes the frame. A consumer that does not recognise the producer skips the tail exactly as **P-1**
  skips an unrecognised `payloadId`. **E-1** covers it: the frame arrives through ingress, so the tail is
  encoded once by the gateway and replicated as bytes, and any encoding is therefore safe.
- **It MAY be empty**, and a length prefix of 0 is well-formed: a gateway whose connections need no
  identity beyond the id sends nothing. Core MUST accept both.
- **It counts against `MAX_PAYLOAD_LENGTH`** like any other payload byte (§12): the whole core payload,
  tail included, is bounded by the frame it sits in.
- **No new reject condition.** §9.2 condition 8 already establishes the 8-byte `MessageHeader` the frame
  is read through, and nothing past it is read, so there is nothing further for the sequencer to check —
  consistent with it passing through every core template it does not decode.

### 7.2 Promotion order

Every synthesized `GatewayActive` names one **`gatewayId`**, never a `gatewaySourceId`. Every instance of
a pair shares the sourceId, so a frame carrying one would designate both at once — which a consumer could
survive only by latching the first match it ever saw, and that in turn makes a restarting instance
re-activate itself off a superseded frame during its cold-start replay.

**The target.** Given the instance that lost the role, the target is the **lowest-`preferenceRank` roster
row of the same `gatewaySourceId`, excluding that instance**, ties broken by **roster order** — the first
such row in the log wins. If there is no such row, because the instance has no roster row or has no
sibling, the sequencer synthesizes **nothing**: it logs, `globalSeqNo` does not move, and that logical
gateway has no active instance until one starts. Fail closed.

It is lowest-rank-**excluding**, not next-rank-up: after rank 0 loses the role to rank 1, a later loss by
rank 1 hands it back to rank 0. That is what makes a wholly-down gateway tier converge on whichever
instance comes up first, rather than on the one the cluster happened to designate first.

**Three triggers, one target rule:**

1. **Bootstrap** — behind the `GatewayRegistered` whose `remaining` reaches 0, one `GatewayActive` per
   **rank-0** row, iterated in roster order.
2. **Session close** — the cluster session a `GatewayStarted` bound (§5, **S-6**) closes. The binding is
   removed first, then the target rule runs against the instance it named.
3. **Activation timeout** — a designated instance has not answered with a `GatewayStarted` within
   `GATEWAY_ACTIVATION_TIMEOUT_MS`.

An **ingress** `GatewayActive` — `clusterctl activate` — is none of these. It is forwarded like any other
core frame, the sequencer decodes nothing of it (**S-2**), and it therefore **arms no deadline**: a manual
activation naming an instance that never starts is not handed over. Trigger 3 exists because a
`GatewayStarted` is the only frame that registers an instance, so an instance that dies or wedges before
publishing one was never registered and no session close can promote past it.

**The deadline.** `GATEWAY_ACTIVATION_TIMEOUT_MS` is **5 × `CLUSTER_HEARTBEAT_INTERVAL_MS` = 5000 ms**
(§12). Every *synthesized* `GatewayActive` arms one at `timestamp + GATEWAY_ACTIVATION_TIMEOUT_MS` on the
consensus clock — **including the one a promotion produces**, so successive failures walk the roster
instead of stalling on the first.

At most **one armed activation per `gatewaySourceId`**, replaced in place, held in **arm order**. Both
halves matter: two logical gateways bootstrapping back to back must not overwrite each other's deadline,
and the order the deadlines are walked in MUST be the log's on every node (**S-3**).

**Evaluation** runs on the 1 Hz `ClusterHeartbeat`'s consensus timestamp, immediately after the heartbeat
frame it belongs to, never on a local timer. The sequencer takes the **first** armed activation in arm
order whose deadline has passed and removes it; if no `GatewayStarted` has meanwhile bound that
`gatewayId` to a session, it designates a new target from it by the rule above; otherwise the activation
was answered and is simply dropped. **At most one promotion per heartbeat.**

Every input is log-derived — the roster from `GatewayRegistered`, the binding set from `GatewayStarted`,
the clock from the consensus timestamp — so **S-3** holds and every node synthesizes the same frame at the
same `globalSeqNo`.

---

## 8. Schemas

| file | schema id | holds | change policy (§11) |
| --- | --- | --- | --- |
| `seqeron-frame.xml` | **210** | `Unsequenced`, `Sequenced`, the two header composites, `messageHeader`, `varDataEncoding`, **and the ten core payloads** (§7) | the envelope **frozen**; core changeable under **V-3** |
| `seqeron-replay.xml` | **212** | the six `Replay*` control messages (§10) | in-place, no version bump |

**The envelope and core are one schema because they are one thing.** Both are seqeron's, both ship in
the same artifact, and **V-3** governs both identically — a separate schema id bought only a distinction
nothing reads. `version` stays **0** for the life of the file, core changes included: **V-3** forbids a
mixed-vintage cluster, so no decoder ever meets bytes another build encoded and there is nothing for a
version to signal. Schema 211 is unallocated.

**An application `xi:include`s nothing of seqeron's and regenerates nothing of it.** It defines its own
payload encoding — a schema if that encoding has one, nothing at all if it does not — and reads the
frame through seqeron's compiled codecs: the jar for Java, the published headers for C++. There is no
`common-types.xml`.

seqeron ships both schemas' codecs as **one artifact per language**: `ReplayerRecovery` decodes
`LeadershipChanged` (core) while handling a `Replaying` (control) inside a `Sequenced` walk, so the
two are one link-time unit.

## 9. Sequencer rules

### 9.1 The `globalSeqNo` contract

- **S-4.** Starts at **1** for the first frame the cluster ever emits, increments by exactly one per
  emitted frame, is never reused, and has **no gaps** — a consumer observing a gap has lost frames and
  MUST recover through the replayer, never skip.
- Ingress frames and synthesized frames draw from the **same** counter.
- **A rejected ingress message does not advance it and leaves no record in the log at all** — not of
  itself, not of the refusal. The whole trace is node-local (§9.6).
- **S-5. Every reject condition of §9.2 is evaluated before the counter advances *and* before any state
  mutation.** _(Today both halves bite: the sequencer increments, then mutates the roster, the session
  binding and the open-connection set, and only then encodes `origin` — the field §4.3 deletes, and the
  field whose out-of-range value throws there.)_
- Recording *position* is only how the archive is addressed; the replayer needs no
  `globalSeqNo → position` index.

### 9.2 Validation

The sequencer MUST reject, **in this order** — each condition establishes what the next may read — and
MUST NOT throw (§9.4).

| # | reject when | today? |
| --- | --- | --- |
| 1 | `length < MIN_INGRESS_LENGTH` (28), or `length > MAX_INGRESS_LENGTH` (§12) | ✓ floor; ceiling **new** |
| 2 | `MessageHeader.schemaId != 210`, or `MessageHeader.version != 0` | ✓ |
| 3 | `MessageHeader.templateId != Unsequenced` | ✓ |
| 4 | `blockLength != unsequencedHeader.ENCODED_LENGTH` (18), exactly | ✓ |
| 5 | the var-data length prefix is not a usable payload length: it is 65535 (`varDataEncoding`'s `nullValue`), or `MIN_INGRESS_LENGTH + payloadLength != length` | ✓ |
| 6 | `sourceId == -1` — reserved for the cluster's own frames (**F-4**) | **new** |
| 7 | `payloadId == 0` | ✓ |
| 8 | `payloadId == 1` and the payload is shorter than the 8-byte `MessageHeader`, or that header's `schemaId` is not 210 (**P-4**) | ✓ |
| 9 | `payloadId == 1` and the inner `templateId` is synthesis-only (`ClusterHeartbeat`, `LeadershipChanged`) | ✓ |
| 10 | `payloadId == 1` and the frame fails **S-6** — a `GatewayStarted` whose payload is shorter than the 8-byte `MessageHeader` plus the **sequencer's own** `GatewayStarted` block length (**16 bytes in all**), or whose `gatewayId` names no roster row, or whose row names a different `gatewaySourceId`; or another core frame claiming a rostered `sourceId` on an unbound session | ✓ body floors; roster and session checks **new** |

Rejection logs and counts (§9.6) and emits nothing — neither the rejected message nor any report of
it — and `globalSeqNo` does not move.

How to implement the sharp ones:

- **Conditions 4 and 5 are equalities, not floors.** Both directions are silent if admitted: a short
  `blockLength` puts the var-data prefix inside the header composite; a long one, or a prefix short of
  the frame end, silently drops bytes and re-emits the frame a size smaller, deterministically, on every
  node. Condition 4 compares against the composite's own `ENCODED_LENGTH`, not a hand-written constant.
  _(Today the 28-byte floor is `Sequencer.MIN_FRAME_LENGTH`.)_
- **The `MAX_BLOCK_LENGTH` check is retired** — under the envelope the block is a constant and the only
  varying length is the payload's, bounded by condition 1. _(Retired in the tree with §15 step 4, which
  took the last traffic off the copy-through branch and the branch with it.)_
- **The ceiling is a compiled-in protocol constant, never a transport read per frame.** **S-3** forbids
  checking against a node's MTU: a node provisioned smaller than its peers would fork `globalSeqNo`. The
  per-node part is **checked at start-up, never per frame** — a node whose ingress or tap MTU cannot
  carry the constant refuses to start (**T-2**, §12).
- **Condition 1 is a backstop** (**T-3**): a conforming producer's encode method refuses an oversized
  payload on the caller's own stack. What condition 1 catches is a producer not running a conforming
  implementation.
- **Conditions 8 and 10 each carry their own floor.** Fitting the frame exactly says nothing about being
  long enough for what the next condition reads. Condition 9 needs no floor — 8 established the eight
  bytes it reads `templateId` from.
- **That floor is the decoder's compiled constant, never the wire's `blockLength`.** An SBE decoder
  reads a fixed-width field at its fixed offset whatever acting block length it was wrapped with, so a
  producer-declared length bounds nothing. The constant is `8 + GatewayStartedDecoder.BLOCK_LENGTH` = 16
  (`gatewayId`, `firstConnectionId`), and the same rule holds for any core template a later condition
  reads a body field from.
- **Condition 6 is an int32 compare** — no enum to decode, nothing that can throw. The `Origin` enum it
  replaces is already gone (§15 step 1), so what is left is the compare itself; until it lands, a frame
  submitted at the reserved `sourceId` is sequenced like any other.
- **Condition 8 precedes 9 and 10** and is the sequencer's instance of **P-4**: reading a `templateId`
  out of an unverified payload is reading a foreign schema's numbering as core's.

### 9.3 Determinism

> **S-3. Every reject decision, and every byte of every synthesized frame, MUST be a pure function of
> the committed log.** Not node config, not wall-clock time, not local state, not map iteration order.

Conditions 1–9 are pure functions of the message bytes; condition 10 reads state, and every input it
reads is log-derived (the roster from `GatewayRegistered`, the binding from `GatewayStarted`). That
split is a property to preserve.

- `timestamp` is the **Raft consensus timestamp**, never `System.currentTimeMillis()`.
- Synthesis over a collection (the bootstrap `GatewayActive` per rank-0 row) MUST iterate a
  deterministically ordered structure.
- Every synthesis *deadline* MUST be evaluated against the consensus clock, never a local timer. The
  `GATEWAY_ACTIVATION_TIMEOUT_MS` promotion is driven off the 1 Hz `ClusterHeartbeat`'s timestamp.
- `ClusterHeartbeat` fires on a **cluster timer**, not a local scheduler.

### 9.4 No callback may signal failure by throwing

`Image.boundedControlledPoll` has already advanced the log position past the message and `AgentRunner`
keeps the agent alive, so a throw drops the frame and carries on — a silent hole in a log with no
snapshots. This holds client-side too (`Image.poll` hands the exception to the error handler and
advances the subscriber position anyway), which is why a client records the fault and raises it from
its own duty cycle instead.

### 9.5 Encoding a frame

`sequenceMessage`: decode `MessageHeader` + `unsequencedHeader`; encode a fresh outer `MessageHeader`
(`Sequenced`, `blockLength` 34, schema 210, version 0 — all four constants, none copied); copy the
18-byte prefix (`payloadId` included); overwrite `sessionId`; append `globalSeqNo` + `timestamp`; copy
the length-prefixed payload verbatim.

Synthesis (`ClusterHeartbeat`, `LeadershipChanged`, bootstrap `GatewayActive`) encodes the core payload
**directly at the payload offset** and back-fills the var-data length — no scratch buffer, no second
copy.

### 9.6 What a rejection leaves behind

> **S-7. Every rejection MUST advance a rejected-frame counter and write a log line naming the condition
> and the offending cluster session. Nothing is emitted on the wire: the sequencer synthesizes no
> report, and the log holds no trace of a frame it refused.**

- **Two node-local Aeron counters:** the total rejected-frame count and a per-condition breakdown. They
  are also a **cross-node determinism canary** — every node rejects the same frames (**S-3**), so nodes
  that have applied the same log prefix MUST agree on every count; a divergence is a forked tap
  (**F-2**). A restarting node rebuilds them over its full-log replay.
- **The log line MUST be written and SHOULD be rate-limited.** It carries the condition, the cluster
  session, the frame length, and the `payloadId`/`sourceId`/`connectionId` where §9.2's order has
  established them. The counters stay exact through the suppression.
- The sequencer holds **no state derived from a rejection** — no deny list, nothing rebuilt on replay,
  nothing that can amplify.

Where the signal actually goes: under **T-3** a conforming producer is refused synchronously by its own
encode method; the state conditions are gated by the implementation — a gateway MUST refuse to publish
anything before it has resolved its own `{gatewayId, gatewaySourceId}` from the roster it read off the
tap (§5, **S-6**), which is what keeps a `GatewayStarted` from arriving ahead of `load-topology`. What
is left for the counters is a producer not running a conforming implementation.

## 10. The replay control protocol — schema 212

Six messages, replayer↔client, on streams 202/203. They carry no header composite, are never sequenced,
and are never recorded.

| message | direction | means |
| --- | --- | --- |
| `ReplayRequest` (6) | client → replayer | "replay for me": `segmentIndex >= 0` walks the per-leader-tenure recording chain from position 0; `segmentIndex < 0` resumes the active recording at `fromPosition` |
| `Replaying` (7) | replayer → client | "attach to `replaySessionId` on stream 201, until `catchUpPosition`" |
| `ReplayPending` (8) | replayer → client | "all slots busy — **hold at your gap**, do not advance past the hole" |
| `ReplayComplete` (9) | client → replayer | "caught up; free my slot now" (rather than at idle-TTL) |
| `ReplayHeartbeat` (20) | client → replayer | "still running my replay" — every ~500 ms, refreshing the slot |
| `ReplayUnavailable` (21) | replayer → client | "this node cannot serve history at all" — permanent for the replayer's lifetime, unlike `ReplayPending` |

- **R-1. `requestId` supersedes.** Incremented on every send *including a verbatim resend*, and echoed
  in the reply. A client MUST ignore any reply whose `requestId` is not its current one.
- **R-2. `NO_REPLAY_NEEDED` terminates a walk only with `recordingId == -1`.** One that still names a
  recording means only that *that* segment is empty — skip it and request the next.
- **R-3. `recordingId` detects a chain shift.** The replayer re-resolves the chain on every request and
  can drop a stale still-recording span. A client MUST remember the `recordingId` it last saw for its
  current index and, on a mismatch, abandon the walk and restart from segment 0.
- **R-4. The slot TTL is an *idle* timeout**, refreshed by `ReplayHeartbeat` — a replay has no upper
  time bound.

**Completion is detected by position, not by image close**: a bounded replay of an active recording does
not close its image at the bound.

**Versioning: in place, no bump.** Nothing records these, and the Java replayer and the C++ apps on a
node are built and restarted together.

### 10.1 Message fields

Each is a standalone SBE message on stream 202 or 203 with its own 8-byte `MessageHeader` (schema 212)
and no frame header, so its encoded length is 8 + block. None has var-data or a repeating group.

| message | id | block | fields |
| --- | --- | --- | --- |
| `ReplayRequest` | 6 | 24 | `clientId` int32, `requestId` int64, `fromPosition` int64, `segmentIndex` int32 |
| `Replaying` | 7 | 36 | `clientId` int32, `requestId` int64, `replaySessionId` int64, `catchUpPosition` int64, `recordingId` int64 |
| `ReplayPending` | 8 | 12 | `clientId` int32, `requestId` int64 |
| `ReplayComplete` | 9 | 4 | `clientId` int32 |
| `ReplayHeartbeat` | 20 | 4 | `clientId` int32 |
| `ReplayUnavailable` | 21 | 12 | `clientId` int32, `requestId` int64 |

- **`clientId`** — identifies the requesting replica on the shared control stream, which is how a client
  tells its own replies from another's. It is **not** sufficient on its own to match a reply to a
  request; `requestId` is (**R-1**).
- **`requestId`** — the client's, incremented on every send including a verbatim resend, echoed by the
  replayer in `Replaying`, `ReplayPending` and `ReplayUnavailable`. `ReplayComplete` and
  `ReplayHeartbeat` carry none: neither is answered, so neither can be superseded.
- **`segmentIndex`** — `>= 0` walks the per-leader-tenure recording chain from position 0 at that index;
  `< 0` resumes the active recording at `fromPosition`. **`fromPosition`** is read only in the resume
  case.
- **`replaySessionId`** — the Aeron replay session to attach to on stream 201, **or the sentinel
  `NO_REPLAY_NEEDED` = −1** (`Aeron.NULL_VALUE`), which is what carries R-2 on the wire: there is no
  separate message for it.
- **`catchUpPosition`** — the position at which the client stops following the replay image and falls
  back to the live tap, de-duplicating on `globalSeqNo`. Completion is detected by reaching it, not by
  image close.
- **`recordingId`** — the recording the answered `segmentIndex` resolved to, or **−1 meaning the chain is
  exhausted**. It carries both R-2's terminator and R-3's chain-shift detection.

**R-2 restated against the fields, because the two sentinels are independent.** A `Replaying` with
`replaySessionId == NO_REPLAY_NEEDED` and `recordingId >= 0` means *that segment is empty* — skip it and
request the next index. The same `replaySessionId` with `recordingId == -1` means *the chain is
exhausted*, which terminates the walk and marks the client caught up. A client that treats the first as
the second stops replaying with history still ahead of it.

**Slot limits are the replayer's, and it publishes them.** A replayer serves at most
`MAX_CONCURRENT_REPLAYS` = **4** concurrent replays, answering `ReplayPending` beyond that, and reclaims
a slot idle for `REPLAY_SLOT_TTL_MS` = **5 000 ms** — ten missed heartbeats, far more than a healthy
client ever misses. `ReplayHeartbeat` refreshes a slot and is sent every **~500 ms** while an attached
replay image is running, which is what makes the TTL an *idle* timeout (**R-4**). `ReplayComplete` frees
a slot immediately rather than at the TTL; a cold-start walk sends none, because each of its slots is
freed by the supersede on the next segment request.

Both are **protocol constants** (§12), published the way **V-1** publishes the library pins, because
together they bound something a consumer must size against:

> **The maximum pending wait is `MAX_CONCURRENT_REPLAYS × REPLAY_SLOT_TTL_MS` = 20 000 ms**, the worst
> case being every slot held by a client that has died, reclaimed one TTL apart. **A consumer's
> recovery-stall fence MUST exceed it, with margin.**
>
> The obligation is the consumer's, and it runs this way round because a replayer cannot know what any
> consumer fences at, while every consumer can read these two numbers. A client answered `ReplayPending`
> holds at its gap (§10) and therefore dispatches nothing, so it spends the wait running down exactly
> the timer that would kill it — a fence at or below the maximum pending wait fires on a replayer that
> is behaving correctly.

_(Today the TTL is 60 000 ms, so the maximum pending wait is 240 000 ms while both gateways fence at
`RECOVERY_STALL_TIMEOUT_MS` = 60 000 — an order of magnitude the wrong way, and equal to the TTL itself
for a single wedged slot-holder, so a waiting client trips its own fatal fence at the very instant the
slot would be reclaimed. Lowering the TTL to 5 000 is what brings the fence back above the wait.)_

## 11. Versioning

**Scope: seqeron's two schemas.** Payload versioning, log lifetime and the operational procedure for
landing a change are all outside it — the first belongs to the application (**V-2**), the other two to
the deployment.

| artifact | policy | blast radius |
| --- | --- | --- |
| `seqeron-frame.xml` (210) — the envelope | **frozen.** Any change is a new wire format; all three repos release together | everything |
| `seqeron-frame.xml` (210) — the core payloads | changeable, under **V-3**. A core payload is a payload like any other, so a core change restamps *no* application frame — the frame around it is untouched | the cluster tier and core's consumers |
| `seqeron-replay.xml` (212) | in place, no bump (§10) | one node's build |
| an application schema | the application's business | one repo, one commit — **E-1** holds here |

Core carries no `sinceVersion` discipline, because **V-3** leaves it nothing to be compatible with: no
seqeron decoder ever meets bytes another build encoded.

- **V-1. Library versions are part of the wire format.** All three repos MUST pin the same Aeron /
  Agrona / SBE versions *for the frame*; skew corrupts frames rather than failing to build. seqeron
  **publishes** the pins as part of the artifact — generated constants in the jar and in the include
  tree — so an application consumes them rather than mirroring them. What an application pins for its
  own payload codecs is its own business.
- **V-2. Payload versioning is the application's and is out of scope here.** seqeron contributes
  **E-1** and nothing else: a payload has exactly one encoder, so there is no cross-implementation
  agreement problem to solve — only a cross-*time* one, whose rules are specific to the encoding the
  application chose and belong with it. How a version is signalled, which direction of compatibility is
  guaranteed, and what a replay of older bytes through today's decoder may do are not specified here.
- **V-3. A frame or core change MUST NOT be rolled across a running cluster, and no seqeron decoder may
  be required to read bytes an earlier build encoded.** Two builds encoding the same synthesized frame
  differently is **F-2** broken; a surviving archive is the same fault displaced in time.
  `seqeron-replay.xml` is exempt from both — nothing records it, and it is node-local.

## 12. Limits

| limit | value | source |
| --- | --- | --- |
| `blockLength` | **18 / 34**, constant — an equality, not a bound (§9.2) | §4.2 |
| per-message overhead | **92** on ingress (32 Aeron data header + 32 Aeron Cluster session header + 28 frame), **76** on the tap (32 + 44 frame) | §4.2 |
| max payload (`MAX_PAYLOAD_LENGTH`) | **1316 bytes**, a pinned constant: 1408 (the pinned MTU) − 92 | **T-2** |
| max ingress frame (`MAX_INGRESS_LENGTH`) | 28 + `MAX_PAYLOAD_LENGTH` — **1344**; reject condition 1 | **T-2**, §9.2 |
| min ingress frame (`MIN_INGRESS_LENGTH`) | 28 bytes; reject condition 1 | §4.2 |
| max sequenced frame | 44 + `MAX_PAYLOAD_LENGTH` — **1360** | **T-2**, §4.2 |
| consensus log | MUST carry a `32 + MAX_INGRESS_LENGTH` = **1376**-byte message in one packet; MTU 1408 gives a `maxPayloadLength` of exactly 1376 | Aeron Cluster; cluster-wide config |
| `globalSeqNo` | int64, starts at 1 | §9.1 |
| `CLUSTER_HEARTBEAT_INTERVAL_MS` | **1000** — the 1 Hz cluster clock | §7, §9.3 |
| `GATEWAY_ACTIVATION_TIMEOUT_MS` | **5000** = 5 × `CLUSTER_HEARTBEAT_INTERVAL_MS` | §7.2 |
| `MAX_CONCURRENT_REPLAYS` | **4** replays served at once; `ReplayPending` beyond | §10.1 |
| `REPLAY_SLOT_TTL_MS` | **5000** — idle-slot reclamation, refreshed by `ReplayHeartbeat` | §10.1 |
| maximum pending wait | `MAX_CONCURRENT_REPLAYS × REPLAY_SLOT_TTL_MS` — **20 000**; a consumer's recovery-stall fence MUST exceed it | §10.1 |

> **T-2. The message is pinned at one Aeron MTU of 1408: headers plus `MAX_PAYLOAD_LENGTH` = 1316.**
> 92 is the larger of §3's two directions' header stacks. A consensus-log packet is `32 + 32 + 1344` =
> **1408 exactly**; the tap's is `32 + 1360` = 1392. The number is a **protocol constant**, compiled
> into every implementation — nothing to configure, nothing to keep in sync, nothing to read from a
> transport per frame. A node validates *against* it at start-up: its ingress and tap MTUs MUST each be
> at least 1408, or the node MUST refuse to start.

> **T-3. The protocol's own encode methods enforce `MAX_PAYLOAD_LENGTH`; §9.2 condition 1 is the
> backstop.** The refusal is **local and permanent** — not back-pressure, and MUST NOT be signalled as
> something a caller retries. What a producer does with the message it could not publish is its own
> business (**P-0**).

**Every frame is one packet, and nothing reassembles.** **T-3** refuses an oversized payload in the
producer's own process, so no frame above the constant ever reaches a transport, and **T-2** keeps the
constant inside one MTU in both directions. A frame is therefore never split: neither the recording nor
a replay ever carries half of one, and **a conforming consumer MUST NOT depend on fragment reassembly**
on ingress, on the tap or on a replay — every message it polls is one whole frame.

The constant comes from the **MTU, not the term length**, which is the tighter of Aeron's two ceilings:
a publication's `maxPayloadLength` is `mtu - 32`, while the term length caps a *message* at
`min(termLength / 8, 16 MB)`, an order of magnitude higher. Taking the MTU is what buys the packet
property. The two MTUs are fixed by §3 — Aeron Cluster ingress over UDP (`aeron.mtu.length`) and the
tap over `aeron:ipc` (`aeron.ipc.mtu.length`), and nothing else. Both MUST be at least 1408, which is
Aeron's own default. Raising the ceiling is a protocol change with a release behind it (§11), never a
deployment knob.

A payload larger than `MAX_PAYLOAD_LENGTH` needs chunking across frames or a side channel, which
re-introduces a completeness contract of its own — a `remaining` countdown over the chunks is the shape
to copy.

## 13. Tooling and payload encodings

### 13.1 `SbeLogPrinter` pipes payloads; it does not decode them

Core's ten are the only payloads it decodes unaided (seqeron owns schema 210, so a `payloadId` 1 frame
prints in full). Everything else goes down a pipe:

```
sbe-log-printer.sh --payload … | application-decode.sh
```

seqeron gains no application dependency, no descriptor loading and no `templateId → type` registry. It
still **labels** a payload from the `PayloadIdRegistered` frames in the recording it is already reading
(§6.3) — the shared protocols, which are the rows that exist; everything else prints under its number.
Two requirements:

1. **Binary-safe rendering.** `varDataEncoding` declares `characterEncoding="US-ASCII"` and SBE's
   `JsonPrinter` renders var-data through it, so arbitrary payload bytes come out mangled. Emit
   **base64 inside the existing frame line**.
2. **Correlation.** Keep the frame line and carry the payload with it, beside its `globalSeqNo`. The
   precedent is in the file (`<undecodable ingress payload: …>`).

This is **required work before the first application message leaves seqeron's schemas** (§15 step 2).

### 13.2 Non-SBE payloads

> **E-1. A payload that arrived through ingress is encoded exactly once, by its producer, and is never
> re-encoded after sequencing. The sequencer's synthesized frames are the exception —
> `ClusterHeartbeat`, `LeadershipChanged`, and the bootstrap and promotion `GatewayActive`s — because
> they have no producer: every node encodes its own copy.**

Application payloads may use **any encoding** — SBE, protobuf, raw FIX bytes, a proprietary binary.
Every replica receives payload *bytes* (replicated through Raft, or read off its own recording), so two
encoders never have to agree.

**The frame and core are fixed-layout SBE and MUST NOT become tag-encoded.** The synthesized frames are
core payloads encoded independently on every node, and the taps must come out byte-identical (**F-2**,
**S-3**), so the requirement that two encoders agree binds core exactly as it binds the frame.

**The envelope is protobuf's framing protocol**: the var-data length prefix delimits, `payloadId` names
the type — no `Any` wrapper or `type_url` needed. What each encoding affords **P-4** differs: SBE gives
`MessageHeader.schemaId`, raw FIX gives `BeginString` (tag 8, always first) and `MsgType` (tag 35),
protobuf gives nothing — for such a payload `payloadId` stands alone and §6.1's allocation hygiene
carries the weight. The backstop is the application's own validation (§1, **P-0**), which catches a
wrongly selected decoder everywhere except across versions of one protocol — where the rules are the
application's (**V-2**).

## 14. Conformance suite

Beside `SequencerTest` in seqeron's Java suite and mirrored in `core_tests` for C++ — no Aeron, no media
driver, sub-second. The fixture is a **synthetic payload seqeron owns**.

| # | asserts | rule |
| --- | --- | --- |
| 1 | **Copy fidelity.** Sequence an `Unsequenced` over known bytes; the sequenced payload is byte-identical and `payloadId` unchanged. Parameterised over empty, 1 byte, and `MAX_PAYLOAD_LENGTH` | §5 |
| 2 | **Prefix property.** `unsequencedHeader` bytes 0..17 equal `sequencedHeader` bytes 0..17 for the same values — `payloadId` included — and the two `ENCODED_LENGTH`s are 18 and 34. Pure schema check, no `Sequencer` | **F-3** |
| 3 | **Core frames round-trip unchanged.** Parameterised over the **eight** ingress-legal payloads of §7: wrap as `payloadId` 1, sequence, assert the egress payload is byte-identical including the inner `MessageHeader`, and for `ClientConnected` over an empty, a short and a `MAX_PAYLOAD_LENGTH`-filling `connectionData` (§7.1). The two synthesis-only templates are covered instead by encoding one of each through the `Sequencer`'s own synthesis path | §7 |
| 4 | **Rejection table.** One case per row of §9.2, each asserting that the rejected message was not sequenced, that **nothing at all was emitted** — `globalSeqNo` unmoved, no synthesized frame behind it — and that the rejected-frame counter advanced by exactly one. Cases: over `MAX_INGRESS_LENGTH` and under 28 (1); a `schemaId` that is not 210 and a non-zero `version` (2); a `templateId` that is not `Unsequenced` (3); a `blockLength` that is not 18, below and above (4); a length prefix past the frame end, one short of it, and one equal to 65535 (5); an ingress `sourceId` of −1 (6); `payloadId` 0 (7); a `payloadId` 1 payload too short for its own `MessageHeader` and one whose `schemaId` is not 211 (8); a `ClusterHeartbeat` and a `LeadershipChanged` submitted on ingress (9); a `GatewayStarted` payload shorter than its 16 bytes and one declaring `blockLength` 0 (10) | **S-4**, **S-5**, **S-7**, **P-4** |
| 4a | **Rejection denies nothing, and repeats identically.** Two rejections under the same application `payloadId` each advance the counter, neither emits anything, and a well-formed frame of that `payloadId` afterwards is accepted unchanged | **S-7**, **C-2** |
| 4b | **The producer refuses before the wire, and survives it.** A payload of exactly `MAX_PAYLOAD_LENGTH` publishes; one byte longer is refused with **nothing offered to any transport**, distinguishably from back-pressure, and the very next well-formed publish succeeds. Against an in-memory transport seam, so no Aeron | **T-3**, §12 |
| 5 | **Synthesis determinism.** Two independently constructed `Sequencer`s fed the same message sequence emit byte-identical frames, heartbeat for heartbeat | **S-3**, **F-2** |
| 6 | **Cross-language golden vectors.** A checked-in binary corpus — one frame per core payload, plus the boundary payload sizes — that both language suites decode and re-encode to the same bytes. **Java regenerates it, C++ only verifies.** Regenerated on every core bump; a change to a synthesized frame's bytes is what flags the release as a **V-3** one | **F-2**, **V-1** |
| 7 | **Selective consumption.** A consumer fed an unallocated `payloadId`, and an unhandled inner template within `payloadId` 1, ignores both without error — and its `globalSeqNo` continuity tracking advances across them | **P-1**–**P-3** |
| 8 | **S-6's three cases.** A `GatewayStarted` agreeing with its roster row binds and is accepted; three are rejected — the same frame naming a different `gatewaySourceId`, one whose `gatewayId` no roster row names while claiming a rostered `sourceId`, and another core frame claiming a rostered `sourceId` on an unbound session — and both negatives are accepted: an application payload carrying that `sourceId`, and a marker at −1 | **S-6** |
| 9 | **Promotion order.** Against a roster of one `gatewaySourceId` with ranks 0, 1, 2: bootstrap activates rank 0 only; closing rank 0's bound session promotes rank 1; closing rank 1's promotes rank 0 again (lowest-rank-excluding, not next-rank-up); a designated instance that publishes no `GatewayStarted` is handed on after exactly `GATEWAY_ACTIVATION_TIMEOUT_MS` of consensus time and not before; one that does publish one arms nothing further; and a `gatewaySourceId` with a single row synthesizes **no** frame on close, leaving `globalSeqNo` unmoved. Two `gatewaySourceId`s bootstrapped back to back each keep their own deadline | §7.2, **S-3** |

**Row 6 must land before extraction**, because afterwards it needs a published artifact rather than a
checkout on both sides, and it must be regenerated and reviewed as a diff on every library bump.

The suite asserts framing and copy fidelity only. A test that wants to decode a payload to check it has
crossed the `payloadId` boundary in the wrong direction.

---

## 15. Landing it (`doc/future-arch.md` §11 step 3)

Each sub-step green before the next, all in one repo. The numbering is fixed — `sbe-frame.xml` and
`doc/future-arch.md` cite these step numbers — so a landed step keeps its place and records what
actually landed.

1. **Landed.** Add the frame schema with the two envelopes, the two renamed composites and the ten core
   payloads (prefix-extended, `payloadId` moved in, `origin` deleted, unpadded) — and, in the same wire
   change, give the FIX session families in the running pair their own direction field. → both codegen
   paths green, the `unsequencedHeader`/`sequencedHeader` rename landed across the pair and every
   consumer of it, both edges reading direction from the payload, copy-through unchanged. The file is
   `src/main/resources/sbe-frame.xml` and says `phixeron` throughout: the `seqeron` rename goes with the
   repo split (`doc/future-arch.md` §11 step 1), not with a protocol step, each of which is already a
   wire change on its own. **Step 6 landed in the same change** — see there for why it could not wait.
2. **Postponed.** Land §13.1's payload pipe and its base64 frame line, before anything on the wire is
   opaque. → `SbeLogPrinter` renders a frame's payload beside its `globalSeqNo` and round-trips
   arbitrary bytes; no schema and no frame changes. It gates the **application** families, not core: a
   core payload is decoded by the same tool that reads the frame around it, so this is a prerequisite
   for step 3 rather than for step 1. `SbeLogPrinter` already unwraps the envelope and prints the core
   payload inline; what is missing is rendering a payload it cannot decode.
3. **Landed.** Move `NewOrderSingle`/`ExecutionReport` onto a payload first. Blast radius: the C++ edge
   (`FixIngressHandler`, `FixConnection`, `Conversions`) plus `OrderExecServer`. → C++ suites and C++
   e2e green; that family has **one** codec set instead of a 200/202 pair. It is
   `src/main/resources/sbe-order.xml`, **schema 220**, **`payloadId` 2** — the first the deployment
   allocates, since 0 and 1 are this document's and §6.4's example holds 4 for the shared-protocol
   family. C++ only: no Java consumer exists, so the pair's Java codecs for it are simply gone, and
   `SequencerTest`'s copy-through exemplar is a `Heartbeat` now. The six order-only enums (`Side`,
   `OrdType`, `TimeInForce`, `HandlInst`, `ExecType`, `OrdStatus`) moved with the two messages rather
   than being duplicated — no remaining message in the pair used them. It is **not** registered
   (§6.3): that is step 8's `PayloadIdRegistered`, and **C-2** means the wire behaves identically
   until then.
4. **Landed.** Move the rest: the FIX session family and `ClientSessionEvent` to `sbe-session.xml`,
   **schema 230, `payloadId` 3** — one schema, because they are one protocol at two fidelities (this
   edge's structured templates, the Artio legs' opaque bytes), and when the C++ edge retires the seven
   templates go while `ClientSessionEvent` stays. `PortfolioQuery{Request,Reply}` fold into
   `sbe-order.xml` rather than taking a number of their own: they are the same application, between the
   same two processes as the order flow, and a `payloadId` names a schema, never a message. Reference
   data goes to `sbe-basicdata.xml`, **schema 240, `payloadId` 4** — the one protocol here that crosses
   application boundaries, and so the only one §6.4's example declares. Its owning repo settles with the
   repo split (`doc/future-arch.md` §11 step 1); until then every schema lives at the shared root, as
   `sbe-order.xml` already did. → both all-Java e2e and the C++ e2e green.

   **Step 7's code half landed here**, because this step is what makes it true: with the last family on
   a payload, nothing publishes a bare schema-200 message to ingress at all, so `sequenceBare` and the
   `schemaId` dispatcher above it had no reachable traffic and no test that could encode any. Gone with
   them: `MIN_INGRESS_LENGTH`, `MAX_BLOCK_LENGTH`, `HEADER_GROWTH`'s bare meaning, the lockstep
   schema-version assertion, the bare branch of `unwrapFrame` and its Java twin, `NO_PAYLOAD_ID` — and
   `sbe-sequenced.xml` itself, which was left with no message in it. Same shape as step 6 landing inside
   step 1: the deletion is not a separate change, it is what the move *is*.
5. **Landed.** Extract the replay set into its own schema. Namespace change only — the six messages,
   their fields and their template ids are untouched. → both `ReplayerRecoveryTest`s green, both e2e
   paths green. The file is `src/main/resources/sbe-replay.xml`, not `seqeron-replay.xml`, for step
   1's reason: the `seqeron` rename goes with the repo split. The schema id **did** move, 200 → §8's
   **212**, because 200 was the old ingress pair's and dies with it — free here and nowhere else,
   since nothing records these six and a node's Replayer and its co-located apps are built and
   restarted together (**V-3** exempts this schema). **Step 7 completes here**: `sbe-unsequenced.xml`
   is gone, and with it the last message defined in seqeron's schemas that is neither the envelope nor
   a core payload.
6. **Landed with step 1, less its last clause.** Repoint every consumer at core's nine existing frames
   as `payloadId` 1 — they are already defined once in the frame schema beside the envelope (step 1);
   this deletes the pair's copies. → the pair's copies are gone, and the sequencer decodes no
   `payloadId` but 1. It could not wait for steps 3–5: **a core payload carries no `header` field**
   (§7.1), so moving core off the pair is what brings the envelope live, and bringing the envelope live
   is what moving core off the pair means. `HEADER_GROWTH` survived here, still measuring the bare
   copy-through path; step 4 took that path away and left it measuring the envelope's own 18→34 growth.
7. **Landed, in halves with steps 4 and 5.** Step 4 deleted `sbe-sequenced.xml` and the copy-through
   branch with it — see there for why the deletion is not a separate change. Step 5 took the last of
   the pair, `sbe-unsequenced.xml`, by moving the replay control protocol out of it. → `:cluster` and
   `core_tests` build and pass with **no application message defined anywhere in seqeron's schemas**.
8. **Schema half landed with step 1.** `PayloadIdRegistered` is defined — §7's tenth core payload — and
   nothing publishes it. What remains: convert the topology file to §6.4's document and schema, and
   teach `SbeLogPrinter` to label payloads from it. The conversion is a hard cutover — the roster files
   and every caller change in one commit — but it is **not** a wire change: past the new rows the frames
   the loader publishes are unchanged. → a payload prints under its protocol's name, an unregistered one
   under its number, no frame affected either way, both e2e suites green with no new start-up step.
9. §14's suite, both languages. → it fails on a deliberate field reorder and on a renumbered core frame.

**Steps 1 and 3–6 are each a wire change**, and **V-3** applies to each.

**Between step 1 and step 4 the tap carried two shapes** — schema 210 envelopes beside bare schema 202
messages — and `Sequencer.sequenceMessage` was a `schemaId` dispatcher over the two, a knowing,
temporary divergence from **§9.2 condition 2**. Step 4 ended it: every frame on the tap is an envelope,
every consumer dispatches on `(payloadId, templateId)`, and condition 2 holds as written.
