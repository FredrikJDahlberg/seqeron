# seqeron protocol — specification

_Normative specification, 2026-09-02. The **what** and **how** only; the design record, the forces
behind each decision and the alternatives weighed are in the design record this condenses and does
not supersede. Rule identifiers are that document's, unchanged._

> **Status: specification of the target, not of the tree.** The tree is mid-§15: every application
> family is on a payload (`sbe-order.xml` 220, `sbe-session.xml` 230, `sbe-basicdata.xml` 240), seqeron's
> own vocabulary has the system family of `sbe-frame.xml` 210 to itself, and the replay control protocol
> stands alone in `sbe-replay.xml` (212). What is left is §14's conformance suite. §15 is the
> migration and records what has landed so far.
> Statements about today's code are marked _(today)_.
>
> **Normative language.** MUST / MUST NOT / SHOULD as usual. Rules carry identifiers — **F-n** frame,
> **T-n** transport, **S-n** sequencer, **P-n** payload, **C-n** registration, **R-n** replay, **V-n**
> versioning, **E-1** encoding. §14 lists the ones a conformance test exists for.

---

## 1. Layers

seqeron owns the frame; the application owns the payload. **The cluster tier decodes no `payloadId` at
all** — its own vocabulary is a family of frames, not a payload.

| layer | what it is | schema | owner |
| --- | --- | --- | --- |
| **L0 transport** | Aeron channels, stream ids, the cluster ingress/egress session protocol | `sbe-cluster.xml` (mirror of `io.aeron.cluster.codecs`) | Aeron, mirrored by seqeron |
| **L1 frame** | two envelope pairs — `Unsequenced`/`Sequenced` for an application payload, `UnsequencedSystem`/`SequencedSystem` for seqeron's own — plus the three synthesized templates | `seqeron-frame.xml` (210) | seqeron |
| **L2 payload** | one opaque, length-prefixed byte range per application frame, named by `payloadId` | the application's own | whoever `payloadId` names |
| **system messages** | seqeron's own eleven events, named by `systemEventType`; a body with no framing of its own, or a template of its own | `seqeron-frame.xml` (210), beside the envelopes | seqeron |
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

Synthesis and forwarding draw from one `globalSeqNo` counter (§9.1). **The synthesized class is a
template, not a flag**: the three events the cluster originates have top-level templates of their own
(§7), no ingress form, and no `UnsequencedSystem` antecedent — a sequenced shape asserts a
transformation, and a frame that never underwent one must not claim it. Their header is nonetheless
`sourceId`, `connectionId` and `sessionId` all −1, because each names an external producer, its
connection and its session, and a cluster-originated message has none (§4.3, §7). **`sourceId == −1` is
the provenance mark** (**F-4**) — it alone is reserved and it alone is refused on ingress, and the other
two follow it; a consumer MUST test `sourceId` and MUST NOT require the other two. All three are wire
bytes under **F-2**, not defaults. It is corroboration now rather than the sole carrier, and §9.2
condition 6 is what keeps it a statement of provenance rather than an unchecked assertion.

The replay control messages (§10) are outside all three classes: not frames, no header composite, never
sequenced, never recorded.

---

## 3. Transport bindings

A consumer MUST validate `MessageHeader.schemaId` on every stream it reads (**T-1**).

| stream | channel / id | carries | recorded? |
| --- | --- | --- | --- |
| cluster ingress | Aeron Cluster ingress, inside the L0 session envelope | `Unsequenced` (100) or `UnsequencedSystem` (102) — schema 210 | in the Raft log |
| **the tap (Feeder)** | `aeron:ipc`, stream **205** | `Sequenced` (101), `SequencedSystem` (103), or one of the three synthesized templates (104–106) — schema 210 | **yes**, by the co-located archive, on every node |
| replayer replay | stream **201** | replayed archive bytes, byte-identical to the tap | no |
| replayer request | stream **202** | schema 212, client → replayer | no |
| replayer control | stream **203** | schema 212, replayer → client | no |

- **F-1. The tap is the record.** No snapshots; recovery is always full-log replay from `globalSeqNo` 1,
  so a frame's bytes on the tap are the only durable statement of it that exists.
- **F-2. Every node's tap is byte-identical.** A replay served from any node's archive is substitutable
  for any other's. §9.3 exists to keep this true.

## 4. The frame layer

```
Unsequenced        (schema 210, template 100) { unsequencedHeader,       payload:varData }
Sequenced          (schema 210, template 101) { sequencedHeader,         payload:varData }
UnsequencedSystem  (schema 210, template 102) { unsequencedSystemHeader, body:varData }
SequencedSystem    (schema 210, template 103) { sequencedSystemHeader,   body:varData }
ClusterHeartbeat   (schema 210, template 104) { sequencedSystemHeader }
LeadershipChanged  (schema 210, template 105) { sequencedSystemHeader,   newLeaderMemberId }
GatewayActive      (schema 210, template 106) { sequencedSystemHeader,   gatewayId }
```

Within each pair the two differ only by the stamp. An application body is one length-prefixed payload
and nothing else; a system body is one length-prefixed message and nothing else. The last three are the
frames the cluster synthesizes: no ingress form, and their fields inline in the block (§7).

### 4.1 Header layout

**F-3. Each unsequenced composite is a byte-prefix of its sequenced counterpart, and the two pairs are
byte-for-byte identical apart from the name of the uint16 at offset 16.**

| offset | field | type | unsequenced | sequenced |
| --- | --- | --- | --- | --- |
| 0 | `sourceId` | int32 | ✓ | ✓ |
| 4 | `connectionId` | int32 | ✓ | ✓ |
| 8 | `sessionId` | int64 | ✓ | ✓ |
| 16 | `payloadId` / `systemEventType` | uint16 | ✓ | ✓ |
| 18 | `globalSeqNo` | int64 | — | ✓ |
| 26 | `timestamp` | int64 | — | ✓ |
| | **ENCODED_LENGTH** | | **18** | **34** |

Consequences that other rules depend on: every frame-layer field sits at a fixed offset on all five
shapes, so one decoder reads the identity of any of them and **`globalSeqNo` is at 18 on every sequenced
shape** — which is what keeps the continuity read (**P-3**) branch-free over every frame on the tap.
Sequencing is **copy-18, append-16** for both families. And `blockLength` on an ingress frame is exactly
18, whichever template it is, which is what §9.2 condition 4 compares against.

**Offset 16 discriminates every frame on the tap.** It is a `payloadId` on the application family and a
`systemEventType` on the system one; the template says which, and a consumer that reads one as the other
is reading a foreign numbering as its own (**P-4**'s failure, one layer up).

**No pad.** The uint16 at 16 is 2-byte aligned and `globalSeqNo` sits unaligned at 18. Both generators
read fixed-width fields through `memcpy` (SBE C++) and `UnsafeBuffer` (Agrona), which compile to single
unaligned loads on x86-64 and ARM64. **Plain accessors only** — `getLongVolatile`/`getAndAddLong` and
C++ atomics on an unaligned address are undefined.

**The composites are named for their templates** rather than all being `header`, which they cannot be
once they share one file.

### 4.2 Frame sizes

| | ingress (100 / 102) | sequenced (101 / 103) |
| --- | --- | --- |
| `MessageHeader` | 8 | 8 |
| `blockLength` (the header composite) | **18** | **34** |
| var-data length prefix | 2 | 2 |
| **fixed overhead** | **28** | **44** |

`MIN_INGRESS_LENGTH` is 28, one constant for both ingress templates. A `ClusterHeartbeat` is 8 + 34 =
**42**: a template of its own, so no length prefix and no body at all.

### 4.3 Provenance

There is no `origin` field _(today: `None`, `Client`, `Gateway`, `Application`)_. Provenance is
`sourceId`:

> **F-4. `sourceId == −1` is reserved for the cluster itself and corroborates the synthesized class.**
> Every other value names an external producer. An ingress frame MUST NOT carry it (§9.2, condition 6).
> What *marks* the class is the template (§2); this is the mark the header carries.

The direction an application needs (`Client` vs `Gateway`) was one application's vocabulary and moves
to the payload: a field of its own on the FIX session families, in the schemas that already define
them. `clusterctl` gets a reserved `sourceId` of its own (§5) so it no longer stamps −1.

## 5. Field authority

The ingress contract; **S-1** is that the sequencer enforces it.

| field | on ingress | on the tap |
| --- | --- | --- |
| `sourceId` | producer-set, MUST NOT be −1 (§9.2, condition 6), **checked against the list** (**S-6**) | copied verbatim; −1 on a synthesized frame (**F-4**) |
| `connectionId` | producer-set | copied verbatim |
| `sessionId` | producer-set, **advisory** | **overwritten** with the true Aeron Cluster session id the frame arrived on |
| `payloadId` | producer-set, MUST be neither 0 nor 1 (condition 7) | copied verbatim |
| `systemEventType` | producer-set, MUST be an allocated ingress-legal value (condition 8) | copied verbatim |
| `globalSeqNo` | **no such field** on either unsequenced header | sequencer-assigned (§9.1) |
| `timestamp` | **likewise** | Raft consensus time at commit |
| `payload` / `body` | producer-set | copied **byte-identical** |

The outer `MessageHeader` is the sequencer's on both sides, and is re-encoded rather than copied:

| field | on ingress | on the tap |
| --- | --- | --- |
| `schemaId` | producer-set, MUST be 210 (condition 2) | 210 |
| `templateId` | producer-set, MUST be `Unsequenced` or `UnsequencedSystem` (condition 3) | **`Sequenced`** / **`SequencedSystem`**, matching |
| `blockLength` | producer-set, MUST be 18 (condition 4) | **34** |
| `version` | producer-set, MUST be 0 | 0 |

`version` is 0 for the life of the frame schema, which is frozen (§11), and is not an extension point a
producer may write into _(today the sequencer copies the ingress `version` onto the tap)_.

**Two kinds of producer.** A **gateway** is an edge producer deployed as an **active/hot-standby pair**:
it appears in the topology list (§6.4) and the cluster designates which instance is live, through
`GatewayRegistered` / `GatewayStarted` / `GatewayActive` / `GatewayActivationRequested` (§7). `gateway`
is seqeron's own word for that deployment role, not any application's — the election is the cluster's,
and any protocol may sit behind it. A **co-located application** is the other kind: one replica per
cluster node, named in no list row and needing no designation, because `LeadershipChanged` (§7) already
picks exactly one — the replica on the leader node is the only one that submits. Only the first kind is
listed, and only the first kind takes part in §7's election; everything in this section that speaks of a
list row, a `gatewaySourceId` or a promotion is about gateways alone. Connections belong to neither kind
exclusively, which is why `ConnectionOpened`/`ConnectionClosed` name no producer role.

**`sourceId` is one id space.** Two values are reserved; the rest is the list's.

| `sourceId` | who | listed? |
| --- | --- | --- |
| **−1** | the cluster itself — the synthesized class, and nothing else (**F-4**) | never; **illegal on ingress** |
| **2** | `clusterctl`, on every marker it submits | never |
| 0, 3, 5, 6, 7, 8, 9 … | every other producer, elected or not _(today: the C++ gateway pair 0, the reference-data application 3, the venue leg 5, the order-entry leg 6, the order-exec application 7, seqeron's own e2e probe 8, its e2e gateway pair 9, the `examples/` ping 10)_ | gateways by `GatewayRegistered.gatewaySourceId`, applications by `ApplicationRegistered.applicationSourceId`; **S-6** checks only the first |

This table is the registry: a new producer takes its id here, and `doc/registries.md` §1 records
that ownership rather than keeping a second copy of the allocation.

`clusterctl`'s id MUST NOT be a `gatewaySourceId` any list row claims. The list check is scoped to
the system family:

> **S-6.** For an `UnsequencedSystem` frame (§9.2, condition 10):
>
> 1. **`GatewayStarted`, and `GatewayActivationRequested`** — the `gatewayId` each names MUST name a
>    list row, and for `GatewayStarted` that row MUST also carry `gatewaySourceId == header.sourceId`.
>    A `gatewayId` the list does not name is rejected whatever `sourceId` it carries.
> 2. **Any other system frame whose `sourceId` the list claims** — it MUST arrive on a cluster session
>    already bound to a `gatewayId` of that `sourceId`.
> 3. **Everything else is unchecked** — every application frame, and every system frame other than
>    those two carrying an unlisted `sourceId`.
>
> The binding is created by `GatewayStarted` and removed when that cluster session closes — the same
> event that promotes a standby (§7) — so the set holds one entry per live gateway session. Both edges
> are log events and the list is log-derived, so the check is **S-3**-safe. A binding MUST NOT be
> removed on anything a node observes locally.

Case 1 is total, so **a `GatewayStarted` arriving before `load-topology` is rejected**: the list it
is checked against is empty. That is the existing start-up order as a wire rule.

## 6. `payloadId`

A `uint16` naming a **decoder namespace and encoding** — one application schema, never one message. The
consumer maps it to a decoder module; that module reads its own framing.

### 6.1 Registry

The registry lives in the log: `clusterctl load-topology <file>` publishes one `PayloadIdRegistered`
system frame per row of the file's `<protocols>` section (§6.4).

| value | names | in scope here? |
| --- | --- | --- |
| 0 | unset — **invalid on the wire** | fixed by this document |
| 1 | **retired.** seqeron's own vocabulary was a payload until §15 step 10 gave it the system family; the number is burned so a producer on an older build fails loudly rather than having core bytes copied through as an application payload | fixed by this document; **refused on ingress** (§9.2, condition 7) |
| 2… | one per application schema or encoding | the deployment's to allocate; **registered only where the protocol is shared** |

_(This deployment today, the first three allocated by §15 steps 3 and 4: **2** = the order family
(`sbe-order.xml` 220, the order flow and the portfolio query over it), **3** = the FIX session family
(`sbe-session.xml` 230, both edges' session layer), **4** = reference data (`sbe-basicdata.xml` 240),
**5** = seqeron's own end-to-end probe (`sbe-probe.xml` 214, one `ProbeMarker`), **6** = the `examples/`
ping (no schema at all: eight raw bytes, which §13.2 admits). **4** is the only one that crosses application boundaries and so the only one §6.4's
example declares; 2, 3, 5 and 6 are private between the processes that speak them and need no row. 5 and 6 are
recorded here despite being out of the registry's scope for the reason the last paragraph of this
section gives: it is nonetheless one id space, and a number nobody wrote down is a number two
applications can pick.)_

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
  It binds **consumers only**: the sequencer decodes no `payloadId` at all, so it has no decoder to
  select and nothing to verify — a consumer whose check fails **skips** the frame, as P-1 skips an
  unrecognised `payloadId`. What retired with §15 step 10 is the sequencer's instance of this rule, not
  the rule.

### 6.3 Registration is labeling, not admission

```
PayloadIdRegistered  (systemEventType 23, block 36)
    { payloadId:uint16, protocolVersion:uint16, protocolName:char[32] }
```

One row per **shared** protocol (§6.1); an application's private `payloadId` has none.
`protocolName` is the label `SbeLogPrinter` puts on a payload it cannot decode (§13.1); `protocolVersion`
is which revision the deployment runs, printed beside it and checked by nothing. It is **not** the
payload's `schemaId` — a `payloadId` may name an encoding that has none. No `description`, no
`remaining` countdown.

- **C-1. `payloadId` 1 is retired and MUST NOT be registrable.** Enforced where the row is written: the
  topology file's schema constrains `payloadId` to 2 or above (§6.4).
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

One operator file, three sections, three system events, and between them **the complete producer view
of the deployment**: `<gateways>` produces the `GatewayRegistered` list of elected pairs (§7),
`<applications>` one `ApplicationRegistered` per co-located application — the producer kind §5 does not
elect — and `<protocols>` the `PayloadIdRegistered` rows above, one per **shared** protocol and nothing
for an application's private `payloadId` (§6.1). It is XML with a schema shipped beside it — **C-1** is
a constraint on what may be *written*, and the schema is where a constraint on writing belongs.

```xml
<topology>
  <gateways>
    <gateway name="GW-A" id="1" sourceId="0" rank="0"/>
    <gateway name="GW-B" id="2" sourceId="0" rank="1"/>
  </gateways>
  <applications>
    <application name="BasicDataServer" sourceId="3"/>
    <application name="OrderExecServer"/>
  </applications>
  <protocols>
    <protocol payloadId="4" version="1" name="phixeron-basicdata"/>
  </protocols>
</topology>
```

Attributes are the payload fields under short names: `name`/`id`/`sourceId`/`rank` are
`gatewayName`/`gatewayId`/`gatewaySourceId`/`preferenceRank` (§7.1), an application's `name`/`sourceId`
are `applicationName`/`applicationSourceId`, and `payloadId`/`version`/`name` are
`payloadId`/`protocolVersion`/`protocolName` (§6.3). `remaining` appears in no section — it is the
publisher's, counted off the row count — and an application row carries no `id` and no `rank`, because
nothing elects it: the instance is the node, and `LeadershipChanged` already names the one that submits.

An application's `sourceId` is **required**, because §5 is one id space and a producer the document does
not place in it is a hole in the deployment view. Declaring it does not require the process to stamp it
yet: today `OrderExecServer` puts the **requesting gateway's** `sourceId` on every `ExecutionReport` and
`PortfolioQueryReply` so that gateway can route the answer back, and so writes its own id nowhere. That
is a gap in the process — the id it would need to identify itself is reserved to it either way.

| constraint | enforced by | why there |
| --- | --- | --- |
| `name` 1..32 printable US-ASCII | schema | the `char[32]` it encodes into, and a launch-time join key |
| `id` int32, `rank` uint8, `version` uint16 | schema | the field widths |
| `sourceId >= 0` | schema | −1 is the cluster's own (**F-4**) and never a list row's |
| `payloadId >= 2` | schema | **C-1** — 0 is invalid on the wire, 1 is retired |
| `id`, `name` and `payloadId` each unique | schema, as identity constraints | a duplicate `gatewayId` silently drops an instance; a duplicate `payloadId` is an operator slip, not **C-3**'s supersede |
| exactly one `rank="0"` per `sourceId` | the loader | not expressible per row; a second rank-0 leaves a logical gateway an arbitrary primary |
| `sourceId` is none of the reserved ids (§5) | the loader | likewise — it is a check against a constant, not a field range |
| an application `sourceId` no gateway row claims | the loader | gateway `sourceId`s repeat by design, so cross-section uniqueness is not expressible; a collision is admitted here and then refused frame by frame by **S-6** case 2 |
| `<gateways>` non-empty; `<applications>` and `<protocols>` MAY be absent or empty | schema | an empty list elects nobody; a deployment with no co-located application, or whose applications share no protocol, declares none |

**The schema is the loader's, not the document's.** A loader MUST resolve the schema from its own
artifact and MUST NOT honour a schema location the document names: a file that names its own schema
can name a lax one, and **C-1** would be advisory. A document MAY carry the location as an editor
affordance.

**Publish order.** The loader MUST validate the whole file before publishing any row — a file that
fails half-way leaves a list the log has already closed. The list rows are then published as one
contiguous run in file order, `remaining` counting down to 0 on the last; **nothing may fall between
them**, because that last row is the completeness edge §7.2 synthesizes the bootstrap `GatewayActive`s
behind. The application rows and then the protocol rows follow it, and carry no countdown of their own:
the sequencer acts on neither, so neither has a completeness edge to publish.

## 7. System messages

**Twelve events in `seqeron-frame.xml` (schema 210), defined once each, beside the envelopes they ride
in** — same owner, same artifact, same change rule, so they are one schema. Core is not an application
and does not ride a `payloadId`: it has the system family of its own (§4), and the field at offset 16
names the event rather than a protocol.

**Nine are submitted and share `UnsequencedSystem`/`SequencedSystem`; three are synthesized and take
top-level templates.** A pair exists to express a relation — submitted, then stamped — and all nine
undergo it. The three share only the absence of one, and a single template discriminating three
unrelated bodies would express nothing.

| event | `systemEventType` | shape | ingress-legal? | sequencer **decodes** it? |
| --- | --- | --- | --- | --- |
| `ConnectionOpened` | 1 | body | ✓ producer | ✓ open-connection set |
| `ConnectionClosed` | 2 | body | ✓ producer | ✓ open-connection set |
| `LeadershipChanged` | 5 | template **105** | ✗ | — (encode only) |
| `ClusterStarted` | 10 | body | ✓ clusterctl | — |
| `ClusterStopped` | 11 | body | ✓ clusterctl | — |
| `ClusterHeartbeat` | 16 | template **104** | ✗ | — (encode only) |
| `GatewayRegistered` | 17 | body | ✓ clusterctl `load-topology` | ✓ list + `remaining == 0` election edge |
| `GatewayActive` | 18 | template **106** | ✗ | — (encode only) |
| `GatewayStarted` | 19 | body | ✓ gateway | ✓ binds cluster session → gatewayId; releases stale connections |
| `PayloadIdRegistered` | 23 | body | ✓ clusterctl `load-topology` | — (encode only; §6.3) |
| `GatewayActivationRequested` | 24 | body | ✓ clusterctl `activate` | ✓ validates the `gatewayId`, then synthesizes `GatewayActive` |
| `ApplicationRegistered` | 25 | body | ✓ clusterctl `load-topology` | — (encode only; §6.4) |

The nine submitted events keep the ids they have always held, and each is also its body codec's
template id — a number never written to the wire, since a body carries no framing. The three
synthesized events keep theirs too: **`systemEventType` is populated on all three**, redundant against
the template id, so that offset 16 discriminates every frame on the tap and a consumer's delivery type
carries one field whichever family it came from. `GatewayActivationRequested` is new and takes **24** —
a fresh number, not `GatewayActive`'s 18, which is still spoken for, so no mixed-vintage recording can
read one as the other. `ClusterHeartbeat` keeps 16 under a name it took in 2026-08 (`Tick` before that)
and is unrelated to `ReplayHeartbeat` (§10).

> **A system body carries no `MessageHeader`.** `header.systemEventType` is what names it, so an encoder
> `wrap`s rather than `wrapAndApplyHeader`s and a decoder supplies `BLOCK_LENGTH` and `SCHEMA_VERSION`
> from its own compiled constants. **V-3** is what licenses that: no seqeron decoder ever meets bytes
> another build encoded, so a per-frame schema and version declaration verifies something already
> guaranteed. Six bytes come off every system frame; a `ClusterHeartbeat`, which needs no body at all,
> is 42 (§4.2).

**`GatewayActive` has one producer: the sequencer.** It synthesizes it at bootstrap (one per rank-0
list row, behind the `GatewayRegistered` whose `remaining` reaches 0) and on three further paths — the
active instance's cluster session closing (`Sequencer.sessionClosed`), a designated instance failing to
answer with a `GatewayStarted` within `GATEWAY_ACTIVATION_TIMEOUT_MS`
(`Sequencer.pendingGatewayActivationTimeout`), and an operator's `GatewayActivationRequested`.

**`clusterctl activate` records the operator's act, and the designation stays the cluster's.** The tool
publishes `GatewayActivationRequested(gatewayId)`; the sequencer validates it against the list,
forwards it, and synthesizes the `GatewayActive` answering it one `globalSeqNo` behind — through the
same path the other three take, which is what gives the manual path the list validation the others get
from iterating the list. That is also what leaves **no `SequencedSystem` without an
`UnsequencedSystem` antecedent**, and makes the synthesized three synthesized-*only*.

**Past participle, because the operator's act is the fact.** Every submitted system message names
something that happened. An imperative (`ActivateGateway`) would be the one command in a family of
events, and would name the effect asked for rather than the act performed. All eleven are events, and
`systemEventType` names them with no outlier.

**The header a synthesized frame carries** (§5's table is the ingress contract and does not cover it):

| field | value |
| --- | --- |
| `sourceId`, `connectionId`, `sessionId` | **−1** _(today: `Sequencer.NO_SOURCE_ID`)_ — reserved for exactly this and refused on ingress (**F-4**) |
| `systemEventType` | the value naming this frame's own template, redundant against the template id |
| `globalSeqNo` | the next value off the counter ingress shares (§9.1) |
| `timestamp` | the Raft consensus timestamp — never `System.currentTimeMillis()` (**S-3**) |

These are wire bytes under **F-2**, not defaults.

> **S-2. The sequencer's entire decode surface is the five rows marked ✓ above.** It reads
> `header.systemEventType` on every ingress frame of the system family, and opens only those five
> bodies past it. **It decodes no `payloadId` at all**: every application frame is opaque to it, and so
> are core's own encode-only events.

**Ingress-legal ✗ means the sequencer MUST reject it on ingress** — and there are two routes to close,
because a synthesis-only event has both a template and a `systemEventType`. §9.2 condition 3 refuses the
template (an ingress frame is `Unsequenced` or `UnsequencedSystem`, never one of the three), and
condition 8 refuses the value inside an `UnsequencedSystem`. Condition 6 (reserved `sourceId`) is
independent of both: it catches an ingress-legal event submitted at −1.

**Ingress-legality is a well-formedness rule, not authorization.** Aeron Cluster is Raft —
crash-fault tolerant, not Byzantine — so a dishonest producer is out of scope, and every rule here
exists against misconfiguration and software defect. The trust boundary is the perimeter: network
topology and authentication at the external FIX edges, which touches no rule here.

### 7.1 Message fields

**No system message carries a `header` field.** The header composite is the frame's (§4.1); a message
that repeated it would nest one envelope inside another. This is the level of nesting the envelope
collapses, and it is why `GatewayStarted`'s floor in §9.2 condition 9 is 8 rather than 8 + 8.

**Ten of the eleven have no var-data and no repeating group**, so a submitted body's encoded length is
exactly its block, the sum of its fields with no padding. `ConnectionOpened` is the one exception and
carries a single opaque var-data field (below). The three synthesized templates' blocks include the
34-byte header composite, since the fields are inline.

| event | `systemEventType` | block | frame bytes | fields |
| --- | --- | --- | --- | --- |
| `ConnectionOpened` | 1 | **0** | 46 + *n* | `connectionData` varData — **opaque to the cluster tier** |
| `ConnectionClosed` | 2 | **0** | 44 | none — `header.connectionId` is the whole message |
| `LeadershipChanged` | 5 | 38 | **46** | `newLeaderMemberId` int32 |
| `ClusterStarted` | 10 | 8 | 52 | `correlationId` int64 |
| `ClusterStopped` | 11 | 8 | 52 | `correlationId` int64 |
| `ClusterHeartbeat` | 16 | 34 | **42** | none — the frame's `timestamp` is the whole message |
| `GatewayRegistered` | 17 | 43 | 87 | `remaining` uint16, `gatewayId` int32, `gatewaySourceId` int32, `gatewayName` char[32], `preferenceRank` uint8 |
| `GatewayActive` | 18 | 38 | **46** | `gatewayId` int32 |
| `GatewayStarted` | 19 | 8 | 52 | `gatewayId` int32, `firstConnectionId` int32 |
| `PayloadIdRegistered` | 23 | 36 | 80 | `payloadId` uint16, `protocolVersion` uint16, `protocolName` char[32] |
| `GatewayActivationRequested` | 24 | 4 | 48 | `gatewayId` int32 |
| `ApplicationRegistered` | 25 | 36 | 80 | `applicationSourceId` int32, `applicationName` char[32] |

Frame bytes are the sequenced form: 44 + block for a submitted body, 8 + block for a synthesized
template.

Field semantics that other rules depend on:

- **`connectionData`** — see below.
- **`newLeaderMemberId`** — the Aeron Cluster `memberId` that is leader from this `globalSeqNo` onward.
  Synthesized once per term, de-duplicated on this value.
- **`correlationId`** — assigned by `clusterctl` and echoed in the frame, so the tool can match its own
  marker coming back off the tap.
- **`remaining`** — list rows still to come after this one; **0 marks the last row**, which is the
  completeness edge the sequencer synthesizes the bootstrap `GatewayActive`s behind.
- **`gatewayId`** — the list row's own identity, unique across the list. **`gatewaySourceId`** — the
  logical gateway the row belongs to, shared by every instance of it, and the value **S-6** matches
  `header.sourceId` against. **`gatewayName`** char[32], US-ASCII, `0x00` in byte 0 signalling absent —
  the launch-time identity an instance resolves its own `{gatewayId, gatewaySourceId}` by.
- **`preferenceRank`** uint8 — **0 is the designated primary**, 1, 2 … are standbys of the same
  `gatewaySourceId`. This is what "one per rank-0 list row" in the bootstrap synthesis means.
- **`firstConnectionId`** — the `header.connectionId` this gateway instance allocates from, chosen past
  the highest its own replay held, so connection ids do not collide across a restart.
- **`protocolVersion` / `protocolName`** — §6.3; `protocolName` is `char[32]`, US-ASCII, trailing
  `0x00`, and neither field is read by the sequencer (**C-2**).

`ClusterHeartbeat`'s empty body is deliberate and is what makes it the cheapest frame in the system:
its content is entirely the frame's consensus `timestamp` and `globalSeqNo`, so the frame is its
`MessageHeader` and its header composite and nothing else (§4.2's 42 bytes).

**`ConnectionOpened` carries an opaque tail; `ConnectionClosed` does not, and the asymmetry is
deliberate.** Both are system events because connection lifecycle is core functionality: the sequencer
keys its open-connection set on `header.sourceId` and `header.connectionId`, and releases every
connection still open under a `gatewaySourceId` behind that logical gateway's next `GatewayStarted` — a
crashed instance publishes none of the `ConnectionClosed`s that would have closed them out. **That set
is node-local sequencer state and the release synthesizes no frame**: `ConnectionClosed` has one
producer, the gateway, and a promoted instance therefore inherits no open connection. A consumer keeping
a view of its own derives it from the same replayed frames, and **S-3** keeps every node's set identical.
For a **disconnect that is the whole message** — `header.connectionId` names a connection every
consumer already saw connect, so there is nothing to add and the payload is its `MessageHeader` alone.

A **connect** is different, and `header.connectionId` alone is not enough for it. The id is minted by
the producer, one per connection, so a counterparty that reconnects arrives under a *new* id while
resuming the *same* session — which means the id cannot carry identity, and a standby that has never
held the socket learns the id→session mapping from this frame or not at all. It needs that mapping at
connect, before the session's first message. But *what* the identity is — a FIX comp-id pair for one
producer, something else for the next — is the producing application's, and the cluster tier has no use
for it. So it rides in `connectionData`, and the following hold:

- **The cluster tier MUST NOT decode `connectionData`.** The sequencer dispatches this event on
  `systemEventType` alone and reads nothing below `blockLength`; **S-2** is unchanged, because opening a
  system body is not the same as opening this field.
- **Its encoding is the producer's own**, identified by `header.sourceId` — the same value that
  routes the frame. A consumer that does not recognise the producer skips the tail exactly as **P-1**
  skips an unrecognised `payloadId`. **E-1** covers it: the frame arrives through ingress, so the tail is
  encoded once by the producer and replicated as bytes, and any encoding is therefore safe.
- **It MAY be empty**, and a length prefix of 0 is well-formed: a producer whose connections need no
  identity beyond the id sends nothing. Core MUST accept both.
- **It counts against `MAX_PAYLOAD_LENGTH`** like any other body byte (§12): the whole message, tail
  included, is bounded by the frame it sits in.
- **No new reject condition.** §9.2 conditions 8 and 9 already establish the event and its block, and
  nothing past that block is read, so there is nothing further for the sequencer to check — consistent
  with it passing through every system event it does not decode.

### 7.2 Promotion order

Every synthesized `GatewayActive` names one **`gatewayId`**, never a `gatewaySourceId`. Every instance of
a pair shares the sourceId, so a frame carrying one would designate both at once — which a consumer could
survive only by latching the first match it ever saw, and that in turn makes a restarting instance
re-activate itself off a superseded frame during its cold-start replay.

**The target.** Given the instance that lost the role, the target is the **lowest-`preferenceRank` list
row of the same `gatewaySourceId`, excluding that instance**, ties broken by **list order** — the first
such row in the log wins. If there is no such row, because the instance has no list row or has no
sibling, the sequencer synthesizes **nothing**: it logs, `globalSeqNo` does not move, and that logical
gateway has no active instance until one starts. Fail closed.

It is lowest-rank-**excluding**, not next-rank-up: after rank 0 loses the role to rank 1, a later loss by
rank 1 hands it back to rank 0. That is what makes a wholly-down gateway tier converge on whichever
instance comes up first, rather than on the one the cluster happened to designate first.

**Four triggers, one target rule:**

1. **Bootstrap** — behind the `GatewayRegistered` whose `remaining` reaches 0, one `GatewayActive` per
   **rank-0** row, iterated in list order.
2. **Session close** — the cluster session a `GatewayStarted` bound (§5, **S-6**) closes. The binding is
   removed first, then the target rule runs against the instance it named.
3. **Activation timeout** — a designated instance has not answered with a `GatewayStarted` within
   `GATEWAY_ACTIVATION_TIMEOUT_MS`.
4. **Operator request** — a `GatewayActivationRequested` naming a listed `gatewayId`, which is
   designated directly rather than through the target rule.

A **fourth trigger** is the operator's: a `GatewayActivationRequested` whose `gatewayId` a list row
names (§7). It runs the same synthesis as the other three and therefore **arms the same deadline** — a
manual activation naming an instance that never starts is handed on like any other. Trigger 3 exists
because a
`GatewayStarted` is the only frame that registers an instance, so an instance that dies or wedges before
publishing one was never registered and no session close can promote past it.

**The deadline.** `GATEWAY_ACTIVATION_TIMEOUT_MS` is **5 × `CLUSTER_HEARTBEAT_INTERVAL_MS` = 5000 ms**
(§12). Every `GatewayActive` arms one at `timestamp + GATEWAY_ACTIVATION_TIMEOUT_MS` on the consensus
clock — **including the one a promotion produces**, so successive failures walk the list instead of
stalling on the first.

At most **one armed activation per `gatewaySourceId`**, replaced in place, held in **arm order**. Both
halves matter: two logical gateways bootstrapping back to back must not overwrite each other's deadline,
and the order the deadlines are walked in MUST be the log's on every node (**S-3**).

**Evaluation** runs on the 1 Hz `ClusterHeartbeat`'s consensus timestamp, immediately after the heartbeat
frame it belongs to, never on a local timer. The sequencer takes the **first** armed activation in arm
order whose deadline has passed and removes it; if no `GatewayStarted` has meanwhile bound that
`gatewayId` to a session, it designates a new target from it by the rule above; otherwise the activation
was answered and is simply dropped. **At most one promotion per heartbeat.**

Every input is log-derived — the list from `GatewayRegistered`, the binding set from `GatewayStarted`,
the clock from the consensus timestamp — so **S-3** holds and every node synthesizes the same frame at the
same `globalSeqNo`.

---

## 8. Schemas

| file | schema id | holds | change policy (§11) |
| --- | --- | --- | --- |
| `seqeron-frame.xml` | **210** | the seven top-level templates, the four header composites, `messageHeader`, `varDataEncoding`, **and the eight submitted system bodies** (§7) | the envelopes **frozen**; the system events changeable under **V-3** |
| `seqeron-replay.xml` | **212** | the six `Replay*` control messages (§10) | in-place, no version bump |

**The envelopes and the system messages are one schema because they are one thing.** Both are seqeron's,
both ship in the same artifact, and **V-3** governs both identically — a separate schema id bought only a
distinction nothing reads. `version` stays **0** for the life of the file, system changes included:
**V-3** forbids a mixed-vintage cluster, so no decoder ever meets bytes another build encoded and there
is nothing for a version to signal. That is also what lets a system body go on the wire with no
`MessageHeader` at all (§7). Schema 211 is unallocated.

**An application `xi:include`s nothing of seqeron's and regenerates nothing of it.** It defines its own
payload encoding — a schema if that encoding has one, nothing at all if it does not — and reads the
frame through seqeron's compiled codecs: the jar for Java, the published headers for C++. There is no
`common-types.xml`.

seqeron ships both schemas' codecs as **one artifact per language**: `ReplayerRecovery` decodes
`LeadershipChanged` (schema 210) while handling a `Replaying` (control, 212) inside the same walk, so the
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
  mutation.**
- Recording *position* is only how the archive is addressed; the replayer needs no
  `globalSeqNo → position` index.

### 9.2 Validation

The sequencer MUST reject, **in this order** — each condition establishes what the next may read — and
MUST NOT throw (§9.4).

| # | reject when | today? |
| --- | --- | --- |
| 1 | `length < MIN_INGRESS_LENGTH` (28), or `length > MAX_INGRESS_LENGTH` (§12) | ✓ |
| 2 | `MessageHeader.schemaId != 210`, or `MessageHeader.version != 0` | ✓ |
| 3 | `MessageHeader.templateId` is neither `Unsequenced` nor `UnsequencedSystem` | ✓ |
| 4 | `blockLength != 18`, exactly — the ENCODED_LENGTH of both ingress composites | ✓ |
| 5 | the var-data length prefix is not a usable body length: it is 65535 (`varDataEncoding`'s `nullValue`), or `MIN_INGRESS_LENGTH + bodyLength != length` | ✓ |
| 6 | `sourceId == -1` — reserved for the cluster's own frames (**F-4**) | ✓ |
| 7 | `Unsequenced` and `payloadId` is 0 or **1** | ✓ |
| 8 | `UnsequencedSystem` and `systemEventType` is not an allocated, ingress-legal value | ✓ |
| 9 | `UnsequencedSystem` and the body is shorter than that `systemEventType`'s compiled `BLOCK_LENGTH` | ✓ |
| 10 | `UnsequencedSystem` and the frame fails **S-6** | ✓ |

Rejection logs and counts (§9.6) and emits nothing — neither the rejected message nor any report of
it — and `globalSeqNo` does not move.

How to implement the sharp ones:

- **Conditions 1–6 cover both ingress templates.** The two 18-byte composites make condition 4 the same
  single equality and condition 5 the same arithmetic, and condition 6 the same int32 compare, whichever
  template the frame is. Only 7–10 branch.
- **Conditions 4 and 5 are equalities, not floors.** Both directions are silent if admitted: a short
  `blockLength` puts the var-data prefix inside the header composite; a long one, or a prefix short of
  the frame end, silently drops bytes and re-emits the frame a size smaller, deterministically, on every
  node. Condition 4 compares against the composite's own `ENCODED_LENGTH`, not a hand-written constant.
- **Condition 3 refuses the synthesis-only templates structurally.** `ClusterHeartbeat`,
  `LeadershipChanged` and `GatewayActive` are top-level templates that are not ingress templates, so a
  producer submitting one is caught before any body is opened. Condition 8 closes the other route in —
  the same event named by `systemEventType` inside an `UnsequencedSystem`.
- **Condition 7 refuses `payloadId` 1 rather than reserving it.** Core no longer rides a payload (§6.1),
  so a producer still on an older build fails loudly instead of having core bytes copied through onto the
  tap as an application payload nothing will ever decode.
- **The `MAX_BLOCK_LENGTH` check is retired** — under the envelope the block is a constant and the only
  varying length is the body's, bounded by condition 1.
- **The ceiling is a compiled-in protocol constant, never a transport read per frame.** **S-3** forbids
  checking against a node's MTU: a node provisioned smaller than its peers would fork `globalSeqNo`. The
  per-node part is **checked at start-up, never per frame** — a node whose ingress or tap MTU cannot
  carry the constant refuses to start (**T-2**, §12).
- **Condition 1 is a backstop** (**T-3**): a conforming producer's encode method refuses an oversized
  payload on the caller's own stack. What condition 1 catches is a producer not running a conforming
  implementation.
- **Conditions 9 and 10 each carry their own floor, and 9 is what establishes 10's.** Fitting the frame
  exactly says nothing about being long enough for what the next condition reads. Condition 8 needs no
  floor — it reads a header field, not a body one.
- **That floor is the decoder's compiled constant, never the wire's `blockLength`.** An SBE decoder
  reads a fixed-width field at its fixed offset whatever acting block length it was wrapped with, so a
  producer-declared length bounds nothing. Condition 9 applies it to every system event a later condition
  reads a body field from, rather than to `GatewayStarted` alone.
- **The sequencer's instance of P-4 is retired with condition 8's predecessor.** There is no longer a
  self-describing payload it must verify before trusting: it selects nothing by `payloadId` and opens no
  payload. **P-4** still governs every application consumer (§6.2, §13.2).

### 9.3 Determinism

> **S-3. Every reject decision, and every byte of every synthesized frame, MUST be a pure function of
> the committed log.** Not node config, not wall-clock time, not local state, not map iteration order.

Conditions 1–9 are pure functions of the message bytes; condition 10 reads state, and every input it
reads is log-derived (the list from `GatewayRegistered`, the binding from `GatewayStarted`). That
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

`sequenceMessage`: decode `MessageHeader` + the 18-byte header; encode a fresh outer `MessageHeader`
(`Sequenced` or `SequencedSystem` matching the ingress template, `blockLength` 34, schema 210, version 0
— all four constants, none copied); copy the 18-byte prefix verbatim, the field at offset 16 included;
overwrite `sessionId`; append `globalSeqNo` + `timestamp`; copy the length-prefixed body verbatim. **One
path serves both families**, because the copy never reads offset 16 and the three fields written back
are at offsets common to both sequenced composites.

Synthesis (`ClusterHeartbeat`, `LeadershipChanged`, `GatewayActive`) is one flat encode per frame: a
template of its own, fields inline, no body, no length prefix, no scratch buffer.

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
encode method; the state conditions are gated by the implementation — a producer MUST refuse to publish
anything before it has resolved its own `{gatewayId, gatewaySourceId}` from the list it read off the
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
| `seqeron-frame.xml` (210) — the envelopes | **frozen.** Any change is a new wire format; all three repos release together | everything |
| `seqeron-frame.xml` (210) — a **submitted** system event | changeable, under **V-3**: a `systemEventType` allocation inside a frozen shape, which restamps *no* application frame | the cluster tier and the system family's consumers |
| `seqeron-frame.xml` (210) — a **synthesized** system event | a frame-layer change: only the sequencer can emit one and it takes a template id | everything |
| `seqeron-replay.xml` (212) | in place, no bump (§10) | one node's build |
| an application schema | the application's business | one repo, one commit — **E-1** holds here |

The system messages carry no `sinceVersion` discipline, because **V-3** leaves them nothing to be
compatible with: no seqeron decoder ever meets bytes another build encoded. That is also what lets a
system body ship with no schema or version declaration of its own (§7).

**The row a change falls in is what the change *is*, not where it lives.** Adding a submitted event and
adding a synthesized one are both edits to one file, and they are a registry allocation and a wire change
respectively.

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
- **V-3. A frame or system-message change MUST NOT be rolled across a running cluster, and no seqeron decoder may
  be required to read bytes an earlier build encoded.** Two builds encoding the same synthesized frame
  differently is **F-2** broken; a surviving archive is the same fault displaced in time.
  `seqeron-replay.xml` is exempt from both — nothing records it, and it is node-local.

## 12. Limits

| limit | value | source |
| --- | --- | --- |
| `blockLength` | **18 / 34** on the four envelope templates, constant — an equality, not a bound (§9.2); 34 / 38 on the synthesized three, whose fields are inline | §4.2 |
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

**This table is where these constants reside.** They belong to the protocol rather than to any
participant in it — the producer's encode methods enforce `MAX_PAYLOAD_LENGTH` (**T-3**), the sequencer's
§9.2 condition 1 is the backstop behind it, and a consumer sizes its buffers from the same numbers — so
each implementation compiles in a **mirror** of this table and none of them owns it. Today those mirrors
are `sequencer/FrameLayer.java` and the `Limits` block in `sequencer/SequencedFrame.hpp`. A change starts
here and lands in both, and is a wire change (**V-3**) whichever way round it is made.

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

Those encode methods are `SystemFrame.wrap`/`wrapPayload` (Java) and `publishPayload`/`publishSystem`
(C++), and the refusal is a value, not an exception: several producer call sites sit inside Aeron poll
callbacks, where a throw is swallowed and the frame silently dropped (§9.4).

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

The system family is all it decodes unaided — seqeron owns schema 210, so every frame of it prints in
full, a submitted body decoded through the `systemEventType` that names it rather than through a header
the body does not carry. Everything else goes down a pipe:

```
sbe-log-printer.sh --payload … | application-decode.sh
```

seqeron gains no application dependency, no descriptor loading and no `templateId → type` registry. It
still **labels** a payload from the `PayloadIdRegistered` frames in the recording it is already reading
(§6.3) — the shared protocols, which are the rows that exist; everything else prints under its number.

**Landed as `-o <payloadId>`** (§15 step 2). It names one protocol and writes that protocol's payloads to
stdout, raw and back to back; every text line the run produces — the `[Catalog]` line, the frame dump, the
errors — moves to stderr, so nothing else touches those bytes. Two things fell out of that shape rather
than having to be designed:

1. **Nothing to render.** `varDataEncoding` declares `characterEncoding="US-ASCII"` and SBE's
   `JsonPrinter` renders var-data through it, so a payload printed *inside* the frame line comes out
   mangled and needs base64 to survive the trip. A payload on a stream of its own is never rendered at
   all, and base64 buys only printability.
2. **No framing.** Nothing sits between one payload and the next. The decoder on the other end owns the
   payload's schema, and that is what delimits it; a length or a correlation header would be seqeron
   asserting something about bytes it has no business reading (**S-2**). Correlation is the frame dump
   on stderr, which the run emits in the same order.

### 13.2 Non-SBE payloads

> **E-1. A payload or system body that arrived through ingress is encoded exactly once, by its producer,
> and is never re-encoded after sequencing. The sequencer's synthesized frames are the exception —
> `ClusterHeartbeat`, `LeadershipChanged` and `GatewayActive` — because they have no producer: every node
> encodes its own copy.**

Application payloads may use **any encoding** — SBE, protobuf, raw FIX bytes, a proprietary binary.
Every replica receives payload *bytes* (replicated through Raft, or read off its own recording), so two
encoders never have to agree.

**The frame and the system messages are fixed-layout SBE and MUST NOT become tag-encoded.** The
synthesized frames are encoded independently on every node, and the taps must come out byte-identical
(**F-2**, **S-3**), so the requirement that two encoders agree binds the system family exactly as it
binds the frame.

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
| 2 | **Prefix property.** Each unsequenced composite's bytes 0..17 equal its sequenced counterpart's for the same values — the field at offset 16 included — the two pairs are byte-identical to each other, and the `ENCODED_LENGTH`s are 18 and 34. Pure schema check, no `Sequencer` | **F-3** |
| 3 | **System frames round-trip unchanged.** Parameterised over the **eight** submitted events of §7: wrap as `UnsequencedSystem`, sequence, assert the egress body is byte-identical and the `systemEventType` carried, and for `ConnectionOpened` over an empty, a short and a `MAX_PAYLOAD_LENGTH`-filling `connectionData` (§7.1). The three synthesized templates are covered instead by encoding one of each through the `Sequencer`'s own synthesis path, asserting the template id, the inline fields and the redundant `systemEventType` | §7 |
| 4 | **Rejection table.** One case per row of §9.2, each asserting that the rejected message was not sequenced, that **nothing at all was emitted** — `globalSeqNo` unmoved, no synthesized frame behind it — and that the rejected-frame counter advanced by exactly one. Cases: over `MAX_INGRESS_LENGTH` and under 28 (1); a `schemaId` that is not 210 and a non-zero `version` (2); a `templateId` that is neither ingress template, and each of the three synthesized ones (3); a `blockLength` that is not 18, below and above, on both ingress templates (4); a length prefix past the frame end, one short of it, and one equal to 65535 (5); an ingress `sourceId` of −1 (6); `payloadId` 0 and `payloadId` 1 (7); an unallocated `systemEventType` and each of the three synthesis-only ones (8); a `GatewayStarted` body shorter than its 8 bytes (9); S-6's cases (10) | **S-4**, **S-5**, **S-7** |
| 4a | **Rejection denies nothing, and repeats identically.** Two rejections under the same application `payloadId` each advance the counter, neither emits anything, and a well-formed frame of that `payloadId` afterwards is accepted unchanged | **S-7**, **C-2** |
| 4b | **The producer refuses before the wire, and survives it.** A payload of exactly `MAX_PAYLOAD_LENGTH` publishes; one byte longer is refused with **nothing offered to any transport**, distinguishably from back-pressure, and the very next well-formed publish succeeds. Against an in-memory transport seam, so no Aeron | **T-3**, §12 |
| 5 | **Synthesis determinism.** Two independently constructed `Sequencer`s fed the same message sequence emit byte-identical frames, heartbeat for heartbeat | **S-3**, **F-2** |
| 6 | **The frame layout is §4's tables.** Every frame-layer field sits at the offset §4.1 gives, on all four header composites; the frame sizes are §4.2's — `MessageHeader` 8, the composites 18 and 34, the var-data prefix 2, fixed overhead 28 and 44, a `ClusterHeartbeat` 42, a ceiling-sized payload frame 1360. And the three boundary payload sizes — empty, one byte, `MAX_PAYLOAD_LENGTH` — each cross and decode intact | **F-2**, **F-3**, §12 |
| 7 | **Selective consumption.** A consumer fed an unallocated `payloadId`, and an unhandled `systemEventType`, ignores both without error — and its `globalSeqNo` continuity tracking advances across them, over all five sequenced shapes | **P-1**–**P-3** |
| 8 | **S-6's three cases.** A `GatewayStarted` agreeing with its list row binds and is accepted; four are rejected — the same frame naming a different `gatewaySourceId`, one whose `gatewayId` no list row names while claiming a listed `sourceId`, a `GatewayActivationRequested` naming an unlisted `gatewayId`, and another system frame claiming a listed `sourceId` on an unbound session — and both negatives are accepted: an application payload carrying that `sourceId`, and a marker at −1 | **S-6** |
| 9 | **Promotion order.** Against a list of one `gatewaySourceId` with ranks 0, 1, 2: bootstrap activates rank 0 only; an operator's `GatewayActivationRequested` is forwarded and answered one `globalSeqNo` behind; closing rank 0's bound session promotes rank 1; closing rank 1's promotes rank 0 again (lowest-rank-excluding, not next-rank-up); a designated instance that publishes no `GatewayStarted` is handed on after exactly `GATEWAY_ACTIVATION_TIMEOUT_MS` of consensus time and not before; one that does publish one arms nothing further; and a `gatewaySourceId` with a single row synthesizes **no** frame on close, leaving `globalSeqNo` unmoved. Two `gatewaySourceId`s bootstrapped back to back each keep their own deadline | §7.2, **S-3** |

**Every row asserts a rule this document states, and builds the frames it reads.** There is no stored
corpus of bytes from an earlier build. A corpus reports only that *something* moved and leaves a reader
to diff hex; the rules above name what broke — which is the whole reason §4.1's offsets and §4.2's sizes
are written down rather than left implied by field order. Anything a corpus would have caught that these
rows do not is a property of the wire format that is missing from §4, and the fix is to state it there.

This also keeps the suite honest about what the two languages share. Both compile codecs from one
`sbe-frame.xml` through one generator, so an assertion that their two outputs agree would test the pinned
tool (**V-1**), not this protocol. What is worth testing per language is the hand-written code above the
codecs — `unwrapFrame` and `SequencedFrameDecoder`, ported by hand and where a real defect was found — and
that is what rows 3, 6 and 7 exercise.

Row 8's "marker at −1" is `clusterctl`'s: `sourceId` 2, which no list row claims (§5), carrying
`connectionId` −1 because the marker is producer-scoped. A `sourceId` of −1 is illegal on ingress under
condition 6, and is row 4's case rather than row 8's.

The suite asserts framing and copy fidelity only. A test that wants to decode a payload to check it has
crossed the `payloadId` boundary in the wrong direction.

---

## 15. Landing it

Each sub-step green before the next, all in one repo. The numbering is fixed — other documents cite
these step numbers — so a landed step keeps its place and records what actually landed.

1. **Landed.** Add the frame schema with the two envelopes, the two renamed composites and the ten core
   payloads (prefix-extended, `payloadId` moved in, `origin` deleted, unpadded) — and, in the same wire
   change, give the FIX session families in the running pair their own direction field. → both codegen
   paths green, the `unsequencedHeader`/`sequencedHeader` rename landed across the pair and every
   consumer of it, both edges reading direction from the payload, copy-through unchanged. The file is
   `cluster/src/main/sbe/sbe-frame.xml` and said `phixeron` throughout: the `seqeron` rename followed the
   repo split, not a protocol step, each of which is already a
   wire change on its own. **Step 6 landed in the same change** — see there for why it could not wait.
2. **Landed, out of order — after steps 3–7 rather than before them.** Land §13.1's payload pipe,
   before anything on the wire is opaque. → `sbe-log-printer.sh -o <payloadId>` writes that protocol's
   payloads to stdout for a decoder that owns their schema; no schema and no frame changes. It gates the
   **application** families, not core: a core payload is decoded by the same tool that reads the frame
   around it, so this is a prerequisite for step 3 rather than for step 1. Nothing broke while it was
   postponed because the printer still bundles all six IR files, so the families that moved onto payloads
   in steps 3 and 4 stayed readable inline the whole time — the pipe is what makes them readable once the
   application schemas are *not* seqeron's to bundle, which is the repo split. **§13.1's base64 frame
   line went with the postponement**: it was the answer to rendering a payload inside the frame line, and
   a payload on a stream of its own is never rendered.
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
   edge's structured templates, the Java legs' opaque bytes), and when the C++ edge retires the seven
   templates go while `ClientSessionEvent` stays. `PortfolioQuery{Request,Reply}` fold into
   `sbe-order.xml` rather than taking a number of their own: they are the same application, between the
   same two processes as the order flow, and a `payloadId` names a schema, never a message. Reference
   data goes to `sbe-basicdata.xml`, **schema 240, `payloadId` 4** — the one protocol here that crosses
   application boundaries, and so the only one §6.4's example declares. Its owning repo settles with the
   repo split; until then every schema lives at the shared root, as
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
   paths green. The file is `cluster/src/main/sbe/sbe-replay.xml`, not `seqeron-replay.xml`, for step
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
8. **Landed.** The schema half came with step 1 — `PayloadIdRegistered`, §7's tenth core payload — and
   nothing published it. This converted the topology file to §6.4's document and schema and taught
   `SbeLogPrinter` to label payloads from it. A hard cutover, as planned: `topology.csv` and the three
   harness lists became `.xml`, `topology.xsd` shipped beside them in the loader's own jar, and the
   four script paths moved, in one commit. Not a wire change — past the new `PayloadIdRegistered` rows
   the frames the loader publishes are byte-identical, and no archive needed purging. → a payload prints
   under its protocol's name, an unregistered one under its number, no frame affected either way, both
   e2e suites green with no new start-up step.

   Three things settled here rather than in the design. **The flat `<gateway name id sourceId rank/>` of
   §6.4 was built**, not `doc/clusterctl.md`'s nested `<primary>`/`<standby>` variant, which that
   proposal preferred for turning three validation rules into content-model structure; the flat shape
   keeps `preferenceRank` a field and leaves the rank-0 scan in the loader. **The document is validated
   as it parses**, against a schema the loader resolves from its own artifact — a `schemaLocation` the
   document names is ignored, or **C-1** would be advisory. And **the deployment registers `payloadId`
   2 and 3 alongside 4**, though §6.1 calls the first two private: the rows cost nothing, and the label
   is what a reader of a dump wants whether or not the protocol crosses an application boundary.
9. **Landed.** §14's suite, both languages. → it fails on a deliberate field reorder and on a renumbered
   core frame. `ConformanceTest` beside `SequencerTest` in the Java suite and in `core_tests`.

   **Only the rows with a C++ implementation behind them are mirrored.** There is no C++ sequencer, so
   rows 1, 4, 4a, 5, 8 and 9 are Java's alone; rows 2, 3, 4b, 6 and 7 are in both. That is not a gap in
   the suite — it is where the two languages actually meet, and what they meet on is this document: each
   side asserts §4's tables against its own generated codecs.

   Three things the suite needed that did not exist, and one it found:

   - **`Sequencer` counts its own rejections.** **S-7**'s counter was `SequencerService`'s Aeron counter
     and nothing else, so no Aeron-free test could assert a rejection cost exactly one. It is a plain
     field now, mirrored onto the operator counter as before.
   - **T-3 was unimplemented.** Both producers encoded any length handed to them and left §9.2 condition
     1 to catch it on the far side of a transport, which is exactly what T-3 says must not happen.
     `SystemFrame.wrap`/`wrapPayload` return `REFUSED` and `publishPayload`/`publishSystem` return
     `Publish::Refused` — three-valued in C++ because `Declined` (the transport's answer) is the one a
     caller may retry and `Refused` never is. The C++ encode buffer was 512 bytes, sized off the widest
     message any current producer writes rather than off the constant, so a legal 1316-byte payload had
     nowhere to go; it is `MAX_INGRESS_FRAME_LENGTH` now.
   - **§12's constants had no home in either language.** `MAX_PAYLOAD_LENGTH` had no C++ definition at
     all and a package-private Java one on `Sequencer`, and the two framing bounds were `Sequencer`'s
     `MIN_FRAME_LENGTH`/`MAX_FRAME_LENGTH` — the replicated state machine holding the numbers a producer
     and a consumer both answer to. §12 is the definition and each language now compiles in a mirror of
     it: `sequencer/FrameLayer.java` and `SequencedFrame.hpp`'s `Limits` block, under §12's own names.
     The encode methods enforce the ceiling; they do not own it.
   - **What it found: both consumers dropped a frame whose payload was shorter than 8 bytes.**
     `SequencedFrameDecoder` and `unwrapFrame` each refused one as "an empty or truncated payload names
     no message to dispatch on" — true, and not a reason to withhold the frame. §5 admits an empty
     payload, §13.2 admits a payload that is not SBE at all, and **P-3** requires continuity to hold over
     frames the consumer comprehends nothing of; dropping one puts a hole in the `globalSeqNo` read that
     reads as a gap that is not there. Both now return the frame with `templateId`/`blockLength`/
     `version` at 0, which is what the system branches already did and what says "no inner declaration"
     — no `(payloadId, templateId)` dispatch can match it, which is **P-1** doing its job. Row 1's empty
     and 1-byte cases are what surfaced it.

   And one thing row 6 asked for that does not work. It called for a checked-in binary corpus that both
   languages **decode and re-encode to the same bytes** — which is an identity under any field layout, so
   long as one build's decoder and encoder share it: two builds whose header composites permute the
   same-width fields differently both pass it. Verified by permuting `sequencedSystemHeader`'s `sourceId`
   and `connectionId` — the round trip stayed green.

   Asserting the decoded values against the generator's literals does pin the layout, but only across
   *builds that could disagree*, and today's two cannot: one SBE run over one `sbe-frame.xml`. What the
   corpus was left doing was reporting that the bytes had moved — a tripwire carrying no knowledge, when
   §4.1 and §4.2 already state the layout it was standing in for. **So the corpus is gone and row 6 now
   asserts those two tables directly**, in both languages, off the generated offset accessors. A field
   reorder fails naming the field and the offset it should have had. Two consequences worth recording:
   the rule and its test are now the same statement in two places rather than a file of bytes between
   them, and there is no regeneration ritual on a deliberate wire change.

10. **Landed. Split core off the application envelope.** Core is not an application and `payloadId` 1 said
    it was. Ten messages that the cluster tier itself decodes rode inside the same
    `Unsequenced`/`Sequenced` body that carries an application's opaque bytes, distinguished only by a
    reserved value in the id space that names application protocols — so the sequencer's own vocabulary
    is a special case of the thing it is supposed to be transparent to, and **P-4** has to be paid on
    every frame to tell the two apart. This step gives core its own shapes and retires `payloadId` 1.

    **Seven top-level templates in schema 210**, two composites, two layouts:

    | template | id | header | body |
    | --- | --- | --- | --- |
    | `Unsequenced` | 100 | `unsequencedHeader` (18) | `payload:varData` |
    | `Sequenced` | 101 | `sequencedHeader` (34) | `payload:varData` |
    | `UnsequencedSystem` | 102 | `unsequencedSystemHeader` (18) | `body:varData` |
    | `SequencedSystem` | 103 | `sequencedSystemHeader` (34) | `body:varData` |
    | `ClusterHeartbeat` | 104 | `sequencedSystemHeader` (34) | — |
    | `LeadershipChanged` | 105 | `sequencedSystemHeader` (34) | `newLeaderMemberId` |
    | `GatewayActive` | 106 | `sequencedSystemHeader` (34) | `gatewayId` |

    The system composites are the application ones byte-for-byte, with **`systemEventType` at offset 16 where
    `payloadId` sits** — same width, same alignment, so **F-3** holds within each pair and `globalSeqNo`
    stays at 18 on all five sequenced shapes. That last property is the one to defend: it is what keeps
    the continuity read in both `ReplayerRecovery`s branch-free over every frame on the tap.

    **`systemEventType` replaces the payload's inner `MessageHeader`.** A system body is its message's SBE
    block with no framing of its own — encoders `wrap` rather than `wrapAndApplyHeader` — and a decoder
    supplies `BLOCK_LENGTH` and `SCHEMA_VERSION` from its own compiled constants. **V-3** is what
    licenses that: no seqeron decoder ever meets bytes another build encoded, so a per-frame schema and
    version declaration is 8 bytes verifying something already guaranteed. Six bytes come off every
    system frame; a `ClusterHeartbeat`, which needs no body at all, goes from 52 to **42**.

    **The seven submitted messages keep their numbers as `systemEventType`s** — `ConnectionOpened` 1,
    `ConnectionClosed` 2, `ClusterStarted` 10, `ClusterStopped` 11, `GatewayRegistered` 17,
    `GatewayStarted` 19, `PayloadIdRegistered` 23 — and the three the cluster synthesizes leave the
    space entirely, because a template id already names them. `systemEventType` is populated on all three
    anyway, redundant against the template id, so that **offset 16 discriminates every frame on the tap**
    and a consumer's delivery type carries one field whichever family it came from.

    **`clusterctl activate` records the operator's act.** It publishes `GatewayActivationRequested(gatewayId)`,
    `systemEventType` **24** — a fresh number, not `GatewayActive`'s vacated 18, so no mixed-vintage
    recording can read one as the other (§7's rule for `PayloadIdRegistered`) — and the sequencer
    answers it by synthesizing `GatewayActive` through the same path bootstrap and both promotions take.
    That is what makes the synthesized three synthesized-*only* and leaves no `SequencedSystem` without
    an `UnsequencedSystem` antecedent, which is the whole point of the split: **a sequenced shape asserts
    a transformation, and a frame with no ingress form must not claim one.** It also gives the manual
    path the list validation the other three have and it today lacks.

    **Past participle, because the operator's act is the fact.** Every submitted system message names
    something that happened — `ConnectionOpened`, `GatewayRegistered`, `GatewayStarted`. An imperative
    (`ActivateGateway`) or a bare request would be the one command in a family of events, and would name
    the effect asked for rather than the act performed. What happened is that an operator asked;
    the sequencer then designates (§7's verb for what `GatewayActive` does). So all eleven system
    messages are events, and `systemEventType` names them with no outlier.

    **Why the seven bundle and the three do not.** A pair exists to express a relation — submitted, then
    stamped — and all seven undergo it. The three share only the absence of one. A single template
    discriminating three unrelated bodies would express nothing, which is the pattern this step exists to
    remove.

    **§2 loses its awkward paragraph.** Sequenced and synthesized frames no longer share a template, so
    the class is no longer marked only by `sourceId == -1` — it is the template. **F-4** stays as the
    provenance mark and stays refused on ingress, but it is now corroboration rather than the sole
    carrier, and §2's closing example inverts: `GatewayActive` stops being the frame that is synthesized
    at bootstrap and forwarded on manual promotion, which is what made "the classes partition frames, not
    payloads" need saying at all.

    **§9.2 flattens.** Conditions 1–6 are unchanged and now cover both ingress templates: the two
    18-byte composites make condition 4 the same single equality, and condition 5 the same arithmetic.
    Condition 3 admits `Unsequenced` **and** `UnsequencedSystem` and thereby **absorbs condition 9** — a
    `ClusterHeartbeat` or a `LeadershipChanged` on ingress is now a top-level template that is not an
    ingress template, caught structurally rather than by opening a payload. **Condition 8 goes**: there
    is no inner `MessageHeader` to verify. The remaining three branch on the template:

    | # | reject when |
    | --- | --- |
    | 7 | `Unsequenced` and `payloadId` is 0 or **1** |
    | 8 | `UnsequencedSystem` and `systemEventType` is not an allocated, ingress-legal value |
    | 9 | `UnsequencedSystem` and the body is shorter than that `systemEventType`'s compiled `BLOCK_LENGTH` |
    | 10 | `UnsequencedSystem` and the frame fails **S-6** |

    `payloadId` 1 becomes **refused on ingress rather than reserved-and-decoded**, so a producer still on
    the old build fails loudly instead of having core bytes copied through as an application payload.
    Condition 9's floor is still the decoder's compiled constant and never the wire's declared length,
    for the reason §9.2 already gives, and it now applies to every system template a later condition
    reads a field from rather than to `GatewayStarted` alone.

    **P-4 does not go with condition 8.** What retires is the *sequencer's* instance of it — there is no
    longer a self-describing payload it must verify before trusting. The rule itself still governs every
    application payload (§6.1, §13.2), where a wrongly selected decoder is still only catchable where the
    encoding self-describes.

    **§11 gets a sharper line than it has.** Adding a submitted system message is a `systemEventType`
    allocation inside a frozen shape — the changeable row, under **V-3**. Adding a synthesized one is a
    frame-layer change, because only the sequencer can emit it and it takes a template id. That splits
    the two rows on what the change actually is, rather than on core-payload-versus-envelope.

    **Blast radius.** Schema and both codegen paths; `unwrapFrame` (`SequencedFrame.hpp`) and
    `SequencedFrameDecoder`, which grow a second family each; the `SequencedEvent` delivery types in both
    languages and `LifecycleEvent`; `CoreFrame` → `SystemFrame`, `IngressPublisher::publishCore` →
    `publishSystem`, `Sequencer.applyCore` → `applySystem`, `CORE_PAYLOAD_ID` deleted; the synthesis
    path, which loses `beginSynthesized`/`endSynthesized`'s payload-length bookkeeping for three flat
    encodes; `ReplayerService`'s `globalSeqNo` self-check, which must read all five sequenced shapes;
    `SbeLogPrinter`; `ClusterCtl` (five publishers, and `activate` changes message); and the core
    producers on both edges — `FixGateway`, `BasicDataServer`, `Gateways`, both `GatewayLifecycle`s,
    `ClusterIngress` ×2. Application payload handling is untouched end to end, which is the test that
    the split is in the right place.

    → both suites and all three e2e paths green; the sequencer decodes **no `payloadId` at all**;
    `grep -r CORE_PAYLOAD_ID` is empty. A wire change, and the largest since step 1: **V-3** applies, and
    every archive purges.

    Two things settled here rather than in the design. **The eight submitted bodies keep their template
    ids as their `systemEventType`s literally** — each is a `<sbe:message>` whose template id is never
    written to the wire, so `ConnectionOpenedDecoder.TEMPLATE_ID` *is* the constant and there is no second
    table to drift; only the three synthesized values (5, 16, 18) need naming in code, because their
    templates are numbered 104–106. And **`blockLength`/`version` are 0 on every system frame a consumer
    is handed**, synthesized ones included: the decoder's compiled constants are the only ones there are
    for a submitted body, and using them for the synthesized three too keeps one decode helper rather than
    two (`decodeSystem`, `SystemFrame`).

**Steps 1, 3–6 and 10 are each a wire change**, and **V-3** applies to each.

**Between step 1 and step 4 the tap carried two shapes** — schema 210 envelopes beside bare schema 202
messages — and `Sequencer.sequenceMessage` was a `schemaId` dispatcher over the two, a knowing,
temporary divergence from **§9.2 condition 2**. Step 4 ended it: every frame on the tap is an envelope,
every consumer dispatches on `(payloadId, templateId)`, and condition 2 holds as written.
