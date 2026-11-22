# Upgrades and versioning

How a seqeron deployment changes versions: seqeron's own protocol, which is fixed, and the application
protocols it carries, which are not. This is the policy that holds today. A model for upgrading applications
without stopping them, with specified snapshot records, upcasters and a rollout order the cluster enforces, is
proposed in [`upgrade-model.md`](../records/plans/upgrade-model.md); §6 says which parts of it are built. "spec" is
[`seqeron-protocol-spec.md`](seqeron-protocol-spec.md) and "plan §n" is [`upgrade-model.md`](../records/plans/upgrade-model.md) §n. How exchanges and log-based
systems version their protocols is surveyed in [`versioning-prior-art.md`](versioning-prior-art.md).

## 1. Two protocols, two policies

| | seqeron protocol | application protocol |
| --- | --- | --- |
| what | the message envelope and the system messages, schema 210 | the payloads, one schema per `payloadId` |
| owned by | seqeron | the application (spec **V-2**) |
| decoded by | the sequencer and every client | the application's own producers and consumers; the cluster copies payloads through unopened |
| changes | rarely, in a seqeron release | whenever the application releases |
| compatibility | none: a change is breaking (spec **V-3**) | whatever the application's encoding provides |
| rollout | whole deployment, archives purged | independent of the cluster; whether producers and consumers move together depends on the encoding |

**A seqeron protocol change** is made on a stopped cluster: every member, gateway host and client moves to
the new release together, and every archive is purged, so the recorded history does not survive it. No
seqeron decoder reads bytes another build encoded, which is why its system messages carry no `sinceVersion`
fields.

**An application change** never touches the cluster. The sequencer validates the envelope and nothing
inside it, so a new payload version is sequenced, recorded and replayed exactly like the old one. The cost
of that opacity is that seqeron cannot help an application stay compatible with itself: the rest of this
document is what the application must do, and the plan proposes what seqeron could do to make it tractable.

## 2. What an application upgrade must handle

A new build of a stateful application meets its protocol in three places:

| data | where it comes from | read by the new build when |
| --- | --- | --- |
| **history** | the log, from `clusterSeqNo` 1 or from a snapshot's cut | it recovers |
| **state** | its own snapshot files ([`snapshot.md`](snapshot.md)) | it restores |
| **in-flight messages** | the live queue, from producers not yet upgraded | it runs alongside older builds |

Each needs a different answer, and today the application answers all three alone. How far back it must read
is §4's.

A versioned encoding is the prerequisite for any of it, and handles most changes by itself (§3). A payload's
`MessageHeader` already carries its schema `version`, which `Payload.version()` exposes; an unversioned or
headerless payload (spec §13.2) leaves the application to sniff versions itself.

## 3. Encoding evolution

Both SBE and Protocol Buffers evolve a schema without breaking its readers, within rules. A change inside
those rules needs nothing from seqeron or the application beyond the new codec; a change outside them needs a
new `payloadId` (§5), or under the plan an upcaster (plan §3).

### 3.1 SBE

The `MessageHeader` carries the message's `blockLength` and `version`, and a decoder wraps with those
*acting* values rather than its compiled ones:

- **Newer decoder, older message.** A field whose `sinceVersion` is above the acting version returns its
  null value, and the decoder reports which fields the acting version holds.
- **Older decoder, newer message.** `blockLength` lets it skip fields appended to the block. Repeating
  groups carry their own `blockLength`, so group entries extend the same way; groups and variable-length
  data appended after the existing ones go unread.
- **Allowed:** fields appended under `sinceVersion`, groups and variable-length data appended at the end,
  new enum values (an older decoder sees them as unknown), renames.
- **Not allowed:** removing, reordering or retyping a field, restructuring a message, changing a field's
  meaning. A deprecated field keeps its place in the layout.

The client tier already decodes this way: C++ `Payload::decode<Decoder>()` wraps with the message's own
`blockLength` and `version`, and `Payload.blockLength()` and `Payload.version()` give Java and C# the same
values to wrap with. The rules are checkable from the XML, and a checker that enforces them is proposed (plan §2).


### 3.2 Protocol Buffers

Fields are identified by number; a missing field decodes to its default and an unknown one is skipped or
kept.

- **Allowed:** adding fields, removing them (their numbers reserved), some widening type changes, new enum
  values. Both directions work, with no version number at all.
- **Not allowed:** reusing a field number, changing a field's meaning, some changes to `oneof`s.
- **No message version.** The `MessageHeader` version, which `Payload.version()` reads, is the application's
  to set.
- **Allocation.** Decoding parses into objects. This is the application's cost, not the client tier's
  zero-allocation dispatch.

### 3.3 Canonical bytes

Spec **A-6** compares snapshot bytes across instances. SBE's fixed layout is canonical in every language.
Protocol Buffers is deterministic only with deterministic serialization, the same library version and the
same language on every instance, and preserved unknown fields can still make two instances differ.

**Recommendation.** SBE for snapshot records: canonical across languages, zero-copy, and seqeron's own
toolchain, whose IR files `SbeLogPrinter` already discovers. Protocol Buffers is workable for payloads and
weaker for snapshots.

## 4. Snapshots and rollouts

A snapshot file carries one `formatVersion` for all its records, and a build that changes its format keeps
reading the ones it replaces through `supportsFormatVersion` ([`snapshot.md`](snapshot.md) §9).

**A snapshot format does not change mid-rollout.** Every instance of a source must write the same bytes for a
round (spec **A-6**), or the ones that differ are fenced (**A-7**). A round cut while a source's instances run
different builds can therefore diverge. A source's rollout falls between two rounds — rounds started by the
operator only (`interval="0"`), or a rollout shorter than the interval — and the new build reads the format the
last round wrote and writes its own from its first round on.

**How far back a build reads.** Without a `<snapshots>` policy no round starts, every member keeps its whole log,
and a consumer that recovers replays from `clusterSeqNo` 1, so it reads every payload version ever recorded. With
one, the loader requires `snapshot="true"` on every row, and each node purges its queues below the floor `F`, its
newest restart-safe cut ([`snapshot.md`](snapshot.md) §10.6): a build reads the snapshot formats of the rounds
since `F` and the payload versions published since `F`. Plan §1 makes that window exact.

## 5. A new source beside the old

The alternative to upgrading a source in place: a new source runs the new protocol version
beside the old one, consumers move to it, and the old source is retired. Below, `O` is the old source,
publishing replies in v1; `N` is the new one, publishing them in v2.

- **`N` is a new source**: its own `sourceId` and topology row, one replica per member as `O` has.
- **`N` is in one of the active groups.** The groups are fixed by the first topology load (spec **S-8**):
  adding, removing or renaming one, or moving a source to another, needs every archive purged. Adding
  groups to a running cluster is proposed (plan §6).
- **v2 has its own `payloadId`**, with a `PayloadIdRegistered` row. A consumer that does not know it skips
  it (spec **P-1**), so `O`'s consumers ignore `N` unchanged. Under `O`'s `payloadId` with a higher schema
  version, they would decode it too, since **P-4** checks only the `schemaId`, and act on two replies to
  every request.
- **The cluster does not change.** The sequencer admits any `sourceId` but −1 and any `payloadId` from 2
  (spec §9.2, **C-2**), and loading the topology again with the added rows is safe
  ([`clusterctl.md`](../operations/clusterctl.md)). An application row labels its source and turns its snapshots on; it
  admits nothing.

The steps for moving every consumer from `O` to `N` — comparing their replies, switching at a cutover marker
in the log, retiring `O` — are proposed (plan §5).

## 6. Status

| piece | status |
| --- | --- |
| opaque payloads, per-application versioning (spec **V-2**) | implemented |
| snapshot rounds, confirmation, restore, `formatVersion` checked on restore by the listener | implemented ([`snapshot.md`](snapshot.md)) |
| operator-only rounds (`interval="0"`, `clusterctl request-snapshot`) | implemented |
| snapshots required cluster-wide | implemented: with `<snapshots>`, the loader requires `snapshot="true"` on every row |
| cluster snapshots and the floor `F` | implemented, but the audit tool ([`snapshot.md`](snapshot.md) §10) |
| snapshot records as specified messages, framing in the spec | proposed (plan §1.1) |
| upcasters, for payloads and snapshot records | proposed (plan §3) |
| a schema checker enforcing §3.1's rules | proposed (plan §2) |
| consumers-first enforced by the cluster: announced versions and an activation frame | proposed (plan §3.4): a schema 210 change |
| a new source beside the old: its own `sourceId`, `payloadId` and rows, loaded into a running cluster | implemented (§5) |
| groups: a queue per group, fixed by the first topology load | implemented (spec §3.1, §7.4) |
| groups added to a running cluster by a later topology load | proposed, a second step (plan §6) |
| a source restoring another source's snapshot | proposed (plan §5.1 step 1) |
| a reply comparator | proposed (plan §5.1 step 2) |
| a cutover marker convention | proposed (plan §5.1 step 3): application-level; a system event for it could join the next schema 210 change, which purges anyway (spec **V-3**) |
| retiring a source | proposed: nothing deregisters one. Each instance's directory keeps its last confirmed files, since no newer round supersedes them |
| knowing that nothing reads v1 | proposed: a `consumes` attribute on topology rows, a label like the others, would let a tool check that a retirement is safe |
| compatibility rules for application protocols in the spec | not specified |
