# Upgrades and versioning

How a seqeron deployment changes versions: seqeron's own protocol, which is fixed, and the application
protocols it carries, which are not. §1, §2 and §4 describe what holds today: seqeron's policies, and what
SBE and Protocol Buffers provide. The rest is a **proposed design** for upgrading applications without
stopping them; none of it is implemented, and the compatibility rules it describes are not yet part of the
spec. "spec" is
[`seqeron-protocol-spec.md`](seqeron-protocol-spec.md).

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
document is about what the application must do, and what seqeron could do to make it tractable.

## 2. What an application upgrade must handle

A new build of a stateful application meets its protocol in three places:

| data | where it comes from | read by the new build when |
| --- | --- | --- |
| **history** | the log, from `globalSeqNo` 1 or from a snapshot's cut | it recovers |
| **state** | its own snapshot files ([`snapshot.md`](snapshot.md)) | it restores |
| **in-flight messages** | the live tap, from producers not yet upgraded | it runs alongside older builds |

Each needs a different answer. Today the application answers all three alone, and since the log is never
truncated, a consumer that recovers from `globalSeqNo` 1 must read every payload version ever recorded.

A versioned encoding is the prerequisite for any of it, and handles most changes by itself (§4). A payload's
`MessageHeader` already carries its schema `version`, which `Payload.version()` exposes; an unversioned or
headerless payload (spec §13.2) leaves the application to sniff versions itself.

## 3. Snapshots as a migration boundary

A snapshot is the application's state encoded at a known point in the log. A build that restores it reads
only what was recorded after the cut, so the snapshot bounds how much of the protocol's past that build must
understand: **the snapshot's schema versions, plus the payload versions published since the cut**, rather
than every version in the log.

That holds only if the snapshot is itself versioned like a payload (§3.1), and under two constraints (§3.2).

### 3.1 Snapshot records are specified messages

Today a record is opaque bytes after seqeron's header record ([`snapshot.md`](snapshot.md) §6), and one
`formatVersion` covers the whole file: only the listener can read it, and a restore stops on any version
its build does not support.

Proposed: every application record is a message of a declared schema, encoded with a `MessageHeader`
exactly as a payload is, under a `payloadId` the topology's `<protocols>` section registers (spec §6.3).
seqeron's spec defines the framing — the length prefix, the header record, and that every later record
begins with a `MessageHeader` — and the application's schema defines the messages.

- **Versioned per record, not per file.** A restore accepts any record whose version its build decodes,
  directly or through an upcaster (§5), and stops only on one it cannot. `formatVersion` becomes the
  schema's version, or goes.
- **One mechanism for state and messages.** The upcasters that translate old payloads translate old
  records, keyed the same way, before `onRestore` sees them.
- **Readable without the application.** A tool with the schema decodes a snapshot file, as `SbeLogPrinter`
  decodes a recording.
- **Canonical encoding is still required.** Spec **A-6** compares bytes, so equal state must encode to
  equal bytes on every instance (§4.3).
- **A record stays within 65,535 bytes**, its `MessageHeader` included.

### 3.2 Constraints

**Snapshots are required cluster-wide.** If any stateful client may recover from `globalSeqNo` 1, the
bound is gone for that client. Today participation is per topology row (`snapshot="true"`) and optional, a
read-only consumer has no snapshot of its own, and a restore that finds no usable file replays the log from
the start ([`snapshot.md`](snapshot.md) §7). Making the bound hold needs cluster snapshots
([`snapshot.md`](snapshot.md) §10, proposed): history before the floor `F`, the cut of the oldest round a
client may still restore, is truncated; a client with no confirmed file at or after `F` restores from a
peer's file, which is portable ([`snapshot.md`](snapshot.md) §4.1), or is fenced; and a stateless consumer
starts at `F`. The topology would state the requirement — for example a `required` attribute on
`<snapshots>`, checked by the loader against every row.

**A snapshot schema version does not change mid-rollout.** Every instance of a source must write the same
bytes for a round (spec **A-6**), or the ones that differ are fenced (**A-7**). A round cut while a
source's instances run different builds can therefore diverge. A source's rollout falls between two rounds
— rounds started by the operator only (`interval="0"`), or a rollout shorter than the interval — and the
new build reads the versions the last round wrote and writes its own from its first round on.

With both, the compatibility window is exact: **the snapshot schema versions of the rounds since `F`, and
the payload versions published since `F`** — about four rounds.

**The cost.** Truncating at `F` gives up a property seqeron has today: every member holds the complete
history from `globalSeqNo` 1, and any consumer can rebuild from it. A deployment that must retain history
beyond `F`, for audit or regulation, takes it from an export of the archive.

## 4. Encoding evolution

Both SBE and Protocol Buffers evolve a schema without breaking its readers, within rules. A change inside
those rules needs nothing from seqeron or the application beyond the new codec; only a change outside them
needs an upcaster (§5).

### 4.1 SBE

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
values to wrap with. A specified snapshot record (§3.1) carries the same header.

### 4.2 Protocol Buffers

Fields are identified by number; a missing field decodes to its default and an unknown one is skipped or
kept.

- **Allowed:** adding fields, removing them (their numbers reserved), some widening type changes, new enum
  values. Both directions work, with no version number at all.
- **Not allowed:** reusing a field number, changing a field's meaning, some changes to `oneof`s.
- **No message version.** An upcaster keys on the `MessageHeader` version, which the application must set
  itself.
- **Allocation.** Decoding parses into objects. This is the application's cost, not the client tier's
  zero-allocation dispatch.

### 4.3 Canonical bytes

Spec **A-6** compares snapshot bytes across instances. SBE's fixed layout is canonical in every language.
Protocol Buffers is deterministic only with deterministic serialization, the same library version and the
same language on every instance, and preserved unknown fields can still make two instances differ.

**Recommendation.** SBE for snapshot records: canonical across languages, zero-copy, and seqeron's own
toolchain, whose IR files `SbeLogPrinter` already discovers. Protocol Buffers is workable for payloads and
weaker for snapshots.

## 5. Upcasters

An **upcaster** handles what the encoding cannot (§4): splitting or merging fields, a change of unit or
scale, a new key, a restructured message. It is a pure function from version N−1 to N, registered by the
consumer, that translates old payloads and old snapshot records (§3.1) before the application sees them,
so the application's logic is written against the newest version only.

Neither encoding removes old fields, so the consumer's current codec reads every older version. An upcaster
decodes with it at the message's acting version and encodes with the current encoder; it is a
transformation over one codec, not a stack of old ones.

### 5.1 Where it runs

In the client tier, in the dispatch path before `onSequenced` and the restore path before `onRestore`: the
façades and `ReplayerStreamReceiver`. Live messages, replayed messages and snapshot records all pass through
there, so all are translated alike.

The Replayer server is the wrong place, for four reasons:

- **It never sees live messages.** Clients read the live tap directly; the Replayer serves history only.
- **It never touches the bytes.** A replay is an Aeron Archive replay to the client. Translating would mean
  reading and republishing every message, on the one thread that serves every client on the node.
- **Translation changes lengths, and positions with them.** Gap resume, snapshot restore at `asOfPosition`
  and `Payload.position()` all depend on recording positions.
- **It couples applications to the cluster.** The Replayer would need every application's schemas, so
  every application release would redeploy the members.

### 5.2 Shape

```java
interface Upcaster {
    /** Writes the message at {@code offset}, MessageHeader first, as the next version into {@code out};
     *  returns its length. */
    int upcast(DirectBuffer in, int offset, int length, MutableDirectBuffer out);
}

Application.builder()   // Gateway.builder() alike
    .upcaster(payloadId, templateId, fromVersion, upcaster)
    …
```

- **Keyed by `(payloadId, templateId, version)`**, from the message's `MessageHeader`. It sees the message
  alone, so one upcaster serves a payload and a snapshot record of the same schema.
- **Chained one step at a time** (N−2 → N−1 → N): each release adds one small step and leaves the older ones
  as they are. A message is translated until no upcaster matches its version.
- **Preserves the delivery.** For a payload, `globalSeqNo`, `sourceId`, `connectionId`, `clusterTimestampNs`,
  `receiveTimeNs` and `position()` are the original message's; the body and `version` are the translation's.
- **Deterministic.** An upcaster is part of the consumer's state machine, so it is a pure function of the
  message, like a reply under spec **A-3**.
- **Allocation-free.** The translation goes into a buffer the façade owns, one copy per translated message.
  The result never goes back on the wire, so it is not bound by the 8,884-byte payload limit.
- **The same in every language**: an interface in Java and C#, a concept in C++, as the listeners are.

### 5.3 Rollout order and rollback

| change | rollout order | rollback past a snapshot round |
| --- | --- | --- |
| within the encoding's rules (§4) | any: older builds read the new version | possible: the older build reads the newer records |
| needs an upcaster | consumers before producers | not possible: nothing turns new into old |

An upcaster turns old into new only, so for such a change **consumers upgrade before producers**, along
every path the payload takes: in request/reply, the gateway must read the application's new reply version
before the application publishes it.

## 6. An application upgrade, end to end

**A change within the encoding's rules** (§4): deploy producers and consumers in any order, each source's
rollout between two rounds. The next round writes the new version; the release can be rolled back until
the old build is retired.

**A change that needs an upcaster:**

1. **Release the reader.** Deploy every consumer of the payload with upcasters from the current versions
   of its payloads and snapshot records to the new ones. Each source's rollout falls between two rounds.
2. **Release the writer.** Deploy the producers publishing the new version. Consumers translate old
   messages still in flight.
3. **Take a round.** `clusterctl request-snapshot` cuts a round in which every instance runs the new build
   and writes the new version.
4. **Retire.** Once `F` reaches that round, no build reads anything older; the old upcasters can go in a
   later release.

## 7. A new source beside the old

§6 upgrades a source in place. This section is the alternative: a new source runs the new protocol version
beside the old one, consumers move to it, and the old source is retired. Below, `O` is the old source,
publishing replies in v1; `N` is the new one, publishing them in v2.

### 7.1 A source and a `payloadId` of its own

- **`N` is a new source**: its own `sourceId` and topology row, one replica per member as `O` has.
- **v2 has its own `payloadId`**, with a `PayloadIdRegistered` row. A consumer that does not know it skips
  it (spec **P-1**), so `O`'s consumers ignore `N` unchanged. Under `O`'s `payloadId` with a higher schema
  version, they would decode it too, since **P-4** checks only the `schemaId`, and act on two replies to
  every request.
- **The cluster does not change.** The sequencer admits any `sourceId` but −1 and any `payloadId` from 2
  (spec §9.2, **C-2**), and loading the topology again with the added rows is safe
  ([`clusterctl.md`](clusterctl.md)). An application row labels its source and turns its snapshots on; it
  admits nothing.

### 7.2 Steps

1. **Add `N`.** It reads the requests `O` reads and publishes v2 replies, so every request has two
   replies. `N` builds its state by replaying from `globalSeqNo` 1, which means decoding every request
   version in the log. Proposed: `N` restores `O`'s newest confirmed snapshot instead, upcasting its records
   (§3.1, §5), and takes part in rounds under its own `sourceId` from then on. A snapshot server
   ([`snapshot.md`](snapshot.md) §11) serves `O`'s snapshot on any host.
2. **Compare.** Before any consumer depends on `N`, a comparator checks `N`'s reply to each request against
   `O`'s, matched by whatever the replies carry to name their request: offline over a member's archive with
   both schemas' IR, as `SbeLogPrinter` reads one, or as a live consumer that publishes nothing.
3. **Switch consumers at a marker.** Every consumer of the replies, gateway or application, moves to v2 at
   a cutover marker in the log, not when its new build is deployed. Recovery replays the log, so a switch
   is reproduced only if the log holds it; one made at deploy time would differ between a run and its replay,
   and between the two instances of a gateway pair.
   - **The new build** acts on v1 replies before the marker and on v2 replies after it. The old build acts
     on v1 throughout.
   - **Until the marker, both builds hold the same state**, so a source's instances upgrade one at a time.
     A gateway pair upgrades its standby, activates it (`clusterctl activate`), then upgrades the other. If
     the new build also changes its snapshot records, §3.2 applies.
   - **Every instance of every consumer runs the new build before the marker is submitted.** An old-build
     instance after it may diverge from its new-build peers and be fenced at the next round (spec **A-7**).
   - **The marker is a message of v2**, submitted by an operator tool. A seqeron system event would change
     schema 210, which spec **V-3** makes a purge.
4. **Retire `O`.** Once the marker is sequenced and every consumer runs the new build, nothing acts on v1
   replies: stop `O`'s replicas and drop its row from the topology document. The row already in the log
   stays, as a label. Requests move to v2 only after this, by the same steps with `N` reading both versions
   meanwhile: `O` would skip a v2 request (**P-1**), and its replies would stop matching `N`'s with no error
   to show it.
5. **Drop v1.** Consumers keep reading v1 replies, and `N` v1 requests, while any recovery can reach the
   history before the marker. A restore with no usable snapshot replays from `globalSeqNo` 1
   ([`snapshot.md`](snapshot.md) §7), so that lasts until cluster snapshots' floor `F` passes the marker
   (§3.2), and without cluster snapshots it lasts indefinitely.

### 7.3 Costs

- **Two sets of replicas** per member while `O` and `N` both run.
- **Both replies in the log.** The log is never truncated, so the second copy stays in its history, and a
  consumer replaying that stretch skips one of them.
- **`N`'s full replay** at its start, until it can restore from `O`'s snapshot.

## 8. Status

| piece | status |
| --- | --- |
| opaque payloads, per-application versioning (spec **V-2**) | implemented |
| snapshot rounds, confirmation, restore, `formatVersion` checked on restore | implemented ([`snapshot.md`](snapshot.md)) |
| operator-only rounds (`interval="0"`, `clusterctl request-snapshot`) | implemented |
| snapshots required cluster-wide | proposed: needs a topology attribute and cluster snapshots |
| cluster snapshots and the floor `F` | proposed ([`snapshot.md`](snapshot.md) §10) |
| snapshot records as specified messages, framing in the spec | proposed (§3.1) |
| upcasters, for payloads and snapshot records | proposed (§5) |
| a new source beside the old: its own `sourceId`, `payloadId` and rows, loaded into a running cluster | implemented (§7.1) |
| a source restoring another source's snapshot | proposed (§7.2 step 1) |
| a reply comparator | proposed (§7.2 step 2) |
| a cutover marker convention | proposed (§7.2 step 3): application-level; a system event for it could join the next schema 210 change, which purges anyway (spec **V-3**) |
| retiring a source | proposed: nothing deregisters one. Its ends stay in each Replayer's index, and a snapshot server ([`snapshot.md`](snapshot.md) §11.4) keeps its last two confirmed rounds, since no newer round supersedes them |
| knowing that nothing reads v1 | proposed: a `consumes` attribute on topology rows, a label like the others, would let a tool check that a retirement is safe |
| compatibility rules for application protocols in the spec | not specified |

## 9. Open questions

1. **Headerless payloads** (spec §13.2) carry no version to key on. Either they are excluded, or the
   upcaster inspects the bytes itself.
2. **An upcaster that fails** cannot throw: as with every callback, the façade records the fault and fences
   from its duty cycle. Whether that is a new `ClusterError` or an existing one is undecided.
3. **Mandating SBE for snapshot records.** Requiring a canonical cross-language encoding in the spec would
   make **A-6** hold for every source (§4.3); leaving it open restricts a source on another format to one
   language and library version.
4. **Read-only consumers** with no state of their own start at `F` under cluster snapshots; one that needs
   history older than `F` reads an export, not the cluster.
5. **Enforcement of the rollout rules.** Rounds between rollouts and reader-before-writer are operational
   rules seqeron cannot check. A build could at least refuse a round while it sees instances of its source
   announcing different builds, which needs a build identity on the log.
6. **One cutover marker per protocol, or one per consumer** (§7.2 step 3). One per protocol makes every
   consumer switch at once; one per consumer lets each move on its own release, at the cost of a marker
   each.
