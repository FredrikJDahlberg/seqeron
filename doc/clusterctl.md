# clusterctl — cluster life-cycle tool (Option A: node-local operator agent)

A small Java operations tool for opening and (primarily) cleanly closing the sequencer cluster.
Its central purpose is an **orderly shutdown** that leaves the recorded log fully
replayable/analysable afterwards.

## Shape

`clusterctl` is **node-local**: it runs co-located on a `SequencerServer` host, sharing that node's
Aeron directory and `clusterDir`. It combines two mechanisms that the original sketch conflated:

- **In-band marker messages** over cluster ingress — `ClusterStarted` / `ClusterStopped` records
  injected into the ordered log so the transitions are themselves sequenced, timestamped, auditable
  events. Ingress publish routes to the leader like any other client; the sequenced echo appears on
  the node-local tap (`aeron:ipc` stream 205) this tool already sits next to.
- **Out-of-band local control** via `io.aeron.cluster.ClusterTool` — role inspection and the
  cluster-terminating `ABORT` toggle. This is filesystem/counter based and **only works on the
  local node's `clusterDir`**, which is why the tool must be node-local (a remote UDP client could
  not do it). Aeron's `io.aeron.cluster.ClusterControl` toggles apply **only on the leader** (its
  javadoc), so the destructive commands must be run on the leader node.

`clusterctl` does **not** launch or restart cluster processes. Bringing nodes up is still
`start-cluster.sh` / launching `SequencerServer` JVMs. `start` only *records* that the system is up;
`shutdown` records the close marker and then terminates the running nodes.

### Implementation

Java — required, since `ClusterTool`, the Aeron cluster ingress client, and the SBE codecs are all
Java. There is no existing Java cluster-*client* today (the Java side is only the service + nodes),
so the ingress-connect/echo handshake is new Java code modelled on the C++ `ClusterStreamSender` +
tap-follow path, not shared with it.

The tool is one Java class, `org.limitless.seqeron.tools.ClusterCtl` (alongside `SbeLogPrinter`),
launched by `cluster/src/main/scripts/clusterctl.sh` — a thin wrapper matching the other scripts that sets
the classpath / `--add-opens` JVM options and forwards the subcommand and its arguments:

```
clusterctl.sh start             # record a "system started" marker
clusterctl.sh shutdown          # orderly stop
clusterctl.sh activate <id>     # manual standby promotion
clusterctl.sh help
clusterctl.sh describe …        # → ClusterTool passthrough
```

(`ClusterCtl` is named to mirror the script and to avoid shadowing Aeron's own
`io.aeron.cluster.ClusterControl` toggle class that this tool drives via `ClusterTool`. Symlink
`clusterctl.sh` to `clusterctl` on an operator PATH if a bare name is wanted.)

## Commands

```
clusterctl.sh start          record a "system started" marker (precondition: elected leader)
clusterctl.sh shutdown       orderly stop; run on every node, no-op on followers
clusterctl.sh activate <id>  manual standby promotion (precondition: elected leader)
clusterctl.sh load-topology <file>
                             publish the deployment topology — gateways, applications,
                             protocols (precondition: elected leader)
clusterctl.sh counters       list this node's seqeron operator counters; no cluster connection
                             needed, safe on every node
clusterctl.sh help           list commands and exit
clusterctl.sh <other...>     pass through to ClusterTool (describe / errors / list-members / …)
```

### start

Records/signals in the log that the system has started. It does **not** start the cluster.

1. **Precondition — an elected leader must exist.** Read the local node's cluster counters (or
   `ClusterTool` role/mark-file) to confirm the cluster has an elected leader. If none,
   `clusterctl.sh start` **exits non-zero and does nothing** — it never blocks waiting for an election.
2. Publish an **unsequenced `ClusterStarted`** (schema 200) with a `correlationId` via cluster
   ingress.
3. Wait (bounded timeout) for the **sequenced `ClusterStarted`** echo (schema 202) carrying the same
   `correlationId` on the local tap. On echo → print the assigned `globalSeqNo`/`timestamp`, exit 0.
   On timeout → exit non-zero.

Note: completion is the tool's **own `ClusterStarted` echo**, not `LeadershipChanged`.
`LeadershipChanged` (`sbe-sequenced.xml` template 5) is emitted by the sequencer on election,
independently of this marker; the "leader exists" check is the up-front precondition in step 1, and
is what makes `start` exit early when there is no leader.

### shutdown (the primary purpose; the last command issued)

One command for both routine end-of-day and an early stop — an "emergency" stop is just this same
shutdown run earlier than usual, so there is no separate command. It records the close marker and
terminates the cluster via `ClusterTool` `ABORT`, so the recorded log stays replayable/analysable
(`ABORT` + the wired termination hook close every node's archive cleanly — see below). Termination is
**consensus-coordinated**: the leader's single `ABORT` sets a common termination log position — taken
after `ClusterStopped` — and every node terminates at exactly that position, so `ClusterStopped` is
the last event in every node's log. Followers are brought down *by that `ABORT`*, not by an
independent local kill (which could stop a node before it applied `ClusterStopped` and break that
guarantee).

Safe to invoke on **every** node (e.g. a systemd unit fired cluster-wide); leader detection routes
the real work to the one leader, so the operator need not know which node leads.

1. **Leader gate.** Check role via `ClusterTool`/cluster counters. On a **follower**: **no-op, exit
   0** — publish nothing, terminate nothing; the leader's `ABORT` brings this node down.
2. On the **leader**: publish an **unsequenced `ClusterStopped`** (schema 200, `correlationId`) via
   ingress; wait (bounded timeout) for the **sequenced `ClusterStopped`** echo on the local tap.
3. **Durability barrier.** Confirm the local archive's tap recording position has advanced past the
   `ClusterStopped` frame (`RecordingPos`) — the marker is on disk, not just in flight.
4. **Terminate** via `ClusterTool` `ABORT` on the local (leader) `clusterDir`. Best-effort about the
   marker: if the echo (2) or barrier (3) does not clear within the timeout — an unhealthy cluster,
   which is often *why* you are stopping early — it aborts anyway, so the cluster still comes down
   cleanly (via `ABORT`, never `SIGKILL`, so the log is preserved). A log that ends *without*
   `ClusterStopped` is then the signal that the marker never landed before the abort.
5. **Verify** (the success criterion is a *readable log*, not "process exited"): after exit, run
   `SbeLogPrinter` against the archive dir and assert the tap recording (stream 205) has a valid
   stopPosition and dumps. If not, fall back to the alternatives below.

**Deployment note (systemd).** Let the leader's `ABORT` stop the follower `SequencerServer`s (they
exit on the coordinated termination); do not have systemd independently `SIGTERM` a follower's
`SequencerServer` in a way that races the `ABORT`, or a node could terminate before applying
`ClusterStopped`. (`SIGTERM` is itself clean now — it drives the same barrier teardown — it just
isn't ordered against the marker.)

### activate

Manual standby promotion. Publishes an **unsequenced `GatewayActive(gatewayId)`** (schema 200, no
`correlationId` field — matched on `gatewayId` instead) via cluster ingress, then waits (bounded
timeout) for its **sequenced** echo on the local tap, printing the assigned `globalSeqNo` on success.
No leader gate in `clusterctl` itself: publish routes to the leader like any other ingress message,
same as `start`; with no elected leader the ingress connect times out and the command exits non-zero,
same failure shape as `start`.

`GatewayActive` needs no sequencer-side special-casing to support this second publisher —
`Sequencer.sequenceMessage` copies it through like any other message (only `GatewayRegistered` is
special-cased, to derive topology and — at `remaining == 0` — trigger the bootstrap activation). Every
gateway instance reacts identically regardless of which of the two publishers (the sequencer's own
bootstrap/promotion, or this command) sent it: the instance whose `gatewayId`/`gatewaySourceId`
matches opens its accept gate, the others stay standby (or close, if previously active).

### load-topology

Publishes the **topology document** — `src/main/resources/topology.xml`, the XML of
`doc/seqeron-protocol-spec.md` §6.4, validated against the packaged `topology.xsd`. Its `<gateways>`
section becomes one unsequenced `GatewayRegistered` per row, `remaining` counting down to 0 on the
last; its optional `<applications>` and `<protocols>` sections become one `ApplicationRegistered` and
one `PayloadIdRegistered` per row behind them, with no countdown of their own. Between them the three
sections put the whole deployment in the log — the producers the cluster elects, the producers it does
not (§5), and what the shared payloadIds are called. Then waits for the last list row's sequenced echo
on the local tap.
Same connect/publish/await-echo shape as `activate`, and the same failure mode with no elected leader.

The application and protocol rows are **labelling and nothing else** (§6.3, §6.4): the sequencer never
decodes them,
registration gates no frame (**C-2**), and their one reader is `SbeLogPrinter`, which names a payload
it cannot decode instead of printing bare numbers. `payloadId` 1 is core and unregistrable
(**C-1**) — enforced by the XSD, which is where a constraint on what may be *written* belongs.

The list is a **deployment assertion**, the same kind of act as `activate` — which is why it is
here rather than riding along in `BasicDataServer`'s reference-data load. It changes when you deploy;
the comp-id table and the trading-day calendar change daily. Keeping it
out of the load is also what lets the cluster tier decode no reference data at all: the `remaining ==
0` row is the sequencer's completeness edge, and it synthesizes one bootstrap `GatewayActive` per
logical gateway behind it.

The whole document is validated before a byte is published, because a file that fails half-way
leaves a list the log has already closed. Nearly all of it is declarative in `topology.xsd` — field
widths, name shape, `sourceId >= 0`, `payloadId >= 2`, and unique `gatewayId`/`gatewayName`/`payloadId`
as `xs:unique` — because the log cannot make those checks for itself: a duplicate id silently drops an
instance. What is left in the loader is the two checks a row cannot make about itself: exactly one
`rank="0"` per `sourceId` (a missing one leaves a logical gateway no primary, a second an arbitrary
one) and a `sourceId` that is none of §5's reserved ids. These used to be `static_assert`s over the
hardcoded table in `BasicDataConstants.hpp`.

The parser resolves the XSD from its own jar and ignores a `schemaLocation` the document names: a file
that may name its own schema may name a lax one, and **C-1** would then be advisory. Entity resolution
is disabled outright — the threat is mild, since an operator who can edit the file already has a shell
on the node, but the JDK's defaults are unsafe.

**Run it before the reference-data load**, once per cluster lifetime: `start` → `load-topology` →
the reference-data load. A load that gets in first produces session rows whose `ownerSourceId` no
list row claims, and every gateway drops them on ingest — fail closed, but a cluster that serves
nobody. Re-running is safe: the sequencer de-dups rows on `gatewayId` and latches the bootstrap once.

**List only what the deployment runs.** A listed pair that no process starts is designated, times
out after `GATEWAY_ACTIVATION_TIMEOUT_MS`, hands the role to its standby, and times out again — one
`GatewayActive` frame every 5s for as long as the cluster is up, in a log recovery replays in full.
`topology.xml` is the full three-pair deployment; the harnesses load the pair each one actually starts
(`src/test/resources/topology-{gw,egw,ogw}.xml`), which is why a `load-topology` argument is
a file rather than a constant.

### counters

Lists this node's seqeron operator counters — the `SequencerService`/`ReplayerService` gauges and
event counts (`org.limitless.seqeron.SeqeronCounters`) — read directly off the co-located Aeron
directory's CnC file via `CountersReader`. No cluster connection (unlike `start`/`shutdown`), so it
works with no elected leader and is safe to run on every node.

### help / passthrough

`help` lists the commands and exits. Any other argument vector is forwarded verbatim to
`ClusterTool` (`describe`, `errors`, `list-members`, `recording-log`, …). These are node-local,
connection-less diagnostics — they work only where a `clusterDir` is present, i.e. on a node.

## Why `ABORT`, and the node fix that makes it safe

Two facts about Aeron 1.51.0 shutdown, both verified against the cluster sources, drive the design:

- **Use `ABORT`, not `SHUTDOWN`.** `SHUTDOWN` takes a **snapshot** before terminating
  (`ConsensusModuleAgent.java:2519`). This repo's invariant is *no snapshots — recovery is always
  full-log replay from gseq 1*, which is what makes the tap recording provably complete. A
  shutdown-snapshot would silently break that (next boot recovers from the snapshot instead of
  replaying). `ABORT` (`:2544`) terminates with no snapshot and coordinates all nodes at one log
  position — the right fit.

- **Termination hook wired in `SequencerServer` (done).** Aeron's default
  `ConsensusModule.Context.terminationHook` is a **no-op** `() -> {}` (`ConsensusModule.java:2089`);
  the documented pattern is `.terminationHook(barrier::signalAll)` (`:268`). `SequencerServer`
  previously used a `ShutdownSignalBarrier` but did **not** wire the hook, so a `ClusterTool`
  `ABORT`/`SHUTDOWN` terminated the consensus + service agents but never signalled the barrier —
  `SequencerServer.main` stayed blocked on `barrier.await()`, the `ClusteredMediaDriver`/`Archive`
  were **never closed through the clean try-with-resources path**, and the half-dead node
  (archive up, consensus dead) had to be `kill`ed, risking an unflushed catalog and an unreadable
  log. `SequencerServer` now wires `terminationHook(barrier::signalAll)`, so `ABORT` unwinds cleanly
  on every node → `Archive.close()` forces the catalog → `SbeLogPrinter` reads the tap. This is the
  change that makes `clusterctl` shutdown safe for log analysis.

Why the log is otherwise fine: `SequencerService.onTerminate` already closes `tapPub`, setting the
tap recording's stopPosition cleanly, and that callback fires on both `ABORT` and `SHUTDOWN`
(`ClusteredServiceAgent.java:1205`). `SbeLogPrinter` reads the archive catalog + segments **offline
from disk**, so "log analysable after shutdown" reduces to "was the archive closed cleanly" — which
the termination-hook fix guarantees.

## Alternatives if `ClusterTool` shutdown can't guarantee a readable log

If, even with the two changes above, a clean archive close cannot be relied on (e.g. `Archive.close`
does not force the catalog, or a node was mid-recording):

- **Alt A (strongest fallback) — local `SIGTERM`** to the `SequencerServer` PID instead of Aeron's
  `ClusterControl` toggle. That path is already clean today: it drives the same `ShutdownSignalBarrier` →
  try-with-resources → `Archive.close()` teardown that `stop-cluster.sh` relies on, and needs no
  termination-hook fix and no snapshot decision. Trade-off: not a consensus-coordinated quiesce, so
  correctness rests on the step-3 durability barrier (which already guarantees `ClusterStopped` is on
  disk on every node before any node is signalled).
- **Alt B — make "log is dumpable" the success criterion** (step 5): after exit, `SbeLogPrinter`
  the archive dir; if the tap recording has no valid stopPosition or fails to dump, escalate to
  Alt A and re-verify.
- **Alt C — belt-and-suspenders:** explicit `AeronArchive.stopRecording(tap)` before terminating,
  on top of `onTerminate`.

Post-shutdown analysis reads any single node's archive dir (every node's tap is byte-identical), via
`SbeLogPrinter` / `sbe-log-printer.sh` with the sequenced SBE IR.

## Schema work (do both toolchains, in lockstep)

Add two messages to **both** `sbe-unsequenced.xml` (200) and `sbe-sequenced.xml` (202), kept
byte-identical past the shared `header` composite (same rule the existing messages follow), and
regenerate on the Java (`generateUnsequencedSbe`/`generateSequencedSbe`) and C++ (`Generate…SbeCodecs`)
sides:

- `ClusterStarted` — header + `correlationId` (and nothing else; empty business body).
- `ClusterStopped` — header + `correlationId`.

The sequencer needs no new logic to stamp these: `onSessionMessage` already copies any message type
through by template id.

## Topology file format — XML, and what the parser costs

_Proposal, 2026-08-29. The original design chose flat CSV over JDK XML and named the condition
for revisiting it: "worth it only if a row grows attributes." Two things have since met it, so this
prices the move._

> **Landed** as `doc/seqeron-protocol-spec.md` §15 step 8 — `topology.xml`, `topology.xsd`, the three
> harness lists and the four script paths, in one commit. **With one divergence: the shape built is
> §6.4's flat `<gateway name id sourceId rank/>`, not the nested `<primary>`/`<standby>` this section
> recommends.** So the three rules "The nesting is the whole argument" turns into structure are not
> structure: `preferenceRank` is still a field, the rank-0-per-`sourceId` scan is still in the loader
> (with the reserved-`sourceId` check beside it), and an instance can still be written under the wrong
> `sourceId`. What did land as costed is everything else — the XSD, the validating DOM parse with the
> XXE hardening below, the merge of both sections into one file and one verb, and `load-protocols`
> never built. Read the rest of this section as the pricing it was, not as a description of the file.

**What changed.** The protocol registry (`doc/seqeron-protocol-spec.md` §6.3) adds a **second record kind**
an operator asserts into the log — `PayloadIdRegistered` — and it has the identical lifecycle to the
list: it changes when you deploy. Left in its own file it needs its own `load-protocols` verb and
its own place in the runbook. §3.6's own criterion for
bundling two things into one loader is lifecycle match, and these match exactly. Meanwhile the list
itself wants a `description` per logical gateway, which today lives in a `#` comment that no parser
sees and nothing keeps honest.

### The shape

One file, two sections, `gatewaySourceId` as a container rather than a repeated column:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<topology xmlns="http://limitless.org/seqeron/topology/1" name="seqeron-dev">

  <protocols>
    <protocol name="simdfixgw" version="1" payloadId="2"
              description="C++/simdfix edge — FIX admin + application messages"/>
    <protocol name="phixeron" version="1" payloadId="3"
              description="Java FIX edges — pre-encoded FIX session bytes"/>
  </protocols>

  <gateways>
    <gateway sourceId="0" description="client-facing, C++ FixGateway">
      <primary name="GW-A"  id="1"/>
      <standby name="GW-B"  id="2"/>
    </gateway>
    <gateway sourceId="5" description="exchange-facing, Java ExchangeGateway">
      <primary name="EGW-A" id="3"/>
      <standby name="EGW-B" id="4"/>
    </gateway>
    <gateway sourceId="6" description="client-facing, Java OrderGateway">
      <primary name="OGW-A" id="5"/>
      <standby name="OGW-B" id="6"/>
    </gateway>
  </gateways>

</topology>
```

**The nesting is the whole argument, not the syntax.** Three of today's five hand-written validation
rules stop being checks and become structure:

| rule today | under this shape |
| --- | --- |
| exactly one rank-0 row per `gatewaySourceId` | element cardinality — one `<primary>`, zero-or-more `<standby>`, in the content model |
| `preferenceRank` is 0..N with no duplicates | **the field is gone.** Rank is `<standby>` document order |
| an instance carrying the wrong `gatewaySourceId` | **unrepresentable** — the instance sits inside its gateway |
| unique `gatewayId` / `gatewayName` | `<xs:unique>` in the XSD, declarative |
| a protocol registering `payloadId` 1 | `<xs:minInclusive value="2"/>` — and it is the **only** enforcement of C-1 there is, since the registry does not gate and no wire check can be made in |

That leaves the parser with no hand-written cross-row validation at all.

A conservative variant keeps `<instance name= id= rank=/>` and a flat `rank` attribute. It is a
smaller diff, buys only the container property, and keeps the rank-0 scan. Not worth the work — if
the flat variant is what gets built, stay on CSV.

#### What each attribute is for, and which ones reach the log

**The rule: an attribute is published only if something reads it.** A file that carries fields the
log never sees is fine; a log that carries fields nothing consumes is the coupling this whole design
is avoiding.

| attribute | purpose | on the wire? |
| --- | --- | --- |
| `topology/@name` | names the deployment this file describes, so a list cannot be read as generic. **Documentation only for now** — a load-time interlock ("refuse a topology whose name is not this cluster's") needs a cluster-side identity, which does not exist yet (`doc/seqeron-protocol-spec.md` §15) | no |
| `protocol/@name` | the protocol's identity, and what `SbeLogPrinter` labels a payload with | **yes** — `PayloadIdRegistered.protocolName` |
| `protocol/@version` | the operator's record of which revision this deployment runs, printed beside the name. **Nothing checks it** — the interlock this section used to specify was rejected, below | **yes** — `PayloadIdRegistered.protocolVersion` |
| `protocol/@payloadId` | the numeric the frame carries (§6.1 of the protocol spec) | **yes** |
| `protocol/@description` | free text for the operator reading the file | no |
| `gateway/@sourceId` | `header.sourceId` of the logical gateway | yes, per instance |
| `gateway/@description` | free text; replaces the `#` comment nothing kept honest | no |
| `primary/@name`, `standby/@name` | the launch-time join key (`SEQERON_*_GATEWAY_NAME`) | yes |
| `primary/@id`, `standby/@id` | instance identity, what a `GatewayActive` names | yes |

**`@version` carries no interlock, and that is a reversal.** This section specified one until
2026-08-30 — each application comparing the asserted version against its own compiled-in constant and
refusing to start on a mismatch — on the ground that it was a direct attack on **V-1**, the silent
disagreement between three repos about a shared format. `doc/seqeron-protocol-spec.md` §6.3 retired it,
and the reason is that it never touched V-1: V-1 is two builds producing different frame bytes from
skewed Aeron / Agrona / SBE **while both declare the same number**, which a number-against-number
comparison passes cleanly. What such a check catches instead is deploy hygiene — the build that
shipped is not the one declared — and it catches it by stopping every application in the fleet on one
wrong character in a hand-edited file. That is the same inverted error profile that retired the
registry's ingress gate: the check polices the one input it depends on being right. The V-1 defence
is mechanical and involves no operator — the conformance suite asserting the frame layout §4 states
(§14 row 6 of the protocol spec) and the published library pins (§11).

`@version` still reaches the log, because an operator's record of what a deployment runs is worth
printing beside the protocol's name. It is read by `SbeLogPrinter` and by nobody else.

**`@payloadId` stays in the file, and that is a decision worth naming.** The alternative is to
register by name alone and have the sequencer allocate the number from the log — deterministic, and
consistent with "identity is reference data, not config" the way `gatewayId` already is. It is
rejected here because an application stamps `payloadId` on **every outbound frame**, so learning it from
the log would mean no gateway can publish until it has replayed its own registration, adding a
start-up ordering dependency to the hot path to remove a five-line table from a file an operator
already edits. `gatewayId` can be learned because nothing is published before activation; a
`payloadId` cannot.

### The XSD

`cluster/src/main/resources/topology.xsd`, ~70 lines, sketched:

```xml
<xs:element name="gateway">
  <xs:complexType>
    <xs:sequence>
      <xs:element name="primary" type="Instance"/>
      <xs:element name="standby" type="Instance" minOccurs="0" maxOccurs="unbounded"/>
    </xs:sequence>
    <xs:attribute name="sourceId"    type="xs:int"  use="required"/>
    <xs:attribute name="description" type="xs:string"/>
  </xs:complexType>
</xs:element>
```

with `Instance` carrying `name` as an `xs:string` restricted to `maxLength="32"` (matching the SBE
`gatewayName` `char[32]`, which is otherwise a limit discovered at publish time) and `id` as
`xs:int`, and document-level `<xs:unique>` over instance `@name`, instance `@id`, protocol
`@payloadId` and protocol `@name`.

**One JDK limit to know before designing against it:** the validator shipped in the JDK is Xerces at
**XSD 1.0**, so `xs:assert` and conditional type assignment are unavailable. The shape above needs
neither — that is not a coincidence, it is why the primary/standby split is the recommended variant
rather than a `rank` attribute with a co-occurrence constraint.

### Parser design

**DOM (`DocumentBuilderFactory`), not StAX, SAX or JAXB.** The document is ~30 elements read once at
tool start-up, so streaming buys nothing and costs a handler state machine; JAXB left the JDK at 11
and would be the new dependency §3.6 refused.

Validate against the XSD during the parse (`factory.setSchema(...)`) with an `ErrorHandler` that
rethrows, so a schema violation and a malformed document arrive on the same path and carry
`SAXParseException`'s line and column. Then walk the DOM into the existing `TopologyRow` records —
`publishListAndAwaitEcho` and the `remaining` countdown are untouched, because the wire is untouched.

**The one non-obvious cost is XXE hardening, and it is mandatory rather than optional:**

```java
final DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);  // also kills billion-laughs
f.setXIncludeAware(false);
f.setExpandEntityReferences(false);
f.setNamespaceAware(true);
schemaFactory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
schemaFactory.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
```

The threat here is mild — an operator who can edit `topology.xml` already has a shell on the node,
which is this tool's whole access-control model — but the defaults are unsafe, the incantation has to
be right, and it is a permanent obligation with no analogue in `Files.readAllLines`. Price it as
eight lines that a reviewer must recognise, not as eight lines.

### Cost

| | CSV today | XML |
| --- | --- | --- |
| parse | `readTopology`, **23 lines** | DOM walk over two sections, **~55 lines** |
| validation | `validateTopology`, **35 lines** | **~5** (`payloadId != 1`); the rest is the XSD |
| hardening | none needed | **~8 lines**, mandatory, easy to get wrong |
| declarative artifact | none | `topology.xsd`, **~70 lines** |
| dependencies | none | none — `javax.xml` is JDK |
| files to convert | 4 (`topology.csv` + 3 test lists) | 4, plus 4 path strings in scripts |
| operator errors caught | at load, by line number | in the editor, by XSD completion, before the cluster is touched |
| verbs | `load-topology` + a new `load-protocols` | `load-topology` alone, publishing both sections |

**Net: not a line saving** — 58 lines of Java become ~68 Java plus a 70-line schema. What it buys is
that the 35 imperative lines become declarative and the error classes above become unrepresentable,
and it is honest to call that a trade rather than a win.

**What it does not cost, which is the part that makes it cheap:**

- **No wire change**, no SBE edit, no archive purge. This is `clusterctl`-local and trivially
  reversible — nearly unique among the decisions in this design space.
- **No C++ parser.** Nothing in C++ reads these files; `Gateways.hpp` consumes the *frames*. The
  format is a one-language concern, which is exactly what §3.6's "three-way mirror" warnings are not
  about.
- **No shell change beyond the path.** No script parses the file — `grep` finds only `clusterctl.sh
  load-topology <path>` call sites in four scripts, so the conversion is an extension rename.
- **One fewer runbook step.** A separate protocols file needs its own command, run ahead of
  `load-topology`; merging the sections into one file and one command is what lets
  `doc/seqeron-protocol-spec.md` §6.3 cost no new step at all, leaving today's `start` → `load-topology` →
  reference-data load. The tool publishes protocols before gateways
  regardless of document order, and the XSD's `xs:sequence` fixes document order to match so the file
  reads the way it publishes.

### Recommendation

**Do it, and only as a bundle with the protocol registry.** The XML pays for itself through the
merge — one artifact, one operator action, one fewer ordering rule — and through making the
sourceId-mismatch and duplicate-rank classes unrepresentable. Split the two apart and the honest
answer is §3.6's original one: a second flat CSV for protocols, and CSV stays.

Land it in one commit: XSD, parser, all four files converted, the four script paths, and
`load-protocols` never built. There is no reason to accept both formats — the argument for a
compatibility window is a live deployment, and this file is read once by a tool an operator runs by
hand.

## Non-goals / open items

- Does not launch or restart cluster processes.
- Destructive commands (`shutdown`, `ABORT` passthrough) run only on the leader node; no
  authentication is added — access control is "you have a shell on the node." If `shutdown` is ever
  exposed to remote invocation, it must gain an Aeron `Authenticator` first.
- No resident per-node daemon. `shutdown` may be fired on every node (leader does the work,
  followers no-op); `start` is a single leader-side invocation. Cross-node termination is
  consensus-coordinated by `ABORT`, not per-node kills.
