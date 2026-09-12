# Shared registries

Three namespaces are shared by every process in the deployment and owned by neither product: the
producer `sourceId` space, the UDP port space, and the Aeron counter type-id space. Inside one
repository all three are held by convention. Now that seqeron is extracted, convention has become a
cross-repo race — nothing stops two repositories claiming the same number, and the collision shows
up as a start-up failure at best and a mis-routed election at worst. So all three are written down
here, and **core owns the registries**: an allocation is taken by editing this file.

Both have drifted once already, and the two drifts are the shape to expect.

- `FixGateway` and `BasicDataServer` co-located egress both defaulted to `9340 + memberId` — two
  claims on one port, inside a single repo, found only when they were run on the same node.
- The cluster reservation is **9300–9329**; a dozen comments and one test restated it as
  "9300–9325", which is not the reservation but what a *three-node* cluster happens to bind.
  `fix_test_server`'s cluster egress sat on 9320 — inside the reservation, outside the
  restatement — so the check that should have caught it was looking at the wrong number.

The second is why core now states its own reservation in code (§2) rather than in prose: a
restated boundary is a copy, and a copy is what drifts.

## 1. `sourceId`

**Normative in `seqeron-protocol-spec.md` §5** ("`sourceId` is one id space"), and that table *is*
the registry — this document does not restate it, because two copies of an allocation table is the
failure mode the registry exists to prevent. One id space over both kinds of producer, §5's elected
gateways and co-located applications alike; −1 and 2 are reserved by the specification and every
other value is allocated in that table.

The registry is held by the **log**, not by code. No product compiles an id in: a gateway resolves
its own `{gatewayId, gatewaySourceId}` from the `GatewayRegistered` row keyed on its launch-time
name (`SEQERON_*_GATEWAY_NAME`), and fails closed when the list names none. The allocation reaches
a running deployment through the `sourceId` attributes of the topology file
(`src/main/resources/topology.xml`, spec §6.4) — the registry's one machine-readable form, and the
only place a number is typed.

What a wrong allocation costs: two logical gateways sharing a `gatewaySourceId` share one election.
`GatewayActive` designates both, and `releaseStaleConnections` drops the other pair's connections.

`payloadId` is a third shared space with the same property, and it is likewise the specification's:
§6.1. It is not repeated here for the same reason §5 is not.

## 2. Ports

**Core reserves the blocks; each product declares its own bases inside its own block, in its own
language.** Core states only its own reservation in code — `CLUSTER_PORT_BLOCK_FIRST` /
`CLUSTER_PORT_BLOCK_LAST` and `isClusterPort()`, in `PortLayout.hpp` and `SequencerServer` — because
a reusable sequencer must not name the processes that connect to it. Which product owns which of the
other blocks is this table's alone.

| block | owner | what is in it |
| --- | --- | --- |
| 9200–9209 | core | the cluster tier's own harness listeners: `TestGateway` TCP listen `9200 + instance` (9200 GW-T-A, 9201 GW-T-B), `cluster/src/main/scripts/ports.sh`, and `examples/cpp`'s cluster egress `9202 + memberId` (UDP, `SEQERON_EXAMPLE_EGRESS_PORT`). Deliberately **not** inside 9300–9329 — that block is three members of stride 10 with nothing spare, and `isClusterPort()` names cluster member ports, which these are not |
| 9300–9329 | core | cluster member ports, `9300 + memberId*10 + {1..5}` — three members, one decade each |
| 9330–9359 | simdfixgw | `OrderExecServer` egress `9330+m`, `FixGateway` egress `9340+m`, `BasicDataServer` egress `9350+m`. 9348 and 9349 were the cluster-tier harnesses' own test-consumer egress and are now free: those harnesses run `ClusterProbe follow`, which opens no cluster session (§11 step 5) |
| 9360–9399 | phixeron | `ExchangeGateway` egress `9360+m` and its archive control `9370+m`, `OrderGateway` egress `9380+m` and its archive control `9390+m` |
| 9000–9029 | products | TCP listen: `FixGateway` `9000+gatewayIndex`, the mock venue 9010, `OrderGateway` 9020 |
| 9400+ | **shared** | see below |
| 8010–8019 | products | a product FIX-engine spike's own archive control — 8011, 8013 |

Two independent Aeron media drivers on one host cannot bind the same UDP port, which is why every
co-located process needs a base of its own rather than sharing one.

### 9400+ is the one block with no single owner

It holds both core's observability HTTP and the products' Aeron replay and test ports, and they
overlap:

| port | owner | transport |
| --- | --- | --- |
| `9400 + memberId` | core — `metrics-exporter.sh` `/metrics` | TCP |
| 9500 | core — `metrics-aggregator.sh` | TCP |
| 9400 | products — `fix_test_server` risk-test replay (`SEQERON_RISK_TEST_REPLAY_PORT`) | UDP |
| 9401 | products — `FixGateway` resend-recovery replay (`SEQERON_RESEND_REPLAY_PORT`) | UDP |
| 9403 | products — `fix_test_server` cluster egress | UDP |

9400 and 9401 are claimed twice and coexist only because a TCP listener and a UDP endpoint on one
number do not collide. That is not a property to lean on across two repositories, and **this block
wants splitting before the extraction** — core's exporters into a range of their own, the replay
ports into the products'. Until then the rule is the narrow one: a new UDP port here must avoid
`9400 + memberId` for the deployment's member count, which is why the egress above is 9403 and not
9402.

### The cluster block bounds the cluster at three members

`9300 + memberId*10` gives member 2 the decade 9320–9329, so the reservation is exactly three
members wide. **A fourth member would take 9330–9339, which is simdfixgw's.** Growing the cluster
is therefore a registry change here first, not a `nodeCount` change — the formula alone will hand
out a port another product owns, and the failure is a bind error on whichever process starts second.

### The formula is a three-way mirror

`PortLayout.hpp` (C++), `SequencerServer` (Java) and `cluster/src/main/scripts/ports.sh` (bash) each
carry it, pinned against the same `(memberId → port)` pairs by `PortLayoutTest` and
`SequencerServerTest` so a change to one side without the others fails a build. All three, and both
tests, are core's and go with it.

The satellite bases are the products' own, in the products' own files: `AppPorts.hpp` (pinned by
`AppPortsTest`) and `ports.sh` on the C++ side, `ExchangeGatewayConfig` and `OrderGatewayConfig` on
the Java side (pinned by `GatewayPortsTest`). Each of those tests asserts its bases fall **outside**
core's reservation by calling core's own predicate — which is the whole point of exporting one.

## 3. Aeron counter type ids

Aeron reserves 0–999 for itself (client/driver 0–99, archive 100–199, cluster 200–299); everything
above is a deployment's own. The blocks below are typed in two files, `SeqeronCounters.java` and its
C++ half `util/SeqeronCounters.hpp`, and read by one — `MetricsExporter` maps a counter to a metric
**by type id**.

| block | owner | what is in it |
| --- | --- | --- |
| 5000–5099 | core | `SequencerService`'s counters |
| 5100–5199 | core | `ReplayerService`'s counters |
| 5200 | core | `APP_RECOVERY_STALLED`, published by `ReplayerStreamReceiver` — core's class, running inside a consumer's replica, in both languages |
| 5201–5299 | consumers | a co-located replica's own counters |

**5200–5299 is the app range**: counters a co-located replica publishes rather than a cluster-tier
process. They carry `{memberId, clientId}` in the key where the others carry `memberId` alone, because
a node runs several replicas publishing the same type id and the memberId alone would render them as
one Prometheus series — the same label set, silently overwritten.

Core reserves 5200 and states no owner for the rest: **which consumer holds which sub-block is the
deployment's registry, not core's** — a deployment running two consumers keeps that table in whichever
repository owns the deployment (in this one's case, `phixeron`'s copy of this document).

### Core exports its own counters by name and a consumer's by label

`MetricsExporter` holds a curated name, help text and type for every counter in **core's** blocks. It
cannot hold one for a consumer's — that is the same "core names its consumers" coupling the port
registry exists to avoid — so an app-range counter it has no metadata for is exported anyway, under a
name taken from **the first token of the counter's own label**, sanitised to a Prometheus name
(`MetricsExporterTest` pins both halves). A consumer therefore labels its counters
`<product>.<area>.<metric> member=… client=…`, exactly as core's own `seqeron.app.recoveryStalled`
does, and adding one needs no edit to seqeron.

Nothing outside core's two blocks and the app range is exported at all: a type id core has never
reserved is another library's.

What a wrong allocation costs: two consumers on one type id render as one metric with two meanings,
and the exporter has no way to tell them apart — the label they are named from is the only thing that
differs, and the first one scraped names the series.
