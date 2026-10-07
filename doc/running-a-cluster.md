# Running a cluster

What a seqeron deployment runs, how to start one member or three, the ports and properties a member
takes, how it restarts, and the scripts and tools that operate it. [`getting-started.md`](getting-started.md)
is the shortest path to a running member; [`overview.md`](overview.md) explains the design this document
configures.

## What runs where

A deployment has one process per cluster member, the applications beside them, and optionally gateway
hosts that run applications but no member.

| process | runs | does |
| --- | --- | --- |
| `SequencerServer` | once per member | the member: Aeron Cluster's consensus module, the sequencer, an embedded media driver, the archive that records the tap, and the Replayer |
| `ReplayerServer` | once per gateway host | the Replayer alone, with its own media driver and archive, relaying a member's tap onto the host's own |
| an application | beside a member or on a gateway host | the client tier, attached to that host's media driver |
| `clusterctl` | on demand, on a member | starts and stops the cluster, designates gateways, loads the topology, requests snapshot rounds |
| `MetricsExporter` | once per member, optionally | serves the member's counters to Prometheus ([`ops.md`](ops.md)) |

Inside the member, `Sequencer` is the replicated state machine: it stamps each ingress frame with the
next `globalSeqNo` and the Raft consensus timestamp, and synthesizes the frames the cluster owns
(`ClusterHeartbeat`, `LeadershipChanged`, `GatewayActive`, `SnapshotStarted`). It has no Aeron dependency
and is unit-tested directly. `SequencerService` is its Aeron adapter and holds no replicated state of its
own. Every member publishes every sequenced frame on a node-local tap (`aeron:ipc`, stream 205), which its
archive records.

The Replayer serves that recording to the clients on its host, for a cold start or a gap, and is the only
component that reads the archive. It also indexes every source's `SnapshotEnd` by round, which is how a
restarting client learns which of its snapshot files the log confirms. Its decisions are pure classes
(`Replayer`, `ReplaySlotAllocator`, `ReplayClientIdCollisions`, `SnapshotIndex`); `AeronReplayer` is the
only part that touches Aeron. On a gateway host the relay (`TapRelay`, `AeronTapRelay`) copies a member's
tap over UDP and moves to the next member when that one is lost
([`fault-tolerance.md`](fault-tolerance.md#33-gateway-host)).

The client tier runs in the application's own process: Java, the header-only C++ library, or the C#
package ([`client-api.md`](client-api.md)). There is no C++ or C# server; the cluster is Java.

Three more processes serve the end-to-end harnesses ([`building.md`](building.md)):

- **`ClusterProbe`** (in the service jar) drives a cluster without an application: `submit` floods
  `ProbeMarker` payloads at ingress, `ping` round-trips one through consensus and back off the tap, and
  `follow` replays history then follows the tap live.
- **`TestGateway`** and **`TestApplication`** (in `seqeron-service/src/test/java`, and so in no jar) are a
  gateway pair and a co-located application that speak no application protocol but hold the same fences
  and snapshot logic as real ones, so the harnesses exercise them under faults.

## Single member

One member is a complete cluster for development. It needs no configuration beyond its member id:

```bash
./gradlew uberJar

java -Dsequencer.memberId=0 -jar build/libs/seqeron-*-uber.jar
# [SequencerServer] Starting member 0 | ingress=aeron:udp?endpoint=localhost:9302 | archive=aeron:udp?endpoint=localhost:9301 | baseDir=/tmp/seqeron-seq
# [SequencerServer/0] Running — Ctrl-C to stop
```

The member embeds its media driver and archive, so no separate `aeronmd` runs. Its state goes to
`$TMPDIR/seqeron-seq/archive-0` and `$TMPDIR/seqeron-seq/cluster-0`. The jar's manifest carries the
`--add-opens` flags Aeron needs, which `java -jar` honours; a `-cp` launch passes them itself
(`seqeron-home.sh`'s `SEQERON_JAVA_OPTS`).

`start-cluster.sh` starts the same member with a `ClusterProbe` consumer beside it, which is usually what
a developer wants:

```bash
./seqeron-service/src/main/scripts/start-cluster.sh
# [cluster.sh] SequencerServer is running
# [ReplayerService/0] ready — tap recording 0 live and verified from globalSeqNo 1; serving replay
# [ClusterProbe/0] Caught up — following live
```

## Three nodes

A production cluster has three members, one per host. Every process in the deployment learns the members'
hosts from one environment variable, so set it identically everywhere, then start each member with its
own id:

```bash
export SEQERON_HOSTS=host0,host1,host2
java \
  -Dsequencer.memberId=0 \
  -Dsequencer.baseDir=/var/lib/seqeron \
  -jar seqeron-*-uber.jar
```

- **`SEQERON_HOSTS`** lists every member's host in member-id order. A member builds Aeron's
  `clusterMembers` string from it and binds and advertises its own entry. `-Dsequencer.hosts` sets the
  same list for one process.
- **`sequencer.baseDir`** is required once there is more than one member. It holds the Raft log and the
  archive, and the default is under `$TMPDIR`.
- **Every other process reads the same list.** An application's default ingress endpoints
  (`PortLayout.ingressEndpoints()`, C++ `protocol::ingressEndpointsCsv()`) name those members; `clusterctl`
  and `ClusterProbe` connect through them; a gateway host relays from their archives.

`seqeron-service/src/test/scripts/start-three-node-cluster.sh` brings three members up on one machine.

## Ports

Each member takes five ports at `base + memberId × 10 + offset`. The base is 9300 unless
`SEQERON_PORT_BASE` sets another, and the formula lives in `scripts/ports.sh` and the three `PortLayout`
files, nowhere else.

| offset | purpose          | member 0 | member 1 | member 2 |
|--------|------------------|----------|----------|----------|
| +1     | archive control  | 9301     | 9311     | 9321     |
| +2     | ingress          | 9302     | 9312     | 9322     |
| +3     | consensus        | 9303     | 9313     | 9323     |
| +4     | cluster log      | 9304     | 9314     | 9324     |
| +5     | file transfer    | 9305     | 9315     | 9325     |

- **The block is 70 ports wide**, 9300–9369 by default: seven members at a stride of ten. That bounds a
  cluster at seven members.
- **`SEQERON_PORT_BASE` moves the block**, for a host where 9300 is taken. It is deployment-wide: a member
  and a client that see different values bind and dial different ports, and the symptom is a connection
  that never completes. A base below 1024, or one too high for the block to fit, is refused at startup.
- **The other blocks do not move with it.** This tier also binds 9200–9209 for its harness listeners and
  `9400 + memberId` for the metrics plane, and keeping those clear of a moved base is the operator's job.
- **Applications own their ports.** A TCP listener, a client's cluster egress port or a replay port
  belongs to the process that binds it, and this repository names none of them. [`ops.md`](ops.md),
  "Ports", lists every port seqeron binds.

The sequenced stream itself has no port: the tap is `aeron:ipc` on each member's own driver.

## System properties

| property                    | default                          | description                        |
|-----------------------------|----------------------------------|------------------------------------|
| `sequencer.memberId`        | `0`                              | this member's Raft id              |
| `sequencer.hosts`           | `SEQERON_HOSTS`, else `localhost` | every member's host, in id order   |
| `sequencer.host`            | this member's entry in `hosts`, else `localhost` | the host this member binds and advertises |
| `sequencer.baseDir`         | `$TMPDIR/seqeron-seq`; required with more than one host | root of the archive and cluster directories |
| `sequencer.aeronDir`        | `$TMPDIR/seqeron-seq-aeron-<id>`| the media driver's directory       |
| `sequencer.idleStrategy`    | `backoff`                        | `backoff` or `yielding`            |
| `sequencer.sessionTimeoutMs` | `1000`                          | how long the cluster keeps a client session with no keep-alives; a gateway that misses it is replaced by its standby |
| `sequencer.leaderHeartbeatIntervalMs` | `20`                   | how often the leader heartbeats its followers |
| `sequencer.leaderHeartbeatTimeoutMs` | `200`                   | how long a follower waits for a leader heartbeat before it starts an election |
| `sequencer.electionTimeoutMs` | `200`                          | how long an election stage waits on the other members |

The Replayer runs in the member's JVM, on its media driver, and takes no configuration of its own.

## Restart and failover

A member keeps its archive and cluster directories across restarts (`deleteArchiveOnStart=false`,
`deleteDirOnStart=false`), rejoins, and replays the Raft log from `globalSeqNo` 1. The cluster takes no
snapshots, which is what keeps every member's recording a complete copy of history, and for the same
reason `clusterctl shutdown` stops the cluster with `ABORT`, which takes none. To start clean, delete the
`archive-<id>` and `cluster-<id>` directories under `baseDir`, or run `purgelog.sh`.

A leader failover is not a break in the tap. The tap publication is created once and never re-created on
a leadership change, since `aeron:ipc` has no port to collide on, so a member's recording is one
continuous run across every leader tenure. [`fault-tolerance.md`](fault-tolerance.md) covers each failure
and its recovery.

## Scripts

The operator scripts are in `seqeron-service/src/main/scripts/`. They start and stop processes or run
tools, and run no tests. Every one of them sources `ports.sh`, `paths.sh` and `seqeron-home.sh`.

They run from this checkout or from an installed distribution, and work out which from what sits beside
them; `SEQERON_HOME` overrides that, and `SEQERON_JAR` the jar. `./gradlew operatorDist` writes the
distribution to `build/install/seqeron`: `bin/` (the scripts), `lib/` (the uber jar) and `ops/` (the
Prometheus and Grafana provisioning). `./gradlew operatorDistZip` archives it as
`build/distributions/seqeron-<version>.zip`, which every release carries.

| script | purpose |
|--------|---------|
| `start-cluster.sh` | start a single-member cluster and a `ClusterProbe follow` consumer in the background; Ctrl-C stops both. `SEQERON_NO_CONSUMERS=1` leaves out the consumer, for a caller that runs its own |
| `start-gateway-host.sh` | make a host that runs no member able to run clients: `ReplayerServer` in gateway-host mode, relaying a member's tap onto the host's own. `SEQERON_NODE_ID` (3), `SEQERON_ARCHIVE_ENDPOINTS` (the three localhost members), `SEQERON_HOST` (`localhost`) |
| `stop-cluster.sh` | stop everything the start scripts launched, and any `SEQERON_EXTRA_PROCESSES="label\|pattern;…"` a caller adds |
| `clusterctl.sh <command>` | the cluster's life cycle — see [Operating it](#operating-it) |
| `sbe-log-printer.sh <archive-dir>` | print a recording as JSON ([`log-printer.md`](log-printer.md)) |
| `metrics-exporter.sh` | serve a member's counters to Prometheus ([`ops.md`](ops.md)) |
| `purgelog.sh [--force]` | delete the archive and cluster directories under `$TMPDIR/seqeron-seq` and the `logs/` directory; the cluster must be stopped first |

## Operating it

`clusterctl` runs on a member, beside its `SequencerServer`, and acts through that member's cluster
session. [`clusterctl.md`](clusterctl.md) is its runbook.

```bash
./seqeron-service/src/main/scripts/clusterctl.sh counters          # this member's counters; needs no cluster session
./seqeron-service/src/main/scripts/clusterctl.sh start             # sequence a "cluster started" marker; needs an elected leader
./seqeron-service/src/main/scripts/clusterctl.sh shutdown          # orderly stop; safe on every member, a no-op on followers
./seqeron-service/src/main/scripts/clusterctl.sh activate <gatewayId>
./seqeron-service/src/main/scripts/clusterctl.sh load-topology <file.xml>
./seqeron-service/src/main/scripts/clusterctl.sh request-snapshot  # start an application snapshot round now
```

- **`start` and `shutdown`** sequence `ClusterStarted` and `ClusterStopped` markers, so the boundaries of a
  run are themselves in the log.
- **`load-topology`** publishes the deployment document: the gateway list, the co-located applications,
  the payload protocols, and optionally the snapshot policy, validated against the packaged
  `topology.xsd`. Run it once per cluster lifetime, after `start` and before any gateway starts. The
  sequencer acts on the gateway list, designating a first instance of each pair once the last row lands,
  and on the snapshot policy, which turns application snapshot rounds on ([`snapshot.md`](snapshot.md)).
  The application and protocol rows only label recordings for the log printer.
- **Anything else** passes through to Aeron's `ClusterTool` against this member's cluster directory
  (`describe`, `errors`, `list-members`, `recording-log`, …), except `snapshot`, Aeron's cluster snapshot,
  which is refused.

`CLUSTERCTL_*` environment variables map onto the `clusterctl.*` system properties. In a member's
container, `clusterctl` is on the `PATH` and already set to that member: `docker exec node-0 clusterctl
describe`.
