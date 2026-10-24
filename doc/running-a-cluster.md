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
| `SequencerServer` | once per member | the member: Aeron Cluster's consensus module, the sequencer, an embedded media driver, the archive that records the queue, and the Replayer |
| `ReplayerServer` | once per gateway host | the Replayer alone, with its own media driver and archive, relaying a member's queue onto the host's own |
| an application | beside a member or on a gateway host | the client tier, attached to that host's media driver |
| `clusterctl` | on demand, on a member | starts and stops the cluster, designates gateways, loads the topology, requests snapshot rounds |
| `MetricsExporter` | once per member, optionally | serves the member's counters to Prometheus ([`ops.md`](ops.md)) |

Inside the member, `Sequencer` is the replicated state machine: it stamps each ingress frame with the
next `clusterSeqNo` and the Raft consensus timestamp, and synthesizes the frames the cluster owns
(`ClusterHeartbeat`, `LeadershipChanged`, `GatewayActive`, `SnapshotStarted`, `SessionClosed`). It has no Aeron dependency
and is unit-tested directly. `SequencerService` is its Aeron adapter and holds no replicated state of its
own. Every member publishes every sequenced frame on the node-local queue of each group it reaches (`aeron:ipc`,
stream 2000 + `groupId`), which its archive records. The queues open when the first `load-topology` activates
the groups, and the cluster sequences nothing before it.

The Replayer serves that recording to the clients on its host, for a cold start, a gap or the check of a
snapshot's cut, and is the only component that reads the archive. Its decisions are pure classes
(`Replayer`, `ReplaySlotAllocator`); `AeronReplayer` is the only part that touches Aeron. On a gateway host the relay (`QueueRelay`, `AeronQueueRelay`) copies a member's
queues over UDP, those of the groups in `replayer.groups`, and moves to the next member when that one is lost
([`fault-tolerance.md`](fault-tolerance.md#33-gateway-host)).
The host's Replayer also purges those recordings as rounds are confirmed, and `replayer.snapshotJoinMs` (default
10000) is how long after a cut an instance on the host has to join its round ([`ops.md`](ops.md), "Cluster
snapshots"). The two instances of a gateway never run on one host.

The client tier runs in the application's own process: Java, the header-only C++ library, or the C#
package ([`client-api.md`](client-api.md)). There is no C++ or C# server; the cluster is Java.

Three more processes serve the end-to-end harnesses ([`building.md`](building.md)):

- **`ClusterProbe`** (in the service jar) drives a cluster without an application: `submit` floods
  `ProbeMarker` payloads at ingress, `ping` round-trips one through consensus and back off the queue, and
  `follow` replays history then follows the queue live.
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
# [ReplayerService/0] ready — queue recording 0 live and verified from clusterSeqNo 1; serving replay
# [ClusterProbe/0] Caught up — following live
```

## Three nodes

A production cluster has three members, one per host. Every process in the deployment learns the members'
hosts from one environment variable, so set it identically everywhere, then start each member with its
own id:

```bash
export SEQERON_HOSTS=host0,host1,host2
java -Xlog:gc \
  -Dsequencer.memberId=0 \
  -Dsequencer.baseDir=/var/lib/seqeron \
  -jar seqeron-*-uber.jar
```

- **`SEQERON_HOSTS`** lists every member's host in member-id order. A member builds Aeron's
  `clusterMembers` string from it and binds and advertises its own entry. `-Dsequencer.hosts` sets the
  same list for one process.
- **`sequencer.baseDir`** is required once there is more than one member. It holds the Raft log and the
  archive, and the default is under `$TMPDIR`. Put it on a disk, never on tmpfs such as `/dev/shm` where
  the Aeron directory belongs: neither would survive a restart of the host, and a member on Linux warns
  at start.
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

The sequenced stream itself has no port: the queue is `aeron:ipc` on each member's own driver.

## System properties

| property                    | default                          | description                        |
|-----------------------------|----------------------------------|------------------------------------|
| `sequencer.memberId`        | `0`                              | this member's Raft id              |
| `sequencer.hosts`           | `SEQERON_HOSTS`, else `localhost` | every member's host, in id order   |
| `sequencer.host`            | this member's entry in `hosts`, else `localhost` | the host this member binds and advertises |
| `sequencer.baseDir`         | `$TMPDIR/seqeron-seq`; required with more than one host | root of the archive and cluster directories; on a disk, never tmpfs, or the member warns at start |
| `sequencer.aeronDir`        | `/dev/shm/seqeron-seq-aeron-<id>`, or under `$TMPDIR` on a host without `/dev/shm` | the media driver's directory; every client sharing the driver defaults to the same (`PortLayout.aeronDir`). On Linux keep it on tmpfs, or the member warns at start ([`ops.md`](ops.md), "Term lengths") |
| `sequencer.idleStrategy`    | `backoff`                        | `backoff` or `yielding`, for every agent thread; on a host short of cores, share the driver's threads too ([`ops.md`](ops.md), "Driver threads") |
| `sequencer.sessionTimeoutMs` | `5000`                          | how long the cluster keeps a client session with no keep-alives; a gateway that misses it is replaced by its standby. Keep it well above the leader heartbeat timeout ([`ops.md`](ops.md), "Consensus timeouts") |
| `sequencer.leaderHeartbeatIntervalMs` | `200`                  | how often the leader heartbeats its followers |
| `sequencer.leaderHeartbeatTimeoutMs` | `2000`                  | how long a follower waits for a leader heartbeat before it starts an election ([`ops.md`](ops.md), "Consensus timeouts") |
| `sequencer.electionTimeoutMs` | `1000`                         | how long an election stage waits on the other members |
| `aeron.cluster.max.sessions` | `64`, rather than Aeron's 10   | the most client sessions the cluster holds at once ([`ops.md`](ops.md), "Cluster sessions") |

The Replayer runs in the member's JVM, on its media driver, and takes no configuration of its own.

## Restart and failover

A member keeps its archive and cluster directories across restarts (`deleteArchiveOnStart=false`,
`deleteDirOnStart=false`), rejoins, and replays the Raft log from `clusterSeqNo` 1, or from the newest cluster
snapshot its `restart-safe.log` names when a `<snapshots>` policy is loaded (`snapshot.md` §10). Without a policy
every member's recording is a complete copy of history, and `clusterctl shutdown` stops the cluster with `ABORT`,
which takes no snapshot. To start clean, delete the
`archive-<id>` and `cluster-<id>` directories under `baseDir`, or run `purgelog.sh`.

The media driver's directory is recreated at every start, but only once no live driver holds it, and the
previous driver's errors are first saved beside it, as `<aeronDir>-<timestamp>-error.log`, so they survive the
restart. A member whose driver closed, as it does on an orderly fatal exit, restarts at once. One killed outright
restarts once its driver's heartbeat is older than `aeron.driver.timeout` (10 s), the wait its archive and cluster
mark files already impose.

A leader failover is not a break in a queue. Each queue publication is created once, at activation, and never
re-created on a leadership change, since `aeron:ipc` has no port to collide on, so a member's recording of it is
one continuous run across every leader tenure. [`fault-tolerance.md`](fault-tolerance.md) covers each failure
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
| `start-cluster.sh` | start a single-member cluster and a `ClusterProbe follow` consumer in the background; Ctrl-C stops both. `SEQERON_TOPOLOGY` names a topology document to load once the member runs; without one the consumer waits for a `load-topology`. `SEQERON_NO_CONSUMERS=1` leaves out the consumer, for a caller that runs its own |
| `start-gateway-host.sh` | make a host that runs no member able to run clients: `ReplayerServer` in gateway-host mode, relaying a member's queues onto the host's own. `SEQERON_GROUPS` (required: the `groupId`s whose queues it relays, comma-separated), `SEQERON_NODE_ID` (3), `SEQERON_ARCHIVE_ENDPOINTS` (the three localhost members), `SEQERON_HOST` (`localhost`) |
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
- **`load-topology`** publishes the deployment document: the groups, the gateway list, the co-located
  applications, the payload protocols, and optionally the snapshot policy, validated against the packaged
  `topology.xsd`. Run it first, before `start` and before any gateway starts: the cluster refuses everything
  before its groups are active. The first load activates the groups, each with its own queue, and fixes them
  for the life of the archive; a later one may add rows. The sequencer acts on the gateway list, designating a
  first instance of each pair once the last row lands, on each row's group, which the instances of that source
  read, and on the snapshot policy, which turns application snapshot rounds on ([`snapshot.md`](snapshot.md)).
  The protocol rows only label recordings for the log printer.
- **Anything else** passes through to Aeron's `ClusterTool` against this member's cluster directory
  (`describe`, `errors`, `list-members`, `recording-log`, …), except `snapshot`, Aeron's cluster snapshot,
  which is refused.

`CLUSTERCTL_*` environment variables map onto the `clusterctl.*` system properties. In a member's
container, `clusterctl` is on the `PATH` and already set to that member: `docker exec node-0 clusterctl
describe`.
