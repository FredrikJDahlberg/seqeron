# clusterctl

`clusterctl` is the operator's tool for a running cluster. Everything an operator does to a seqeron
cluster that the cluster must agree on — that a run started or stopped, which gateway instance serves,
what the deployment contains, when application state is snapshotted — `clusterctl` does by putting a
frame on the log, so the action is ordered with everything else and every replica sees it. It also stops
the cluster cleanly and reads a member's counters. It does not launch or restart processes; that is
`start-cluster.sh`'s job, or the deployment's own supervisor's.

It runs on a cluster member. It reaches that member's default queue (spec §3.1), which the member's replayer
names when asked (spec **R-6**), through the member's Aeron directory, over `aeron:ipc`, and reads its
cluster directory for Aeron's `io.aeron.cluster.ClusterTool`, which works only on local files. It is one class, `org.limitless.seqeron.tools.ClusterCtl`, launched by
`seqeron-service/src/main/scripts/clusterctl.sh`. "spec §n" refers to `doc/seqeron-protocol-spec.md`.

## Configuration

The defaults match `SequencerServer`'s, so a default single-member cluster needs none. The uber jar
(`./gradlew uberJar`) is the one prerequisite.

| environment variable | meaning | default |
| --- | --- | --- |
| `CLUSTERCTL_MEMBER_ID` | the co-located member's id | 0 |
| `CLUSTERCTL_BASE_DIR` | root of the cluster data directories | `$TMPDIR/seqeron-seq` |
| `CLUSTERCTL_AERON_DIR` | the co-located member's Aeron directory | `/dev/shm/seqeron-seq-aeron-<id>`, or under `$TMPDIR` on a host without `/dev/shm` |
| `CLUSTERCTL_INGRESS_ENDPOINTS` | the members' ingress endpoints | from `SEQERON_HOSTS`, else three members on `localhost` |
| `CLUSTERCTL_EGRESS_HOST` | the host the leader replies to; on a follower of a multi-host cluster, this member's own name | this member's entry in `SEQERON_HOSTS`, else `localhost` |
| `SEQERON_JAR` | the uber jar | `build/libs/seqeron-<version>-uber.jar` |

## Commands

| command | needs a leader | action |
| --- | --- | --- |
| `start` | yes | sequence `ClusterStarted` |
| `shutdown` | acts on the leader; a no-op elsewhere | sequence `ClusterStopped`, then stop the cluster |
| `activate <gatewayId>` | yes | ask the cluster to make a gateway instance active |
| `request-snapshot` | yes | start an application snapshot round |
| `load-topology <file>` | yes | publish the deployment's topology document |
| `trim-log [--dry-run]` | no | purge this member's Raft log and snapshot recordings below its two newest restart-safe snapshots |
| `counters` | no | list this member's operator counters |
| `help` | no | print usage |
| `snapshot` | — | refused: the cluster takes no Aeron snapshots (`request-snapshot` is the application kind) |
| anything else | no | passed to `ClusterTool` (`describe`, `errors`, `list-members`, `recording-log`, …) |

A command that publishes works the same way every time:

1. It connects to the cluster, first over the member's own `aeron:ipc` ingress (500 ms), then over the
   configured UDP endpoints, and fails if no leader answers.
2. It publishes its frame with `sourceId` 2, which spec §5 reserves for `clusterctl`, and `connectionId` −1.
3. It waits up to 5 s for the frame, or the frame the sequencer answers it with, to appear on the default
   queue.

Until a topology is loaded, the sequencer refuses every frame but the `<groups>` section's (spec §9.2
condition 13), and the replayer names no default queue, so every publishing command but `load-topology`
exits 1 at once, `start` included.

It exits 0 on success; 1 if the cluster (for `trim-log`, the member's archive) is unreachable, no leader answers, no replayer names the default
queue, a topology file conflicts with the log, the frame does not appear in time, or the abort fails; 2 for a
usage error or an invalid topology file.

### start

Publishes `ClusterStarted` with a fresh `correlationId`, waits for it on the queue and prints its
`clusterSeqNo`. It starts nothing: it records that the cluster is up and accepting ingress. Its success
shows that a leader is elected and ingress works, which a running process alone does not.

### shutdown

Stops the cluster at a point every member agrees on. It is safe to run on every member, for example from
a unit started on all of them, because only the leader acts:

1. On a follower (by `ClusterTool.isLeader`), it prints a message and exits 0. The leader's abort stops
   this member.
2. On the leader, it publishes `ClusterStopped` and waits for it on the queue.
3. It calls `ClusterTool.abort` on the local cluster directory, whether or not step 2 succeeded. A cluster
   that is being stopped is often unhealthy, and the marker may not arrive in time; the cluster is still
   stopped through the abort, never by killing processes. A log that ends without `ClusterStopped` shows
   the marker did not arrive.

The abort sets a termination position just after `ClusterStopped`, and every member stops exactly there,
so `ClusterStopped` is the last frame in every member's log. Followers must be stopped by that abort, not
signalled separately: a follower stopped on its own may not yet have applied `ClusterStopped`. `SIGTERM`
to a `SequencerServer` is itself clean, since it runs the same shutdown, but it is not ordered against the
marker, so a supervisor should not send it to a follower during a `clusterctl shutdown`.

**Why `ABORT` and not `SHUTDOWN`.** Aeron's `SHUTDOWN` takes a cluster snapshot before stopping, and the
next start would recover from that snapshot instead of replaying the log. The cluster never snapshots:
replaying from `clusterSeqNo` 1 is what keeps every member's recording complete
([`fault-tolerance.md`](fault-tolerance.md) §0). `ABORT` takes none. Aeron's default termination hook does
nothing, which would leave `SequencerServer` waiting after the consensus module stopped, so
`SequencerServer` sets it to release its shutdown barrier. On `ABORT` every member then closes its archive
through the normal shutdown path, and `SequencerService.onTerminate` closes the queue publications, giving
each recording a valid stop position.

After a shutdown every member's archive holds the same queues (spec **F-2**), one per group, and
`sbe-log-printer.sh` reads them offline, one queue at a time.

### activate

Makes a named gateway instance the active one, for a planned handover or a pair whose bootstrap is
already spent. It publishes `GatewayActivationRequested(gatewayId)`, waits for the `GatewayActive` the
sequencer synthesizes in answer, matched on `gatewayId`, and prints its `clusterSeqNo`. The sequencer checks
the request against the gateway list; for an unlisted `gatewayId` it synthesizes nothing, and the command
exits 1 after the timeout. The instance named opens its external connections, and its siblings stand by
(spec §7.2).

### request-snapshot

Starts a snapshot round now, rather than at the next interval. It publishes `SnapshotRequested`, which makes a round
due; the leader's service then asks the cluster for a snapshot, and the command waits for the `SnapshotStarted` that
snapshot publishes and prints the round and its `clusterSeqNo`, which is the round's cut. It is a cluster snapshot as
well as an application round (`snapshot.md` §10).
If the loaded topology has no `<snapshots>` element, nothing answers the request and the command exits 1
after the timeout. What the participating sources then do is [`snapshot.md`](snapshot.md) §4.

### load-topology

Publishes the deployment's topology document (spec §6.4): an XML file validated against the
`topology.xsd` packaged in the jar. It becomes a sequence of frames, in this order:

| section | frames | the sequencer |
| --- | --- | --- |
| `<groups>` | one `GroupsRegistered` | on the first load, activates the groups, each with its own queue (spec §7.4); on a later one, must equal them. Either way it makes the command's session the topology session (spec **S-9**) |
| `<gateways>` (optional) | one `GatewayRegistered` per row, `remaining` counting down to 0 on the last | builds the gateway list, and after the last row designates the rank-0 instance of each gateway not designated before |
| `<applications>` (optional) | one `ApplicationRegistered` per row | records the source's group; the `snapshot` attribute is the façades' ([`snapshot.md`](snapshot.md) §1) |
| `<protocols>` (optional) | one `PayloadIdRegistered` per row | reads nothing; the rows label payloads for the log printer |
| `<snapshots>` (optional) | one `SnapshotPolicyRegistered` | turns snapshot rounds on, by `interval` seconds and `bytes` sequenced (spec §7.3) |

**Validation.** A published list cannot be retracted, so the whole file is validated before anything is
published.

- The schema checks field widths, name format (1–32 printable US-ASCII characters), `sourceId` ≥ 0,
  `payloadId` ≥ 2, group `id` 1–16, the required attributes, at least one group, that every gateway and
  application names a declared group, and the uniqueness of group `id` and `name`, gateway `id` and
  `name`, application `name` and `sourceId`, and `payloadId`.
- The loader (`TopologyDocument`) checks what one row cannot express: exactly one `rank="0"` per gateway
  `sourceId`; no reserved `sourceId` (spec §5); no application `sourceId` a gateway also uses; the same
  `snapshot` and `group` on every row of one gateway `sourceId`; and `snapshot="true"` on every row when
  `<snapshots>` is present, of which there is at least one.
- A row may carry a `description` attribute for readers of the file; it is not published.

**Checks against the log.** The schema cannot see what is already loaded, and a row the sequencer refuses
would leave the load partial, so the command checks the file against the default queue before it publishes
a row (spec §6.4):

1. On a cluster whose groups are active, it compares the file's `<groups>` with the active table, and
   publishes nothing if they differ (spec **S-8**).
2. It publishes the `GroupsRegistered` and stops unless that frame appears: the sequencer refuses it while
   another `load-topology` holds the topology session, or when its groups differ from the active ones.
3. It reads the default queue up to its `GroupsRegistered` and stops if a source in the file was registered
   with another group: a source's group never changes (spec **S-8**). The topology session keeps any other
   load from registering rows until the command closes its session.
4. It publishes the rows, waits for every frame to appear on the default queue, in order, reports the first
   that does not as refused, and closes its session.

A command that dies holds the topology session until the cluster times its session out; a second
`load-topology` before then stops at step 2.

**Parsing.** The schema always comes from the jar: a `schemaLocation` in the document is ignored, since a
document that chose its own schema could choose a weaker one. DOCTYPE declarations are rejected and
external DTD and schema access is disabled, which blocks external-entity and entity-expansion attacks.
Anyone who can edit the file already has a shell on the member, but the JDK's defaults are unsafe.

**When to run it.** First, before `start` and before any gateway starts: the cluster refuses every other
command until its groups are active, a `GatewayStarted` that arrives before the list is rejected (spec
**S-6**), and a gateway publishes nothing until it finds its own row. Running it again is safe, and is how
a deployment grows. A later file may add gateways, applications and protocols; its `<groups>` must equal
the active ones, and it may not move a source to another group (spec **S-8**). Adding groups to a running
cluster is proposed (spec §7.4). The sequencer de-duplicates rows on
`gatewayId` and designates a gateway's bootstrap instance only once, so a file loaded again changes
nothing. The first load holds up every member's queues for as long as its archive takes to start
recording them, at most 5 s (spec §7.4).

**List only what the deployment runs.** A listed pair that no process starts is designated, times out
after `GATEWAY_ACTIVATION_TIMEOUT_MS` (5 s), passes the role to its standby, and times out again, adding a
`GatewayActive` frame every 5 s for the life of the cluster to a log that recovery replays in full. Each
document in this repository lists only what its user starts:

| document | loaded by |
| --- | --- |
| `seqeron-service/src/test/resources/topology-test-gateway.xml` | the test harnesses |
| `seqeron-service/src/test/resources/topology-test-snapshot.xml` | `snapshot-test.sh`, adding the `TestApplication` replicas it starts |
| `seqeron-examples/topology.xml` | the C++ gateway example and `cpp-client-test.sh` |
| `seqeron-examples/topology-csharp.xml` | the C# gateway example, `csharp-client-test.sh` and `csharp-windows-test.sh` |

### trim-log

Purges what this member's two newest restart-safe snapshots make unneeded ([`snapshot.md`](snapshot.md) §10.7). Open
source Aeron trims nothing, so a member's Raft log and snapshot recordings otherwise grow with uptime, which matters
for a deployment that runs for weeks and not for one that resets daily. It acts on this
member alone, needs no leader, and is run on each member. It reads the member's `recording.log` and
`restart-safe.log` from its cluster directory and purges through the member's archive, over `aeron:ipc`:

1. It keeps the two newest snapshots that `restart-safe.log` names and the log from the older one. With fewer than two
   it does nothing and exits 0.
2. It purges every snapshot recording older than the two, the consensus module's and the service's.
3. It purges each log recording's segments below the older snapshot's log position, at the segment boundary at or below it.

It is safe to repeat: a recording already gone counts as done. The archive refuses a log purge while a replay reads
below the position or while another delete runs; the command prints the refusal, leaves the log as it was and exits 1,
and the next run trims it. A member that was down past the trim horizon may need log its leader has purged and is
rebuilt from `ClusterBackup` (`snapshot.md` §10.7).

`trim-log --dry-run` prints what a run would purge and purges nothing, one line per recording, each already-purged
recording left out: `would purge snapshot recording <id>` and `would purge log recording <id> below position <p>`. It
exits 0. Purging cannot be undone, so an operator who wants the history keeps a copy of the archive directory first.

### counters

Lists this member's operator counters (`org.limitless.seqeron.protocol.SeqeronCounters`: the
`SequencerService` and `ReplayerService` gauges and counts), read from the Aeron directory's CnC file with
`CountersReader`. It opens no cluster session, so it works without a leader and on every member.
[`ops.md`](ops.md) describes each counter.

### Passthrough

Any other command goes unchanged to `ClusterTool` against this member's cluster directory, and so works
only where that directory exists: on a member.

## Limits

- **No authentication.** `shutdown` and the `ClusterTool` passthrough act on a member's cluster directory,
  so shell access to a member guards them. What the other commands submit are ordinary ingress frames,
  which any process that reaches a member's ingress port can submit too. Restricting them needs an Aeron
  `Authenticator`.
- **Leader-only effect.** `shutdown` and `ClusterTool`'s control actions take effect only on the leader.
- **No resident agent.** Each invocation runs one command and exits.
