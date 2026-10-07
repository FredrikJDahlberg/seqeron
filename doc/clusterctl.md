# clusterctl

`clusterctl` is the operator's tool for a running cluster. Everything an operator does to a seqeron
cluster that the cluster must agree on — that a run started or stopped, which gateway instance serves,
what the deployment contains, when application state is snapshotted — `clusterctl` does by putting a
frame on the log, so the action is ordered with everything else and every replica sees it. It also stops
the cluster cleanly and reads a member's counters. It does not launch or restart processes; that is
`start-cluster.sh`'s job, or the deployment's own supervisor's.

It runs on a cluster member. It reaches that member's tap through the member's Aeron directory, over
`aeron:ipc`, and reads its cluster directory for Aeron's `io.aeron.cluster.ClusterTool`, which works only
on local files. It is one class, `org.limitless.seqeron.tools.ClusterCtl`, launched by
`seqeron-service/src/main/scripts/clusterctl.sh`. "spec §n" refers to `doc/seqeron-protocol-spec.md`.

## Configuration

The defaults match `SequencerServer`'s, so a default single-member cluster needs none. The uber jar
(`./gradlew uberJar`) is the one prerequisite.

| environment variable | meaning | default |
| --- | --- | --- |
| `CLUSTERCTL_MEMBER_ID` | the co-located member's id | 0 |
| `CLUSTERCTL_BASE_DIR` | root of the cluster data directories | `$TMPDIR/seqeron-seq` |
| `CLUSTERCTL_AERON_DIR` | the co-located member's Aeron directory | `$TMPDIR/seqeron-seq-aeron-<id>` |
| `CLUSTERCTL_INGRESS_ENDPOINTS` | the members' ingress endpoints | from `SEQERON_HOSTS`, else `0=localhost:9302` |
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
| `counters` | no | list this member's operator counters |
| `help` | no | print usage |
| `snapshot` | — | refused: the cluster takes no Aeron snapshots (`request-snapshot` is the application kind) |
| anything else | no | passed to `ClusterTool` (`describe`, `errors`, `list-members`, `recording-log`, …) |

A command that publishes works the same way every time:

1. It connects to the cluster, first over the member's own `aeron:ipc` ingress (500 ms), then over the
   configured UDP endpoints, and fails if no leader answers.
2. It publishes its frame with `sourceId` 2, which spec §5 reserves for `clusterctl`, and `connectionId` −1.
3. It waits up to 5 s for the frame, or the frame the sequencer answers it with, to appear on the local
   tap.

It exits 0 on success; 1 if the cluster is unreachable, no leader answers, the frame does not appear in
time, or the abort fails; 2 for a usage error or an invalid topology file.

### start

Publishes `ClusterStarted` with a fresh `correlationId`, waits for it on the tap and prints its
`globalSeqNo`. It starts nothing: it records that the cluster is up and accepting ingress. Its success
shows that a leader is elected and ingress works, which a running process alone does not.

### shutdown

Stops the cluster at a point every member agrees on. It is safe to run on every member, for example from
a unit started on all of them, because only the leader acts:

1. On a follower (by `ClusterTool.isLeader`), it prints a message and exits 0. The leader's abort stops
   this member.
2. On the leader, it publishes `ClusterStopped` and waits for it on the tap.
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
replaying from `globalSeqNo` 1 is what keeps every member's recording complete
([`fault-tolerance.md`](fault-tolerance.md) §0). `ABORT` takes none. Aeron's default termination hook does
nothing, which would leave `SequencerServer` waiting after the consensus module stopped, so
`SequencerServer` sets it to release its shutdown barrier. On `ABORT` every member then closes its archive
through the normal shutdown path, and `SequencerService.onTerminate` closes the tap publication, giving
the recording a valid stop position.

After a shutdown any member's archive holds the complete log, since every tap is identical, and
`sbe-log-printer.sh` reads it offline.

### activate

Makes a named gateway instance the active one, for a planned handover or a pair whose bootstrap is
already spent. It publishes `GatewayActivationRequested(gatewayId)`, waits for the `GatewayActive` the
sequencer synthesizes in answer, matched on `gatewayId`, and prints its `globalSeqNo`. The sequencer checks
the request against the gateway list; for an unlisted `gatewayId` it synthesizes nothing, and the command
exits 1 after the timeout. The instance named opens its external connections, and its siblings stand by
(spec §7.2).

### request-snapshot

Starts an application snapshot round now, rather than at the next interval. It publishes
`SnapshotRequested`, waits for the `SnapshotStarted` the sequencer synthesizes at the next `globalSeqNo`,
and prints the round and that `globalSeqNo`, which is the round's cut. A round still open is superseded.
If the loaded topology has no `<snapshots>` element, nothing answers the request and the command exits 1
after the timeout. What the participating sources then do is [`snapshot.md`](snapshot.md) §4.

### load-topology

Publishes the deployment's topology document (spec §6.4): an XML file validated against the
`topology.xsd` packaged in the jar. It becomes a sequence of frames, and the command waits for the last
gateway row on the tap:

| section | frames | the sequencer |
| --- | --- | --- |
| `<gateways>` | one `GatewayRegistered` per row, `remaining` counting down to 0 on the last | builds the gateway list, and after the last row designates the rank-0 instance of each gateway |
| `<applications>` (optional) | one `ApplicationRegistered` per row | reads nothing; the `snapshot` attribute is the façades' ([`snapshot.md`](snapshot.md) §1) |
| `<protocols>` (optional) | one `PayloadIdRegistered` per row | reads nothing; the rows label payloads for the log printer |
| `<snapshots>` (optional) | one `SnapshotPolicyRegistered` | turns snapshot rounds on (spec §7.3) |

**Validation.** A published list cannot be retracted, so the whole file is validated before anything is
published.

- The schema checks field widths, name format (1–32 printable US-ASCII characters), `sourceId` ≥ 0,
  `payloadId` ≥ 2, the required attributes, and the uniqueness of gateway `id` and `name`, application
  `name` and `sourceId`, and `payloadId`.
- The loader (`TopologyDocument`) checks what one row cannot express: exactly one `rank="0"` per gateway
  `sourceId`; no reserved `sourceId` (spec §5); no application `sourceId` a gateway also uses; the same
  `snapshot` value on every row of one gateway `sourceId`; and at least one row with `snapshot="true"`
  when `<snapshots>` is present.
- A row may carry a `description` attribute for readers of the file; it is not published.

**Parsing.** The schema always comes from the jar: a `schemaLocation` in the document is ignored, since a
document that chose its own schema could choose a weaker one. DOCTYPE declarations are rejected and
external DTD and schema access is disabled, which blocks external-entity and entity-expansion attacks.
Anyone who can edit the file already has a shell on the member, but the JDK's defaults are unsafe.

**When to run it.** Once per cluster lifetime, after `start` and before any gateway starts: a
`GatewayStarted` that arrives before the list is rejected (spec **S-6**), and a gateway publishes nothing
until it finds its own row. Running it again is safe. The sequencer de-duplicates rows on `gatewayId` and
designates bootstrap instances only once.

**List only what the deployment runs.** A listed pair that no process starts is designated, times out
after `GATEWAY_ACTIVATION_TIMEOUT_MS` (5 s), passes the role to its standby, and times out again, adding a
`GatewayActive` frame every 5 s for the life of the cluster to a log that recovery replays in full. Each
document in this repository lists only what its user starts:

| document | loaded by |
| --- | --- |
| `seqeron-service/src/test/resources/topology-test-gateway.xml` | the test harnesses |
| `seqeron-service/src/test/resources/topology-test-snapshot.xml` | `snapshot-test.sh`, adding the `TestApplication` replicas it starts |
| `seqeron-examples/topology.xml` | the C++ gateway example |
| `seqeron-examples/topology-csharp.xml` | the C# gateway example, `csharp-client-test.sh` and `csharp-windows-test.sh` |

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
