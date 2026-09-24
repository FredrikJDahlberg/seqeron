# clusterctl

`clusterctl` is the operator tool for a running cluster: it records start and stop markers in the log,
shuts the cluster down, loads the deployment topology, promotes a gateway instance, and reads a node's
counters. It does not launch or restart processes; `start-cluster.sh` or the deployment's own supervisor
does that.

It runs on a cluster node. It uses that node's Aeron directory to reach the co-located tap over
`aeron:ipc`, and that node's cluster directory for Aeron's `io.aeron.cluster.ClusterTool`, which works
only on local files. It is one class, `org.limitless.seqeron.tools.ClusterCtl`, launched by
`seqeron-service/src/main/scripts/clusterctl.sh`. Section references of the form "spec §n" are to
`doc/seqeron-protocol-spec.md`.

## Configuration

The defaults match `SequencerServer`'s, so a default single-node cluster needs none.

| environment variable | meaning | default |
| --- | --- | --- |
| `CLUSTERCTL_MEMBER_ID` | the co-located member's id | 0 |
| `CLUSTERCTL_BASE_DIR` | root of the cluster data directories | `$TMPDIR/seqeron-seq` |
| `CLUSTERCTL_AERON_DIR` | the co-located member's Aeron directory | `$TMPDIR/seqeron-seq-aeron-<id>` |
| `CLUSTERCTL_INGRESS_ENDPOINTS` | member ingress endpoints | `0=localhost:9302` |
| `CLUSTERCTL_EGRESS_HOST` | host the leader replies to; on a follower of a multi-host cluster, this node's own name | `localhost` |
| `SEQERON_JAR` | the uber jar | `build/libs/seqeron-<version>-uber.jar` |

Prerequisite: `./gradlew uberJar`.

## Commands

| command | needs a leader | action |
| --- | --- | --- |
| `start` | yes | record `ClusterStarted` |
| `shutdown` | runs on the leader; a no-op elsewhere | record `ClusterStopped`, then abort the cluster |
| `activate <gatewayId>` | yes | request that a gateway instance be made active |
| `load-topology <file>` | yes | publish the deployment's topology document |
| `counters` | no | list this node's operator counters |
| `help` | no | print usage |
| `snapshot` | — | refused; the cluster takes no snapshots |
| anything else | no | passed to `ClusterTool` (`describe`, `errors`, `list-members`, `recording-log`, …) |

Commands that publish connect to the cluster first over the co-located member's `aeron:ipc` ingress
(500 ms), then over the configured UDP endpoints, and fail if no leader answers. Each marker carries
`sourceId` 2, reserved for `clusterctl` (spec §5), and `connectionId` −1. Each command waits up to 5 s
for its frame to appear on the local tap.

**Exit codes:** 0 on success; 1 if the cluster is unreachable, no leader answers, the frame does not
appear on the tap in time, or the abort fails; 2 for a usage error or an invalid topology file.

### start

Publishes `ClusterStarted` with a fresh `correlationId` and waits for it on the tap, then prints its
`globalSeqNo`. It records that the cluster is up and accepting ingress; it starts nothing. Success
confirms that a leader is elected and ingress works, not merely that the processes are running.

### shutdown

Safe to run on every node, for example from a unit started on all of them: only the leader acts.

1. On a follower (per `ClusterTool.isLeader`), print a message and exit 0. The leader's abort stops this
   node.
2. On the leader, publish `ClusterStopped` and wait for it on the tap.
3. Call `ClusterTool.abort` on the local cluster directory, whether or not step 2 succeeded. If the
   cluster is unhealthy, which is often the reason for stopping it, the marker may not arrive in time;
   the cluster is still stopped through `ABORT`, never by killing processes. A log that ends without
   `ClusterStopped` shows that the marker did not arrive.

`ABORT` sets a termination log position after `ClusterStopped`, and every node stops at exactly that
position, so `ClusterStopped` is the last frame in every node's log. Followers must be stopped by that
abort rather than signalled separately: a follower stopped independently may not yet have applied
`ClusterStopped`. `SIGTERM` to a `SequencerServer` is itself clean (it runs the same shutdown), but it is
not ordered relative to the marker, so a supervisor should not send it to a follower during a
`clusterctl shutdown`.

#### Why `ABORT` and not `SHUTDOWN`

- `SHUTDOWN` takes a snapshot before terminating. The cluster must never snapshot: recovery is always
  full-log replay from `globalSeqNo` 1, which is what keeps every node's tap recording complete
  (`doc/fault-tolerance.md` §0). After a snapshot, the next start would recover from it instead of
  replaying. `ABORT` takes no snapshot.
- Aeron's default `ConsensusModule.Context.terminationHook` does nothing, which would leave
  `SequencerServer` waiting on its shutdown barrier after the consensus module stopped. `SequencerServer`
  sets the hook to `barrier::signalAll`, so on `ABORT` every node closes its archive through the normal
  shutdown path, and `SequencerService.onTerminate` closes the tap publication, giving the recording a
  valid stop position.

After a shutdown, any node's archive holds the complete log (every tap is identical), and
`sbe-log-printer.sh` reads it offline.

### activate

Publishes `GatewayActivationRequested(gatewayId)` and waits for the `GatewayActive` the sequencer
synthesizes in response, matched on `gatewayId`, then prints its `globalSeqNo`. The sequencer validates
the request against the gateway list; for an unlisted `gatewayId` it synthesizes nothing and the command
exits 1 after the timeout. The instance named opens its external connections; its siblings stand by or
step down (spec §7.2).

### load-topology

Publishes the topology document (spec §6.4), an XML file validated against the `topology.xsd` packaged in
the jar. The `<gateways>` section becomes one `GatewayRegistered` per row, with `remaining` counting
down to 0 on the last; the optional `<applications>` and `<protocols>` sections follow as one
`ApplicationRegistered` and one `PayloadIdRegistered` per row. The command waits for the last gateway
row on the tap.

Only the gateway rows affect the cluster: the sequencer builds the gateway list from them and, after
the row with `remaining` = 0, designates the rank-0 instance of each gateway. Application and protocol
rows are labels, read only by `SbeLogPrinter` (spec §6.3).

**Validation.** The whole file is validated before anything is published, because the list cannot be
retracted once published.

- The schema checks field widths, name format (1–32 printable US-ASCII characters), `sourceId` ≥ 0,
  `payloadId` ≥ 2, the required attributes, and uniqueness of gateway `id` and `name`, application `name`
  and `sourceId`, and `payloadId`.
- The loader (`TopologyDocument`) checks what one row cannot express: exactly one `rank="0"` per gateway
  `sourceId`, no reserved `sourceId` (§5 of the spec), and no application `sourceId` that a gateway also
  uses.
- Each row may carry a `description` attribute, for readers of the file; it is not published.

**Parsing.** The schema is loaded from the jar; a `schemaLocation` in the document is ignored, since a
document that chose its own schema could choose a weaker one. DOCTYPE declarations are rejected and
external DTD and schema access is disabled, which blocks external-entity and entity-expansion attacks.
Anyone who can edit the file already has a shell on the node, but the JDK's defaults are unsafe.

**When to run it.** Once per cluster lifetime, after `start` and before any gateway starts: a
`GatewayStarted` that arrives before the list is rejected (spec **S-6**), and a gateway will not publish
until it finds its own row. Running it again is safe: the sequencer de-duplicates rows on `gatewayId`
and designates bootstrap instances only once.

**List only what the deployment runs.** A listed pair that no process starts is designated, times out
after `GATEWAY_ACTIVATION_TIMEOUT_MS` (5 s), passes the role to its standby, and times out again,
adding one `GatewayActive` frame every 5 s for the life of the cluster to a log that recovery replays in
full. The test harnesses load `seqeron-service/src/test/resources/topology-test-gateway.xml`, and the
C++ gateway example loads `seqeron-examples/topology.xml`, each listing only the pair it starts.

### counters

Lists this node's operator counters (`org.limitless.seqeron.protocol.SeqeronCounters`: the
`SequencerService` and `ReplayerService` gauges and counts), read from the Aeron directory's CnC file with
`CountersReader`. It opens no cluster connection, so it works without a leader and on every node.
`doc/ops.md` describes each counter.

### Passthrough

Any other command is passed unchanged to `ClusterTool` against this node's cluster directory. These
commands work only where the cluster directory exists, that is, on a node.

## Limits

- **No authentication.** Access control is shell access to a node. If `shutdown` is ever made remotely
  invocable, the cluster must first get an Aeron `Authenticator`.
- **Leader-only effect.** `shutdown` and the `ClusterTool` control actions take effect only on the
  leader.
- **No resident agent.** Each invocation runs one command and exits.
