# Fault tolerance and recovery

How the sequencing tier survives node loss, leader failover, a failing local archive and a lost frame,
and how each part returns to a correct state. It covers the cluster nodes, the replayer and the client
tier. Applications built on seqeron document their own recovery. Section references of the form
"spec §n" are to `doc/seqeron-protocol-spec.md`.

## 0. Recovery is full-log replay

Every recovery path here reduces to one operation: replay the sequenced log from `globalSeqNo` 1.
The cluster takes no snapshots. `SequencerService.onTakeSnapshot` throws and `onStart` refuses a
snapshot image. `clusterctl shutdown` uses Aeron's `ABORT` action, which takes no snapshot, rather than
`SHUTDOWN`, which does.

Every node records its own copy of the sequenced stream (§1.1). A node restored from a snapshot would
hold a recording that starts where the snapshot did; full-log replay is what keeps every node's
recording complete, so any node can serve history to its co-located clients without a peer. The cost is
that recovery time and archive size grow with uptime; the 1 Hz heartbeat alone adds about 86,400 frames
a day.

All state downstream of the log (`globalSeqNo`, the gateway list, which instance is active, the open
connections) is a function of the log. None of it has separate persistence or a separate recovery
procedure: a failed component restarts and replays.

## 1. Cluster nodes

### 1.1 Every node records a complete copy

`SequencerService` runs on every node, leader and follower alike, and republishes every sequenced frame
on the node-local tap (`aeron:ipc`, stream 205), which the node's Aeron Archive records. Every node
applies the same committed log in the same order, so the taps are byte-identical (spec **F-2**). There
is no cross-node replication of recordings and no leader-only archive.

The tap publication and its recording are created once, in `onStart`, and survive leadership changes
(`aeron:ipc` has no port to conflict on). A node's recording is therefore one continuous run across
every leader tenure it has seen.

Consequence: any node can serve full history or gap replay to its co-located clients, and losing a node
loses no history.

### 1.2 Leader failover

Aeron Cluster runs the election. On every new term, each node's `SequencerService` has `Sequencer`
synthesize a `LeadershipChanged` frame carrying the new `leadershipTermId`, one per term, including a
term the same member wins again. The frame is sequenced and recorded like any other, so every node's
recording holds a complete history of leadership, and a replica can determine the current leader from
the log alone.

**Producer sessions survive a failover.** The C++ `ClusterStreamSender` handles a `NewLeaderEvent` by
switching only its ingress publication to the new leader's endpoint; the cluster session id and term
are updated in place. `send()` polls the egress stream between offer attempts, so a `NewLeaderEvent` can
arrive and switch the publication while a send is retrying; an offer loop that did not poll would spin
on the dead leader's publication indefinitely. A co-located sender that fell back from IPC to UDP
returns to IPC if leadership returns to its own member. The Java `ClusterStreamSender` gets the same
behaviour from `AeronCluster`, except that a co-located sender whose leader moves away reconnects over
UDP on a new session.

**A failover can lose ingress without any error.** A send returns once the frame is on the leader's
ingress publication; the cluster confirms nothing on egress. Frames the old leader had not committed are
lost, as is everything offered to its publication until the producer learns of the new leader (in
`failover-test.sh`, about 5,500 frames at 100 µs pacing). The session survives, so neither side sees a
fault. §5 describes how a producer detects and resends these frames.

### 1.3 A node that cannot record terminates itself

`TapPublisher.emit` retries the tap offer until it succeeds, because a dropped frame would leave a
permanent gap in the node's recording. The offer can only be back-pressured by the local archive: the
recording is the tap's only tethered subscriber, and client subscriptions are untethered.

Retrying is bounded. `TapPublisher` distinguishes three cases by watching the archive's recording
position:

| archive state | response |
| --- | --- |
| slow: back-pressured, recording position advancing | keep retrying; raise the back-pressure alert after 200 ms |
| stalled: no progress for 1 s | fatal |
| recording gone | fatal immediately |

A stopped recording back-pressures nothing (the untethered client subscriptions keep the publication
connected), so the 1 Hz heartbeat also calls `TapPublisher.checkRecordingAlive`. If the consensus module
refuses the heartbeat timer for 1 s continuously, that is fatal too: otherwise the cluster clock would
stop with no visible signal.

A fatal condition logs `FATAL: … terminating this node`, sets `seqeron_sequencer_tap_stalled`, and exits
with code 70 (`EXIT_TAP_FATAL`); `doc/ops.md` has the operator procedure. It does not throw: a cluster
callback that throws has already had its log position advanced, and `AgentRunner` keeps the agent
running, so the frame would be silently dropped (spec §9.4).

The remaining members hold identical recordings and keep quorum; an election moves leadership if the
failed node led. In a three-member cluster a second failure loses quorum. A restarted node replays the
full log and rebuilds its recording. Start-up has the same bound: the recording must be live within 5 s
(`TAP_RECORDING_START_TIMEOUT_MS`), so a node that exits 70 again immediately still has broken storage.

### 1.4 Shutdown and restart

`clusterctl shutdown` runs on the leader. It publishes a `ClusterStopped` marker, waits (best effort)
for it on the tap, then calls `ClusterTool.abort`. `SequencerServer` connects
`ConsensusModule.Context.terminationHook` to a shutdown barrier, so each node closes its archive and
flushes the recording before exiting. `clusterctl start` publishes `ClusterStarted` and waits for it on
the tap, confirming that a leader is elected and ingress is accepted. `doc/clusterctl.md` has both
procedures.

## 2. Gateways

A gateway is an active/standby pair (spec §5). The client tier's `app/Gateway` (Java and C++)
implements everything seqeron defines for one instance: the list row, designation, `GatewayStarted`,
connection ids, connection lifecycle frames, confirmed ingress and the fences. The application supplies
its external connection handling. `TestGateway` is the reference consumer, and `chaos-runner.sh`
runs a pair of them under fault injection (§6).

### 2.1 Fences

A fence stops an instance that can no longer trust its view of the log. Each is reported once through
`Listener.onFenced(ClusterError, detail)`; the application then releases the cluster session, usually
by exiting, which lets the sequencer promote the standby (§2.2).

| `ClusterError` | condition |
| --- | --- |
| `CLUSTER_SESSION_LOST` | the cluster closed the session, or no new leader arrived within the sender's timeout. A lost session cannot be re-established in process |
| `TAP_STALLED` | no `ClusterHeartbeat` on the co-located tap for 20 s (20 heartbeat intervals) while caught up |
| `RECOVERY_STALLED` | recovery has delivered nothing for 60 s (3 × the tap-stall timeout) on an instance that has been caught up before |
| `INGRESS_CONFIRM_FAULTED` | an own frame on the tap differs from the oldest pending one (spec §16 A-4) |

- The tap-stall timer measures local monotonic time. Consensus time arrives in the `ClusterHeartbeat`
  frames being watched for, so it would stop together with the tap.
- The tap-stall check is suspended while the instance is not caught up, so a replay in progress is not
  reported as a stall. The recovery-stall fence (`RecoveryStallFence`) covers that period instead; it is
  armed only after the first catch-up, because a cold start replays the whole log and has no useful
  bound.
- The recovery-stall timeout (60 s) exceeds the replayer's maximum pending wait of 20 s (spec §10.1), so
  a correctly behaving replayer cannot trip it.

Being superseded is not a fence. When a `GatewayActive` names a sibling, the instance calls
`Listener.onStandby`, closes its external connections, keeps its cluster session and continues following
the tap, so it can be designated again. It publishes nothing on the way out; to the cluster this looks
the same as the process dying.

### 2.2 Promotion

The sequencer decides which instance is active by synthesizing `GatewayActive` naming one `gatewayId`.
Every instance and every replica sees the same frame at the same `globalSeqNo`, so two instances never
act as active on inconsistent information. The triggers (spec §7.2):

- **Bootstrap:** after the last `GatewayRegistered` row, the rank-0 instance of each gateway.
- **Session close:** when the session bound by an active instance's `GatewayStarted` closes (crash,
  network loss or shutdown; Aeron Cluster reports all as a session close). The binding is keyed on
  `GatewayStarted`, not on `sourceId`, because other producers may legitimately carry a gateway's
  `sourceId`.
- **Activation timeout:** a designated instance that publishes no `GatewayStarted` within 5 s is passed
  over.
- **Operator request:** `clusterctl activate <gatewayId>` publishes `GatewayActivationRequested` and
  waits for the resulting `GatewayActive`.

If there is no eligible sibling, the sequencer synthesizes nothing and the gateway has no active
instance until one starts.

On the instance, `app/GatewayLifecycle` tracks the most recent `GatewayActive` for its gateway rather
than latching the first, publishes `GatewayStarted` before calling `onActivated`, and stands down
without publishing when a sibling is named.

### 2.3 Connections across a handover

A promoted instance rebuilds connection state from the log. `onConnectionOpened` and
`onConnectionClosed` are delivered for every connection the logical gateway opened, whichever instance
opened it, including during replay. `connectionData` carries whatever identity the application needs to
map a new connection to an existing session (spec §7.1).

`GatewayStarted` carries `firstConnectionId`, chosen above the highest id seen in replay, so connection
ids do not repeat across a handover. On `GatewayStarted` the sequencer releases every connection still
open under that `gatewaySourceId`, because a crashed instance never publishes their `ConnectionClosed`
frames.

## 3. Stream recovery

A consumer reads the co-located tap directly for live data, untethered, so a slow consumer is dropped
instead of back-pressuring the sequencer (§1.3). It asks the co-located replayer for history on cold
start and after a gap.

### 3.1 `ReplayerService` (one per node)

The replayer is not on the live delivery path; clients use it only to catch up.

- **Integrity check.** Before reporting ready, it replays the first frame of its oldest tap recording
  and checks that `globalSeqNo` is 1. If not, the node's recording is missing or corrupt: `ready` stays
  false for the process lifetime and every request is answered with `ReplayUnavailable`, so clients do
  not each discover the fault separately.
- **Archive errors.** An archive call that throws during a replay moves the service to a stalled state.
  It retries every second and answers `ReplayPending` meanwhile, without affecting live delivery.
- **Slots.** At most `MAX_CONCURRENT_REPLAYS` = 4 replays run at once; a freed slot goes to a waiting
  client immediately. A slot idle for 5 s (`REPLAY_SLOT_TTL_MS`) is reclaimed, covering a client that
  died mid-replay. Many concurrent replays occur mainly when a node restarts and all its clients cold
  start together.
- **Control replies** are offered with a bounded retry and then dropped, never blocked on. The client
  resends on a timer, so a client that is not reading cannot hold up replies to the others.
- **Duty-cycle failure.** An exception escaping the duty cycle clears the ready counter and exits the
  process with code 70 (`EXIT_DUTY_CYCLE_FATAL`) after closing the archive client, so a supervisor
  restarts it rather than leaving a process that looks ready but serves nothing.

### 3.2 Client-side recovery (`ReplayerStreamReceiver`)

`ReplayerStreamReceiver` (Java and C++) is the Aeron adapter; the decisions are in `ReplayerRecovery`,
which is unit-tested directly.

- **Cold start** walks the recording chain from segment 0, one recording per leader tenure, until the
  replayer reports the chain exhausted (spec **R-2**).
- **Gap.** A live frame whose `globalSeqNo` is ahead of the next expected value clears `isCaughtUp()`
  and requests a resume at the position of the last delivered frame, repairing only the gap. If the
  resumed replay's first frame is not the expected `globalSeqNo` (the node restarted and its recording
  changed), the client falls back to a full walk.
- **Retained frames.** While a walk or resume is in progress, live frames beyond the gap are kept in a
  bounded FIFO (65,536 frames or 16 MiB) and delivered when the replay reaches them, so recovery ends
  without a second gap. Past the bound, the client drops them and walks again.
- **`isCaughtUp()` can clear again** on a later gap; it is not latched. The gateway fences (§2.1) and the
  leader gate (§4) depend on this.
- **First frame.** The first frame a client ever delivers must have `globalSeqNo` 1, or the process
  aborts: this node's recording must reach the start of the log.
- **Per-replay subscription.** The replay stream (201) is shared by all clients on the node, and an
  Aeron publication is limited by its slowest tethered subscriber. A client therefore subscribes to it
  only for the duration of a replay, filtered to that replay's session id. A subscription held open
  between replays would never be polled for other clients' sessions and would stop their replays about
  32 MiB in, half of a 64 MiB term.
- **Stall detection.** A bounded replay of a recording still being written does not close its image at
  the bound, so an open, attached, stalled image would otherwise go unnoticed. A replay that makes no
  progress for 5 s (`REPLAY_STALL_TIMEOUT_MS`), or whose image never attaches, is requested again. A
  resume is retried by re-anchoring through `requestResume()`, so the anchor check above still applies.
- **Chain changes.** The replayer resolves the recording chain on every request and may drop a stale
  span, so a segment index can refer to a different recording on retry. `Replaying` carries the
  `recordingId`; if it differs from the one the client saw for that index, the client restarts the walk
  from segment 0 (spec **R-3**).

`ClusterStreamClient` reads an archive's recorded segments directly where no replayer is available: each
historical segment in full, then the last, possibly still-recording, segment without a bound, so one
image delivers history followed by live data.

## 4. Leader-only work

Some side effects must be performed by exactly one replica, the one on the leader, and must survive a
failover. The client tier provides `app/LeaderGate` and `app/OutstandingWork` (Java and C++; spec §16
A-1 to A-3).

- **Whether work is outstanding is replicated.** A request is outstanding from its sequenced request
  until its sequenced reply, so every replica holds the same set.
- **Dispatch is local.** It records that this node is handling a request. It is cleared when the gate
  closes, and when a reply offer fails (the request stays outstanding, since the log decides).
- A newly promoted leader dispatches every outstanding request not yet dispatched. A request is
  discharged only by a sequenced reply, so it is never answered twice by design and never lost; if a
  reply does not reach the log, the request remains outstanding for the next leader.
- **Every `LeadershipChanged` closes the gate**, including one naming the same member. A replica
  applies several frames per duty cycle and can apply a change away and back within one; a reply sent
  during that election may have been lost with the old leader's uncommitted log. `OutstandingWorkPropertyTest`
  covers this case.
- **Dispatch is in insertion order**, which is `globalSeqNo` order. Side effects are externally visible in
  emission order, and a hash map's iteration order differs between replicas.

Delivery is at least once across a failover. A reply that is a pure function of the sequenced request,
keyed on its `globalSeqNo`, makes a re-emission byte-identical, so consumers can drop duplicates by key.

## 5. Ingress across a failover

A producer that must not lose a frame confirms each one on its own tap (spec §16 A-4, A-5).
`sequencer/client/PendingSends` holds every placed frame until the producer's own tap shows it, matched
by cluster session id. After a `LeadershipChanged` with a newer term, any frame stamped with an older
term that has not appeared is lost, and the lost frames are exactly the newest ones stamped with that
term. From the first sign of a new term until those frames are resent, `IngressPublisher` places nothing
new, and the sender abandons a send already retrying through the election (`setIngressHold`). Lost
frames are resent oldest first.

`failover-test.sh` runs two producers across a leader kill: the one using `PendingSends` must see every
frame on the tap exactly once and in order; the other reports what it lost.

## 6. Operational tooling and verification

- **`clusterctl`** (`doc/clusterctl.md`): `start` and `shutdown` bracket a run with sequenced markers;
  `activate` is the manual promotion (§2.2); `snapshot` is refused.
- **Metrics** (`doc/ops.md`): `seqeron_sequencer_tap_stalled` (set when a node is about to terminate,
  §1.3), `seqeron_sequencer_gateway_promotion_total` (§2.2), the `seqeron_replayer_*` family (§3.1), and
  Prometheus's per-node `up`.
- **`chaos-runner.sh`**: randomized fault injection against a three-node cluster with a `TestGateway`
  pair, reproducible from its printed seed. Faults: kill and restart the leader or a follower, `SIGKILL`
  a follower, `SIGSTOP`/`SIGCONT` a node, drop one live tap frame (§3.2), and stop a node's tap recording
  (§1.3, asserting the node exits). After each round it checks that there is one leader, the gateway
  pair still serves, and consumers still deliver in order. At the end it stops every member and checks
  that every node's recording is gap-free, strictly increasing in `globalSeqNo`, and at the same high
  mark on all three nodes (§1.1).
- **`gap-recovery-test.sh`**: a caught-up consumer drops one live frame after a leader failover and must
  resume and keep delivering (§3.2).
- **`replay-bench.sh`**: times a cold `ClusterProbe follow` from launch to caught up against a preloaded
  archive, optionally under load. `replay-bench.sh 400000` builds about 70 MB of history; it must converge
  in well under a second, and `NEVER CAUGHT UP` indicates a replay stall (§3.2), not a slow machine.
- **`replayer-restart-test.sh`**: kills member 0's `ReplayerServer` as it starts serving a cold start,
  then kills member 0's `SequencerServer` and checks that its replayer and client fail fast and that a
  fresh cold start walks a real two-recording chain (§3.2).
- **`failover-test.sh`**: a leader kill with a replay consumer and the confirmed-ingress check of §5.
- **`docker-failover-test.sh`**: `ROUNDS` (default 15) leader kills against `docker/compose.yml` under
  continuous `ProbeMarker` load, restoring each killed member before the next round. It checks that
  every round changes leadership and the killed member rejoins; that an observer on each surviving node
  delivers in `globalSeqNo` order throughout; and that a cold start at the end replays the whole
  multi-tenure history from one recording and reaches live. Needs Docker and `./gradlew operatorDist`;
  CI runs it as `failover.yml`. `ROUNDS=3` is a quick local run.

All scripts are under `seqeron-service/src/test/scripts`.

## 7. Not covered

- **Media driver and network faults.** Killing `aeronmd`, partitions and added latency are not
  exercised: killing the driver also kills co-located C++ clients, and `tc netem`/`dnctl` is not set up
  on the development host.
- **Ingress lost outside one producer process.** `PendingSends` keeps its state in memory and counts
  losses against a leadership change. A frame lost without a leader change (an ingress image that drops
  and rejoins within the session timeout) has no term boundary to be counted against, and a restarted
  producer, or a standby promoted in its place, starts with nothing pending. Covering either needs a
  durable outbox or a per-producer sequence number that the sequencer de-duplicates on.
- **Producer authentication.** The cluster checks well-formedness, not identity (spec §7).
  Authentication belongs at the system's external edges.
- **Single-node clusters** have no failover; the mechanisms above need at least two members.
