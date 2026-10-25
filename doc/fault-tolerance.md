# Fault tolerance and recovery

How seqeron survives the loss of a member, a leader failover, a failing local archive, a lost frame and the
failure of a gateway, and how each part returns to a correct state. It covers the cluster members, the
Replayer and the client tier; an application built on seqeron documents its own recovery.
[`overview.md`](overview.md) summarizes the failure model, and "spec §n" refers to
[`seqeron-protocol-spec.md`](seqeron-protocol-spec.md).

The model is crash faults, not Byzantine ones. A process may stop, restart, stall or lose its network; it
does not lie. The cluster checks that frames are well formed, not who sent them.

## 0. Recovery is full-log replay

Every recovery path in this document reduces to one operation: replay the sequenced log from `clusterSeqNo`
1. A client whose source takes part in application snapshots starts that replay later, at the cut of its
newest snapshot the log confirms (§3.4); the snapshot is a file of its own, and the log holds its digest.

The cluster takes snapshots only while a `<snapshots>` policy is loaded; without one `SequencerService.onTakeSnapshot`
throws, and `clusterctl shutdown` stops the cluster with Aeron's `ABORT`, which takes no snapshot, rather than
`SHUTDOWN`, which does. Without a policy every member records the whole sequenced stream (§1.1), so any member can
serve history to its clients without a peer, and the cost is that recovery time and archive size grow with the history the archive holds,
which suits a deployment that resets it between runs. The 1 Hz heartbeat alone adds about 86,400 frames a day.

With a policy, a member restarts from the newest cluster snapshot that its `restart-safe.log` names, and its queue
recording starts at that snapshot's cut rather than at `groupSeqNo` 1. What lies below the cut is in the
applications' own snapshot files, so a consumer that cannot restore from one (a bare `ReplayerStreamReceiver` has no
way to write it) cannot follow a restarted member. A cut is named only when every co-located application that joined
its round has confirmed its file, so a member that dies between a cut and that mark restarts from the snapshot before
and replays more log, never less. Recovery then replays at most the bytes since the snapshot before the newest safe
one, about twice the policy's `bytes`.

All state downstream of the log — `clusterSeqNo`, the gateway list, which instance is active, the open
connections — is a function of the log. None of it has separate persistence or a separate recovery
procedure: a failed component restarts and replays.

## 1. Cluster nodes

### 1.1 Every node records a complete copy

`SequencerService` runs on every member, leader and follower alike, and republishes every sequenced frame on
the member's queue of each group the frame reaches (`aeron:ipc`, stream 2000 + `groupId`), which the member's
Aeron Archive records. Every member applies the same committed log in the same order, so each queue is
byte-identical across members (spec **F-2**). There is no cross-member replication of recordings and no
leader-only archive.

The queue publications and their recordings are created once, when the first topology load activates the
groups (spec §7.4), and survive leadership changes, since `aeron:ipc` has no port to conflict on. A member's
recording of a queue is therefore one continuous run across every leader tenure it has seen.

So any member can serve full history, or a gap, to its own clients, and losing a member loses no history.

### 1.2 Leader failover

Aeron Cluster runs the election. On every new term, each member's sequencer synthesizes a
`LeadershipChanged` frame carrying the new `leadershipTermId` — one per term, including a term the same
member wins again. The frame is sequenced and recorded like any other, so every recording holds a complete
history of leadership, and a client can tell the current leader from the log alone.

**Producer sessions survive a failover**, in every language:

- The C++ `ClusterStreamSender` handles a `NewLeaderEvent` by switching only its ingress publication to the
  new leader's endpoint, updating the cluster session id and term in place. `send()` polls egress between
  offer attempts, so the event can arrive, and switch the publication, while a send is retrying; an offer
  loop that did not poll would spin on the dead leader's publication indefinitely. A co-located sender that
  fell back from IPC to UDP returns to IPC if leadership returns to its own member.
- The Java and C# senders get the same behaviour from `AeronCluster`, with one difference: a co-located
  sender whose leader moves away reconnects over UDP on a new session.

**A failover can lose ingress without any error.** A send returns once the frame is on the leader's ingress
publication, and the cluster confirms nothing on egress. Frames the old leader had not committed are lost,
as is everything offered to its publication before the producer learns of the new leader — in
`failover-test.sh`, at 100 µs pacing, either about 100 frames or about 10,000, in roughly equal shares of runs. The session survives, so neither side sees a fault.
§5 describes how a producer detects and resends those frames.

### 1.3 A node that cannot record terminates itself

A dropped frame would leave a permanent gap in a member's recording, so `QueuePublisher.emit` retries the queue
offer until it succeeds. Only the local archive can back-pressure that offer for long: the recording is the
queue's one tethered subscriber, and client subscriptions are untethered. A client that stops polling holds
the window only until its driver drops it, within about 0.2 s ([`ops.md`](ops.md), "Untethered
subscribers"), well inside the stall timeout below.

The retry is bounded. `QueuePublisher` watches the archive's recording position and tells three cases apart:

| archive state | response |
| --- | --- |
| slow: back-pressured, recording position advancing | keep retrying; raise the back-pressure alert after 200 ms |
| stalled: no progress for `sequencer.queueStallTimeoutMs`, default 5 s | fatal |
| recording gone | fatal at once |

A stopped recording back-pressures nothing, since the untethered client subscriptions keep the publication
connected, so the 1 Hz heartbeat also calls `QueuePublisher.checkRecordingAlive`. If the consensus module
refuses the heartbeat timer for 1 s without a break, that is fatal too; otherwise the cluster clock would
stop with no visible signal.

A fatal condition logs `FATAL: … terminating this node`, sets `seqeron_sequencer_queue_stalled`, and exits
with code 70 (`NodeDriver.EXIT_FATAL`); [`ops.md`](ops.md) has the operator's procedure. It does not throw:
a cluster callback that throws has already had its log position advanced, and `AgentRunner` keeps the agent
running, so the frame would be silently dropped (spec §9.4).

How the process ends depends on where the condition is found. Inside a retry — a stalled recording, an
offer no retry can clear, a refused timer — the service thread cannot leave it, and a graceful shutdown
waits for that thread, so the process halts at once and a failed leader is replaced within the election
timeout. From the heartbeat check the callback returns, and the member shuts down gracefully. Any shutdown
after a fatal, including a Replayer's, is halted 15 s after it began (`FATAL_SHUTDOWN_BACKSTOP_MS`), since
it closes the archive that may be what failed. A halted member leaves its mark files active, and a restart
is refused until their liveness timeout (about 10 s) has passed.

The remaining members hold identical recordings and keep quorum, and an election moves leadership if the
failed member led. In a three-member cluster a second failure loses quorum. A restarted member replays the
full log and rebuilds its recording. Start-up has the same bound — the recording must be live within 5 s
(`QUEUE_RECORDING_START_TIMEOUT_MS`) — so a member that exits 70 again at once still has broken storage.

### 1.4 A node whose applied log is truncated takes itself down

Aeron Cluster 1.53.2 can lose an entry it has committed. A follower that restarts while the leader waits on a member
that cannot copy the log truncates the entries it acknowledged, and if the leader is cut before they are copied back,
the other two elect without them (`CommittedEntryLossTest` scripts it). Rejoining, the old leader truncates what it
committed and applied. Then it either never completes an election again, or, when the others' log has reached the same
position, rejoins with a queue that differs from theirs from the lost entry on. Its peers keep quorum, so nothing else
shows either.

Raft never truncates an applied entry, so the consensus module's truncation event says when it happens: the log cut
below the position the node has applied. `SequencerServer` exits 70 on it. The restart
replays the log the others kept and rebuilds the member's recording.

It cannot undo what was read before. The member's earlier recording, and the restarted follower's, hold frames at
clusterSeqNos the log now gives to others, and a client that consumed them acted on history the cluster discarded.

### 1.5 Shutdown and restart

`clusterctl shutdown` runs on the leader. It publishes a `ClusterStopped` marker, waits (best effort) for it
on the queue, then calls `ClusterTool.abort`. `SequencerServer` connects
`ConsensusModule.Context.terminationHook` to a shutdown barrier, so each member closes its archive and
flushes the recording before exiting. `clusterctl start` publishes `ClusterStarted` and waits for it on the
queue, confirming that a leader is elected and ingress is accepted. [`clusterctl.md`](clusterctl.md) has both
procedures.

## 2. Gateways

A gateway is an active/standby pair (spec §5). The client tier's `Gateway` façade, in every language,
implements everything seqeron defines for one instance: the list row, the designation, `GatewayStarted`,
connection ids, the connection lifecycle frames, confirmed ingress and the fences. The application supplies
its external connections. `TestGateway` is the reference consumer, and `chaos-runner.sh` runs a pair of them
under fault injection (§6).

### 2.1 Fences

A gateway that keeps acting on a view of the log it can no longer trust may act on stale state. A fence
stops it first. Each is reported once, through the
listener's `onFenced(ClusterError, detail)`. The façade releases its cluster session first, which lets the
sequencer promote the standby (§2.2) whatever the application does next; it usually exits.

| `ClusterError` | condition |
| --- | --- |
| `CLUSTER_SESSION_LOST` | the cluster closed the session, or no new leader arrived within the sender's timeout, on an instance that had announced its activation on it; or, on any other, no new session replaced it within 20 s and the attempt then under way failed |
| `QUEUE_STALLED` | no `ClusterHeartbeat` on the co-located queue for 20 s (20 heartbeat intervals) while caught up, on a designated instance or the leader's replica |
| `RECOVERY_STALLED` | recovery has delivered nothing for 60 s (3 × the queue-stall timeout) on an instance that has been caught up before, on a designated instance or the leader's replica |
| `INGRESS_CONFIRM_FAULTED` | an own frame on the queue differs from the oldest pending one (spec §16 A-4) |
| `SNAPSHOT_DIVERGED` | the instance's snapshot of a round differs from the one its source sequenced (spec §16 A-7, §3.4) |
| `SNAPSHOT_UNRESTORABLE` | the instance cannot restore its newest confirmed snapshot (§3.4) |
| `GROUP_UNKNOWN` | a group the instance addresses is not in the log's group table (spec §16 A-9) |

- **The queue-stall timer runs on local monotonic time.** Consensus time arrives in the very
  `ClusterHeartbeat` frames being watched for, so it would stop with the queue.
- **A replay is not a stall.** The queue-stall check is suspended while the instance is not caught up. The
  recovery-stall fence (`RecoveryStallFence`) covers that time instead, armed only after the first catch-up,
  because a cold start replays the whole log and has no useful bound.
- **A correct Replayer cannot trip it.** The recovery-stall timeout (60 s) exceeds the Replayer's longest
  pending wait of 20 s (spec §10.1).
- **Only an instance that acts fences on a stall:** the designated gateway instance, or the replica on the
  leader's member (any replica off the cluster). Another holds nothing a stale view could act on, so it logs
  `QueueStalled` or `RecoveryStalled` once, keeps following the queue and recovering, and logs when the stall
  clears. It cannot begin to act while stalled, since designation and leadership reach it on the queue. A
  standby that never recovers stays up and unready, so alert on that log line.
- **Only an announced instance fences on a lost session.** Closing the session a designated instance's
  `GatewayStarted` bound is what promotes its sibling (§2.2), so a replacement session could act before the
  instance has read, at that close's `SessionClosed`, whether it was superseded. Any other instance
  holds nothing the close changed — a standby, and one designated after its session was lost,
  which announces on the replacement instead: it opens a new session as the first was opened, starting an
  attempt at most once a second and advancing it a step per duty cycle, and fences with `CLUSTER_SESSION_LOST`
  only if none opens within the queue-stall timeout and the attempt then under way fails, since one whose request
  an election swallowed spends its whole timeout. What was unconfirmed on the old session holds new frames
  until the queue settles it, and what it lost is resent first (§5).

Being superseded is not a fence. When a `GatewayActive` names a sibling, the instance's listener gets
`onStandby`; it closes its external connections, keeps its cluster session and goes on following the queue,
so it can be designated again. It publishes nothing on the way out: to the cluster this looks the same as
the process dying. The sequencer refuses whatever its session sends after that `GatewayActive` (spec
**S-6**), including frames placed before the instance read it, so the instance drops what it still has
unconfirmed rather than resend it.

### 2.2 Promotion

The sequencer decides which instance is active by synthesizing `GatewayActive` naming one `gatewayId`. Every
instance and every replica sees the same frame at the same `clusterSeqNo`, so no two instances act as active
on inconsistent information. It does so on four triggers (spec §7.2):

1. **Bootstrap:** after the last `GatewayRegistered` row, the rank-0 instance of each gateway.
2. **Session close:** when the session an active instance's `GatewayStarted` bound closes — through a crash,
   a network loss or a shutdown, which Aeron Cluster reports alike as a session close. The binding is keyed on
   `GatewayStarted`, not on `sourceId`, because other producers may legitimately carry a gateway's
   `sourceId`. An instance that has since been superseded keeps its binding until its session closes, and
   that close promotes nothing. Every close, promotion or not, is then sequenced as `SessionClosed`.
3. **Activation timeout:** a designated instance that publishes no `GatewayStarted` within 5 s is passed
   over.
4. **Operator request:** `clusterctl activate <gatewayId>` publishes `GatewayActivationRequested` and waits
   for the resulting `GatewayActive`.

With no eligible sibling, the sequencer synthesizes nothing, and the gateway has no active instance until
one starts.

On the instance, `GatewayLifecycle` follows the most recent `GatewayActive` for its gateway rather than
latching the first, publishes `GatewayStarted` before calling `onActivated`, and stands down without
publishing when a sibling is named.

### 2.3 Connections across a handover

A promoted instance rebuilds its connection state from the log. `onConnectionOpened` and
`onConnectionClosed` are delivered for every connection the logical gateway opened, whichever instance
opened it, during replay as live. `connectionData` carries whatever identity the application needs to map
a new connection onto an existing session (spec §7.1).

`GatewayStarted` carries `firstConnectionId`, chosen above the highest id seen in replay, so connection ids
never repeat across a handover. A crashed instance never publishes its connections' `ConnectionClosed` frames,
so an application that tracks connections treats `GatewayStarted` as closing every connection still open under
that `gatewaySourceId` (spec §7.1).

## 3. Stream recovery

A consumer reads the co-located queue directly for live data, untethered, so a slow consumer is dropped
instead of back-pressuring the sequencer (§1.3). It asks the co-located Replayer for history at a cold start
and after a gap.

### 3.1 `ReplayerService` (one per node)

The Replayer is not on the live path; clients use it only to catch up. Its job is to serve one recording,
correctly, to many clients at once, and to say so plainly when it cannot.

- **One recording.** It serves only the member's active queue recording. Every start of the member replays the
  log from `clusterSeqNo` 1 into a new recording, so the active one holds the whole log, and an older one is a
  prefix of it that nothing reads.
- **Integrity.** Each client checks that the first frame it dispatches is `clusterSeqNo` 1, and that a resumed
  replay opens on the frame it resumed from; either failing is fatal to the client. A member's recording that
  is missing or corrupt therefore stops the clients that read it rather than feeding them a short history.
- **Archive errors.** A request whose archive call throws is answered `ReplayPending`, without touching live
  delivery; the client's resend is the retry. The Replayer's own calls wait a second after one throws, and every
  error is counted (`seqeron_replayer_archive_errors_total`) and logged once per episode. The archive
  control session is read every duty cycle, because the archive closes a session whose pings go unread; a
  session it closes anyway cannot be reopened, so that ends the duty cycle.
- **Slots.** At most `MAX_CONCURRENT_REPLAYS` = 4 replays run at once, and a freed slot goes to a waiting
  client at once. While the queue's recording trails the queue by more than a quarter of the publication window,
  a new replay starts only when none is running: replays compete with the recorder for the disk, and the
  recorder is what holds the sequencer back. A slot frees when its replay's publication is gone: when the
  client closes its replay subscription at the replay's end, or when the driver times out a client that died
  mid-replay (spec §10, R-3). Many concurrent replays happen mainly when a member restarts and all
  its clients cold-start together.
- **Control replies** are offered with a bounded retry, then dropped, never blocked on. The client resends
  on a timer, so a client that is not reading cannot hold up replies to the others.
- **Duty-cycle failure.** An exception escaping the duty cycle clears the ready counter and exits the
  process with code 70 — the member's, since the Replayer runs in its JVM — after closing the archive client,
  so a supervisor restarts it, rather than leaving a member that looks ready but serves nothing.

### 3.2 Client-side recovery (`ReplayerStreamReceiver`)

`ReplayerStreamReceiver`, in every language, is the Aeron adapter; its decisions are in `ReplayerRecovery`,
which is unit-tested directly, case for case across the languages.

- **Cold start.** It walks the active recording from its start to the tip the Replayer reported when it
  answered, unless it restores a snapshot first (§3.4).
- **Gap.** A live frame whose `groupSeqNo` is ahead of the next expected one clears `isCaughtUp()` and
  requests a resume at the position of the last delivered frame, repairing only the gap. If the resumed
  replay's first frame is not the expected `groupSeqNo` — the member restarted, and its recording changed —
  the client falls back to a full walk. A frame is a duplicate by its `clusterSeqNo`, which increases along a
  queue but not by one (spec **F-6**).
- **Retained frames.** While a walk or resume is in progress, live frames beyond the gap are kept in a
  bounded FIFO (65,536 frames or 16 MiB) and delivered when the replay reaches them, so recovery ends without
  a second gap. Past the bound, the client drops them and walks again.
- **`isCaughtUp()` is not latched.** It can clear again on a later gap; the gateway fences (§2.1) and the
  leader gate (§4) depend on that.
- **The first frame is 1.** The first frame a client ever delivers must have `groupSeqNo` 1, or recovery
  stops and the receiver's next poll throws: the member's recording must reach the start of the queue.
- **One replay subscription per replay.** The replay stream (201) is shared by every client on the member,
  and an Aeron publication runs at its slowest tethered subscriber. A client therefore subscribes to it only
  for the duration of its replay, filtered to that replay's session id. A subscription held open between
  replays would never be polled for other clients' sessions, and would stop their replays about 32 MiB in,
  half of a 64 MiB term.
- **Stalled replays.** A bounded replay of a recording still being written does not close its image at the
  bound, so an open, attached, stalled image would go unnoticed. A replay that makes no progress for 5 s
  (`REPLAY_STALL_TIMEOUT_MS`), or whose image never attaches, is requested again. A resume, and a walk that
  has delivered a frame, are retried by re-anchoring through `requestResume()`, so the anchor check above
  still applies.

`ClusterStreamClient` (C++) reads an archive's recording directly where no Replayer is available: from its
start and without a bound, so one image delivers history followed by live data.

### 3.3 Gateway host

A gateway host runs clients but no member (`start-gateway-host.sh`). Its `ReplayerServer` runs its own media
driver and archive, and a relay copies a member's queue onto the host's own: one archive replay of the
member's active recording, over UDP, that follows it live. Clients on the host then recover exactly as §3.1
and §3.2 describe, against the host's archive. The relay's decisions are in `QueueRelay`, which is unit-tested
directly.

- **The next frame only.** The relay republishes a frame only if its `clusterSeqNo` is one past the last it
  published, and drops anything at or below it, so the host's queue holds each frame once, in order, whichever
  member it came from.
- **Member lost.** Nothing received for 3 s (`SOURCE_TIMEOUT_MS`, three cluster heartbeats), an ended
  replay, or an archive request unanswered for 1 s moves the relay to the next member in
  `SEQERON_ARCHIVE_ENDPOINTS`. It resumes there at the recording position its last frame ended at, since
  every member's active recording starts at `clusterSeqNo` 1 and its positions follow from the frames. If the
  frame at that position is not the next one, it replays that member's recording from its start and skips
  what it already has; a member that has recorded less than the relay has published is passed over. After
  every member has failed in a row, it waits 500 ms before trying again.
- **Restart.** A restarted relay starts a new local recording and relays the log from `clusterSeqNo` 1, as a
  member's full-log replay does, so the host's new recording starts at `clusterSeqNo` 1, as a client checks
  (§3.1). The cost is the
  whole history over the network per restart.
- **The local recording.** A local queue that refuses a frame is not spun on: the frame is offered again next
  cycle, and the member's replay waits under Aeron flow control. A local recording that stops, or makes no
  progress for `replayer.queueStallTimeoutMs` (default 5 s) while the queue is back-pressured, exits the process
  with code 70.
- **No member reachable.** The host's queue goes silent, and its clients' queue-stall checks (§2.1) fire, as
  they would on a member whose cluster lost quorum.

### 3.4 Snapshot restore

A client whose façade has a `SnapshotListener` restores before it dispatches anything
([`snapshot.md`](snapshot.md) §7). It takes the newest confirmed file in its snapshot directory, one it named
only after its source's sequenced `SnapshotEnd` matched it, replays its member's recording from the file's cut
to see that the frame there is the round's `SnapshotStarted`, reads the file's records, and resumes after the
cut as it would after a gap. A file whose cut the recording does not hold gives way to the next older one;
with none left, it walks the recording as §3.2 describes.

- **One replay before the records.** The check of the cut is a replay whose first frame decides it; the
  records then come from the local file, and the resume after them is an ordinary one.
- **After the restore**, every fallback that would walk the recording resumes at the snapshot instead: the
  history before its cut is no longer this client's to replay.
- **A crash.** A file is named only once its round is confirmed and the file forced to disk, and an older one
  is deleted only after that, so a crash costs at most the round in flight. A file torn regardless has no
  trailer and gives way to an older file.
- **A purged log.** Its rounds start again at 1, so a file from before the purge may name a round the new log
  also has. Its cut is a different frame, or none, in the new recording, so the check of the cut rejects it.
- **A damaged file.** A file whose records do not match its trailer gives way to an older file, the instance
  keeping two, before the listener sees any of it. A `formatVersion` or header version the build does not
  read fences the client with `SNAPSHOT_UNRESTORABLE`; a restart repeats it until the build is fixed or the
  file is removed.
- **No file.** A new host or a lost disk has nothing to restore, and walks from `clusterSeqNo` 1. Every
  instance of a source writes the same bytes, so a peer's file copied into its directory restores it instead.
- **Divergence.** At every later round each instance compares its own serialization with the sequenced end.
  One that differs is fenced with `SNAPSHOT_DIVERGED` (spec §16 A-7). Its file of that round is never
  confirmed, so its restart restores the round before, or walks, and rebuilds the state from the log.
- **After a member restart.** The member records its queue again from `clusterSeqNo` 1. A client whose cut the
  new recording has not reached is answered `ReplayPending` and waits for it.

## 4. Leader-only work

Some side effects must be performed by exactly one replica, the one beside the leader, and must survive a
failover: a reply, an order sent outward, a notification. The client tier provides `LeaderGate` and
`OutstandingWork` for this, in every language (spec §16 A-1 to A-3).

- **Whether work is outstanding is replicated.** A request is outstanding from its sequenced request until
  its sequenced reply, so every replica holds the same set.
- **Dispatch is local.** It records that this replica is handling a request. It is cleared when the gate
  closes, and when a reply offer fails; the request stays outstanding, since the log decides.
- **A newly promoted leader dispatches every outstanding request not yet dispatched.** A request is
  discharged only by a sequenced reply, so it is never answered twice by design and never lost; if a reply
  does not reach the log, the request stays outstanding for the next leader.
- **Every `LeadershipChanged` closes the gate**, including one naming the same member. A replica applies
  several frames per duty cycle and can see leadership change away and back within one; a reply sent during
  that election may have been lost with the old leader's uncommitted log. `OutstandingWorkPropertyTest` covers
  this case.
- **Off the cluster** (`Application`'s `offCluster`, §3.3), one instance is the only dispatcher: its gate
  opens once caught up, whoever leads, and closes on every `LeadershipChanged` as above. Exactly one must run,
  since nothing elects between two.
- **A lost cluster session closes the gate** until a new one opens: the replica replaces it without blocking, as
  §2.1 describes for a standby, and fences only if none opens within the queue-stall timeout and the attempt then
  under way fails. Whatever it
  dispatched meanwhile is redispatched when the gate reopens, since nothing elects a replica and its requests
  stay outstanding on the log.
- **Dispatch follows insertion order**, which is `clusterSeqNo` order. Side effects become visible in emission
  order, and a hash map's iteration order would differ between replicas.

Delivery is at least once across a failover. A reply that is a pure function of the sequenced request, keyed
on its `clusterSeqNo`, makes a re-emission byte-identical, so consumers can drop duplicates by key.

## 5. Ingress across a failover

A producer that must not lose a frame confirms each one on its own queue (spec §16 A-4, A-5).
`PendingSends` holds every placed frame until the producer's own queue shows it, matched by cluster session
id. After a `LeadershipChanged` with a newer term, any frame stamped with an older term that has not appeared
is lost, and the lost frames are exactly the newest ones stamped with that term.

A closed session is the same kind of boundary. The consensus module appends nothing from a session once it
begins closing it, and the close follows everything it did append, so a frame not on the queue before that
session's `SessionClosed` is lost. A session whose open never committed has no `SessionClosed`, but was lost
to an election, which the term bounds. A producer whose session the cluster closed replaces it (§2.1, §4), and
the queue's `SessionClosed` for its own session also counts as that loss.

From the first sign of a new term, or from a session's replacement, until every older frame is seen or resent,
`IngressPublisher` places nothing new, and the sender abandons a send already retrying through the election
(`setIngressHold`). The lost frames are resent oldest first, never onto a session the queue has shown closed.
The result is every frame sequenced exactly once, in order.

`failover-test.sh` runs two producers across a leader kill: the one using `PendingSends` must see every frame
on the queue exactly once and in order, and the other reports what it lost. `cpp-client-test.sh` and
`csharp-client-test.sh` run the same check with the C++ and C# clients.

## 6. Operational tooling and verification

- **`clusterctl`** ([`clusterctl.md`](clusterctl.md)): `start` and `shutdown` bracket a run with sequenced
  markers; `activate` is the manual promotion (§2.2); `request-snapshot` starts an application snapshot round
  (§3.4); `snapshot` is refused.
- **Metrics** ([`ops.md`](ops.md)): `seqeron_sequencer_queue_stalled` (set when a member is about to terminate,
  §1.3), `seqeron_sequencer_gateway_promotion_total` (§2.2), the `seqeron_replayer_*` family (§3.1), and
  Prometheus's per-member `up`.

Each failure above has a harness that produces it against a live cluster, all under
`seqeron-service/src/test/scripts`:

| harness | what it does, and what must hold |
| --- | --- |
| `chaos-runner.sh` | randomized faults against three members and a `TestGateway` pair, reproducible from its printed seed: kill and restart the leader or a follower, `SIGKILL` a follower, `SIGSTOP`/`SIGCONT` a member, drop one live queue frame (§3.2), stop a member's queue recording (§1.3, asserting the member exits). After each round there must be one leader, the pair must still serve, and consumers must still deliver in order. At the end every member's recording must be gap-free, strictly increasing in `clusterSeqNo`, and at the same high mark on all three (§1.1) |
| `failover-test.sh` | a leader kill with a replay consumer, and the confirmed-ingress check of §5 |
| `gap-recovery-test.sh` | a caught-up consumer drops one live frame after a leader failover, and must resume and keep delivering (§3.2) |
| `paused-subscriber-test.sh` | a caught-up consumer is `SIGSTOP`ped while more than two queue windows go by; its member must stay up, with no queue-stall fatal (§1.3), and once resumed the consumer must heal the gap its eviction left (§3.2) |
| `session-replace-test.sh` | a `TestApplication` on a follower is `SIGSTOP`ped past its session timeout, then the other two members are, so its member has no quorum; each time it must open a new session rather than fence (§2.1, §4), and the cluster heartbeat must keep reaching it while an attempt is pending. Then both `TestGateway` instances are paused past their session timeout: the standby, designated while its own session was lost, must replace it and serve rather than fence (§2.1) |
| `replayer-restart-test.sh` | member 0's `SequencerServer` is killed under a caught-up client; the client must fail fast, and a fresh cold start must be served from the member's new recording alone (§3.1) |
| `gateway-host-test.sh` | a gateway host whose relay reads the leader, which is then killed; a `confirm` producer on the host must see every frame exactly once, in order, and the relay must move to another member. The host is then restarted, and a cold start there must catch up from its new recording (§3.3) |
| `snapshot-test.sh` | the `TestGateway` pair on three members with snapshot rounds every 2 s. The standby restarts and restores, the active instance is killed and the restored one takes over, and the killed one returns as a hot standby, restoring from the file it confirmed while it served. Each restore must report the state the client traffic implies, and no instance may be fenced, so every round is also compared against the restored state (§3.4) |
| `csharp-client-test.sh` | the C# client against three members: confirmed ingress across a leader kill (§5), a cold start from `clusterSeqNo` 1 (§3.2), a C# gateway pair handed over when its active instance is killed (§2.2, §2.3), an application replica restored from a snapshot it confirmed (§3.4), and a cold start with no snapshot file on a member restarted at a cut, which must fail from its poll (§3.2) |
| `cpp-client-test.sh` | the C++ client against three members: confirmed ingress across a leader kill (§5), a cold start from `clusterSeqNo` 1 (§3.2), the C++ gateway pair handed over when its active instance is killed (§2.2, §2.3), an application replica restored from a snapshot it confirmed (§3.4), and `ClusterStreamClient` replaying a member's recording that starts at a cut, where a cold start with no snapshot file must fail from its poll (§3.2) |
| `csharp-windows-test.sh` | the C# client on Windows beside a gateway host, against one member in WSL1: confirmed ingress (§5), a cold start through the gateway host (§3.2, §3.3), and the C# gateway pair handed over (§2.2, §2.3). One member, because three cannot hold Raft's heartbeats under WSL1; the leader kill is `csharp-client-test.sh`'s |
| `docker-failover-test.sh` | `ROUNDS` (default 15) leader kills against `docker/compose.yml` under continuous `ProbeMarker` load, restoring each killed member before the next. Every round must change leadership and the killed member must rejoin; an observer on each surviving member must deliver in `clusterSeqNo` order throughout; and a cold start at the end must replay the whole multi-tenure history from one recording and reach live. Needs Docker and `./gradlew operatorDist`; CI runs it as `failover.yml`, and `ROUNDS=3` is a quick local run |
| `replay-bench.sh` | times a cold `ClusterProbe follow` from launch to caught up against a preloaded archive, optionally under load. `replay-bench.sh 400000` builds about 70 MB of history, which must converge in well under a second; `NEVER CAUGHT UP` indicates a replay stall (§3.2), not a slow machine |

`failover.yml` also runs the faults no kill or pause produces, against the same `docker/compose.yml` cluster: a
leader partitioned from its peers while its clients stay connected, one link cut and no other, a follower without
NAKs or without retransmits, slow and duplicating leader links, a member whose ballots are dropped, and a leader
starved of CPU. The tests are under `seqeron-service/src/test/scripts/faulteron` and drive
[faulteron](https://github.com/FredrikJDahlberg/faulteron), an eBPF injector, at a pinned commit
([`building.md`](building.md)).

## 7. Not covered

- **Killing a standalone media driver.** A member kill also kills the embedded driver its co-located clients
  share, and `chaos-runner.sh` asserts that the consumer and gateway instance there exit; a standalone `aeronmd`
  is never killed.
- **Ingress lost outside one producer process.** `PendingSends` keeps its state in memory and counts losses
  against a leadership change or a session close. A frame lost with neither — an ingress image that drops and
  rejoins within the session timeout — has no boundary to be counted against, and a restarted producer, or a
  standby promoted in its place, starts with nothing pending. Covering either needs a durable outbox or a
  per-producer sequence number that the sequencer de-duplicates on.
- **Producer authentication.** The cluster checks well-formedness, not identity (spec §7). Authentication
  belongs at the system's external edges.
- **Snapshots under cluster faults.** `snapshot-test.sh` kills the leader and restores its clients against
  the member's new recording, but the property test injects replay faults and no failover or publisher change
  mid-round. A round missed because its publisher died mid-round is covered by unit tests only.
