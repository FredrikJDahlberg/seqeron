# Enterprise review: code quality and correctness

Reviewed 2026-11-15, at `334a4732` on `replayer-simplify` (v0.18.0). Paths are repository-relative; Aeron paths are
under `aeron-cluster/src/main/java/io/aeron/cluster/` at the pinned 1.53.2.

**Scope.** The question was what stands between seqeron and use in an enterprise, given that the usual feature list
(authentication, TLS, fsync, multi-site) can be built. So this review looks at the code that would be kept, not the
features that would be added. Read: `Sequencer`, `SequencerService`, `SequencerServer`, `QueuePublisher`, `LogDeriver`,
`LogVersion`, `ReplayerService`, `PendingSends` and `Logger`, the `Gateway` and `ClientProxy` outlines, and the Aeron
code that `onTakeSnapshot` and session timeouts depend on. Not read: the C++ hosted stubs, the C# port beyond its
size, `DerivingReplayer`, `AeronLogRelay` and `RecordingReader` (`service-review.md` covers those), `SnapshotChoice`,
`RestartSafeLog`, `tools` and `metrics`. Run: `./gradlew test`, green. Not run: the integration, system, C++ and C#
suites. No finding below has a test that reproduces it yet.

**Verdict.** The core is well built. The state machine has no Aeron dependency and is tested directly. Every decision
that touches Aeron sits behind a pure seam. The integration suite checks the derived history against the live queues
frame for frame, and every condition that could silently corrupt the stream stops the node instead. Engineering
discipline is not what stands in the way. Five things do:

- one defect that can make a member's queue diverge from its peers' (1);
- one configuration that silently disables gateway failover (2);
- history that is re-derived by whatever sequencer code is current, so a fix that changes output already published
  purges the archive, and nothing detects such a change (3);
- queue publication on the consensus thread, which lets a stray subscriber take members down (4);
- exactly-once ingress that ends when the producer process ends (5).

1 and 2 are fixed. 3 and 5 change the protocol, so they belong before 1.0, while a breaking release is still
cheap. 7 comes first among the rest: it is what lets most fixes ship without a purge (3).

## Defects

**1. A member whose snapshot fails before Aeron reaches the service skips the round's marker and diverges.**
`SequencerService.onTakeSnapshot` (`seqeron-service/src/main/java/org/limitless/seqeron/sequencer/SequencerService.java:391`)
changes replicated state: `sequencer.takeSnapshot` advances `clusterSeqNo`, `snapshotRound` and every queue's
`groupSeqNo`, and publishes `SnapshotStarted`. Aeron calls that method only after several local steps succeed
(`service/ClusteredServiceAgent.java:988-1004`): it connects to the archive, starts the snapshot recording, waits for
the recording counter, writes the session snapshot and checks for an archive error. If any of those steps throws,
`executeAction` (`:1057-1092`) catches the exception, acks with a null recording id and rethrows into the container's
error handler. `SequencerServer` only logged that handler
(`seqeron-service/src/main/java/org/limitless/seqeron/sequencer/SequencerServer.java:426`). The consensus module
records the empty ack as a `ClusterEvent` straight into its error log (`ServiceAck.areAllRelevantIdsNonNull`, which
reaches no error handler), skips the snapshot and returns to `ACTIVE` (`ConsensusModuleAgent.java:3104-3150`).
`STORAGE_SPACE` is the only error that terminates the node.

- **Failure.** An archive connect timeout or error response during a round, on one member, while the archive is busy.
  That member's `Sequencer` never applies the round. From then on, every frame on its queues carries a `clusterSeqNo`
  and `groupSeqNo` one below its peers', and `roundDue` stays set. Its own Replayer derives the marker regardless
  (`LogDeriver.java:176-179`), so the member's live queue and its replay of the same queue disagree. A client that
  heals a gap there gets frames that do not continue what it delivered. The node's next cluster snapshot then persists
  the divergent state. The same applies when the log is replayed at restart, because Aeron re-takes each snapshot it
  replays.
- **Fixed.** Making every exception in that handler fatal, as first proposed here, would be too broad: the handler
  also receives the container's Aeron client errors, which a remote client can provoke with a bad egress channel.
  `SequencerService.onServiceError` instead recognises a skipped round by its stack, which passed through Aeron's
  `onTakeSnapshot` and not the service's, from a handler running outside the attempt. It then applies no further
  entry, takes no snapshot, and raises the fatal (exit 70) before the member publishes another frame.
  `SequencerServiceSnapshotTest` covers the classification and the latch. `ClusterSnapshotTest` breaks one member's
  snapshot archive connection, through a new `serviceArchive` overload of `SequencerServer.launch`, and fails without
  the fix. `doc/fault-tolerance.md` §1.5 describes the failure.

**2. A busy cluster with a low snapshot `bytes` setting never fails over a dead gateway.** Each snapshot resets every
client session's inactivity clock (`ConsensusModuleAgent.java:3123`). `Sequencer.clusterHeartbeat` sets a round due
whenever `bytesSinceSnapshot >= snapshotBytes`, checked at 1 Hz (`Sequencer.java:742-745`). With the default 5 s
session timeout, any `bytes` value the cluster sequences within 5 s means rounds arrive faster than sessions can
expire. A crashed active gateway then keeps its session indefinitely, and its standby is never promoted. An
`interval` below the session timeout has the same effect. `doc/ops.md` ("Cluster snapshots") documents both, but
nothing enforces either: `topology.xsd` bounds neither attribute, and the session timeout is a node-local property
the sequencer never sees.

- **Fix.** Enforce a minimum spacing in replicated state. Give `SnapshotPolicyRegistered` a minimum gap between rounds
  that applies to the byte and operator triggers too, and have `clusterctl load-topology` refuse a gap at or below the
  session timeout it is told.
- **Fixed, without the wire change.** Only the leader asks for a round, and the leader's own consensus module times
  sessions out, so the leader's own `sequencer.sessionTimeoutMs` is the value that matters, and the service already
  has it. `SequencerService.requestRound` now asks only once the session timeout plus one heartbeat has passed since
  the last round started (`Sequencer.snapshotDue(timestamp, minGapNs)`), for every trigger. The first round is
  exempt. No log entry changes, so neither `LOG_VERSION` nor schema 210 changes. Any gap above the timeout lets a
  dead session expire eventually, and the extra heartbeat makes it expire at the next round. `clusterctl
  request-snapshot` now waits 30 s for its round. A round toggled through Aeron's own `ClusterTool` still bypasses
  the gap. `SequencerTest` covers the gate. `doc/ops.md`, `snapshot.md` §10.2 and spec §7.3 describe it.

**3. The sequencer's code is part of the recorded history, and nothing detects a change to it.** A member recovers,
and its Replayer derives every queue, by running the current `Sequencer` over the whole log (`LogDeriver`). Spec V-3
states the consequence. Any change to what §9.2 rejects, or to how a synthesized frame is encoded, produces a
different history from the same log, so it needs a `LogVersion` bump and a purged archive. This affects enterprise use
in three ways:

- **No rolling upgrades.** Every member, gateway host and client upgrades together, and history is lost.
- **A fix that changes published output purges the archive.** Fixing a crash and replaying the log is the usual
  repair, and seqeron supports it: the log holds the inputs and every queue is derived. Which fixes cost a purge depends
  on what the bug did:
  - *It crashed.* The old build published nothing for the entry that stopped it. A fix that changes the output from
    that entry on, and of no entry before it, derives the same history up to the crash and the right one after. Nothing
    anyone saw changes, so the archive survives. V-3 asks for a bump here anyway.
  - *It published wrong output.* Replaying repairs the sequencer's state, but consumers already acted on the old
    frames. Refusing a frame that was accepted shifts every later `clusterSeqNo` and `groupSeqNo`, so snapshot cuts,
    `SnapshotEnd` checks and gap positions name other frames, and a reply already sent cannot be replayed away. Only
    this kind of fix needs the purge, and it conflicts with any record-retention requirement unless old archives are
    exported and kept readable by old builds.

  Today a bug is often the second kind when it should be the first: an exception in a callback is logged and the node
  sequences on (7).
- **The version bump is manual.** No test fails when a code change alters the frames `LogDeriver` derives from an
  existing log. `LogVersionTest` checks the validator, not behaviour. A forgotten bump is caught by nothing: a node
  restarted on the new build silently sequences a different history from its peers'.

The cheap guard is a golden-log test. Commit a fixture of log entries covering every system event, rejection and
election path, and the digest of the frames `LogDeriver` produces from it. The test fails unless the digest and
`LOG_VERSION` change together. It also tells the two kinds of fix apart. A crash fix adds a fixture of its own, ending
at the entry that crashed the old build, and leaves the existing digests alone, so it needs no bump. A fix that changes
a digest changed output the old build published, and the test demands the bump.

Once 7 and the golden-log test are in, V-3's second rule can narrow to that distinction. Proposed wording, replacing
"A change to what §9.2 rejects is the same kind of change: a log replayed under another rule sequences another
history":

> A change to what §9.2 rejects, or to any frame the sequencer derives, is the same kind of change when it alters the
> output of a log entry an earlier build processed without stopping: a log replayed under it sequences another
> history. A change that alters only the output of entries on which an earlier build stopped with a fatal is not: the
> earlier build published nothing for them, so the history up to that entry is unchanged. `LOG_VERSION` stays, and the
> archive is kept.

The structural fix is to version the rules inside the log. A system event names the rule set, the `Sequencer` branches
on it, and a fix that changes published output takes effect from the position where it was announced, so the archive
survives. A retroactive fix stays possible, by purging. This is how replicated state machines usually ship fixes:
`doc/versioning-prior-art.md` §7 surveys Temporal's version markers and replay tests, Kafka's `metadata.version`,
CockroachDB's finalization and Bitcoin's activation heights, and §8 maps them onto seqeron.

**4. The live queue is published on the consensus service thread, so any tethered subscriber can halt a member.**
`QueuePublisher.emit` spins on the service thread while an offer is back-pressured, and halts the node after
`sequencer.queueStallTimeoutMs`, 5 s by default (`QueuePublisher.java:179-208`). Only the subscriber chooses whether
it is untethered: the client tier appends `tether=false` (`ReplayerStreamReceiver.java:36`), and nothing on the
publication side can require it.

- **Failure.** A process subscribes to stream 2001 over `aeron:ipc` with a default (tethered) subscription and then
  pauses: a GC pause, `SIGSTOP`, a debugger. That process could be an operations tool, a third-party consumer or a
  hand-written test harness. The member's sequencer stops within one term window and the member halts 5 s later. If
  the same tool runs on every member host, as monitoring usually does, the whole cluster loses quorum together.
- **Mitigation.** At the 1 s back-pressure alert, read the queue's subscriber-position counters and log which
  registration is holding the queue. An operator then sees the cause instead of a halt.
- **Fix.** Take the queue off the consensus thread. The service keeps the replicated state, and a separate agent
  publishes each queue from the committed log. The log-following machinery already exists (`LogFollower`,
  `LogDeriver`). A slow subscriber then delays that one agent, never the member's consensus participation.

**5. Exactly-once ingress ends at the producer process boundary.** `PendingSends` confirms ingress and resends across
leader failovers. That part is sound (see "Checked and sound"), but it is in memory only (`PendingSends.java:44`). The
envelope carries no producer sequence number, and the sequencer keeps no per-source deduplication state. As a result:

- When a producer restarts, or a standby is promoted, nothing pending is known. Frames that were not committed are
  lost. A producer that resends to be safe creates duplicates.
- `Application` documents at-least-once delivery across a failover (`doc/overview.md`, "Co-located applications"),
  and asks for idempotent replies.

For an order-entry or payments path, that leaves the hardest part of exactly-once to each application. The fix is a
per-source sequence number in the ingress header, with the highest accepted value per `sourceId` kept as replicated
state, so the sequencer refuses a resend it has already sequenced. That changes schema 210, so it costs a purge after
1.0 and nothing before.

## Code quality

**6. Logging is synchronous stderr, unthrottled, and repeated on every replay.** The default sink calls
`System.err.println` on the calling thread (`Logger.java:134-141`), and the sink is one process-wide static
(`Logger.install`). `Sequencer.reject` logs at error level for every refused frame, on the consensus thread, with
`String.format` (`Sequencer.java:719-724`). Three consequences:

- **Flooding.** A misbehaving producer floods every member's log identically, at the speed of ingress.
- **Stalls.** A stderr pipe that blocks, such as a stalled journald or container log driver, stalls the service thread.
- **Replayed alerts.** The `Sequencer` cannot tell live from replay, so each member start re-logs every historical
  rejection and promotion failure twice: once in the service's log replay and once in its `LogFollower`'s derivation.
  An alerting rule on error lines fires on every restart.

The event codes are already a good basis for structured logs. What is missing:

- an asynchronous, bounded sink with an SLF4J bridge;
- per-code rate limiting (`Episode` already does this in the Replayer);
- a silent sink for the derivation's `Sequencer`.

**7. An unexpected exception in a cluster callback is logged and the node carries on.** `CLAUDE.md` notes that a throw
in a callback drops the frame. The service's error handler (`SequencerServer.java:426`) makes that the policy for bugs
as well. An exception partway through `Sequencer.publish`'s loop over groups leaves some queues' `groupSeqNo` advanced
and others not. The rest of the tier stops rather than corrupting the stream, and this path should too.
Since the handler also receives Aeron client errors (see 1), the fix belongs in the callbacks: each one catches its
own exceptions and raises the fatal. A deterministic bug then crash-loops every member at the same entry. That is the correct outcome for a sequencer, but it needs a runbook entry for a poison log entry.

This is also what keeps fixes cheap (3). A bug that stops every member before it publishes is repaired the usual way:
ship the fix, restart, and each member replays its log past the entry and sequences on, with no `LOG_VERSION` bump and
no purge. A bug that sequences on publishes inconsistent output, and fixing it rewrites history consumers already
acted on. The runbook entry is therefore the standard repair, not a rare one: every member stops at entry N; ship a
build that fixes entry N and passes the golden-log test with the existing digests unchanged; restart the members.

**8. Configuration is about 50 system-property reads under six prefixes, with almost no cross-checks.** The prefixes
are `sequencer.*`, `replayer.*`, `proxy.*`, `probe.*`, `aeron.*` and environment variables. `SequencerServer.Config`
is an unvalidated record. One documented constraint is not enforced: the session timeout must stay well above the
leader heartbeat timeout. The other, round spacing against the session timeout, is now enforced (2). An enterprise deployment
wants three things: one validated configuration per process, the effective values printed at start, and a refusal to
start on inconsistent combinations. `ClientProxy` already refuses invalid values; the member does not.

**9. Cost of change.**

- **Three hand-synchronized implementations.** The client tier's state machines are maintained case for case in Java
  and C#, and partly in C++. `ReplayerRecovery` is 965 lines of Java and 1,035 of C#, and its tests 1,805 and 1,747.
  The conformance rows and parallel property tests keep them honest, but every client change costs two or three times
  over and needs a reviewer fluent in all three. An adopter should know which languages are supported, at what level.
- **`Sequencer` is 1,443 lines.** It holds ingress validation, the gateway election, snapshot rounds, topology
  retention and its own snapshot codec. Moving the election and the snapshot codec into classes of their own is a
  pure move under the existing tests.
- **Supply chain and governance.** The project is pre-1.0 with breaking minor releases and one maintainer. Java
  artifacts are built by JitPack from tags rather than published to Maven Central. Nothing is signed and there is no
  SBOM. All of this can be fixed, and all of it fails procurement until it is.

## Minor

- **A topology reload can move a `gatewayId` to another logical gateway.** `addGatewayRow` updates
  `gatewaySourceId` in place (`Sequencer.java:955-963`), and `admitRow` checks only that the row's `sourceId` keeps
  its group. `designatedGateway` keeps the old key, so the moved instance designates under the wrong pair. Refuse a
  row that changes an existing instance's `gatewaySourceId`.
- **The truncation guard parses Aeron's message text** (`SequencerServer.java:137`). `CommittedEntryLossTest` runs it
  against a real truncation, so an Aeron upgrade that rewords the message fails that test. Keep the test on the
  upgrade checklist.
- **`MAX_CONCURRENT_REPLAYS` is a constant (4)** (`ReplayerService.java:45`), so the replay concurrency a large host
  needs cannot be configured.
- **Leftovers from earlier edits.** Five doc comments still carry the stray punctuation the citation removal left
  (`*. ` and `*, ` at the start of a line), in `LogDeriver.java:24`, `SequencerService.java:40`,
  `SequencerServer.java:117`, `Sequencer.java:262` and `SnapshotListener.java:21`. `LogDeriver.java:108-109` has a
  stray `;` on a line of its own. `QueuePublisher`'s method names still say "recording" for the service's subscriber.

## Where the enterprise features meet the design

- **Authorization belongs in the replicated state, not only at the transport.** The first session to send
  `GroupsRegistered` holds the topology session (S-9), and operator events such as `GatewayActivationRequested` and
  `SnapshotRequested` are accepted from any session. An Aeron `Authenticator` proves who opened a session; the
  `Sequencer` must also record which principal may send which events. Both are replicated decisions.
- **Durability can be turned on.** Aeron's archive and cluster support file sync levels. Expose that setting and
  measure it on the volumes `doc/ec2-tests.md` already describes.
- **Multi-site.** Fixed port blocks (`PortLayout`) and a seven-member limit are fine for one site. A second site
  will want Aeron's cluster backup or standby rather than stretched Raft. Keep that in mind when designing 4: queues
  derived from the log fit a backup node naturally.

## Checked and sound

- **`PendingSends`' boundary argument holds on Aeron.** A new term's `NewLeadershipTermEvent` is appended after every
  entry the new leader holds from earlier terms. So a frame stamped term T either precedes the first
  `LeadershipChanged` above T, or is lost. Loss that crosses no boundary breaks the match against the oldest pending
  frame and latches `isFaulted()`, so it stops the producer rather than passing silently.
- **`LogDeriver` mirrors `SequencerService` everywhere except 1.** It handles session admission, timers, closes,
  terms and policy-gated rounds the same way, and `DerivedQueueTest` checks the result frame for frame.
- **`Sequencer.sequenceMessage` changes no state until every §9.2 condition has passed**, and the gateway
  deposition check (S-6 case 3) keeps a superseded instance off the log.
- **The client library never exits the process.** Fences reach the application through its listener.
- **Fail-stop paths.** `QueuePublisher` halts rather than drop a frame, and the fatal backstop guarantees exit 70 even
  when the graceful shutdown wedges.
