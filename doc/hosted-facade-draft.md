# §17 Hosted façade protocol (draft)

> **Draft, not normative.** Proposed as §17 of [`seqeron-protocol-spec.md`](seqeron-protocol-spec.md);
> §15 stays unassigned. Rule ids use the new prefix **H**. Schema id 218 and streams 301–303 are
> proposals; ids in use are 111, 210, 212, 214, 216 and streams 100, 122, 201–203, 2001+.

A **hosted façade** splits an `Application`, `Gateway` or `Consumer` across two processes on one host. The
**host** is a Java process that runs the real façade (`app`) and owns everything of §16 that concerns the
cluster: the session, confirmed ingress (**A-4**, **A-5**), the leader gate (**A-1**), group resolution
(**A-8**, **A-9**), fencing, the replay and recovery path, and the snapshot files and rounds (**A-7**). The
**client** is the application, in any language, whose only seqeron code is a thin stub with the façade's
operation names. The host calls the client's listener over a stream rather than in-process; the client
calls `publish`, `reply` and the connection operations over a second.

**The aim is that no client re-implements an Aeron API or ports a seqeron state machine.** Today the C++
client hand-writes the Aeron Cluster client protocol (`ClusterStreamSender`; Aeron's C++ client has no
cluster API), and both the C++ and C# clients port `PendingSends`, `ReplayerRecovery`, `SnapshotStore` and
the façades from Java, case for case. Hosted, they carry none of it, at the cost of one IPC hop per message.
**The direct C++ and C# façades are retired**: the hosted stubs are the only C++ and C# clients. Java's direct
façades stay, since the host is built on them.

The client keeps what is the application's: its state, a deterministic reply (**A-3**), deterministic
snapshot records (**A-6**) and `OutstandingWork` (**A-2**). The host's façade is built with no tracker.

## 17.1 Scope

- Host and client share a host and a media driver: the member's, or a gateway host's. The streams are
  `aeron:ipc`; there is no UDP binding. A client on a host with no driver and no JVM is out of scope.
- Trust is the Aeron directory's: streams 301 and 303 are readable and stream 302 writable by any process that can open
  the host's directory, so the first process to send `Attach` wins (**H-8**). That is the boundary which guards
  the queue and cluster ingress today. Start the host before the client and restrict the directory to the
  application's user; `attachId` (**H-13**) separates incarnations and is not a secret.
- One client per host instance. A host runs one façade, for one `sourceId` (or gateway pair
  instance), configured through the host's own properties; the protocol carries no façade configuration.
- A hosted `Consumer` is the same protocol with fewer messages: after `Attached` and `GroupsResolved` it
  receives `RestoreRecord`s, `Payload`, `CaughtUp`, `ClusterTime`, `Heartbeat` and `Fenced`, and sends only `Attach`,
  `Heartbeat` and `Detach`. It has no `Gate`, `Role`, command, `Verdict` or `SnapshotCut`,
  and the host opens no cluster session for it. Its read path (`ReplayerRecovery`, `ReplayerStreamReceiver`,
  `SnapshotStore`) is most of what a client would otherwise port, so a consumer that is not hosted is the
  exception.

## 17.2 Transport

| stream | channel / stream id | direction | schema |
| --- | --- | --- | --- |
| host events | `aeron:ipc`, stream 301 | host → client | 218 |
| client commands | `aeron:ipc`, stream 302 | client → host | 218 |
| host verdicts | `aeron:ipc`, stream 303 | host → client | 218 |

Both are reliable (not untethered), one publication each, unrecorded, and never leave the node.
**T-1** applies: a reader MUST check `MessageHeader.schemaId` is 218. A message above one MTU is
fragmented by Aeron; a snapshot record (at most 65,535 bytes) fits the 16 MiB IPC term (`ops.md`).

## 17.3 Messages

Every message after `Attach`, in both directions, carries the `attachId` the host assigned in `Attached` (**H-13**);
the tables leave it out.

Host → client:

| message | fields | meaning |
| --- | --- | --- |
| `Attached` | `attachId`, `nonce`, `schemaVersion`, `hostVersion`, `kind` (Gateway, Application, Consumer), `sourceId`/`gatewayName` | accepts an `Attach` |
| `GroupsResolved` | repeating `name`, `groupId`, `mask` | the `GroupDirectory`, once, before the first `Gate` or `Role` |
| `RestoreRecord` | `recordIndex`, `formatVersion`, `data` | one `onRestore` call, in call order; `recordIndex` 0 means clear state. No records means start empty (**H-7**) |
| `Payload` | `clusterSeqNo`, `groupSeqNo`, `sourceId`, `connectionId`, `groupMask`, `sourceSessionId`, `clusterTimestampNs`, `receiveTimeNs`, `position`, `payloadId`, then the payload bytes with their own `MessageHeader` | `onSequenced`: one application payload, with what the façade's `Payload` exposes |
| `Connection` | `connectionId`, `opened`, `data` | Gateway only: `onConnectionOpened` / `onConnectionClosed` |
| `ClusterTime` | `clusterTimeNs`, `receiveTimeNs` | `onClusterHeartbeat` |
| `Gate` | `gateEpoch`, `leading`, `canPublish`, `caughtUp` | every change of the façade's gate state, in event order: `leading` is `isLeading()` (Application) or `isServing()` (Gateway) and drives `onLeadershipChanged` and `OutstandingWork.onNotLeader()`; `canPublish` is `canPublish()` (Application) or `canAccept()` (Gateway), `leading` and ingress not held (**A-5**), and is what the stub's `canPublish()` returns (**H-3**) |
| `Role` | `active`, `firstConnectionId` | Gateway only: `onActivated` / `onStandby`, sent before the `Gate` that follows it |
| `CaughtUp` | `clusterSeqNo` | `onCaughtUp` |
| `SnapshotCut` | `roundId` | write the records for this cut (**H-6**) |
| `Verdict` | `correlationId`, `result`, `reason`, `connectionId` | stream 303: the answer to the one command the client is blocked on; `connectionId` answers `OpenConnection` (**H-4**) |
| `Heartbeat` | `hostTimeNs` | liveness |
| `Fenced` | `error`, `detail` | `onFenced`: terminal |

Client → host:

| message | fields | meaning |
| --- | --- | --- |
| `Attach` | `nonce`, `schemaVersion`, `clientId`, `formatVersion`, repeating `supportedFormat`, `sourceId`/`gatewayName` | starts a façade (**H-8**) |
| `Publish` | `correlationId`, `gateEpoch`, `connectionId`, `groupMask`, `payloadId`, `payload` | `publish` |
| `Reply` | `correlationId`, `gateEpoch`, `requestClusterSeqNo`, `payloadId`, `payload` | `reply`; the host looks the request up (**H-9**) |
| `OpenConnection` | `correlationId`, `data` | Gateway only: `openConnection`, answered by a `Verdict` carrying the id (**H-4**) |
| `CloseConnection` | `connectionId` | Gateway only: `closeConnection`, not answered |
| `Activated` | `opened` | Gateway only: the result of `onActivated`, answering `Role` (**Q3**) |
| `GateClosed` | | the edge closed on its own (`gateClosed()`) |
| `SnapshotRecord` | `roundId`, `recordIndex`, `data` | one `onSnapshot` result |
| `SnapshotEnd` | `roundId`, `recordCount` | no more records; `recordCount` −1 abandons the round |
| `Heartbeat` | `clientTimeNs`, `appliedPosition` | liveness, and the `position` of the last `Payload` the client applied (**H-12**) |
| `Detach` | | orderly shutdown |

The façade consumes seqeron's system frames on the host (topology rows, leadership, snapshot rounds,
`SessionClosed`, the connection lifecycle) and calls its listener with what a direct listener receives, so no
system frame crosses to the client and the client needs no frame decoder (`unwrapFrame`,
`SequencedFrameDecoder`). A payload is not re-encoded: the host copies its bytes, and the stub supplies the
`Payload` accessors over the received message. The delivery rules of `client-api.md` ("Message delivery") that
concern the listener apply unchanged.

## 17.4 Rules

- **H-1. The host runs the real façade.** Every rule of §16 that the façade enforces holds for the hosted
  pair as for a direct one, except **A-2**, **A-3** and **A-6**, which are the client's.
- **H-2. Callbacks are in order, once, and the client applies them in stream order.** `Payload`, `Connection`,
  `ClusterTime`, `Gate`, `Role` and `CaughtUp` share stream 301 and are the façade's callbacks in the order it made them. A client
  that sees `Gate` has applied every `Payload` before it, which is what **A-1**'s "caught up" requires of
  state the host cannot see.
- **H-3. A gate epoch fences stale leader work.** `Gate` carries a `gateEpoch`, increased on each change of
  `leading`, including a close and reopen that name the same state, since every leadership change closes an open
  gate. A change of `canPublish` alone (a resend hold beginning or ending) does not increase it. A `Publish` or
  `Reply` carries the epoch of the `Gate` the client last applied; one whose `gateEpoch` is not the current one
  MUST NOT be placed, and the host answers `Declined(EPOCH)`. While `canPublish` is false the host answers
  `Declined(HELD)`. The client runs `OutstandingWork.onNotLeader()` when it applies `Gate` with `leading=false`
  and not on a `canPublish` change, before anything else it does on that event.
- **H-4. A command returns its result from the call, as in a direct façade.** `publish`, `reply` and
  `openConnection` do not return until the host's `Verdict` arrives on stream 303, so the caller has the
  same `Published`, `Declined` or `Refused` from the same call (`protocol.Publish`). `Published` means placed in
  confirmed ingress, not sequenced; `Declined` is retryable (`EPOCH`, `HELD`, `FULL`); `Refused` is final
  (`PAYLOAD_ID`, `GROUP`, `SIZE`). The caller retries as it does against a direct façade, and a `Declined` `Reply`
  releases its dispatch (**A-2**) as a failed reply offer does there. Verdicts have a stream of their own so the
  waiting call reads nothing from stream 301, and no `Payload`, `Gate` or other callback runs inside it. The wait
  is bounded by the façade's own bound on a placement (10 s, `client-api.md`) plus `hostTimeoutMs`, since the
  host sends no `Heartbeat` while the façade spins in the placement; the stub calls `keepAlive()`, which sends
  a client `Heartbeat`, while it waits (**H-12**). Beyond that the host is lost (**H-8**).
- **H-5. One command is outstanding at a time.** A façade is single-threaded, so a client has one command
  without a `Verdict`. Stream 302 need hold one maximum-size command (8,874 bytes of payload plus the envelope) and
  stream 303 one `Verdict`. **While the host spins on a back-pressured stream 301 it also reads stream 302 and
  answers a `Publish`**, as a `reply` inside `onSequenced` is answered in a direct façade; otherwise a client
  blocked on its verdict and a host blocked on stream 301 would wait on each other until `CLIENT_STALLED` (**H-8**).
- **H-6. A snapshot cut is a barrier.** The host sends `SnapshotCut` at the point the façade calls
  `onSnapshot` for the round: after the last `Payload` the façade dispatched before the cut and before any
  later one, and sends nothing but `Heartbeat` until the round's `SnapshotEnd`. The client serializes its state as
  of the cut (**A-6**), one `SnapshotRecord` per `onSnapshot` call, then sends `SnapshotEnd`. **The host keeps the cluster
  session alive while it waits**, by calling the façade's `keepAlive()` on every spin, so the round is not bounded by
  `sequencer.sessionTimeoutMs`. It is bounded by the client: one that has sent neither a `SnapshotRecord` nor a
  `Heartbeat` for `clientTimeoutMs` is a lost client, the host returns −1 from `onSnapshot` (the façade abandons the
  round, deleting its partial file) and closes the façade (`CLIENT_LOST`). A client that serializes for longer than
  `clientTimeoutMs` sends `Heartbeat` from the callback (**H-12**). The host writes the file, computes the length and `crc32c`, and compares
  them with the sequenced `SnapshotEnd` (**A-7**); a mismatch fences `SNAPSHOT_DIVERGED`, and the client's records
  are what diverged.
- **H-7. Restore precedes every `Payload`, by stream order alone.** The façade restores before it dispatches
  anything, calling `onRestore` once per record with no begin, end or count. The host forwards each call as a
  `RestoreRecord` on stream 301 in call order, so the records precede every `Payload` and no handshake is needed. A
  client that receives no `RestoreRecord` starts empty; one that receives them resets its state at `recordIndex` 0.
  The host's `supportsFormatVersion` answers from the client's `supportedFormat` list; a format it lacks fences
  `SNAPSHOT_UNRESTORABLE`.
- **H-8. An attach starts a façade, and a loss ends it.** The host starts its façade on `Attach`, assigns a
  new `attachId` in `Attached`, and, while one attachment is live, ignores an `Attach` carrying its `nonce` and refuses one carrying another.
  A client retries `Attach` until it hears `Attached`, and a follower's host takes about a second to start a façade
  (the IPC ingress it tries first does not answer), so retries are expected and are not a second client. It closes the façade, releasing the cluster session (so a
  standby takes over), on `Detach`, on `clientTimeoutMs` without a `Heartbeat` or `SnapshotRecord` (`CLIENT_LOST`), and on an offer
  to stream 301 back-pressured for the queue-stall timeout (`CLIENT_STALLED`). The host then waits for a new
  `Attach`, which restores and replays from scratch: a restarted client has no state, so the façade never
  resumes mid-stream. **`clientTimeoutMs` MUST NOT exceed `sequencer.sessionTimeoutMs`** (default 5 s), so a
  hung client costs a gateway's pair no longer to hand over than a hung direct gateway does. In return a slow
  client callback no longer endangers the cluster session, since the host's duty cycle keeps it alive, and so does
  its wait in a snapshot cut (**H-6**).
- **H-9. `Reply` carries a request by `clusterSeqNo`.** The host retains the `RequestRef` of every
  application payload it delivered while the client may still reply to it, and releases it when the reply is
  sequenced. A `Reply` for an unknown request is `Refused(REQUEST)`.
- **H-10. After `Fenced` the host sends nothing.** The host releases the cluster session before sending it,
  as the façade does, and the client exits with 70.
- **H-11. A payload is copied once on each side.** The host never decodes a payload, and `Payload` carries
  the queue's payload bytes.
- **H-12. The client heartbeats every duty cycle.** `doWork()` sends `Heartbeat` (rate-limited) and a
  callback that spins calls `keepAlive()`, which sends one, as for the direct façades. The host exports the
  difference between its latest dispatched `position` and `appliedPosition` as a counter labelled with the
  client's `clientId` (`ops.md`), since the client no longer registers counters in the driver itself.
- **H-13. Every message belongs to one attachment.** The host's publications on streams 301 and 303 and the
  client's on stream 302 outlive any one client, so an unread message from a dead attachment can still be in a stream when
  the next one begins, and the new façade's epochs start again. A reader MUST drop a message whose `attachId` is
  not the current one, in both directions. `attachId` is never reused within a host's life (a counter, or a
  random 64-bit value). The client sends `Attach` with no `attachId` and ignores everything but an `Attached` carrying its own `nonce` until it
  arrives, so a reply to a dead client's `Attach` is not taken for its own. A second client's `Attach` while one
  attachment is live is refused (**H-8**).

- **H-14. The stub's queries are mirrors.** `canPublish()`, `isLeading()`, `isServing()`, `canAccept()`,
  `isCaughtUp()` and `lastClusterSeqNo()` return what the stub last applied from `Gate`, `CaughtUp` and `Payload`.
  They lag the host by a hop and the client's polling, so a `publish` that passes the stub's check can still be
  declined: the call's result is authoritative, and a gate that has closed is seen first at the host (**H-3**).

## 17.5 Failure model

| failure | effect |
| --- | --- |
| client crashes or hangs | host fences `CLIENT_LOST`, session released; standby takes over |
| host crashes | the client sees no `Heartbeat` for `clientTimeoutMs`, and exits as if fenced |
| member leader changes | host emits `Gate(leading=false)` in stream order; the client's unanswered leader work is redispatched at the next open (**A-2**) |
| client slower than the queue | host back-pressured, fences `CLIENT_STALLED` |
| host's cluster session lost | the façade's own rules (`client-api.md`, "Guarantees"); `Fenced` is forwarded |
| media driver lost | raises from the host's `doWork()` and from the client's, as for a direct façade |

A hosted pair ends when either process does, so its availability is that of both; the cluster sees either loss
as a direct façade's fence. Nothing in the cluster tier changes, and no frame of schema 210 differs.

## 17.6 Versioning

Schema 218 is exempt from **V-3**, as 212 is: it is unrecorded and never leaves the node. It is versioned with
SBE's `sinceVersion`, because the two ends deploy separately: an application is built and released on its own
schedule, and the host is run by the operator. Within a major version a change only adds fields, and the host
keeps serving the previous minor. `Attach.schemaVersion` is the highest version the client speaks and
`Attached.schemaVersion` is the one both use, the lower of the two; each encodes at it. A major version the host
does not serve is refused with `Refused(VERSION)`. **V-1** still pins the Aeron, Agrona and SBE versions of
the client's driver connection.

## 17.7 Consequences for the tree

- Java host: a `seqeron-service` class composing `Application` or `Gateway` with a remote listener.
  `seqeron-client` stays free of it, so the direction rule holds.
- C++ and C# stubs: `RemoteApplication<Listener>`, `RemoteGateway<Listener>` and `RemoteConsumer<Listener>`
  with the façades' operation names, listener concepts and `Config`. `client-api.md` gains a column for them.
- What a client sheds. C++: the hand-written cluster client (`ClusterStreamSender` and the schema 111
  codecs, the only code that uses them), the archive client, and the ports named above. C#: only the ports,
  since Aeron.NET's `AeronCluster` is already reused, so the cluster-client argument is C++'s alone. A C#
  client still depends on Aeron.NET for the driver connection (**V-1**).
- A host is a JVM per client (§17.1). The JVM is not new to the box, since a member or gateway host runs one, but
  a box with several clients runs several hosts, each its own process and heap, unless one process hosts several
  (not specified here). Open question 6 says where a host runs.
- Retired: the direct C++ and C# façades and what only they use. C++: `ClusterStreamSender`, `PendingSends`,
  `IngressPublisher`/`IngressTracker`, `Session`, `Gateway`, `Application`, `GatewayLifecycle`, `SnapshotTaker`,
  `LeaderGate`, the stall fences, queue and group resolution, `ReplayerRecovery`, `ReplayerStreamReceiver`,
  `SnapshotStore`, `ClusterStreamClient` and `Consumer`, with their tests and the schema 111 codecs. C#: the
  same set minus the cluster sender. About 25,000 hand-written lines before the stubs and the host are
  counted. What stays in a C++ or C# client is the schema 218 codecs, `app` types the stub shares (`Payload`,
  `GroupDirectory`, `OutstandingWork`, the listener concepts), `protocol/Publish` and `util`.
- Conformance is by golden trace, so that the stubs' tests are not three ports of one suite. A trace is streams
  301, 302 and 303 recorded from the Java host through a scripted scenario: attach, restore, steady state, a leader
  change, a hold, a cut, a lost client, a detach. Each stub is tested by replaying the host half into it and
  comparing the commands it emits and the listener calls it makes with the recording; the Java host is the oracle.
  The traces are checked in beside the schema. The §14 rows and `FailoverProcessTest` also rerun against a hosted
  pair.

## 17.8 Open questions

1. **Verdicts.** Decided: as in the current implementation, the call returns its result (**H-4**). The stub blocks
   on the `Verdict`, which costs a round trip of about 1.5 µs against a loop of 235–310 µs, and credit goes away
   with it (**H-5**). Open until built: the host's servicing of stream 302 inside its stream 301 spin (**H-5**),
   which is the one place the host does work re-entrantly, and the timing of the blocking call.
2. **Cost.** The hops measure 0.2–0.5 µs each way with the cluster out of process, and about 1.5 µs for both
   with it in the host's JVM (§17.9.1, §17.9.2); against a C++ `Application` the whole loop shows no
   resolvable difference (§17.9.2). The remaining risk is the host's thread: it needs its own core, since the
   extra spinning threads slowed an in-JVM commit by tens of microseconds until the thread counts matched.
3. **Gateway edge.** Decided: a hosted `Gateway` keeps the direct `Gateway`'s contract call for call, and the edge
   stays in the client. The pair, the election and `GatewayStarted`/`GatewayActive` are the cluster's and are
   unchanged: each instance is a host and a client, and the cluster designates one. The callbacks map one to one:
   `onGroupsResolved` to `GroupsResolved`, `onActivated` and `onStandby` to `Role`, `onSequenced` to `Payload`,
   `onConnectionOpened` and `onConnectionClosed` to `Connection`, `onCaughtUp` to `CaughtUp`, `onClusterHeartbeat`
   to `ClusterTime`, `onFenced` to `Fenced`; and the calls to `Publish`, `OpenConnection`, `CloseConnection` and
   `GateClosed`. Where it differs:
   - **`onActivated` returns a boolean.** The host's listener sends `Role(active, firstConnectionId)` and waits
     for the client's `Activated(opened)`, calling `keepAlive()` as it spins, then returns that value; `false` is
     retried on the next `doWork()`, as in a direct gateway. It blocks the host's duty cycle for as long as the
     client's dial takes, exactly as a direct gateway's `onActivated` blocks its own. A client silent for
     `clientTimeoutMs` is lost (**H-8**).
   - **`openConnection` returns the id.** It blocks on the `Verdict` (**H-4**), which carries the id the façade
     allocated, or `NO_CONNECTION` if it is not serving, as the direct call returns it. `canAccept()` is the last
     `Gate`'s `canPublish`, so a client that checks it before accepting, as the contract asks, rarely sees
     `NO_CONNECTION`. One that does (a gate edge in flight) is followed by the `Role` or `Gate` that closes the
     edge, which drops every connection. `closeConnection` returns nothing and is not answered.
   - **`publish` on a connection returns `Declined` while its `ConnectionOpened` has not landed,** as the direct
     call does; the result comes back in the call (**H-4**).
   - **`onStandby` runs after the façade has stood down,** not before it moves on. A connection accepted in
     that window is dropped by the standby, and its publishes are declined (**H-3**).
   - **A host fault now closes the edge.** A client that loses its host exits (**H-8**), so a host crash drops the
     edge connection where a direct gateway's equivalent was the process; a client crash costs the same as a
     direct one, and the cluster hands over within `clientTimeoutMs`, which is no longer than the session timeout.
   `Activated` is the one message this adds for a gateway.
4. **Supervision.** Decided: as for every other seqeron process, the deployment's supervisor (systemd or
   similar) starts and restarts them, and the repository ships no unit files for any. The host and the client are two
   units, and the protocol makes their order and their restarts independent of each other, so no unit needs to
   name the other. The exit codes are the repository's: 70 for "fenced or lost, restart me", 0 for an orderly stop
   (`ops.md`).
   - **Either may start first.** A client retries `Attach` until it gets an `Attached` with its nonce (**H-8**); a host
     waits for an `Attach`. A client that gets none within its attach timeout exits non-zero and is restarted.
   - **A host fault restarts the client too, without the supervisor linking them.** A host that dies or goes silent
     makes the client exit 70 (`HOST_LOST`), so its own unit restarts it. A host that fences sends `Fenced` and
     exits 70.
   - **A client fault does not restart the host.** On `CLIENT_LOST` the host closes the façade, releasing the cluster
     session, and waits for a new `Attach` (**H-8**); the restarted client attaches to the running host.
   - **A restart is the usual one.** Whatever restarts replays or restores from the newest confirmed snapshot, and a
     gateway returns as a standby, active only when the cluster names it.
   What this adds to an operator's list is a second unit per hosted instance and the host's own counters
   (**H-12**); the two units' restart rates need alerts, since a loop of `HOST_LOST` is a host fault.
5. **Batching.** Decided: batch where the transport already does, and no further. A `Payload` stays one frame per
   message, so a cut, a `Gate` edge or a `Connection` falls between frames and needs no rule about a batch
   boundary. Both ends read in batches: the host takes the queue through the façade's own poll, which is bounded by
   a fragment limit, and the client drains stream 301 up to a fragment limit per duty cycle, answering a batch with
   one `Heartbeat` and its `appliedPosition`. A multi-frame message would save one IPC offer per frame, which
   §17.9 puts at under a microsecond, and it would hold the first frame back until the last is encoded. Open until
   measured: the throughput at which the per-frame offer, not the hop, is the limit (the one 20,000 msg/s pass was
   noise-bound, §17.9).
6. **Where the host runs.** As its own JVM beside the member or `ReplayerServer`, or inside it. Inside
   saves a process and shares the driver, but a host fault then takes the member or Replayer down with it. The
   recommendation: inside `ReplayerServer` on a gateway host, where no consensus runs in that JVM; a JVM of its own
   beside a member, which a host fault must never take down.
7. **Retiring the direct clients.** Decided: they are retired (§17.7). What remains is the sequence: whether one
   release carries both a direct client and its stub, and how a deployment moves from one to the other.
8. **A keep-alive for `Application`.** Decided: `Application` exposes `keepAlive()` as `Gateway` does, and the host
   calls it while it waits in a cut (**H-6**).

## 17.9 Measured hop

Aeron 1.53.2, JDK 23, busy-spinning threads, a macOS laptop (8 performance cores) with no CPU pinning.
Host and client are threads in one JVM, which costs the same IPC as two processes but is not a cross-process
run. §17.9.1 runs the cluster in that JVM too; §17.9.2 runs it as three separate processes and adds the C++
`Application` as the baseline.

### 17.9.1 The real host

A prototype host runs the real `Application` on the leader member's driver, behind two `aeron:ipc` streams to a
client thread. The wire format is the bench's own, not schema 218. Three members run in the same JVM. A direct
`Consumer` beside each phase gives a paired timestamp per `clusterSeqNo`. Stamps are intended send times.

| segment, p50 / p99 | 5k msg/s | 20k msg/s |
| --- | --- | --- |
| send hop: host receives a command, minus its intended send | 0.9 µs / 19 µs | 0.9 µs / 38 µs |
| return hop: client receives a frame, minus the host's `onSequenced` | 0.54–0.63 µs / 6–11 µs | 0.58–0.67 µs / 9–16 µs |
| whole loop, `publish()` until the application sees its own frame sequenced: direct | 264–283 µs | 335–338 µs |
| the same, hosted | 288–302 µs | 349–355 µs |

- **The hops total about 1.5 µs at the median**, about 0.9 µs out and 0.6 µs back. This is 3–5 times the
  relay's 125–250 ns (§17.9.3), the difference being façade dispatch, the body copy and the framing.
- **The blocking `Verdict` (H-4) costs one round trip**, so about 1.5 µs at the median, on top of the
  façade's placement. The prototype's `Publish` is asynchronous and the blocking call has not been timed.
- **The whole-loop difference is about 14 µs at p50 and is not resolved.** It is comparable to the spread of
  the direct p50 between runs (264–283 µs at 5k msg/s). The 0.6–0.9 µs hops do not account for it. The p99 and
  p99.9 of the two loops do not differ beyond that spread.
- **Thread count matters, and must match.** Compared with one spinning application thread, the hosted phase's
  host and client threads slowed the commit itself (a 3-member cluster shares the JVM): +24–34 µs at 5k msg/s
  and +72–88 µs at 20k msg/s at p50, falling to the figure above once the direct phase ran an idle spinner as
  well. A real host takes a core from its machine; give it a dedicated one.
- **The tail is the machine's.** The hop's p99.9 (140–350 µs) matches "direct `Application` minus `Consumer`"
  (120–350 µs), the noise floor here.
- **Phase order does not explain the difference.** Hosted-first and direct-first runs agree.

### 17.9.2 Against the C++ client, cluster out of process

The same loop on the real `Application` of each arm, against three member JVMs as separate processes with
member 0 leading. The C++ arm is `app::Application` built Release against Aeron 1.53.2, with a 20-byte payload,
the Java probe's size. The Java arms run in a JVM of their own, with no `Consumer` beside them; the C++ arm has no
paired reference, so only the whole loop is compared. Runs were interleaved. Whole loop, p50:

| rate | C++ direct | Java direct | Java hosted |
| --- | --- | --- | --- |
| 5k msg/s | 258–286 µs (4 runs) | 260–290 µs (5 runs) | 279–283 µs (5 runs) |
| 20k msg/s | 236–312 µs (3 runs) | 233–351 µs (4 runs) | 247–317 µs (5 runs) |

- **No arm is distinguishable from another.** Hosted is at most about 20 µs above direct at 5k msg/s and level at
  20k msg/s, which is the spread between runs of a single arm. The C++ client is not faster either: the loop is
  dominated by the cluster commit of about 250 µs, and client-side differences are below it.
- **Tails:** p99 is 0.5–0.9 ms at 5k msg/s and 1.0–1.5 ms at 20k msg/s in the quiet runs. One pass at 20k msg/s
  hit every arm, the C++ one included, with p99 of 9–25 ms; that is the machine.
- **The hop is smaller with the cluster out of process:** the return hop is 0.38–0.46 µs at p50 and 12–24 µs at
  p99 at 5k msg/s, and 0.21–0.25 µs and 8–12 µs at 20k msg/s. In §17.9.1 the members' threads compete with the
  host's for cores.
- **Not in this measurement:** a hosted C++ stub. The hosted arm is a Java façade and a Java client thread, so a
  stub's own decode work is not included; it is small beside 250 µs, not zero.

### 17.9.3 A bare relay

The best case: a producer standing in for the queue, and a relay thread that does nothing but copy and
re-offer each message on a second stream. Receive path only.

| message, rate | direct p50 / p99 / p99.9 (ns) | relayed p50 / p99 / p99.9 (ns) | added p50 / p99 / p99.9 (ns) |
| --- | --- | --- | --- |
| 128 B, 100k/s | 125 / 500 / 11,583 | 250 / 3,083 / 18,500 | 125 / 2,583 / 6,917 |
| 128 B, 1M/s | 125 / 458 / 13,791 | 250 / 1,208 / 18,500 | 125 / 750 / 4,709 |
| 1 KiB, 100k/s | 125 / 750 / 22,583 | 375 / 1,500 / 26,542 | 250 / 750 / 3,959 |

### 17.9.4 Not measured

- The C# client, and a hosted C++ or C# stub.
- The blocking verdict path (the prototype's `Publish` is asynchronous and was never declined), schema 218 and a hosted `Consumer`.
  Restore, the snapshot cut and SIGKILL are exercised in §17.10, not timed.
- Latency with host and client in separate processes (§17.9.2 separates only the cluster), and a pinned Linux
  machine.

## 17.10 Prototype: restore, cut and SIGKILL

A Java host runs the real `Application` with snapshots; a Java client holds `TestApplication`'s state (a marker count
and a digest) and writes its one 16-byte record byte for byte. Host and client are child processes beside member
processes, as `HostedSnapshotTest` starts them, with the bench's own wire and not schema 218. The hosted replica is
a fourth replica of one source beside two direct `TestApplication` replicas, so each side compares the other's rounds
with the sequenced `SnapshotEnd` and fences on a difference (**A-7**). Snapshot rounds run every 2 s,
`clientTimeoutMs` is 2 s, and the client's host timeout is 3 s. Two complete runs of the class passed.

| scenario | observed |
| --- | --- |
| steady state, hosted pair on a follower | rounds complete; the façade compared them with the direct publisher's; no `FENCED` |
| hosted pair on the leader | the gate opened, the client's markers were sequenced through `Publish`, a direct replica compared the hosted replica's published ends and no instance fenced |
| client `kill -9`, successor attaches at once | the host refused it as another client until `CLIENT_LOST`, 2.06 s after the kill; the successor attached, the host restored a snapshot and the client held 142–231 markers; rounds resumed |
| host `kill -9` | the client exited 70 on `HOST_LOST`; the restarted pair restored and rounds resumed |
| client `kill -9` mid-cut (the second cut after attach) | the host abandoned the round after `clientTimeoutMs` and did not hang; the direct replicas kept completing rounds; the next attach restored from round 12, the last confirmed, not the abandoned one |
| a command under another `attachId` | dropped and logged by the host; no effect |
| control: the client writes a digest one off | the host fenced `SNAPSHOT_DIVERGED`, sent `Fenced`, the client exited 70, and no direct replica fenced |

Three findings from it are in the rules above: the host could not hold the session while it waited in a cut, which `Application.keepAlive()` now allows (**H-6**), the
façade offers no restore begin, end or count (**H-7**), and `Attach` needs a nonce (**H-8**, **H-13**).

Not covered: leader failover and the gate epoch, the blocking `Verdict` path (no command was declined), a host killed in
a cut, state of more than one record or a record near the 65,535-byte limit, and a C++ or C# stub.
