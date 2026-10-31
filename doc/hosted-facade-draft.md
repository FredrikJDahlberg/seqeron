# §17 Hosted façade protocol (draft)

> **Draft, not normative.** Proposed as §17 of [`seqeron-protocol-spec.md`](seqeron-protocol-spec.md);
> §15 stays unassigned. Rule ids use the new prefix **H**. Schema id 218, streams 301–304 and counter type ids
> 5300–5303 are proposals (§17.8 says where they live); ids in use are schemas 111, 210, 212, 214, 216, streams
> 100, 120–123, 201–203, 206, 2001+, and counter types 5000–5299.

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
The motivation is a smaller code base, and with it less maintenance and complexity; §17.8 sets what is added
against what is retired.
**The first stage is C++ alone**: the direct C++ façades are retired and the hosted stub is the only C++ client.
The C# client stays direct. Java's direct façades stay, since the host is built on them.

The client keeps what is the application's: its state, a deterministic reply (**A-3**), deterministic
snapshot records (**A-6**) and `OutstandingWork` (**A-2**). The host's façade is built with no tracker.

**The protocol reuses Aeron's and the replayer's patterns rather than inventing its own.** The host and client
stand in the relation of Aeron Cluster's consensus module and its clustered service, which Aeron also runs as
two processes over IPC:

| here | precedent |
| --- | --- |
| stream 303: the façade's callbacks, in order | the log stream a clustered service follows |
| stream 304: commands and snapshot records, always read, commands queued on the host (**H-5**) | the service-to-consensus-module stream, read in full, service messages queued in `PendingServiceMessageTracker` |
| a command carries the gate epoch and is dropped once it has passed (**H-3**) | ingress carries `leadershipTermId`, and `ConsensusModuleAgent.onIngressMessage` drops a message of another term |
| `Published` is an offer, which an epoch change can lose without notice (**H-4**) | `MOCKED_OFFER`: a clustered service's offer on a follower succeeds and sends nothing |
| liveness and lag in counters, not messages (§17.2.1) | the driver's client heartbeat-timestamp counter, Aeron's position counters |
| `Attach` with `clientId` and `requestId`, a resend answered again, `AttachPending` (**H-8**) | the replayer's `ReplayRequest`: **R-1**, **R-4**, `ReplayPending` |
| shared streams to attach, then a session's own image (**H-15**) | streams 202/203, then `Replaying.replaySessionId` on stream 201 |
| `SnapshotCut` answered by records and `SnapshotEnd` (**H-6**) | a snapshot in log order answered by `ServiceAck` |

## 17.1 Scope

- Host and client share a host and a media driver: the member's, or a gateway host's. The streams are
  `aeron:ipc`; there is no UDP binding. A client on a host with no driver and no JVM is out of scope.
- Trust is the Aeron directory's: streams 301 and 304 are writable and streams 302 and 303 readable by any process
  that can open the host's directory, so the first process to attach wins (**H-8**). That is the boundary which
  guards the queue and cluster ingress today. Restrict the directory to the application's user; `clientId`
  (**H-15**) separates clients and is not a secret.
- One client per host instance. A host runs one façade, for one `sourceId` (or gateway pair instance, or
  consumer), configured through the host's own properties, and is named by a `hostName` unique on its driver; the
  protocol carries no façade configuration.
- A hosted `Consumer` is the same protocol with fewer messages, as the direct `Consumer` has fewer callbacks: it
  receives `Payload`, `CaughtUp`, `ClusterTime` and `Fenced`, and sends only `Attach` and `Detach`. It has no
  stream 304, no `Gate`, `Role`, `GroupsResolved`, restore or `SnapshotCut`, and the host opens no cluster session
  for it. Its read path (`ReplayerRecovery`, `ReplayerStreamReceiver`) is most of what a client would otherwise
  port, so a consumer that is not hosted is the exception.

## 17.2 Transport

| stream | channel / stream id | direction | publication | carries |
| --- | --- | --- | --- | --- |
| attach requests | `aeron:ipc?term-length=64k`, stream 301 | client → host | shared by every client on the driver | `Attach`, `Detach` |
| attach replies | `aeron:ipc?term-length=64k`, stream 302 | host → client | shared by every host on the driver | `Attached`, `AttachPending` |
| events | `aeron:ipc`, stream 303 | host → client | one exclusive publication per attachment | the façade's callbacks |
| commands | `aeron:ipc?term-length=1m`, stream 304 | client → host | one exclusive publication per attachment | commands, `Activated`, snapshot records |

All four are unrecorded, carry schema 218, and never leave the node. Streams 303 and 304 are reliable. A host
subscribes to 301, and a client to 302, untethered (`tether=false`), as a client subscribes to the replayer's 203: a
reader that stops polling is left behind rather than holding back every other attach on the driver, and attach is
resend-driven (**H-8**), so an `Attach` or reply it missed is sent again. A `Detach` is not resent: a host that
misses one ends the attachment by **H-9** instead. **T-1** applies:
a reader MUST check `MessageHeader.schemaId` is 218. A message above one MTU is fragmented by Aeron, up to an
eighth of its stream's term. Stream 303 takes the driver's 16 MiB IPC default (`ops.md`), since its window is what
absorbs a client that falls behind. Stream 304 takes 1 MiB: the host reads it every duty cycle (**H-5**), so it
needs no depth, and its 128 KiB message limit holds a snapshot record (at most 65,535 bytes) and the largest
command; at the default it would hold 48 MiB of the driver's shared memory per attachment. Streams 301 and 302
carry a few small messages per attachment, so they take Aeron's minimum term, 64 KiB, whose 8 KiB message limit an
`Attach` is far below; at the default they would hold 96 MiB between them.

Streams 301 and 302 are the replayer's 202 and 203 for attachment: every host reads 301, and a client reads 302
only until it is attached; untethered, neither back-pressures another's attach. Streams 303 and 304 are
an attachment's own, as a replay is on stream 201: each side subscribes with the other's session id in the
channel (`aeron:ipc?session-id=`), so a reader sees only its attachment's image and a reliable publication is
held back only by its own peer.

Neither end batches beyond what the transport does: one `Payload` per queue frame, so a cut, a `Gate` edge or a
`Connection` falls between messages and needs no rule about a batch boundary, and each end polls with a fragment
limit per duty cycle.

### 17.2.1 Counters

Liveness and lag travel outside the streams, in the driver's counters, as the driver tracks each Aeron client by a
heartbeat-timestamp counter (`DRIVER_HEARTBEAT_TYPE_ID`). Nothing a stream holds or back-pressures can delay them.

| counter | written by | value | read by |
| --- | --- | --- | --- |
| client heartbeat | client | its monotonic clock in ms, every duty cycle, every spin of a retry (**H-5**) and every `keepAlive()` | host: `CLIENT_LOST` (**H-9**) |
| client applied | client | `position` of the last `Payload` applied | host: lag (**H-14**) |
| host heartbeat | host | its monotonic clock in ms, every duty cycle, every spin of a wait and every spin of the façade's ingress send (**H-10**) | client: `HOST_LOST` (§17.6) |
| host retired | host | total SBE length of the windowed commands that have left the host's queue, placed or dropped | stub: the command window (**H-5**) |

Each side allocates its own two counters with Aeron's `addCounter`, labelled with its `clientId`, before it
attaches; `Attach` and `Attached` carry their ids and registration ids. **A heartbeat is stale when its value has
not changed for the timeout, measured on the reader's own monotonic clock**, so no clock is compared across
processes, and a reader that was itself blocked finds the counter changed and judges the writer alive. A counter its
owner's driver has freed is stale. The driver may reuse a freed id, so a reader also takes a counter as stale once
its state is no longer allocated or its registration id differs from the one it was given.

## 17.3 Messages

Attach, streams 301 and 302:

| message | stream | fields | meaning |
| --- | --- | --- | --- |
| `Attach` | 301 | `clientId`, `requestId`, `hostName`, `kind` (Gateway, Application, Consumer), `protocolVersion`, `aeronVersion`, `commandSessionId`, `heartbeatCounterId` and `appliedCounterId` with their registration ids, `formatVersion`, repeating `supportedFormat` | starts a façade (**H-8**); `commandSessionId` is the client's stream 304 publication, absent for a Consumer |
| `Detach` | 301 | `clientId` | orderly shutdown (**H-9**) |
| `Attached` | 302 | `clientId`, `requestId`, `eventSessionId`, `heartbeatCounterId` and `retiredCounterId` with their registration ids, `commandWindow`, `sourceId` | accepts an `Attach`, or refuses it with `eventSessionId` `NO_ATTACH` (−1) and a `reason` (`BUSY`, `VERSION`, `KIND`), as `Replaying` refuses with `NO_REPLAY_NEEDED` (**R-2**) |
| `AttachPending` | 302 | `clientId`, `requestId` | not now: the host is still closing a previous attachment's façade; resend, as for `ReplayPending` |

Host → client, stream 303:

| message | fields | meaning |
| --- | --- | --- |
| `GroupsResolved` | repeating `name`, `groupId`, `mask` | the `GroupDirectory`, once, before the first `Gate` or `Role` |
| `RestoreRecord` | `recordIndex`, `formatVersion`, `data` | one `onRestore` call, in call order; `recordIndex` 0 means clear state. No records means start empty (**H-7**) |
| `Payload` | `clusterSeqNo`, `groupSeqNo`, `sourceId`, `connectionId`, `groupMask`, `sourceSessionId`, `clusterTimestampNs`, `receiveTimeNs`, `position`, `payloadId`, then the payload bytes with their own `MessageHeader` | `onSequenced`: one application payload, with what the façade's `Payload` exposes |
| `Connection` | `connectionId`, `opened`, `data` | Gateway only: `onConnectionOpened` / `onConnectionClosed` |
| `ClusterTime` | `clusterTimeNs`, `receiveTimeNs` | `onClusterHeartbeat` |
| `Gate` | `epoch`, `leading`, `canPublish`, `caughtUp` | every change of the façade's gate state, in event order: `leading` is `isLeading()` (Application) or `isServing()` (Gateway) and drives `onLeadershipChanged` and `OutstandingWork.onNotLeader()`; `canPublish` is `canPublish()` (Application) or `canAccept()` (Gateway), `leading` and ingress not held (**A-5**), and is what the stub's `canPublish()` returns (**H-16**); `epoch` is the host's (**H-3**) |
| `Role` | `active`, `firstConnectionId` | Gateway only: `onActivated` / `onStandby`, sent before the `Gate` that follows it; the stub allocates connection ids from `firstConnectionId` (**H-4**) |
| `CaughtUp` | `clusterSeqNo` | `onCaughtUp` |
| `SnapshotCut` | `roundId` | write the records for this cut (**H-6**) |
| `Fenced` | `error`, `detail` | `onFenced`: terminal |

Client → host, stream 304. The host queues a command and places it later (**H-3**, **H-5**); it handles the
rest as it reads them:

| message | fields | handled | meaning |
| --- | --- | --- | --- |
| `Publish` | `epoch`, `connectionId`, `groupMask`, `payloadId`, `payload` | queued | `publish` (**H-4**) |
| `Reply` | `epoch`, `sourceId`, `connectionId`, `groupMask`, `payloadId`, `payload` | queued | `reply`; the three ids are the request's `RequestRef` (**H-11**) |
| `OpenConnection` | `epoch`, `connectionId`, `data` | queued | Gateway only: `openConnection`, with the id the stub allocated (**H-4**) |
| `CloseConnection` | `epoch`, `connectionId` | queued | Gateway only: `closeConnection` |
| `GateClosed` | `epoch` | queued | Gateway only: the edge closed on its own (`gateClosed()`) |
| `Activated` | `opened` | on read | Gateway only: the result of `onActivated`, answering `Role` (§17.5) |
| `SnapshotRecord` | `roundId`, `recordIndex`, `data` | on read | one `onSnapshot` result (**H-6**) |
| `SnapshotEnd` | `roundId`, `recordCount` | on read | no more records; `recordCount` −1 abandons the round |

The façade consumes seqeron's system frames on the host (topology rows, leadership, snapshot rounds,
`SessionClosed`, the connection lifecycle) and calls its listener with what a direct listener receives, so no
system frame crosses to the client and the client needs no frame decoder (`unwrapFrame`,
`SequencedFrameDecoder`). A payload is not re-encoded: the host copies its bytes, and the stub supplies the
`Payload` accessors over the received message. The delivery rules of `client-api.md` ("Message delivery") that
concern the listener apply unchanged.

## 17.4 Rules

- **H-1. The host runs the real façade.** Every rule of §16 that the façade enforces holds for the hosted
  pair as for a direct one, except **A-2**, **A-3** and **A-6**, which are the client's. The host adds two of its
  own: it queues the client's commands (**H-5**), and it drops a command whose epoch has passed (**H-3**), which a
  direct `Gateway` would place, since `Gateway.publish` checks no gate.
- **H-2. Events are the façade's callbacks, in order, once, applied in stream order.** `Payload`, `Connection`,
  `ClusterTime`, `Gate`, `Role` and `CaughtUp` share stream 303 and are the façade's callbacks in the order it made
  them. A client that sees `Gate` has applied every `Payload` before it, which is what **A-1**'s "caught up"
  requires of state the host cannot see.
- **H-3. A command is placed only in the epoch it was decided in.** The host's epoch rises at every edge of
  `leading`, opening or closing, the close and reopen of a new term with the same leader included; a change of
  `canPublish` or `caughtUp` alone leaves it. `Gate` carries it, and the stub stamps every queued command with the
  epoch of the last `Gate` it applied, or the one above it once it has answered `Activated(true)` (§17.5). The host
  places the command at the head of its queue when that epoch is current and `leading` is true, and drops it,
  counted, otherwise, as the consensus module acts on ingress only in its current term and drops the rest without a
  reply. A command is therefore never placed under a gate other than the one its client decided it under: one in
  flight over an edge is dropped, never placed late. What a client loses at an edge is exactly what it stamped with
  an earlier epoch that the host had not yet placed. The client runs `OutstandingWork.onNotLeader()` when it applies
  `Gate` with `leading=false`, before anything else it does on that event, and **A-2** redispatches the tracked work
  at the next open.
- **H-4. A command is an offer, as in a direct façade.** `publish` and `reply` return at once with the result of an
  offer (`protocol.Publish`), `openConnection` with an id or `NO_CONNECTION`, and the host sends nothing back.
  `Published` means the stub offered the command to stream 304, which the host places if its epoch is still current
  when it reaches the head of the queue (**H-3**); the host's confirmed ingress (**A-4**, **A-5**) carries it on
  from there. It is what an Aeron Cluster offer means: an epoch change can lose it without notice (§17.5).
  `Declined` is retryable: the stub's mirror says the gate is shut or ingress held (**H-16**), the command window is
  full (**H-5**), or stream 304 is back-pressured or not yet connected. `Refused` is final and is the stub's own
  check, the direct façade's local ones: a payload above `MAX_PAYLOAD_LENGTH`, a retired `payloadId`, a `groupMask`
  naming a group `GroupsResolved` lacks. The caller retries as against a direct façade, and a `Declined` `Reply`
  releases its dispatch (**A-2**) as a failed reply offer does there.
  `openConnection` takes its id from the stub: ids start at the last `Role`'s `firstConnectionId` and count up, as
  the façade allocates them. The stub returns `NO_CONNECTION` when its mirror says it is not serving, the command
  window is full, or stream 304 is back-pressured or not yet connected, and takes the id only once the offer lands,
  so a `NO_CONNECTION` uses none up. While `canAccept()` is true, `NO_CONNECTION` is retryable, as `Declined` is.
  The host opens the connection through the façade when it places the `OpenConnection`, and fences if the façade
  allocates a different id.
- **H-5. The host reads stream 304 every duty cycle, and queues the commands.** As the consensus module reads a
  service's messages and acks from one stream and appends the messages from its own tracker, the host never stops
  reading at a command it cannot place yet. It moves each queued message (§17.3) into a FIFO of its own and handles
  the others as it reads them, including while it waits in `onActivated` or in a cut. The one stretch it reads
  neither 301 nor 304 is a façade send that spins through an election, up to the send's 10 s stall bound (**H-10**).
  Each duty cycle it places commands from the head of the queue by **H-3**, up to a per-cycle limit, until the queue
  is empty or the façade declines one (ingress held, its tracker full, the transport back-pressured, or a `Publish`
  whose connection's `ConnectionOpened` is not yet placed); that one stays at the head for the next cycle. The host
  places commands only from its own duty cycle, never inside a façade callback, so a `reply` made inside
  `onSequenced` is placed on a later cycle, in the order the commands were offered.
  **The command window bounds the queue.** It covers `Publish`, `Reply` and `OpenConnection`, the three calls the
  stub can decline (`Declined`, or `NO_CONNECTION` from `openConnection`, **H-4**), and counts each by its SBE
  length: its `MessageHeader` and body, the length the stub encodes and the host reads, not the Aeron frame. The
  host retired counter rises by that length as each leaves the queue, placed or dropped. The stub keeps the same sum
  for the windowed commands it has offered and declines one that would put more than `commandWindow` bytes (from
  `Attached`) between the two. `commandWindow` is at least the length of a `Reply` carrying a payload of
  `MAX_PAYLOAD_LENGTH`, so every command fits it, and at most stream 304's publication window, half its 1 MiB term
  or the driver's `aeron.ipc.publication.term.window.length` if that is smaller, so the window rather than the
  stream's back-pressure declines a command. The window bounds the host's queue and the time a command waits in it,
  and the host fences a client that exceeds it.
  **What cannot be declined is outside the window.** `closeConnection()` and `gateClosed()` return nothing, and
  `Activated`, `SnapshotRecord` and `SnapshotEnd` answer the host, so the stub retries each offer within the call
  until it lands, writing the client heartbeat on every spin, as a direct `closeConnection` puts its work in a
  backlog rather than fail. The retry is bounded by the host's duty cycle, or by the send spin when the host is in
  one, and the heartbeat keeps a host that returns from that spin from judging the client lost. The host's queue
  holds at most the window, one `CloseConnection` per open connection and one `GateClosed`.
- **H-6. A snapshot cut is a barrier.** The host sends `SnapshotCut` at the point the façade calls `onSnapshot` for
  the round: after the last `Payload` the façade dispatched before the cut and before any later one, and sends
  nothing more on stream 303 until the round's `SnapshotEnd`. The client serializes its state as of the cut
  (**A-6**), one `SnapshotRecord` per `onSnapshot` call, then sends `SnapshotEnd`. The host ends its poll of stream
  304 after each `SnapshotRecord`, so it holds at most one, and returns it from the `onSnapshot` call it waits in,
  0 for `SnapshotEnd` and −1 for one that abandons; commands it reads meanwhile are queued, not placed. **The host
  keeps the cluster session alive while it waits**, by calling the façade's `keepAlive()` on every spin, so the
  round is not bounded by `sequencer.sessionTimeoutMs`. It is bounded by the client: when the client heartbeat
  counter is stale for `clientTimeoutMs`, the host returns −1 from `onSnapshot` (the façade abandons the round,
  deleting its partial file) and closes the façade (`CLIENT_LOST`). A client that serializes for longer than
  `clientTimeoutMs` calls `keepAlive()` from the callback (**H-14**). The host writes the file, computes the length
  and `crc32c`, and compares them with the sequenced `SnapshotEnd` (**A-7**); a mismatch fences `SNAPSHOT_DIVERGED`,
  and the client's records are what diverged.
- **H-7. Restore precedes every `Payload`, by stream order alone.** The façade restores before it dispatches
  anything, calling `onRestore` once per record with no begin, end or count. The host forwards each call as a
  `RestoreRecord` on stream 303 in call order, so the records precede every `Payload` and no handshake is needed. A
  client that receives no `RestoreRecord` starts empty; one that receives them resets its state at `recordIndex` 0.
  The host's `supportsFormatVersion` answers from the client's `supportedFormat` list; a format it lacks fences
  `SNAPSHOT_UNRESTORABLE`.
- **H-8. An attach starts a façade.** `Attach` follows the replayer's request rules: `clientId` is drawn once from
  the client's driver (`Aeron.nextCorrelationId`, **R-4**), and `requestId` rises with every new `Attach` and is
  kept on a resend (**R-1**). Before it attaches, the client allocates its counters and, unless it is a Consumer,
  creates its exclusive publication on stream 304. A host answers only an `Attach` naming its own `hostName`. It
  refuses a `protocolVersion` or `aeronVersion` other than its own with `VERSION` (§17.7), and a `kind` other than
  its own with `KIND`, and answers `AttachPending` while it is still closing a previous attachment's façade.
  Otherwise it creates its stream 303 publication, subscribes to the client's stream 304 image and answers
  `Attached` at once. It builds the façade only once that publication is connected, which only the attached client's
  subscription makes it, so every callback has a reader, restore included, which the façade runs in its constructor;
  it loses a client whose subscription does not connect within `clientTimeoutMs`. An attachment begins at the
  `Attached` that accepts it; until it ends, the host answers a resend of that request with the same `Attached`, any
  other `Attach` with `Attached(NO_ATTACH, BUSY)`, and acts on a `Detach` only from that `clientId`. A client
  resends `Attach` until it hears `Attached` or a refusal, then closes its stream 302 subscription; it retries on
  `BUSY`, exits 78 on `VERSION` or `KIND` (§17.6), and exits 70 after its attach timeout.
- **H-9. A loss ends it.** The host closes the façade, releasing the cluster session (so a standby takes over): on
  `Detach`, discarding the commands it has not placed, as a direct `close()` discards what it has not placed; when
  the client heartbeat counter is stale for `clientTimeoutMs` or the client's stream 304 image goes away
  (`CLIENT_LOST`; a Consumer, which has no stream 304, by its counter alone); and when an offer to stream 303 stays
  back-pressured while the publication's limit has not advanced for half the queue-stall timeout (`CLIENT_STALLED`).
  The limit advances with whatever the client reads, so a client that is slow but reading is not stalled, as **R-3**
  judges a replay by its publication's limit (`AeronReplayer.replayProgress`). Half the timeout puts
  `CLIENT_STALLED` ahead of the façade's own `QUEUE_STALLED`: the façade checks that fence only in `doWork()`, which
  cannot run during the spin, and counts from a `ClusterHeartbeat` dispatched at most a heartbeat period before the
  spin began. The host then waits for a new `Attach`, which restores and replays from scratch: a restarted client
  has no state, so the façade never resumes mid-stream.
- **H-10. `clientTimeoutMs` MUST NOT exceed `sequencer.sessionTimeoutMs`** (default 5 s), so a hung client costs a
  gateway's pair no longer to hand over than a hung direct gateway does. In return the host's duty cycle keeps the
  cluster session alive while a client callback runs, and so does its wait in a snapshot cut (**H-6**); a slow
  callback costs the cluster no more than a slow direct one. Liveness is read from a counter, so time the host
  spends blocked in the façade, in a send that spins through an election, costs the client nothing. **Nor need it
  cost the host**, once the façade's builder takes the `IdleStrategy` that send spins on, as `AeronCluster.Context`
  takes one (§17.8; today it is a fixed `YieldingIdleStrategy`): the host's idle strategy writes the host heartbeat
  on every spin. The client's `hostTimeoutMs`, the counterpart of `clientTimeoutMs`, is therefore not tied to the
  send's 10 s stall bound. A hung host's session times out within `sequencer.sessionTimeoutMs` whatever the client
  does, so `hostTimeoutMs` decides only how soon the client restarts the pair.
- **H-11. `Reply` names its request by value.** `Payload` carries the request's `sourceId`, `connectionId` and
  `groupMask`, which are its `RequestRef`, and `Reply` carries the same three, as the direct `reply` takes a
  `RequestRef` the application kept. The host retains nothing per request.
- **H-12. After `Fenced` the host sends nothing.** The host releases the cluster session before sending it,
  as the façade does, and the client exits with 70.
- **H-13. A payload is copied once.** The host never decodes a payload: it copies the queue's payload bytes into
  `Payload`, and the stub reads them in place (§17.3).
- **H-14. Liveness and lag are counters** (§17.2.1). A callback that spins calls `keepAlive()`, as for the direct
  façades. The client writes its applied counter after each `Payload` it applies, and the host exports the
  difference between its latest dispatched `position` and that counter, labelled with the client's `clientId`
  (`ops.md`).
- **H-15. An attachment is a pair of images.** Events and commands travel on exclusive publications made for the
  attachment and read by session id, so a message a dead attachment left in a stream is on an image the next
  attachment never reads, and no message on streams 303 and 304 carries an attachment id. On streams 301 and 302 a
  client ignores a reply carrying another `clientId` (**R-4**) or a `requestId` other than its current one
  (**R-1**).
- **H-16. The stub's queries are mirrors.** `canPublish()`, `isLeading()`, `isServing()`, `canAccept()`,
  `isCaughtUp()` and `lastClusterSeqNo()` return what the stub last applied from `Gate`, `CaughtUp` and `Payload`,
  and from its own `Activated(true)` (§17.5). They lag the host by a hop and the client's polling, so a `publish`
  that passes the stub's check can reach the host after the gate has closed, where its epoch has passed and it is
  dropped (**H-3**). The `Gate` that closed it reaches the client in stream order, and `OutstandingWork`
  redispatches (**A-2**) as it does for work the cluster lost at a failover.

## 17.5 Where a hosted façade differs from a direct one

What `client-api.md` states for the hosted column. Every callback maps one to one: `onGroupsResolved` to
`GroupsResolved`, `onLeadershipChanged` to `Gate`, `onActivated` and `onStandby` to `Role`, `onSequenced` to
`Payload`, `onConnectionOpened` and `onConnectionClosed` to `Connection`, `onCaughtUp` to `CaughtUp`,
`onClusterHeartbeat` to `ClusterTime`, `onFenced` to `Fenced`; and the calls to `Publish`, `Reply`,
`OpenConnection`, `CloseConnection` and `GateClosed`. The pair, the election and `GatewayStarted`/`GatewayActive`
are the cluster's and are unchanged: each gateway instance is a host and a client, and the cluster designates one.
Where they differ:

- **`Published` can be lost at a gate edge without notice** (**H-3**, **H-4**), where the direct call that would
  have reached a shut gate returns `Declined`. **An Application MUST track in `OutstandingWork` (A-2) every
  publish whose loss it cannot accept, keyed on the sequenced event that caused it**: the request a reply answers,
  or the `ClusterTime` tick a timer fires on. Every publish of an Application is leader-only, and its only input is
  the sequenced stream, so every such publish has a sequenced cause; `OutstandingWork` holds it on every replica
  from that event (`onRequest`) until its sequenced result (`onReply`), and the next open redispatches it. For a
  reply this is **A-2** already. What hosting adds is that a publish answering no request, a timer's, must be
  tracked too, where a direct Application can leave a `Published` one to confirmed ingress (**A-4**, **A-5**).
  `Published` keeps its name but not that guarantee, so moving a C++ Application to the stub changes its contract
  (§17.9, question 5). A Gateway's commands lost at an edge belong to connections the same edge dropped.
- **`onActivated` returns a boolean.** The host's listener sends `Role(active, firstConnectionId)` and waits for
  the client's `Activated(opened)`, calling `keepAlive()` as it spins, then returns that value; `false` is retried
  on the next `doWork()`, as in a direct gateway. It blocks the host's duty cycle for as long as the client's dial
  takes, as a direct gateway's `onActivated` blocks its own. A direct gateway serves as soon as `onActivated`
  returns true, so a stub that answers `Activated(true)` mirrors `leading` and `canPublish` as true at once and
  stamps with the epoch one above its last `Gate`'s, the one the host's open edge takes (**H-3**); the `Gate` that
  follows confirms it. An initiator can therefore open the connection it dialled without waiting for that `Gate`.
- **`openConnection` returns an id the stub allocated** (**H-4**). `canAccept()` is the last `Gate`'s `canPublish`,
  or true from the stub's own `Activated(true)`, so a client that checks it before accepting, as the contract asks,
  rarely sees `NO_CONNECTION`. A direct gateway returns it only when not serving; the stub also returns it for a
  full window or a back-pressured stream 304, which the caller retries (**H-4**). A connection accepted while an
  edge is in flight is dropped by the host by its epoch and followed by the `Role` or `Gate` that closes the edge,
  which drops every connection. `closeConnection` returns nothing.
- **`publish` on a connection whose `ConnectionOpened` is unplaced is `Published`**, where the direct call returns
  `Declined`: the command waits in the host's queue behind its `OpenConnection` (**H-5**).
- **`onStandby` runs after the façade has stood down,** not before it moves on. A connection accepted in that
  window is dropped by the standby, and its commands by their epoch.
- **The queries lag** by a hop (**H-16**).
- **A host fault ends the client.** A client that loses its host exits (§17.6), so a host crash drops a gateway's
  edge where a direct gateway's equivalent was the process; a client crash costs the same as a direct one, and the
  cluster hands over within `clientTimeoutMs`, which is no longer than the session timeout.

## 17.6 Failure model and supervision

| failure | effect |
| --- | --- |
| client crashes or hangs | its heartbeat counter is stale for `clientTimeoutMs` or its stream 304 image goes away: host closes the façade (`CLIENT_LOST`), session released; standby takes over |
| host crashes or hangs | its heartbeat counter is stale for `hostTimeoutMs` or its stream 303 image goes away: the client exits 70 (`HOST_LOST`); the cluster times out the host's session within `sequencer.sessionTimeoutMs` regardless |
| member leader changes | host emits `Gate(leading=false)` with a new epoch in stream order and drops every command of an earlier epoch (**H-3**); the client's unanswered leader work is redispatched at the next open (**A-2**) |
| client slower than the queue | host back-pressured and the client reads nothing of stream 303 for half the queue-stall timeout: `CLIENT_STALLED`; a slow client that keeps reading is not fenced |
| host's cluster session lost | the façade's own rules (`client-api.md`, "Guarantees"); `Fenced` is forwarded |
| media driver lost | raises from the host's `doWork()` and from the client's, as for a direct façade |

A hosted pair ends when either process does, so its availability is that of both; the cluster sees either loss
as a direct façade's fence, within the same `sequencer.sessionTimeoutMs`. The one failure a direct client does not
have is the loss of the host process itself, which costs the client a restart. Nothing in the cluster tier
changes, and no frame of schema 210 differs.

**Supervision is the deployment's**, as for every other seqeron process: its supervisor (systemd or similar)
starts and restarts them, and the repository ships no unit files for any. The host and the client are two units,
and the protocol makes their order and their restarts independent of each other, so no unit names the other. The
exit codes are the repository's: 70 for "fenced or lost, restart me", 0 for an orderly stop (`ops.md`). The client
adds 78 for an attach the host refused with `VERSION` or `KIND`: a configuration error a restart does not cure, so
its unit does not restart on 78 (systemd's `RestartPreventExitStatus=78`).

- **Either may start first.** A client resends `Attach` until it is answered (**H-8**); a host waits for one.
- **A host fault restarts the client too, without the supervisor linking them.** A host that dies or goes silent
  makes the client exit 70 (`HOST_LOST`), so its own unit restarts it. A host that fences sends `Fenced` and
  exits 70.
- **A client that crashes or hangs does not restart the host.** On `CLIENT_LOST` the host closes the façade,
  releasing the cluster session, and waits for a new `Attach` (**H-9**); the restarted client attaches to the
  running host. A client that breaks the protocol, by exceeding the command window (**H-5**) or opening a
  connection under an id the façade did not allocate (**H-4**), or whose records diverge (**H-6**), fences the
  façade, and the host exits 70 with it.
- **A restart is the usual one.** Whatever restarts replays or restores from the newest confirmed snapshot, and a
  gateway returns as a standby, active only when the cluster names it.

What this adds to an operator's list is a second unit per hosted instance and the host's counters (**H-14**); the
two units' restart rates need alerts, since a loop of `HOST_LOST` is a host fault.

## 17.7 Versioning

Schema 218 is exempt from **V-3**, as 212 is: it is unrecorded and never leaves the node. It changes in place with
no version bump, as 212 does: `version` stays 0 and the schema has no `sinceVersion` fields. `Attach` carries
`protocolVersion`, a constant of `HostedProtocol` raised with every change to schema 218 or to a rule of §17, and a
host refuses a client of another with `Attached(NO_ATTACH, VERSION)`. A host and its client therefore agree on the
protocol, not on the release, so a release that leaves §17 alone does not make them upgrade together. That is the
one check beyond **T-1**: the replayer's clients are restarted with it and need none, but a mismatch here would
otherwise surface as a decode failure at runtime. Two releases may pin different Aeron versions, so `Attach` also
carries `aeronVersion`, refused with `VERSION` too: **V-1** still pins the Aeron, Agrona and SBE versions of the
client's driver connection.

## 17.8 Consequences for the tree

- Java host: `org.limitless.seqeron.proxy.ClientProxy`, a `seqeron-service` class composing `Application`, `Gateway` or `Consumer` with a remote listener and
  the command queue. `seqeron-client` stays free of it, so the direction rule holds.
- Java client tier: the `Application` and `Gateway` builders take the `IdleStrategy` that `ClusterStreamSender.send`
  spins on, `YieldingIdleStrategy` by default as now (**H-10**). It is a change to the client tier's surface, so the
  C# façades' options and `client-api.md` follow it, and the direct C++ façades until they are retired.
- Where the ids live. Streams 301–304, `NO_ATTACH`, the schema id and the protocol version (§17.7) are a
  `ProxyProtocol` class in `protocol`,
  beside `ReplayProtocol`, with a C++ twin, since the proxy and the stub share them; the channels are in the Java
  class only, since the two ends build theirs differently. The four counters take a new
  range of `SeqeronCounters`, 5300–5399, after the sequencer's, the replayer's and the façades', and `MAX_TYPE_ID`
  widens to it, so `clusterctl` lists them; the three copies of `SeqeronCounters` stay in step. The C++ stub
  connects only to its own driver and needs no port, so C++ `PortLayout` retires with the direct façades, and
  `cluster_probe`, rewritten against the stub, no longer needs it. `ports.sh` is then mirrored by the Java and C#
  `PortLayout`s alone, and the rule that they change together covers three files rather than four.
- C++ stubs: `RemoteApplication<Listener>`, `RemoteGateway<Listener>` and `RemoteConsumer<Listener>`
  with the façades' operation names, listener concepts and `Config`. Their counters are Aeron's `addCounter` and
  `CountersReader`, an Aeron API used rather than re-implemented. `client-api.md` gains a column for them. C#
  stubs are not in the first stage.
- What a client sheds. C++: the hand-written cluster client (`ClusterStreamSender` and the schema 111
  codecs, the only code that uses them), the archive client, and the ports named above. C#, if it follows: only
  the ports, since Aeron.NET's `AeronCluster` is already reused, so the cluster-client argument is C++'s alone.
  A C# client still depends on Aeron.NET for the driver connection (**V-1**).
- A host is a JVM per client (§17.1). The JVM is not new to the box, since a member or gateway host runs one, but
  a box with several clients runs several hosts, each its own process and heap. The host's GC, JIT and safepoint
  pauses sit in the C++ client's path in both directions, so a C++ client inherits the host's tail. Open questions
  2, 4 and 6 say what that costs, where a host runs and whether one process hosts several.
- Retired in the first stage: the direct C++ façades and what only they use: `ClusterStreamSender`, `PendingSends`,
  `IngressPublisher`/`IngressTracker`, `Session`, `Gateway`, `Application`, `GatewayLifecycle`, `SnapshotTaker`,
  `LeaderGate`, the stall fences, queue and group resolution, `ReplayerRecovery`, `ReplayerStreamReceiver`,
  `SnapshotStore`, `ClusterStreamClient`, `Consumer`, `SequencedFrame` and `PortLayout`, with their tests and the
  schema 111 codecs: about 9,000 of the 10,100 lines under `seqeron-client/src/main/cpp`, and most of its 9,100 test
  lines, before the generated codecs. What stays in the C++ client is the schema 218 codecs, the `app` types the
  stub shares (`Payload`, `GroupDirectory`, `OutstandingWork`, `ClusterError`, `SnapshotListener`, the listener
  concepts), `protocol/Publish`, `SeqeronCounters` and `util`. The C# façades, the same set minus the cluster
  sender, stay, and with them the Java/C# parity they carry.
- Net accounting, first stage. Retired: the C++ list above. Added and maintained: the Java host and its command
  queue, the schema 218 codecs in Java and C++, the C++ stubs with their three local publish checks and the
  command window (**H-4**, **H-5**), the golden traces and their replay harness, and the host's counters.
  Added complexity that is not lines: a second protocol (§17.7), two-process failure modes and a second unit to
  supervise per instance (§17.6), and a JVM and a core per client (§17.10.1). The parity the C++ client carried
  does not go with it: `ReplayerRecovery`, `PendingSends` and the façades stay case for case in Java and C#, two
  languages rather than three. **The added lines are only partly counted, and the first stage is not decided until
  they are.** What exists: the C++ stubs of `Application`, `Gateway` and `Consumer` share a core,
  `hosted/detail/Attachment.hpp`, of 612 lines with its comments, and are 140, 252 and 49 lines on top of it (a build of
  the direct `Application` and what only it uses is part of the ~9,000 above), the schema is 217 lines of XML and its
  generated C++ codecs 16,800 more, and the host, `ClientProxy`, which runs any of the three façades, is about 910 lines of Java with its
  configuration checks and the allocation window, plus `Liveness`, `CommandLedger` and `AttachRules`, its Aeron-free seams. A C# stub and the golden-trace harness are not counted,
  so it is a floor, not the answer.
- Conformance is by golden trace, so that the stubs' tests are not three ports of one suite. A trace is streams
  303 and 304 and the counters recorded from the Java host through a scripted scenario: attach, restore, steady
  state, a leader change, a hold, a cut, a lost client, a detach. Each stub is tested by replaying the host half
  into it, under a scripted listener that makes the scenario's calls, and comparing the commands it emits and the
  listener calls it makes with the recording; the Java host is the oracle. Session ids, counter ids, timestamps
  and positions differ between runs, so a trace records them normalized and the comparison ignores them. The
  traces are checked in beside the schema, with a task that regenerates them from the host and one that checks
  the committed copy is current, as for the SBE codecs. The §14 rows and `FailoverProcessTest` also rerun against
  a hosted pair.

## 17.9 Open questions

1. **The command window.** Built in the prototype host (§17.11): a queue sized to the window, a retired counter the
   stub reads, no reply to a drop. With a tracker of four and a window of 1,024 bytes (about seventeen
   `Publish` messages), a client flooding 20,000 markers had every one placed and sequenced exactly once and in order,
   the queue peaked at 986 bytes, the façade declined at the head and the stub at the window; a stub that ignored the
   window was closed by the host. A gate edge dropped 17 queued commands, counted, and none was placed late. Still
   open: the size for a real deployment (the prototype's 64 KiB default was never reached outside the test), a held
   ingress of seconds rather than an edge, and `Reply` and `OpenConnection` in the window, which the prototype lacks.
2. **Cost.** The hops measure 0.2–0.5 µs each way with the cluster out of process, and about 1.5 µs for both
   with it in the host's JVM (§17.10.1, §17.10.2), on the laptop of §17.10; against a C++ `Application` the whole
   loop shows no resolvable difference (§17.10.2). The remaining risk is the host's thread: it needs its own core,
   since the extra spinning threads slowed an in-JVM commit by tens of microseconds until the thread counts
   matched. The laptop's commit of about 250 µs hides the hop; a tuned cluster commits faster, and there the hop
   and the host's pauses weigh more. **The first stage waits on two results**: the proposed shape measured, a C++
   client process beside a separate host JVM, pinned, on Linux; and an allocation test that holds the host's
   steady state to zero bytes a frame, as the C# suite does for its client. The second is met by the
   prototype host (§17.10.5). The first is measured in an aarch64 Linux VM (§17.10.6) and open on the deployment's
   own hardware: with the host spinning, a C++ client beside it shows no resolvable difference at the median, and
   a host that backs off adds 66–107 µs.
3. **Throughput.** The rate at which the per-message offer, not the hop, is the limit is unmeasured (the one
   20,000 msg/s pass was noise-bound, §17.10). A multi-message `Payload` would save one IPC offer per frame, under
   a microsecond (§17.10), and would hold the first frame back until the last is encoded.
4. **Where the host runs.** As its own JVM beside the member or `ReplayerServer`, or inside it. Inside
   saves a process and shares the driver, but a host fault then takes the member or Replayer down with it. The
   recommendation: a JVM of its own beside a member, which a host fault must never take down. On a gateway host
   it is open: inside `ReplayerServer` no consensus runs in that JVM, but a host fault takes down recovery for
   every consumer that replayer serves.
5. **Retiring the direct clients.** C# is not in the first stage. Open until the measurements of §17.10.4:
   retiring the direct C++ façades (§17.8), whether one release carries both the direct client and its stub,
   and how a deployment moves from one to the other. Before it moves, every C++ Application must track its
   publishes as §17.5 requires; one that retries a `Declined` publish instead loses that publish when hosted.
6. **Several façades in one host.** A box with several clients runs several JVMs unless one process hosts several
   façades, each with its own `hostName`; not specified here.

## 17.10 Measured hop

Aeron 1.53.2, JDK 23, busy-spinning threads, a macOS laptop (8 performance cores) with no CPU pinning.
Host and client are threads in one JVM, which costs the same IPC as two processes but is not a cross-process
run. §17.10.1 runs the cluster in that JVM too; §17.10.2 runs it as three separate processes and adds the C++
`Application` as the baseline.

### 17.10.1 The real host

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
  relay's 125–250 ns (§17.10.3), the difference being façade dispatch, the body copy and the framing.
- **A command is an offer (H-4)**, so a `publish` adds the one-way send hop, about 0.9 µs at the median, to the
  façade's placement, and no round trip. The prototype's `Publish` is asynchronous too; this bench's host answers a
  decline, which the prototype host of §17.11 no longer does.
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

### 17.10.2 Against the C++ client, cluster out of process

The same loop on the real `Application` of each arm, against three member JVMs as separate processes with
member 0 leading. The C++ arm is `app::Application` built Release against Aeron 1.53.2, with a 20-byte payload,
the Java probe's size. The Java arms run in a JVM of their own, with no `Consumer` beside them; the C++ arm has no
paired reference, so only the whole loop is compared. Runs were interleaved. Whole loop, p50:

| rate | C++ direct | Java direct | Java hosted |
| --- | --- | --- | --- |
| 5k msg/s | 258–286 µs (4 runs) | 260–290 µs (5 runs) | 279–283 µs (5 runs) |
| 20k msg/s | 236–312 µs (3 runs) | 233–351 µs (4 runs) | 247–317 µs (5 runs) |

- **No arm is resolved from another.** Hosted is at most about 20 µs above direct at 5k msg/s and within the
  spread at 20k msg/s, which is the spread between runs of a single arm. The C++ client is not faster either: the
  loop is dominated by the cluster commit of about 250 µs, and client-side differences are below it.
- **Tails:** p99 is 0.5–0.9 ms at 5k msg/s and 1.0–1.5 ms at 20k msg/s in the quiet runs. One pass at 20k msg/s
  hit every arm, the C++ one included, with p99 of 9–25 ms; that is the machine.
- **The hop is smaller with the cluster out of process:** the return hop is 0.38–0.46 µs at p50 and 12–24 µs at
  p99 at 5k msg/s, and 0.21–0.25 µs and 8–12 µs at 20k msg/s. In §17.10.1 the members' threads compete with the
  host's for cores.
- **Not in this measurement:** a hosted C++ stub. The hosted arm is a Java façade and a Java client thread, so a
  stub's own decode work is not included; it is small beside 250 µs, not zero.

### 17.10.3 A bare relay

The best case: a producer standing in for the queue, and a relay thread that does nothing but copy and
re-offer each message on a second stream. Receive path only.

| message, rate | direct p50 / p99 / p99.9 (ns) | relayed p50 / p99 / p99.9 (ns) | added p50 / p99 / p99.9 (ns) |
| --- | --- | --- | --- |
| 128 B, 100k/s | 125 / 500 / 11,583 | 250 / 3,083 / 18,500 | 125 / 2,583 / 6,917 |
| 128 B, 1M/s | 125 / 458 / 13,791 | 250 / 1,208 / 18,500 | 125 / 750 / 4,709 |
| 1 KiB, 100k/s | 125 / 750 / 22,583 | 375 / 1,500 / 26,542 | 250 / 750 / 3,959 |

### 17.10.4 Not measured

- The C# client, and a real hosted C++ or C# stub: §17.10.6 uses a bench client on the prototype wire, with no schema
  218 decode and none of the stub's local checks.
- The counters other than the retired one (§17.2.1), the stub's local checks (**H-4**), schema 218 and a hosted
  `Consumer`. The command queue and window are exercised in §17.11 and allocation-checked in §17.10.5, and their
  cost is in §17.10.6. Restore, the snapshot cut and SIGKILL are exercised in §17.11, not timed.
- The proposed shape on bare metal or the deployment's hypervisor: §17.10.6 ran it in a Docker Desktop VM on an
  Apple-silicon laptop.
- The host's allocation with a command queue, counters or a `Gateway`, which the prototype lacks (§17.10.5).

### 17.10.5 Allocation in steady state

`HostedAllocationTest` (systemTest) starts three members, the prototype host on the leader and its client, which
publishes a marker every millisecond. The host counts the bytes its loop thread allocates
(`ThreadMXBean.getThreadAllocatedBytes`) across 50,000 frames, delivered payloads, published markers and heartbeats,
after a warm-up of 100,000, and the test asserts at most 1,024 bytes and that the gate epoch did not move in the
window. The loop thread runs the real `Application`, the relay and the command queue (§17.11), so the count covers the
façade's receive and publish paths and the placement of each command.

- **Result:** 0 bytes over 50,000 frames in four of five runs with the queue, and 120 bytes in the fifth; 0 in both
  runs made before the queue. A rerun of twelve, with the host logging when its window opens, gave 0 bytes in all
  twelve, and three runs on the schema 218 wire gave 0 in all three. The 120 bytes is one small object in a window of
  about a minute, not a per-frame cost, and was not reproduced or sourced, so the host is shown to allocate nothing in
  19 of 20 runs and not in the other. On schema 218 the window's frames are payloads and placed commands only, since
  the heartbeats are counters. A control with a warm-up of 20 frames
  allocates 195,856 bytes over the same window, so the count is not blind.
- **Not in the window:** a snapshot cut, attach and restore (the topology has no snapshot policy), a leadership
  change, a declining façade, counters other than the retired one and a `Gateway`. The payload is 18 bytes and the
  rate 1k msg/s; a larger payload or rate is not shown to stay flat.

### 17.10.6 A C++ client beside a separate host JVM, on Linux

`hop_bench` (`seqeron-client/src/test/cpp-bench`, bench only) is a C++ client with two arms: `direct`, the C++
`Application`, and `hosted`, the prototype wire of `ProxyProtocol.java` to the Java host, byte for byte. Both publish an
18-byte `ProbeMarker` open loop at 5k and 20k msg/s and time each from its intended send until the process sees its
own frame sequenced. The first 20% of each run is dropped. Three member JVMs, the host JVM and the client are
separate processes in one aarch64 Ubuntu 24.04 container, in Docker Desktop's VM on an Apple-silicon laptop with 6
vCPUs, so a Linux VM but not the deployment's hardware. `taskset` pins inside the VM: the members to two vCPUs,
the host to two and the client to two. Arms alternate within a rate. Whole loop, p50, 60,000 markers a run:

| host loop | rate | C++ direct | C++ hosted |
| --- | --- | --- | --- |
| busy-spin | 5k msg/s | 318–328 µs (4 runs) | 321–322 µs (4 runs) |
| busy-spin | 20k msg/s | 245–312 µs (4 runs) | 284–324 µs (4 runs) |
| `BackoffIdleStrategy` | 5k msg/s | 320 µs (2 runs) | 402 µs (2 runs) |

- **The host's idle strategy decides it.** A host that backs off between messages added 66–107 µs at p50, in every
  pinning tried (none; members on three, two and four vCPUs; one vCPU for the whole host JVM). A parked thread wakes
  late on Linux, once for the command and once for the sequenced frame. The prototype host's default was the backing
  off one; `proxy.spin` busy-spins it, and the difference is gone. A host must therefore spin, which takes a core
  (§17.9, question 2), and the figures of §17.10.1 and §17.10.2, which spun, hold.
- **With a spinning host no arm is resolved from another.** At 5k msg/s the p50s agree within 4 µs. At 20k msg/s the
  direct arm's own p50 spans 245–312 µs, wider than its difference from the hosted arm. The tails are the VM's: p99
  is 0.5–1.5 ms in the quiet runs and 9.6 ms in one direct run, p99.9 is 0.6–7 ms in both arms, and neither arm's tail is
  lower than the other's across runs.
- **With the command queue** (§17.11) the busy-spin runs were repeated, three a rate: at 5k msg/s direct p50 is
  319–323 µs and hosted 317–323 µs; at 20k msg/s direct 338–389 µs and hosted 304–367 µs. The queue costs nothing
  resolvable. These runs did not keep the window; §17.10.7 does.
- **Not shown:** a hosted stub's schema 218 decode and local checks, a host with a command queue, counters or a
  `Gateway`, and a tuned cluster, whose faster commit would expose the hop more (§17.9, question 2).

### 17.10.7 Higher rates, with the window, and the host's pauses

The same set-up and busy-spin host, with the bench's hosted arm now keeping H-5's window from the counter
`Attached` names (64 KiB, about 1,100 `Publish` messages). Each run lasts about 10 s, two runs a rate, arms
alternating, the first 20% dropped; p50 / p99.9 in µs, run 1 then run 2:

| rate | C++ direct | C++ hosted |
| --- | --- | --- |
| 50k msg/s | 348, 409 / 9,499, 7,380 | 387, 323 / 4,837, 5,328 |
| 100k msg/s | 318, 280 / 8,659, 4,475 | 379, 355 / 7,260, 5,416 |
| 200k msg/s | 419, 352 / 36,148, 1,417 | 381, 327 / 14,112, 3,462 |

- **No knee up to 200k msg/s.** The p50 stays at 280–420 µs in both arms and neither arm's differs from the other's
  by more than its own spread. No run closed the attachment (`CLIENT_WINDOW`) and none fenced. The limit lies above
  200k msg/s, or is the cluster's, which three JVMs on two vCPUs make likely and which would be the same for both
  arms; the sweep does not separate the two. The window declines of the hosted arm were not logged.
- **The first run of each rate is the worse one** in both arms, direct's ingress back-pressured (163, 3,848 and
  25,476 retried offers) where the hosted arm's did not, a JIT and warm-up effect on a cluster just started. Run
  for run, the tails do not rank the arms.
- **The host's pauses do not explain the tail.** Over a host JVM's life (four arms) its log shows one GC pause of
  2.4–3.8 ms and no other safepoint above 0.2 ms; each member shows two or three pauses of 3–9 ms. The direct arm,
  which has no host, has the same p99.9, so the tail is the members' and the VM's. A pause in the host's loop would
  still sit in the hosted client's path, and one of 3.8 ms is within the p99.9 seen.
- **Not shown:** the multi-message `Payload`, whose saving needs a rate above the one the offer limits, and a tuned
  cluster.
- **On schema 218** (C++ bench against the host on the new wire, counters and window, busy-spin host, 60,000 markers,
  two runs a rate): at 5k msg/s direct p50 was 322 and 319 µs and hosted 318 and 318; at 20k msg/s direct 262 and 289
  µs and hosted 337 and 288. The peak queue was 784 bytes of the 64 KiB window and no attachment was closed. The
  hosted arm's p50 at 20k msg/s spans the direct arm's, as before.

## 17.11 The host: restore, cut and SIGKILL

The host is `ClientProxy` in `seqeron-service`, configured by `proxy.*` properties (`running-a-cluster.md`). Its decisions
are `Liveness`, `CommandLedger` and `AttachRules`, unit-tested without Aeron; the system tests below launch it from the
uber jar.

A Java host runs the real `Application` with snapshots; a Java client holds `TestApplication`'s state (a marker count
and a digest) and writes its one 16-byte record byte for byte. Host and client are child processes beside member
processes, as `HostedSnapshotTest` starts them, with the bench's own wire and not schema 218. The hosted replica is
one of a source's three, beside two direct `TestApplication` replicas, so each side compares the other's rounds
with the sequenced `SnapshotEnd` and fences on a difference (**A-7**). Snapshot rounds run every 2 s (every 15 s in
the long-cut scenario), `clientTimeoutMs` is 2 s, and the client's host timeout is 3 s.

The prototype speaks schema 218 (`sbe-hosted.xml`) on the four streams of §17.2, with the attach of **H-8** and the
per-attachment images of **H-15**, and the counters of §17.2.1. It differs from §17.3 and the rules in these ways:

- **`Publish` and the window.** `Publish` carries the gate epoch and the host drops a stale one (**H-3**), answering
  nothing for a command it drops or declines (**H-4**). The queue and window are **H-5**'s with one difference: the
  host closes the attachment (`CLIENT_WINDOW`) rather than fencing. The windowed length is the encoded message, header
  included, which the stub counts and the host retires. `Publish` carries the `groupMask` and the host places it with that mask; `Reply` is queued through the same window
  and placed with the `RequestRef` it names (**H-11**). The prototype allocates one `RequestRef` per reply.
- **Counters.** The client heartbeat, the host heartbeat and the host retired counters are as §17.2.1; the client
  applied counter is written (the position of the last `Payload`) and not read, so **H-14**'s lag is not exported. A
  heartbeat is written on every duty cycle, in a long cut and in a send's spin, and not in a stalled client.
- **Attach.** By `clientId` and `requestId`, with `hostName`, `protocolVersion` and `aeronVersion` checked. A resend
  of the live request is answered again, any other client with `BUSY`. The façade is built once the events publication
  is connected, so the restore has a reader.
- **`GroupsResolved`** is sent once, from `onGroupsResolved`, before the first `Gate`.
- **Gateway.** For `Kind.GATEWAY`, when `probe.gatewayName` names the row, the host builds a `Gateway`. Its
  `onActivated` sends `Role` and spins, keeping the cluster session alive, until the client's `Activated`; on `true`
  the host takes the open edge at once (epoch up, `Gate`), so the first `OpenConnection` is stamped with the epoch it
  is placed under. `onStandby` sends `Role(false)` and then the `Gate`. `OpenConnection` is windowed; `CloseConnection`
  and `GateClosed` are queued behind it in order but are outside the window and the retired counter (**H-5**), and
  `GateClosed` takes the edge down, epoch up, as the façade's `gateClosed()` does. A `Publish` on a connection whose
  `ConnectionOpened` is not yet placed stays at the head of the queue, un-retired, until the façade accepts it. An
  `OpenConnection` under an id the façade did not allocate closes the attachment (`CLIENT_PROTOCOL`).
- **Consumer.** For `Kind.CONSUMER`, when `proxy.consumerGroup` names a group, the host builds a `Consumer` for it. It
  subscribes to no stream 304, so the attachment is judged lost by the client's heartbeat counter alone (**H-9**), and
  `Attached` carries no `sourceId` (-1).
- **Not sent.** `ClusterTime`. `AttachPending`
  is never needed, since the host closes a façade synchronously. The snapshot round id is the host's own cut count.
- **`kill -9` of the host or a client** is the same as before. The ghost scenario changed: with per-attachment images a
  command under another attachment's id cannot be sent, so the client publishes from a second publication on stream
  304, which the host does not read, and the test asserts nothing of it is sequenced.

The measurements of §17.10.1 to §17.10.7 were taken on the earlier bench-only wire (a 16-byte header, a nonce attach
and heartbeat messages) except the last run of §17.10.7, which is on schema 218. The epoch and exactly-once rows below
are **H-3** and **H-4** as specified: the client learns of no drop and counts what the host did.

| scenario | observed |
| --- | --- |
| steady state, hosted pair on a follower | rounds complete; the façade compared them with the direct publisher's; no `FENCED` |
| hosted pair on the leader | the gate opened, the client's markers were sequenced through `Publish`, a direct replica compared the hosted replica's published ends and no instance fenced |
| client `kill -9`, successor attaches at once | the host refused it as another client until `CLIENT_LOST`, 2.06 s after the kill; the successor attached, the host restored a snapshot and the client held 142–231 markers; rounds resumed |
| host `kill -9` | the client exited 70 on `HOST_LOST`; the restarted pair restored and rounds resumed |
| client `kill -9` mid-cut (the second cut after attach) | the host abandoned the round after `clientTimeoutMs` and did not hang; the direct replicas kept completing rounds; the next attach restored from round 12, the last confirmed, not the abandoned one |
| a command from a publication that is not the attachment's (**H-15**) | never read by the host; no marker sequenced |
| a cut of 8 s, longer than the 5 s session timeout (rounds every 15 s) | the host kept the session by calling `keepAlive()` while it waited, the client heartbeating; a round was confirmed, with no `CLIENT_LOST` and no abandoned round. Control: without the host's `keepAlive()` no round is confirmed in 80 s |
| leadership moves to the hosted member (the leader paused until the others elect, repeated until the hosted member wins; no member killed) | the gate epoch rose 0 to 1; the client applied it; a marker sent under the previous epoch was dropped by the host, counted, and never sequenced; the client's markers were sequenced and its rounds agreed with the direct replicas |
| leadership leaves it (its member paused until the others elect), pair alive | the gate epoch rose again and the client stopped publishing at once; it kept following the new leader's markers; the rounds went on and no instance fenced |
| the client's own markers across both changes | every marker it offered was sequenced exactly once and in order, or dropped by the host: 168 offered, 1 dropped (the stale probe), 168 sequenced |
| a new term with the same leader | the host saw `not leading` and `leading` in the same millisecond, the epoch rose by two, and the eight commands in flight were dropped by their epoch and not placed (observed before the queue, when the host answered each drop) |
| control: the host does not check the epoch | the stale marker was placed and sequenced, and the test failed on the decline, on both stale-marker checks and on the exactly-once count |
| control: the client writes a digest one off | the host fenced `SNAPSHOT_DIVERGED`, sent `Fenced`, the client exited 70, and no direct replica fenced |

Four findings from it are in the rules above: the host could not hold the session while it waited in a cut, which
`Application.keepAlive()` now allows (**H-6**); the façade offers no restore begin, end or count (**H-7**); a client
must tell its own answer to `Attach` from one meant for a dead client (**H-8**, **H-15**); and a new term with the
same leader closes and reopens the gate within a millisecond, so the epoch rises at every edge and a command in
flight over both is dropped, never placed late (**H-3**).

Leadership was moved by pausing members (`SIGSTOP`), not by killing the leader, since a kill and restart of a member
is the pattern behind the known Aeron 1.53.2 committed-entry loss. Pausing the other follower first did not make the
hosted member win (the other follower was elected in one run), so the test pauses the leader and repeats until the
hosted member is elected, which took one or two pauses in the runs made.

The command queue and window have their own scenarios (`HostedCommandQueueTest`): a flood against a tracker of four
and a window of 1,024 bytes, a stub that ignores the window, and a gate edge across a paused leader (17 queued
commands dropped, 4,197 offered, 4,180 sequenced, none out of order).

Not covered: a held ingress of seconds, a hosted `Gateway` pair's snapshots, a
host killed in a cut, a leader killed and not paused, state of more than one record or a record near the 65,535-byte
limit, and a C# stub. Liveness by counter, the attach of **H-8**, the stub's other local checks and a C++ stub of an
`Application` are covered by §17.11.1.

### 17.11.1 The C++ stub against the host

`hosted::Application` (`seqeron-client/src/main/cpp/org/limitless/seqeron/hosted/Application.hpp`) is the C++ stub of an
`Application`: its listener is the direct `Application`'s, a `Payload` reaches it as `app::Payload` over the received
`Payload` message, and `publish` returns `protocol::Publish` after the local checks of a direct call (a retired
`payloadId`, a payload above `MAX_PAYLOAD_LENGTH`), the mirrored gate (**H-16**) and the window (**H-5**). It answers
the host's `SnapshotCut` from a `SnapshotListener`, restores from `RestoreRecord` and keeps the heartbeat counter. `reply` takes the
`RequestRef` of the request, and `GroupsResolved` hands the listener a `GroupDirectory` and gives `publish` the direct
call's group-mask check (`Refused` for a group the table lacks). `hosted::Consumer` (`.../hosted/Consumer.hpp`, 49 lines) is the stub of a `Consumer`: it sends
only `Attach` and `Detach`, takes no stream 304, and delivers `Payload` and `CaughtUp` to the direct `Consumer`'s listener. `hosted::Gateway`
(`.../hosted/Gateway.hpp`) is the stub of a `Gateway` on the same core (`hosted/detail/Attachment.hpp`): its listener is the direct
`Gateway`'s, `onActivated` is answered with `Activated` and the stub then serves under the epoch one above the last `Gate`'s
(§17.5), `openConnection` allocates its ids from the last `Role` and returns `NO_CONNECTION` for a full window or a
back-pressured stream, and `closeConnection` and `gateClosed` retry within the call. `hosted_probe`
(`seqeron-client/src/test/cpp-probe`) holds the probe's state in it, as `HostedTestApplicationClient` does.

Five system tests drive the stubs against the Java host, with `HOSTED_PROBE` naming the binary:

- `HostedCppStubTest`: it attaches, the gate opens, 2,000 markers are sequenced once and in order, and the two local
  refusals fire. With the host's tracker at 4 and a 1 KiB window, 20,000 markers offered flat out are all placed and
  sequenced once, the stub having declined 223,070 times at the window, with a peak queue of 1,008 of 1,024 bytes. At
  a gate edge (the leader paused 4.5 s) the stub stopped at the `Gate` and offered 3,582, the host dropped 18 by their
  epoch and 3,564 were sequenced. In a fourth scenario the stub is handed the directory, a mask naming an absent group
  is refused and one naming the own group is published, and of 2,000 markers every tenth is answered: the 200 replies
  are each sequenced once under the sourceId, connectionId and groupMask of their request.
- `HostedSnapshotTest` with `HOSTED_CLIENT=cpp` runs its clients as the probe: restore, cuts, a client `kill -9`
  with an immediate successor, a host `kill -9`, an abandoned cut, the 8 s cut, the leading hosted replica, and the
  corrupt record fenced with `SNAPSHOT_DIVERGED`, each check as with the Java client and the direct replicas
  comparing the stub's rounds (**A-7**). Not run with it: the stale-epoch scenario and the ghost command, which need
  a client that sends what the stub never would.
- `HostedGatewayStubTest` (`hosted_gateway_probe`, beside `hosted_probe`; one gateway row, no standby, no snapshot
  policy): the stub opens three connections under the ids the first `Role` named (0, 1, 2), publishes 20 markers on each,
  and every one is sequenced under its own `connectionId`; the three `ConnectionOpened` and three `ConnectionClosed`
  reach the listener. A `Publish` that reached the host before its `ConnectionOpened` was placed stayed at the head
  (`headDeclines` 1), and nothing fenced. In a second scenario the stub then calls `gateClosed()`: the host takes the
  edge down and asks again with a second `Role` whose `firstConnectionId` is 3, past the highest id issued, and three
  more connections run the same way.
- `HostedGatewayFailoverTest` (a pair, `GW-H-A` and `GW-H-B`, each a host and a stub on its own member, no snapshot
  policy): the active holds three connections and publishes steadily. With its stub killed, its host logs `CLIENT_LOST`
  and the standby's `Role` carries `firstConnectionId` 3, past the active's ids; its three connections and 60 markers
  run under their own ids, and nothing fenced. With the active's host paused (`SIGSTOP`) the stub exits `HOST_LOST` and
  the standby takes over the same way. A paused host resumed with its stub still attached gets `Role(false)`, opens no
  further connection and never asks again. The leader is never paused or killed.
- `HostedConsumerStubTest` (`hosted_consumer_probe`; a consumer host on a follower, an `Application` stub publishing 500
  markers on the leader): a consumer attached while the markers are published, and one attached after they are in the
  log, each caught up and saw all 500 once and in order; a consumer killed with `SIGKILL` was judged lost by the host
  from its counter, and its successor attached.
