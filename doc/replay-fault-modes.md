# Replay fault modes: disturbances the replayer's tests do not yet apply

Written 2026-10-24, at `3750dfa` on `main`. "R-n" and "§n" refer to
[`seqeron-protocol-spec.md`](seqeron-protocol-spec.md). It proposes tests, and tier 1, tier 2 modes 1 to 5, replay load under the faulteron tests, and tier 3's server decorator and client hook have been done. Gaps closed
in [`replay-test-review.md`](replay-test-review.md)'s status table are not repeated.

**Scope.** Faults that reach the replayer and its clients through their transport, as opposed to the decisions the
unit suites drive: the faulteron injector (github.com/FredrikJDahlberg/faulteron), process and resource faults, and
fault seams in the integration suite.

**The constraint.** On a member, every replay stream is IPC: request 202, control 203 and replay 201 on
`ReplayProtocol.IPC_CHANNEL`, and the queue. IPC is shared memory and never reaches an interface, so faulteron cannot
touch a member's replayer, and no tc hook could be extended to. The only replay traffic on UDP is a gateway host's
relay: `AeronQueueRelay`'s archive control session and its replay, stream 206. So faulteron serves tier 1 alone, and
IPC disturbances come from processes, resources and seams (tiers 2 and 3).

## Tier 1: faulteron against the relay. Done.

**The harness.** `docker/compose.yml`'s `node-3`, behind the `gateway-host` profile, runs `ReplayerServer` with
`replayer.archiveEndpoints`. The relay's replay and control-response channels bind ephemeral ports
(`AeronQueueRelay.java:328,373`), so rules select by `--peer`, not by port. `start_gateway_host` orders the endpoints
follower F1, follower F2, leader: the host's producer talks to the leader alone, so a rule aimed at a follower
touches only the relay. That makes a `--stream` selector in faulteron unnecessary.

**Already covered in-process.** `InProcessCluster.cutArchive` drops the relay's streams 122 and 206 at a member, and
`ClusterSnapshotTest` uses it. A clean cut of the host from its source is not a gap. In-process cuts only drop
frames, so faulteron's value here is partial loss, one half of repair, delay, jitter and duplicates.

| mode | fault | crosses | assert | test |
| --- | --- | --- | --- | --- |
| unrepaired relay | on the host, `--peer F1`: `rule --type data --loss 5`, `block --type nak` | `QueueRelay.SOURCE_TIMEOUT_MS`, 3 s: the image stops at the hole, and the 1 Hz heartbeat makes silence abnormal | the relay leaves F1 and follows F2; a host `confirm` is exact; a fresh host `follow` catches up | `relay-link-test.sh` |
| slow archive control | `--schema 101 --delay`, on the host toward F2 for requests or on F2 toward the host for responses, then the host cut from F1 to force a connect | `ARCHIVE_MESSAGE_TIMEOUT_NS`, 1 s, over a connect's two exchanges | at 1500 ms the relay gives F2 up for the leader, at 200 ms it follows F2; no FATAL; a host `confirm` is exact | `slow-archive-control-test.sh` |
| jittery relay link | on F1, `--peer node-3 --delay 20 --jitter 30 --duplicate 10` | Aeron's reordering and duplicate discard under the relay's "only the next frame" rule | the relay stays on F1; a host `confirm` is exact; a fresh host `follow` catches up | `relay-link-test.sh`, `FAULT=jitter` |

- faulteron delays only what leaves an interface, so the jitter rule runs on the member.
- A following relay sends almost no control requests, so the slow-control test forces a connect.
- An archive connect is two exchanges under one 1 s deadline, so its cutoff is a 500 ms delay, not 1 s.
- `RESOLVE_TIMEOUT_MS` times a local bind, which no network fault reaches.
- `replayer.queueStallTimeoutMs` bounds the host's own recording, which a slow source cannot stall.

## Tier 2: IPC disturbances through processes and resources

**1. A client paused mid-walk. Done.**
- An untethered subscriber is evicted only once it trails the fastest subscriber by a window less a quarter, 6 MiB
  at a 16 MiB term. The control stream carries replies of a few dozen bytes, so a paused client is not evicted from it,
  and reads a reply it was sent while paused.
- What a pause under the 10 s client liveness timeout crosses instead:
  - The walk's replay image is tethered, so the archive waits on it, and the client reads on where it stopped. A
    client's `poll` reads the image before its 5 s replay stall watchdog judges the replay, so a longer pause does
    not cost it the replay.
  - Paused with `Replaying` unread past the archive's 5 s connect timeout, it subscribes to a replay the archive has
    given up. The archive logs an error, and the Replayer counts one in `seqeron_replayer_archive_errors_total`. The
    stall watchdog re-requests it 5 s later.
  - More than a window of frames reaching the queue while it is paused evicts it from the queue. It rejoins at the
    tip 10 s later, and the next frame there shows the hole, which a resume replay heals. Its queue image goes away
    meanwhile, and until a live frame after the rejoin it is not caught up, whatever its replay reached.
- Test: `PausedWalkTest` stops polling a client mid-walk for 2 s, for 7 s, and for 7 s under a flood, and with its
  reply unread for 7 s. Each converges in order from groupSeqNo 1 and is never caught up short of the queue's tip;
  the mid-walk pauses start no other replay, and the other two need one. In local runs the mid-walk pauses caught up within 10 ms of resuming, the reply-unread case
  in 5 s, and the flood case in about 4 s, once the queue rejoined.

**2. A client alive but stuck, holding a replay slot. Done.**
- A client whose handler blocks keeps its Aeron client alive, so its replay subscription, which is tethered, stays.
  Its own 5 s replay-stall watchdog runs on the blocked thread, so it does not fire.
- R-3 used to give a replay no time limit, so four such clients took every slot (`MAX_CONCURRENT_REPLAYS`). Every
  other client on the node was then answered `ReplayPending` until its recovery-stall fence fired.
- R-3 now bounds progress, not duration. A replay whose publisher limit has not moved for
  `STALLED_REPLAY_TIMEOUT_MS`, 10 s, is stopped and its slot freed. Its client sees the image close short of
  `catchUpPosition` and requests again.
- Tests: `ReplaySlotAllocatorTest` and `ReplayerServiceTest` at the seam. `ReplaySlotsTest` on a real driver: four
  holders that stop reading lose their slots and a fifth client is served.

**3. A stalled Replayer on a gateway host. Done.**
- On a host, `ReplayerServer` is its own JVM, so it can be SIGSTOPped or CPU-throttled alone. On a member it shares
  the node's JVM.
- The request subscription is tethered, so clients' offers fail while it is stopped. `sendReplayRequest` ignores the
  result, and the resend timer retries.
- Assert: clients see `ReplayPending` and stalls, not faults, and converge after.
- Test: `StalledHostReplayerTest` reads no requests of the host's Replayer through `FaultableReplayer`, which
  `launchGatewayHost`'s wrapper overload installs, while a client cold-starts on a host whose queue holds history and
  the relay adds more. For 7 s the client is answered nothing, dispatches nothing and is never caught up. Once the
  Replayer reads again it walks the queue in order from groupSeqNo 1 and catches up at the tip. A stopped Replayer
  answers not even `ReplayPending`, so the client sees silence, which its resend timer covers.

**4. A CPU-throttled member under replay load. Done.**
- `lib.sh`'s `throttle` slows the Replayer's duty cycle, the archive and a co-located client together.
- A `follow` walks from the start and searches for no cut. Only a restore whose cut the Replayer has not kept
  searches, and on a running member the cut hand-over keeps every cut, so the search follows a member or host
  restart. [`replay-review.md`](replay-review.md) finding 2's race is the client's 500 ms resends queued behind one
  slow search. The search keeps the cut it finds, so the resends behind it are answered from memory and the latest is
  served.
- What a throttled walk crosses is a Replayer slower than the resend. A resend keeps its `requestId` (R-1), and the
  Replayer answers it with the replay it already started, so resends queued behind a slow answer cost no replay. The
  control stream, untethered and carrying replies of a few dozen bytes, drops none.
- Tests: `ReplayerServiceTest` queues four restores under new ids behind one search, which runs once, and the fourth
  is answered `Replaying`; four resends of one request start one replay, and each is answered with it. `throttled-leader-test.sh` cold-starts a `follow` on the leader as its cap is applied: within 30 s of
  the producer's end it is caught up and not since evicted from its queue, and
  `seqeron_replayer_control_replies_dropped_total` stays at zero. In a local run its Replayer started one replay for
  it under the cap.

**5. Clock steps. Done.**
- `AeronReplayer.scan`'s 2 s deadline and every timer in `ReplayerService`, read through `Replayer.epochMillis()`,
  used the wall clock. A forward step ended a healthy scan as `STALLED`, reclaimed slots, and closed the restart-safety
  join window early, so a cut could be declared safe before its applications joined. A backward step held the duty
  cycle up to N + 2 s and suspended slot reclaim.
- Both now read `Clocks.monotonicMs()`, as the client and `AeronQueueRelay` already did, through
  `Replayer.monotonicMs()`. A wall-clock step reaches nothing in the replayer, so no test applies one.

**Replay load under the existing faulteron tests. Done.** `unrepaired-follower-test.sh` cold-starts a `follow` on the
starved follower under the fault and another after the heal, each through its Replayer, and holds both to ending
caught up rather than evicted from the queue, as mode 4 does through a CPU cap.

## Tier 3: deterministic IPC faults, in-process

This tier pins the timer boundaries the scripts can only approach: the 500 ms resend, the 5 s replay stall, the
recovery-progress timeout, and the 1 000-spin control drop.

- **Server. Done.** `FaultableReplayer`, a `Replayer` decorator around `AeronReplayer` that `InProcessCluster` installs
  through `SequencerServer.launch`'s `replayerWrapper` overload, keeps real Aeron and makes the timing exact. It can:
  - return `BACK_PRESSURED` from `offerControl`, reaching `MAX_CONTROL_OFFER_SPINS` and `onControlReplyDropped`;
  - starve `pollRequests` for a set time;
  - make `startReplay` slow, or throw;
  - end a replay, or hold its `replayProgress` still, mid-replay.
  - `ReplayerFaultsTest` applies each: the control drop, a starved `pollRequests`, resends behind a slow `startReplay`
    starting one replay, a throwing `startReplay`, a frozen `replayProgress` freeing its slot at the stall timeout, and
    ended replays freeing theirs at once.
- **Client. Done.** `ReplayerStreamReceiver`'s fault overload takes `controlFaults` and `replayFaults` beside
  `queueFaults`, to drop a reply or a replay fragment on the poll thread. `ReceiverFaultsTest` cold-starts a client
  over a history with each: a dropped `Replaying` is resent under its request id and starts no second replay, and a
  run of dropped replay fragments is healed by a resume. It found that a replay reaching its bound past a hole in
  its history reported the client caught up short of the tip, since the image's position advances over dropped frames.
  `ReplayerRecovery` now resumes instead, in all three languages, through the resend timer so that a hole every
  replay repeats is retried every 500 ms and not as fast as a replay runs. A recording that keeps its hole shows as
  `recoveryStalled` after the 30 s no-progress timeout, which `ReceiverFaultsTest` holds it to, and heals once the
  recording covers the hole. The C++ and C# receivers take the same three hooks as trailing arguments, which no
  test there drives; `replayFaults` takes the position each frame starts at.
- **Relay.** `SequencerServer.UdpFaults` picks frames by stream id and direction. Picking by frame type as well would
  give the unrepaired relay in-process, without the compose service.

## Suggested order

1. Tier 3 relay frame-type selection.
