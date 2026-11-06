# Hosted façades: design record

The hosted façade protocol is [`seqeron-protocol-spec.md`](seqeron-protocol-spec.md) §17 ("spec §17" below). This
is its design record: what it adds to and retires from the tree, the questions still open, the measurements behind
it, what the prototype host and the C++ stubs were shown to do, and the plan for moving the C# client.

## 1. Consequences for the tree

- Java host: `org.limitless.seqeron.proxy.ClientProxy`, a `seqeron-service` class composing `Application`, `Gateway` or `Consumer` with a remote listener and
  the command queue. `seqeron-client` stays free of it, so the direction rule holds.
- Java client tier: the `Application` and `Gateway` builders take the `IdleStrategy` that `ClusterStreamSender.send`
  and the cluster connect spin on, a yielding spin and `AeronCluster`'s own connect wait by default (**H-10**). It is a change to the client tier's surface, so the
  C# façades' options and `client-api.md` follow it. The direct C++ façades, which it also covered, are retired.
- Where the ids live. Streams 301–304, `NO_ATTACH`, the schema id and the protocol version (spec §17.7) are a
  `ProxyProtocol` class in `protocol`,
  beside `ReplayProtocol`, with a C++ twin, since the proxy and the stub share them; the channels are in the Java
  class only, since the two ends build theirs differently. The four counters take a new
  range of `SeqeronCounters`, 5300–5399, after the sequencer's, the replayer's and the façades', and `MAX_TYPE_ID`
  widens to it, so `clusterctl` lists them; the three copies of `SeqeronCounters` stay in step. The C++ stub
  connects only to its own driver and needs no port, so C++ `PortLayout` retired with the direct façades, and
  `cluster_probe`, which needed it, went with them. `ports.sh` is mirrored by the Java and C# `PortLayout`s alone, and
  the rule that they change together covers three files rather than four.
- C++ stubs: `RemoteApplication<Listener>`, `RemoteGateway<Listener>` and `RemoteConsumer<Listener>`
  with the façades' operation names, listener concepts and `Config`. Their counters are Aeron's `addCounter` and
  `CountersReader`, an Aeron API used rather than re-implemented. `client-api.md` gains a column for them. C#
  stubs are not in the first stage.
- What a client sheds. C++: the hand-written cluster client (`ClusterStreamSender` and the schema 111
  codecs, the only code that uses them), the archive client, and the ports named above. C#, if it follows: only
  the ports, since Aeron.NET's `AeronCluster` is already reused, so the cluster-client argument is C++'s alone.
  A C# stub still depends on Aeron.NET's `Aeron.Client` for the driver connection, and drops `Aeron.Cluster` (§5).
- A host is a JVM per client (spec §17.1). The JVM is not new to the box, since a member or gateway host runs one, but
  a box with several clients runs several hosts, each its own process and heap. The host's GC, JIT and safepoint
  pauses sit in the C++ client's path in both directions, so a C++ client inherits the host's tail. Open questions
  2, 4 and 6 say what that costs, where a host runs and whether one process hosts several.
- Retired in the first stage: the direct C++ façades and what only they use: `ClusterStreamSender`, `PendingSends`,
  `IngressPublisher`/`IngressTracker`, `Session`, `Gateway`, `Application`, `GatewayLifecycle`, `SnapshotTaker`,
  the stall fences, queue resolution, `ReplayerRecovery`, `ReplayerStreamReceiver`,
  `SnapshotStore`, `ClusterStreamClient`, `Consumer` and `PortLayout`, with their tests and the schema 111 and replay
  codecs (kept on the `cpp-direct-client` branch): about 9,000 of the 10,100 lines under `seqeron-client/src/main/cpp`, and most of its 9,100 test
  lines, before the generated codecs. What stays in the C++ client is the schema 218 codecs, the `app` types the
  stub shares (`Payload`, `GroupDirectory`, `OutstandingWork`, `ClusterError`, `SnapshotListener`, the listener
  concepts, in `app/Listeners.hpp`), `app/detail/GroupResolution` with `protocol/SequencedFrame` and `protocol/Snapshot`
  (the stub's core reads the host's `Payload` and `GroupsResolved` through them), `LeaderGate` (kept for
  `OutstandingWorkPropertyTest`'s model), `protocol/Publish`, `SeqeronCounters` and `util`. The C# façades, the same set minus the cluster
  sender, stay, and with them the Java/C# parity they carry.
- Net accounting, first stage. Retired: the C++ list above. Added and maintained: the Java host and its command
  queue, the schema 218 codecs in Java and C++, the C++ stubs with their three local publish checks and the
  command window (**H-4**, **H-5**), and the host's counters.
  Added complexity that is not lines: a second protocol (spec §17.7), two-process failure modes and a second unit to
  supervise per instance (spec §17.6), and a JVM and a core per client (§3.1). The parity the C++ client carried
  does not go with it: `ReplayerRecovery`, `PendingSends` and the façades stay case for case in Java and C#, two
  languages rather than three. **The added lines are only partly counted, so the figures below are a floor.
  The first stage is decided: the local tests show the hosted client is good enough.** What exists: the C++ stubs
  of `Application`, `Gateway` and `Consumer` share a core, `hosted/detail/ClientLink.hpp`, of 612 lines with its comments, and are 140, 252 and 49 lines on top of it (a build of
  the direct `Application` and what only it uses is part of the ~9,000 above), the schema is 217 lines of XML and its
  generated C++ codecs 16,800 more, and the host, `ClientProxy`, which runs any of the three façades, is about 910 lines of Java with its
  configuration checks and the allocation window, plus `Liveness`, `CommandLedger` and `AttachRules`, its Aeron-free seams. A C# stub is not counted,
  so it is a floor, not the answer.
- Conformance is the Java system tests run against each language's probe, so that the stubs' tests are not three
  ports of one suite: the assertions are Java's and the stub is the probe executable the test launches, as
  `HostedSnapshotTest` selects one with `HOSTED_CLIENT`. The Java host is the one implementation of its half and is
  normative where spec §17 is silent. The §14 rows and `FailoverProcessTest` also rerun against a hosted pair.

## 2. Open questions

1. **The command window.** Built in the prototype host (§4): a queue sized to the window, a retired counter the
   stub reads, no reply to a drop. With a tracker of four and a window of 1,024 bytes (about seventeen
   `Publish` messages), a client flooding 20,000 markers had every one placed and sequenced exactly once and in order,
   the queue peaked at 986 bytes, the façade declined at the head and the stub at the window; a stub that ignored the
   window was closed by the host. A gate edge dropped 17 queued commands, counted, and none was placed late. `Reply`
   and `OpenConnection` are windowed as `Publish` is (§4). Still open: the size for a real deployment (the
   prototype's 64 KiB default was never reached outside the test), and a held ingress of seconds rather than an edge.
2. **Cost.** The hops measure 0.2–0.5 µs each way with the cluster out of process, and about 1.5 µs for both
   with it in the host's JVM (§3.1, §3.2), on the laptop of §3; against a C++ `Application` the whole
   loop shows no resolvable difference (§3.2). The remaining risk is the host's thread: it needs its own core,
   since the extra spinning threads slowed an in-JVM commit by tens of microseconds until the thread counts
   matched. The laptop's commit of about 250 µs hides the hop; a tuned cluster commits faster, and there the hop
   and the host's pauses weigh more. **The first stage rests on two results**: the proposed shape measured, a C++
   client process beside a separate host JVM, pinned, on Linux; and an allocation test that holds the host's
   steady state to zero bytes a frame, as the C# suite does for its client. The second is met by the
   prototype host (§3.5). The first is measured in an aarch64 Linux VM (§3.6): with the host spinning, a C++
   client beside it shows no resolvable difference at the median, and a host that backs off adds 66–107 µs. The
   deployment's own hardware is not measured. A bad result there is a tuning matter for the host, and the direct
   client stays on `cpp-direct-client` if it is not.
3. **Throughput.** The rate at which the per-message offer, not the hop, is the limit is unmeasured (the one
   20,000 msg/s pass was noise-bound, §3). A multi-message `Payload` would save one IPC offer per frame, under
   a microsecond (§3), and would hold the first frame back until the last is encoded.
4. **Where the host runs.** Decided: beside a member, a JVM of its own, which a host fault must never take down; on
   a gateway host, inside `ReplayerServer` (`proxy.hosts` there), where no consensus runs, at the cost that a host
   fault takes down recovery for every consumer that Replayer serves (spec §17.6). Its `Application` is off the cluster,
   the one instance whose gate opens once caught up.
5. **Retiring the direct clients.** The direct C++ façades are retired (§1; branch `cpp-direct-client`), so a
   C++ application moves to the stub with no release carrying both. A `Published` publish survives a gate edge, the
   stub retaining and resending it (spec H-17), so the application's contract is the direct façade's. Built: the host
   reports each unplaced `Publish` (`Dropped`) and how far it has decided (`Resolved`), and the C++ `Application`
   keeps the command in `RetainedCommands` until then, resending in offer order once the gate reopens. The ring is
   tested alone and against a model of the host with seeded edges (`RetainedCommandsTest`), and end to end by
   `HostedCppStubTest` (§4.1). C# is not in the
   first stage, and stays direct until the measurements of §3.4; §5 is the plan for moving it.
6. **Several façades in one host.** Decided: one process runs several hosts (`proxy.hosts`), each with its own
   `hostName`, `clientId` and snapshot directory, on a thread of its own, all on one Aeron client. That saves the
   JVMs, their heaps and all but one client conductor, not the cores: each host still spins, and a blocking connect
   or a long cut stalls only its own client. A single thread serving every façade would save the cores too, but the façade's connect would first have
   to be made asynchronous.

## 3. Measured hop

Aeron 1.53.2, JDK 23, busy-spinning threads, a macOS laptop (8 performance cores) with no CPU pinning.
Host and client are threads in one JVM, which costs the same IPC as two processes but is not a cross-process
run. §3.1 runs the cluster in that JVM too; §3.2 runs it as three separate processes and adds the C++
`Application` as the baseline.

### 3.1 The real host

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
  relay's 125–250 ns (§3.3), the difference being façade dispatch, the body copy and the framing.
- **A command is an offer (H-4)**, so a `publish` adds the one-way send hop, about 0.9 µs at the median, to the
  façade's placement, and no round trip. The prototype's `Publish` is asynchronous too; this bench's host answers a
  decline, which the prototype host of §4 no longer does.
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

### 3.2 Against the C++ client, cluster out of process

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
  p99 at 5k msg/s, and 0.21–0.25 µs and 8–12 µs at 20k msg/s. In §3.1 the members' threads compete with the
  host's for cores.
- **Not in this measurement:** a hosted C++ stub. The hosted arm is a Java façade and a Java client thread, so a
  stub's own decode work is not included; it is small beside 250 µs, not zero.

### 3.3 A bare relay

The best case: a producer standing in for the queue, and a relay thread that does nothing but copy and
re-offer each message on a second stream. Receive path only.

| message, rate | direct p50 / p99 / p99.9 (ns) | relayed p50 / p99 / p99.9 (ns) | added p50 / p99 / p99.9 (ns) |
| --- | --- | --- | --- |
| 128 B, 100k/s | 125 / 500 / 11,583 | 250 / 3,083 / 18,500 | 125 / 2,583 / 6,917 |
| 128 B, 1M/s | 125 / 458 / 13,791 | 250 / 1,208 / 18,500 | 125 / 750 / 4,709 |
| 1 KiB, 100k/s | 125 / 750 / 22,583 | 375 / 1,500 / 26,542 | 250 / 750 / 3,959 |

### 3.4 Not measured

- The C# client, and a real hosted C++ or C# stub: §3.6 uses a bench client on the prototype wire, with no schema
  218 decode and none of the stub's local checks.
- The counters other than the retired one (spec §17.2.1), the stub's local checks (**H-4**), schema 218 and a hosted
  `Consumer`. The command queue and window are exercised in §4 and allocation-checked in §3.5, and their
  cost is in §3.6. Restore, the snapshot cut and SIGKILL are exercised in §4, not timed.
- The proposed shape on bare metal or the deployment's hypervisor: §3.6 ran it in a Docker Desktop VM on an
  Apple-silicon laptop.
- The host's allocation with a command queue, counters or a `Gateway`, which the prototype lacks (§3.5).

### 3.5 Allocation in steady state

`HostedAllocationTest` (systemTest) starts three members, the prototype host on the leader and its client, which
publishes a marker every millisecond. The host counts the bytes its loop thread allocates
(`ThreadMXBean.getThreadAllocatedBytes`) across 50,000 frames, delivered payloads, published markers and heartbeats,
after a warm-up of 100,000, and the test asserts at most 1,024 bytes and that the gate epoch did not move in the
window. The loop thread runs the real `Application`, the relay and the command queue (§4), so the count covers the
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

### 3.6 A C++ client beside a separate host JVM, on Linux

`hop_bench` (bench only, on the `cpp-direct-client` branch, in its `cpp-bench` directory, removed with the
direct client it measured against) is a C++ client with two arms: `direct`, the C++
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
  (§2, question 2), and the figures of §3.1 and §3.2, which spun, hold.
- **With a spinning host no arm is resolved from another.** At 5k msg/s the p50s agree within 4 µs. At 20k msg/s the
  direct arm's own p50 spans 245–312 µs, wider than its difference from the hosted arm. The tails are the VM's: p99
  is 0.5–1.5 ms in the quiet runs and 9.6 ms in one direct run, p99.9 is 0.6–7 ms in both arms, and neither arm's tail is
  lower than the other's across runs.
- **With the command queue** (§4) the busy-spin runs were repeated, three a rate: at 5k msg/s direct p50 is
  319–323 µs and hosted 317–323 µs; at 20k msg/s direct 338–389 µs and hosted 304–367 µs. The queue costs nothing
  resolvable. These runs did not keep the window; §3.7 does.
- **Not shown:** a hosted stub's schema 218 decode and local checks, a host with a command queue, counters or a
  `Gateway`, and a tuned cluster, whose faster commit would expose the hop more (§2, question 2).

### 3.7 Higher rates, with the window, and the host's pauses

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

## 4. The host: restore, cut and SIGKILL

The host is `ClientProxy` in `seqeron-service`, configured by `proxy.*` properties (`running-a-cluster.md`). Its decisions
are `Liveness`, `CommandLedger` and `AttachRules`, unit-tested without Aeron; the system tests below launch it from the
uber jar.

A Java host runs the real `Application` with snapshots; a Java client holds `TestApplication`'s state (a marker count
and a digest) and writes its one 16-byte record byte for byte. Host and client are child processes beside member
processes, as `HostedSnapshotTest` starts them, with the bench's own wire and not schema 218. The hosted replica is
one of a source's three, beside two direct `TestApplication` replicas, so each side compares the other's rounds
with the sequenced `SnapshotEnd` and fences on a difference (**A-7**). Snapshot rounds run every 2 s (every 15 s in
the long-cut scenario), `clientTimeoutMs` is 2 s, and the client's host timeout is 3 s.

The prototype speaks schema 218 (`sbe-hosted.xml`) on the four streams of spec §17.2, with the attach of **H-8** and the
per-attachment images of **H-15**, and the counters of spec §17.2.1. It differs from spec §17.3 and the rules in these ways:

- **`Publish` and the window.** `Publish` carries the gate epoch and the host drops a stale one (**H-3**), answering
  nothing for a command it drops or declines (**H-4**). The queue and window are **H-5**'s with one difference: the
  host closes the attachment (`CLIENT_WINDOW`) rather than fencing. The windowed length is the encoded message, header
  included, which the stub counts and the host retires. `Publish` carries the `groupMask` and the host places it with that mask; `Reply` is queued through the same window
  and placed with the `ReplyAddress` it names (**H-11**). The host caches them (256 slots, keyed by the three fields), so a reply allocates nothing.
- **Counters.** As spec §17.2.1. The host exports **H-14**'s lag as `seqeron_hosted_lag_bytes` (type id 5297, labelled
  with the host's `proxy.clientId`), allocated at the attach and freed with the attachment, and measured from the
  attachment's first frame until the client has applied it. A heartbeat is written on every duty cycle, in a
  long cut and in a send's spin, and not in a stalled client.
- **Attach.** By `clientId` and `requestId`, with `hostName` and `protocolVersion` checked and the client's
  `aeronVersion` logged. A resend of the live request is answered again, any other client with `BUSY`. The façade is
  built once the events publication is connected, so the restore has a reader.
- **`GroupsResolved`** is sent once, from `onGroupsResolved`, before the first `Gate`.
- **Gateway.** For `Kind.GATEWAY`, when `proxy.gatewayName` names the row, the host builds a `Gateway`. Its
  `onActivated` sends `Role` and spins, keeping the cluster session alive, until the client's `Activated`; on `true`
  the host takes the open edge at once (epoch up, `Gate`), so the first `OpenConnection` is stamped with the epoch it
  is placed under. `onStandby` sends `Role(false)` and then the `Gate`. `OpenConnection` is windowed; `CloseConnection`
  and `GateClosed` are queued behind it in order but are outside the window and the retired counter (**H-5**), and
  `GateClosed` takes the edge down, epoch up, as the façade's `gateClosed()` does. A `Publish` on a connection whose
  `ConnectionOpened` is not yet placed stays at the head of the queue, un-retired, until the façade accepts it. An
  `OpenConnection` under an id the façade did not allocate fences the host with `CLIENT_PROTOCOL` (**H-4**).
- **Consumer.** For `Kind.CONSUMER`, when `proxy.consumerGroup` names a group, the host builds a `Consumer` for it. It
  subscribes to no stream 304, so the attachment is judged lost by the client's heartbeat counter alone (**H-9**), and
  `Attached` carries no `sourceId` (-1).
- **Not sent.** `AttachPending` is never needed, since the host closes a façade synchronously.
- **`kill -9` of the host or a client** is the same as before. The ghost scenario changed: with per-attachment images a
  command under another attachment's id cannot be sent, so the client publishes from a second publication on stream
  304, which the host does not read, and the test asserts nothing of it is sequenced.

The measurements of §3.1 to §3.7 were taken on the earlier bench-only wire (a 16-byte header, a nonce attach
and heartbeat messages) except the last run of §3.7, which is on schema 218. The epoch and exactly-once rows below
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
| the client abandons every cut (`SnapshotEnd(-1)`; the C++ stub, whose listener returns -1) | the host abandoned each round, the hosted replica confirmed none, and no instance fenced. Before the host read `recordCount` it took the records it held, none, for the client's state and fenced `SNAPSHOT_DIVERGED` |
| the client is replaced by a build that reads format 2 alone, the confirmed round format 1 | the host fenced `SNAPSHOT_UNRESTORABLE` and the client was handed no record. Before the host read the `Attach`'s formats it wrote every round as format 1 and restored format 1 to any client |
| the same, by a build that writes format 2 and still reads format 1 | the client was handed the round as format 1, its rounds went on agreeing with the direct replicas, and nothing fenced. Before the C++ stub sent its listener's `supportedFormatVersions()` it declared its own format alone and was fenced `SNAPSHOT_UNRESTORABLE` |
| a client with no snapshot listener, which lists no format | the host's façade took part in no round and cut the client nothing, and the hosted member's cuts became restart-safe. Before, it joined every round and abandoned it, and the member's cuts never did |

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

Not covered: a held ingress of seconds, the snapshots of a hosted `Gateway` pair (a single gateway's are), a
host killed in a cut, a leader killed and not paused, state of more than one record or a record near the 65,535-byte
limit, and a C# stub. Liveness by counter, the attach of **H-8**, the stub's other local checks and a C++ stub of an
`Application` are covered by §4.1.

### 4.1 The C++ stub against the host

`hosted::Application` (`seqeron-client/src/main/cpp/org/limitless/seqeron/hosted/Application.hpp`) is the C++ stub of an
`Application`: its listener is the direct `Application`'s, a `Payload` reaches it as `app::Payload` over the received
`Payload` message, and `publish` returns `protocol::Publish` after the local checks of a direct call (a retired
`payloadId`, a payload above `MAX_PAYLOAD_LENGTH`), the mirrored gate (**H-16**) and the window (**H-5**). It answers
the host's `SnapshotCut` from a `SnapshotListener`, restores from `RestoreRecord` and keeps the heartbeat counter. `reply` takes the
`ReplyAddress` of the request, and `GroupsResolved` hands the listener a `GroupDirectory` and gives `publish` the direct
call's group-mask check (`Refused` for a group the table lacks). `hosted::Consumer` (`.../hosted/Consumer.hpp`, 49 lines) is the stub of a `Consumer`: it sends
only `Attach` and `Detach`, takes no stream 304, and delivers `Payload` and `CaughtUp` to the direct `Consumer`'s listener. `hosted::Gateway`
(`.../hosted/Gateway.hpp`) is the stub of a `Gateway` on the same core (`hosted/detail/ClientLink.hpp`): its listener is the direct
`Gateway`'s, `onActivated` is answered with `Activated` and the stub then serves under the epoch one above the last `Gate`'s
(spec §17.5), `openConnection` allocates its ids from the last `Role` and returns `NO_CONNECTION` for a full window or a
back-pressured stream, and `closeConnection` and `gateClosed` retry within the call. `hosted_probe`
(`seqeron-client/src/test/cpp-probe`) holds the probe's state in it, as `HostedTestApplicationClient` does.

Five system tests drive the stubs against the Java host, with `HOSTED_PROBE` naming the binary:

- `HostedCppStubTest`: it attaches, the gate opens, 2,000 markers are sequenced once and in order, and the two local
  refusals fire. With the host's tracker at 4 and a 1 KiB window, 20,000 markers offered flat out are all placed and
  sequenced once, the stub having declined 223,070 times at the window, with a peak queue of 1,008 of 1,024 bytes. At
  a gate edge (the leader paused 4.5 s) the stub stopped at the `Gate` and offered 3,582, the host dropped 18 by their
  epoch and 3,564 were sequenced. In a fourth scenario the stub is handed the directory, a mask naming an absent group
  is refused and one naming the own group is published, and of 2,000 markers every tenth is answered: the 200 replies
  are each sequenced once under the sourceId, connectionId and groupMask of their request. In a fifth the stub attaches
  while the cluster has no leader (both followers paused, the leader stood down), and the host waits in the façade's
  cluster connect for more than the stub's 3 s host timeout; the stub keeps the host, whose idle strategy beats
  through the wait (**H-10**); the connect that met the election times out, the host connects again, and the stub
  catches up. Before the idle strategy beat, the stub gave the host up; before the host retried, it exited 70 at that
  timeout. In a sixth the host's façade cannot reach the cluster (the host on a follower, its UDP ingress naming
  absent members): after three attempts the host logs `CLUSTER_UNREACHABLE` and exits 70, and the stub exits
  `HOST_LOST`. Before, the connect's exception escaped the host's main thread. In a seventh the stub is paused past
  the host's 2 s client timeout: the host closes the attachment (`CLIENT_LOST`) and stays up, and the resumed stub
  finds its events image gone and exits `HOST_LOST`. Before, it ran on unattached. In an eighth both are paused across
  the attach, so the host drops the stub before its events subscription connects; the resumed stub finds no image
  within its host timeout and exits `HOST_LOST`. Before, it waited on. In a ninth the stub floods and is paused: the
  host's lag counter rises above 0, and is 0 once the resumed stub has applied every marker; it goes with the
  attachment when the host drops the stub. The first scenario also checks that the stub hears `ClusterTime`.
  Further scenarios: a client that dies while the façade's connect fails is dropped (`CLIENT_LOST`) before a second
  attempt; with stream 301 back-pressured from another publication and the host paused, the stub's `Detach` lands once
  the host resumes, and the host closes the attachment as detached, not lost; two hosts in one process, one unable to
  reach the cluster, are rebuilt apart, the other's client catching up undisturbed; and a host inside a gateway host's
  `ReplayerServer` serves its stub off the cluster, every marker sequenced, and outlives the stub. In the first
  scenario's gate edge the stub also retains exactly the 16 commands the host dropped (H-17). On a gateway host,
  whose host is off the cluster and so reopens its gate after a leadership change, a flooding stub with the leader
  paused until deposed loses nothing: the host dropped 16 by their epoch, and all 20,000 markers were sequenced once
  and in order, the stub ending with nothing retained. Each failed before its fix except the last three, which test
  what did not exist.
- `HostedSnapshotTest` with `HOSTED_CLIENT=cpp` runs its clients as the probe: restore, cuts, a client `kill -9`
  with an immediate successor, a host `kill -9`, an abandoned cut, the 8 s cut, the leading hosted replica, and the
  corrupt record fenced with `SNAPSHOT_DIVERGED`, each check as with the Java client and the direct replicas
  comparing the stub's rounds (**A-7**). In that mode only, a stub alone for its source cuts six records 500 ms apart
  without `keepAlive()`, longer than the host's 2 s client timeout, and keeps the client by the heartbeat it writes
  after each record. Not run with it: the stale-epoch scenario and the ghost command, which need a client that sends
  what the stub never would.
- `HostedGatewayStubTest` (`hosted_gateway_probe`, beside `hosted_probe`; one gateway row, no standby): with no
  snapshot policy, the stub opens three connections under the ids the first `Role` named (0, 1, 2), publishes 20 markers on each,
  and every one is sequenced under its own `connectionId`; the three `ConnectionOpened` and three `ConnectionClosed`
  reach the listener. A `Publish` that reached the host before its `ConnectionOpened` was placed stayed at the head
  (`headDeclines` 1), and nothing fenced. In a second scenario the stub then calls `gateClosed()`: the host takes the
  edge down and asks again with a second `Role` whose `firstConnectionId` is 3, past the highest id issued, and three
  more connections run the same way. In a third, on a row with `snapshot="true"` and rounds every 2 s, the stub answers
  each cut with one record, rounds are confirmed, and a successor stub attached after a `kill -9` is handed the last
  confirmed round before its `Role`.
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

## 5. Moving the C# client (proposed)

The second stage moves C# to the stub, as the first moved C++. Spec §17 needs no change for it: the host already runs
any of the three façades, and it checks a client's protocol version, not its Aeron version (spec §17.7), so a stub on
Aeron.NET 1.52.2 attaches to a host on Aeron 1.53.2.

**What C# sheds and keeps.** Retired, as for C++: the direct façades (`Application`, `Gateway`, `Consumer`, `Session`)
and their options, `GatewayLifecycle`, `SnapshotTaker`, `SnapshotFrames`, the stall fences, `QueueResolution`,
`Tuning`, everything in `Sequencer.Client` and `Replayer.Client`, `PortLayout`, `ReplayProtocol` and the replay codecs:
about 6,100 of the 8,175 lines under `seqeron-client/src/main/csharp`, and about 5,500 of the 7,700 test lines,
retired or rewritten. Kept, as C++ keeps them: the listener interfaces, `Payload`, `GroupDirectory`,
`GroupResolution`, `OutstandingWork`, `ClusterError`, `ISnapshotListener`, `ReplyAddress`, `LeaderGate` (for
`OutstandingWorkPropertyTest`'s model), `Publish`, `SeqeronCounters`, `SnapshotHeader`, `SnapshotFormat`, `Util`, and
the offline reader (`SequencedFrameDecoder`, `SystemFrame`, `FrameLayer` and the frame codecs), as C++ keeps
`unwrapFrame`. Of Aeron.NET, `Aeron.Client` stays for the driver connection and `Aeron.Cluster` goes, since only
`ClusterStreamSender` uses it. The parity rules shrink with it: `ReplayerRecovery` and `PendingSends` become Java's
alone, `ports.sh` is mirrored by Java's `PortLayout` alone, and `ProxyProtocol` gains a third copy.

**Where it runs.** C# is deployed on Windows beside a gateway host's media driver (`csharp-windows-test.sh`), where
the host runs inside `ReplayerServer` (question 4), so the box already runs the JVM the host needs. `ClientProxy` has
not yet run on Windows.

**The contract change applies to C# too.** Before a C# `Application` moves, it must track in `OutstandingWork` every
publish whose loss it cannot accept, a timer's included (spec §17.5).

**Stages**, each done when its check passes:

1. **The shared contract in C#.** `generateCSharpHostedSbe`, with `checkCSharpSbeCurrent` covering it; a C#
   `ProxyProtocol`; the 5300 range in C# `SeqeronCounters`. Done when the check passes and `GeneratedCodecsTest`
   asserts schema 218.
2. **The C# stub**, in `Org.Limitless.Seqeron.Hosted`, as C++'s is in `hosted`: an `internal` `ClientLink`, and
   `Application`, `Gateway` and `Consumer`, each with an options class carrying C++'s `Config` (`SourceId`,
   `Snapshots`, `HostTimeoutMs`, `AttachTimeoutMs`, `HostName`). The listener types stay in `App`. Done when it
   passes the stub tests of stage 3 and `HotPathAllocationTest` holds its dispatch and `publish` to zero bytes a frame.
3. **The system tests.** A C# hosted probe with an `Application`, a `Gateway` and a `Consumer` mode, and the stub
   tests of §4.1 run with either stub, as `HostedSnapshotTest` already selects one with `HOSTED_CLIENT`. Done when
   every scenario passes with each.
4. **Windows, and the measurements of §3.4.** `csharp-windows-test.sh` runs the host in the gateway host's
   `proxy.hosts` and attaches the probe to it; the hop and the host's allocation are measured from a C# client on
   Linux and on Windows. Done when they show no resolvable difference from direct at the median, as §3.6 does for
   C++, and are recorded in §3.
5. **Retire the direct C# client**, kept on a `csharp-direct-client` branch and tag `csharp-direct-client-final`. The
   examples move to the stub (`FollowStream` to a hosted `Consumer`, with `topology-csharp.xml` gaining the proxy
   hosts), `FacadeSurfaceTest` and `checkFacadeOnly` admit `Hosted`, and the phases of `csharp-client-test.sh` that
   drive the retired classes (A–E, H) go. Done when `dotnet test`, the examples against the packed package, CI on
   Linux and Windows, and the hosted system tests pass.
6. **The documents.** `client-api.md`'s C# column, `getting-started.md`, `building.md`, the parity rules in the
   `CLAUDE.md` files, and §1 and question 5 here.

**Open.**

- Whether the release that ships the C# stub also retires direct C#, as the C++ move did, or carries both for one
  release.
- Whether stage 4 gates the start or only the retirement (stage 5). Question 5 makes it gate the start.
