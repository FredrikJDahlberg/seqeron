# Output off the sequencer's duty cycle: plan

Proposed. Step 2 runs as a shadow (below); nothing else is implemented. It answers issues 4 and 6 of
`doc/reviews/enterprise-review.md`. Aeron references are to 1.53.2.

## 1. The rule

The service's duty cycle handles input: it applies a log entry to the `Sequencer` and returns. It never waits on
output — a queue's subscribers, a log sink, or the Replayer. Each output leaves through a hand-off that does not
block. When a hand-off is full:

- a log line is dropped and counted;
- a queue frame is not held anywhere: every frame is a function of the committed log, so whatever publishes the
  queue reads it from there.

Waits inside consensus stay: `writeState` on the archive's snapshot publication, `scheduleHeartbeat` on the consensus
module, and the `cut` line forced to `restart-safe.log` before the snapshot acks.

## 2. Where the duty cycle waits on output

| Wait | On | Bound |
|---|---|---|
| `QueuePublisher.emit` | the queue's subscribers | `sequencer.queueStallTimeoutMs` (5 s), then the member halts |
| `QueuePublisher.activate` | the service's own subscriber on each queue connecting | 5 s, then the member halts |
| `Logger`'s default sink | `String.format`, then `System.err.println` on the calling thread | none |
| `RestartSafeLog.isCut`, `appendCut` in `onTakeSnapshot` | the Replayer's `appendSafe`, which holds the same lock across `force(true)` | one fsync |

### 2.1 Back-pressure on IPC

An IPC publication is flow-controlled. `IpcPublication.updatePublisherPositionAndLimit` sets the publisher limit to
the lowest subscriber position plus the window, which is half the term: 8 MiB at `NodeDriver`'s 16 MiB IPC term.

- **A tethered subscriber** holds the limit for as long as its client is alive. One that stops reading stops the
  queue 8 MiB later, and the member halts after the stall timeout (review issue 4).
- **An untethered subscriber** holds it until evicted. Eviction starts once it is 6 MiB (¾ of the window) behind;
  after the untethered window-limit timeout (100 ms in `NodeDriver`, checked on its 10 ms driver timer) the image
  lingers, and the position leaves the limit only after the linger timeout, another 100 ms. The publisher has the
  remaining 2 MiB of headroom, then waits out the rest.

The second case applies to seqeron's own clients, which all subscribe untethered. A queue frame is 160–288 bytes for
a 50–200-byte payload (54 bytes of envelope and length prefix, 32 of data header, aligned to 32), so 2 MiB is 7,300
to 13,100 frames. The duty cycle then waits at most about 210 ms less the time those frames take to arrive:

| Ingress | 200-byte payloads | 50-byte payloads |
|---|---|---|
| 50k msg/s | ~65 ms | none |
| 200k msg/s | ~175 ms | ~145 ms |

### 2.2 Why configuration does not remove it

- Tethering is the subscriber's choice: `tether=` on its channel, else the driver's `aeron.tether.subscriptions`
  (`SubscriptionParams`). Nothing on the publication overrides it.
- Untethering the member driver's default also untethers Aeron Cluster's subscriptions on it. On the leader the
  consensus module records the log through a spy and hands the service a spy or IPC channel, both built without the
  configured log channel's parameters (`ConsensusModuleAgent.joinLogAsLeader`), so `tether=true` cannot be supplied
  for them. An untethered log recording or service log image is evicted 100 ms after falling a window behind.
  `Aeron`, `ConsensusModuleAgent` and `ClusteredServiceAgent` are final, so the channels cannot be rewritten
  without patching Aeron.
- Shorter eviction timeouts or a longer term change the numbers in 2.1, not the coupling. Eviction is never
  instant, and a timeout near zero evicts healthy consumers.
- Permissions on the Aeron directory limit subscribers to processes the operator runs, which include every
  application.

## 3. Plan

### Step 1: the service publishes no queue

- `SequencerService`'s `FrameSink.publish` stops offering. It advances each queue's position by `QueuePositions`,
  as `LogFollower.publish` does, and keeps calling `DefaultQueueRows.onPublish`.
- `recordQueueCuts` takes each marker's start from those positions instead of `lastOfferResults`, so the snapshot's
  marker positions and `CutHandover` keep their meaning. `QueueStart` and `DerivationStart` are unchanged.
- The queue publications, `queueDrains`, `QueuePublisher.emit`, `activate` and `checkRecordingAlive` leave the
  service. `scheduleHeartbeat` stays.

### Step 2: a queue agent on its own thread

An agent in the member's JVM, on its own `AgentRunner`, publishes every queue:

- It starts from the service's `DerivationStart`: it creates each active group's publication at `queueStarts`, on
  today's channel, stream, term length and MTU, and a `LogDeriver` restored from the start state re-emits a restored
  marker, as `LogFollower` does. A topology load the derivation reaches activates the groups it names.
- It reads the log's recording with `RecordingReader`, bounded by the commit position. A follower whose recording
  trails its commit position gets a short read, and the agent retries.
- It keeps its own subscriber on each queue and reads past every frame it offers, as the service does today, so an
  offer lands with no application subscribed.
- It never spins. A back-pressured frame is kept and offered next cycle, and no log entry is read while one is kept:
  `AeronLogRelay`'s discipline. A landed offer must end where `QueuePositions` says, or the member halts.
- The `queueBackPressureAlerts` and `queueStalled` counters move to it.

Its decisions sit in a class free of Aeron, unit-tested the way `QueuePublisher` and `LogRelay` are.

Step 2 lands before step 1, as a shadow: `ShadowQueueAgent`, on a thread of the member's `ReplayerServer` while
`sequencer.shadowQueues` is set, publishes every queue on stream 3000 + `groupId` beside the service's, and its
decisions are `QueueEmitter`'s. `DerivedQueueTest` holds each member's shadow queues to its queues, frame for frame
and position for position, across a leader stop and a restart from a cluster snapshot. The latency measurement below
compares the two streams on one member (`ClusterProbe shadow`), and step 1 then retires the service's.

### Step 3: bound how far the agent falls behind

The `LogTrimmer` must keep the log from the agent's position, as it keeps it from `LogFollower`'s first checkpoint,
so a queue held forever would hold the log until the archive reports `STORAGE_SPACE`. When one queue has been
back-pressured for `sequencer.queueStallTimeoutMs`, the agent:

1. logs the registration id of each subscriber position at the queue's limit (counter type 4, registration id at
   key offset 0);
2. derives the frames it has not published without offering them, up to the commit position;
3. closes that queue's publication, which takes down every image on it, and creates it again at the derived
   position.

Consumers heal the skipped frames by replay, as after an untethered eviction. The member keeps running, and the
property's meaning changes from "halt after" to "skip after".

### Step 4: logging off the calling thread (review issue 6)

- `Logger` hands each record to a bounded ring drained by one logging thread per process, which formats and writes.
  `String.format` and the write leave the calling thread. The boxing of `Object...` arguments stays, to be measured.
- A full ring drops the record and counts it in a new counter.
- Records are rate-limited per event code, as `Episode` does in the Replayer.
- The `LogDeriver`s that follow the log log nothing, so a restart does not repeat every historical rejection.
- `Logger` is client-tier API with C++ and C# twins (`Logger.hpp`, `Logger.cs`). The default sink changes in all
  three, and `doc/client-api.md` with it. `LoggerSink.record` is then called on the logging thread.

### Step 5: `RestartSafeLog`

The lock guards only the in-memory `cuts` and `safe` sets. `append` writes its line with one `FileChannel.write`,
which the channel serializes, and forces outside the lock, so the service's `isCut` and `appendCut` never wait for
the Replayer's fsync.

### Step 6: tests

- The agent's decisions: a kept frame on back-pressure, no read while one is kept, the position check, the skip
  after the stall timeout.
- `DerivedQueueTest` compares the live queue with the derivation frame for frame; it also compares the marker
  positions the service records with the ones the agent's offers produce.
- A system test that stops (`SIGSTOP`) a child JVM holding a tethered queue subscription during high-rate ingress:
  `clusterSeqNo` keeps advancing at the same ingress latency, the other members' queues keep advancing, and the
  stopped member's queue skips and its consumers heal.
- A flood of rejected frames with a sink that blocks: the service's apply rate does not change, and the drops are
  counted.

## 4. Costs and open questions

- **Latency.** Today a frame reaches the queue in the duty cycle that applies it. From the log it waits for the
  agent's next poll and a file read. On the leader the entry is in the page cache by commit, since commit waits for
  the leader's own recording (`disk-load-plan.md` §2). Measure on EC2 (`doc/ec2-tests.md`) before step 1 merges. If
  the cost is too high, add a fast path: the service writes each frame into a single-producer ring, and the agent
  falls back to the log only when the ring is full.
- **A second derivation.** The member already runs one `LogDeriver` in `LogFollower`. The agent adds another, on
  another thread. Sharing one would need a thread-safe hand-off of the follower's runs and checkpoints; whether that
  is worth it is open.
- **Re-created queues.** Step 3 relies on `ReplayerRecovery` healing across a queue publication replaced
  mid-run, with a new session id. Verify it in Java and C#, and in the C++ hosted stubs.
- **Documents to change with the code.** `seqeron-service/CLAUDE.md`, `doc/overview.md`, `doc/fault-tolerance.md`
  and `doc/ops.md` (the stall timeout's meaning, the counters).
