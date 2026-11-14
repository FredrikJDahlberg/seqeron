# Low-rate sizing and failure plan

What a production member needs at 10 msg/s on average and 100 msg/s at peak, and whether the cluster holds its
guarantees while members fail. Nothing here has been run: the scripts are written and syntax-checked only, and every
figure below is an estimate to replace with a measurement.

## 1. Why the existing benchmarks do not answer it

`ec2-results.md` drives 50k to 909k msg/s through busy-spinning members on 16 pinned cores. That fixes two things
about the production question:

- **CPU.** A spinning thread uses a core at any rate, so those runs say nothing about the least a backing-off
  member needs. Production uses `backoff` (`sequencer.idleStrategy`) and `SHARED` driver and archive threading, so
  CPU follows load and the limit has to cover bursts, elections and recovery.
- **Memory.** Heap, term buffers and `/dev/shm` are set by configuration, not rate. The rate sets only how fast mapped
  pages become resident, and a 64 MiB Raft log term takes hours to cycle at 10 msg/s (`ops.md`, "Term lengths").

The thing to establish is the floor at which a member still meets the 2 s leader heartbeat, and what a failure costs.

## 2. Assumptions

- 50 to 200 bytes of payload; about 300 bytes a message on the log with its headers.
- Three members, one per failure domain. Quorum is two, so one member may fail.
- `backoff` idle strategy and `-Daeron.threading.mode=SHARED`, `-Daeron.archive.threading.mode=SHARED`.
- No `<snapshots>` policy, so recovery is a full-log replay (`fault-tolerance.md` §0) and the log is never trimmed.
- At 10 msg/s the log grows about 260 MB a day, about 95 GB a year, per member. Check against step 4.

## 3. Method

`docker/compose.sizing.yml` lays CPU, memory, `/dev/shm` and JVM flags over `docker/compose.yml`, so one container
is one member with the limits a pod would have. The scripts are in `seqeron-service/src/test/scripts/sizing/`.

The load and every counter read run in a throwaway container that shares the member's `/dev/shm`
(`--ipc container:node-N`). A probe inside the member's container would spin in its CPU quota and distort the
measurement. Memory is read from the member's cgroup as `anon` plus `shmem`, the working set; `memory.current` also
counts page cache, which the kernel reclaims.

Prerequisites: `./gradlew operatorDist`, docker with cgroup v2 on Linux. Docker Desktop's VM is good enough to
rehearse the scripts and not to measure.

### Step 1: CPU and memory floor (`sweep.sh`)

`CPUS="1 2 3 4" HEAPS="256m 512m 1g" SHMS="512m 1g" ./sweep.sh`

Each cell is a fresh cluster under its limits carrying 100 msg/s for five minutes, then 2,000 msg/s for 30 s, with
the order check (`ClusterProbe confirm`: every frame once, in order) running throughout. A cell passes when the
order check passes, no election occurs and no member is CPU-throttled. `summary.csv` also records average and peak
cores used, the working set and the longest GC pause.

Result: the smallest CPU, heap and `/dev/shm` that pass, and the margin above them.

### Step 2: failures at the production rate (`faults.sh`)

`./faults.sh`, at the limits step 1 chose.

| Scenario | Fault | Records |
|---|---|---|
| `kill-leader` | SIGKILL the leader | failover time, rejoin time |
| `kill-follower` | SIGKILL a follower | rejoin time |
| `wipe-follower` | SIGKILL, delete the member's volume, restart | time to a current queue after a copy of the log |
| `throttle-leader` | cap the leader at 0.3 CPU for 20 s | elections the starvation causes |

Pass: the order check passes (nothing lost, duplicated or reordered), the failover is within the 3 to 4 s `ops.md`
gives, and the rejoin completes. A network partition needs the faulteron injector: `networkFaultTest` already runs
those (`PartitionLeaderTest` and the others in `seqeron-service/src/test/scripts/CLAUDE.md`).

### Step 3: recovery against a long log (`recovery.sh`)

`FRAMES=5000000 ./recovery.sh`, then again at four times the size.

It writes synthetic frames at full speed, kills a follower and times its restart until the queue is current, with
the CPU and memory used. Replay is linear in the log, so two sizes give the line and the extrapolation to a year of
log (step 4's growth rate). A restart in year two is the longest outage a member has; it is the figure that decides
whether a snapshot policy is needed (`snapshot.md`), at the price that every consumer must then restart from a
snapshot file.

### Step 4: soak (`sweep.sh`, one cell)

`CPUS=2 HEAPS=512m DURATION=7200 SAMPLE_SECONDS=60 BURST_SECONDS=0 RATE=10 ./sweep.sh`

Two hours at the average rate. Read from `samples.csv`:

- the slope of `anon` and `shmem`: a flat working set, or a leak;
- `data_kb` per hour, times 8,760, for the yearly log size and the volume to provision;
- `shm_kb` growth, extrapolated to the point every term has cycled, against the `/dev/shm` limit.

Two hours cannot show year-scale effects; it measures the rates to extrapolate from.

## 4. Not covered here

These need a real cluster, and `docker` cannot reproduce them:

- **Node loss and fencing.** A StatefulSet will not replace a pod on an unreachable node, and a zonal volume attaches
  only in its own zone, so a lost node leaves two members until a remediation operator (Node Health Check with Self
  Node Remediation, or a Machine Health Check) removes the node. Measure node loss until the member has rejoined; the
  cluster has no tolerance for another fault until then. Decide between three members and five on that number.
- **Planned disruption.** A drain under a PodDisruptionBudget of one, and a rolling restart that waits for each member
  to be current before the next.
- **Exit 70.** A member that exits with `NodeDriver.EXIT_FATAL` (a truncated applied log, a stalled queue) must be
  restarted by the platform, and the event must reach an alert rather than a restart loop.
- **The network.** UDP across the pod network, and NetworkPolicy on the 70-port block.

One short session on OpenShift on EC2 covers them. The manifests are not part of this plan.

## 5. Cost

Prices are `ec2-tests.md`'s, us-east-1 on-demand.

| Part | Host | Time | Cost |
|---|---|---|---|
| Steps 1 to 3 | 1 × c6in.4xlarge, $0.91/hr | about 5 h | about $5 |
| Step 4 | same host | 2 h | about $2 |
| OpenShift session | three workers and a control plane or a managed service | about 5 h | about $10 to 15 (estimate) |

The 4 CPU cells put 12 of the host's 16 vCPUs under three members, with the dockerd and the probes beside them: read
that cell as an upper bound, not a floor.

## 6. Open questions

- The latency target. At `backoff` the first frame after a quiet period pays the idle strategy's wake-up; the
  hosted-façade run measured 61 µs at p50 (`ec2-results-recorded.md`). If the production target is in milliseconds,
  this does not matter.
- Whether production sets a `<snapshots>` policy. Step 3 gives the number to decide it on.
- Whether `SequencerServer` runs its shutdown barrier on SIGTERM, which the pod's termination relies on.
