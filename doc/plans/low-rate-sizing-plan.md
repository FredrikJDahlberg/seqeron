# Low-rate sizing and failure plan

What a production member needs at 10 msg/s on average and 100 msg/s at peak, and whether the cluster holds its
guarantees while members fail. Steps 1 to 4 ran once on 14 November 2026 (section 7). The figures in sections 2 and
5 are estimates made before that run.

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

`CPUS=3 HEAPS=512m DURATION=1800 SAMPLE_SECONDS=30 BURST_SECONDS=0 RATE=10 ./sweep.sh`

Thirty minutes at the average rate. Read from `samples.csv`:

- the slope of `anon` and `shmem`: a flat working set, or a leak;
- `data_kb` per hour, times 8,760, for the yearly log size and the volume to provision;
- `shm_kb` growth, extrapolated to the point every term has cycled, against the `/dev/shm` limit.

Thirty minutes cannot show year-scale effects; it measures the rates to extrapolate from.

### Step 5: OpenShift session

The platform's own effects, and only those: steps 1 to 4 carry over, since a pod's CPU limit is the same CFS quota
docker applied. About 2.5 to 3 hours on a managed cluster, which a few hours of preparation beforehand make possible.

**Before the cluster exists** (no cloud cost; the session cannot start without them):

- the manifests: StatefulSet (`podManagementPolicy: Parallel`), headless Service with `publishNotReadyAddresses`,
  PodDisruptionBudget of one, NetworkPolicy for the port block, anti-affinity across nodes and zones,
  `fsGroupChangePolicy: OnRootMismatch` on the volume;
- a readiness check that does not start a JVM each period, since an exec probe runs inside the container's CPU quota;
- the test script, run from one command, each scenario waiting for the previous member to be current;
- the image in a registry the cluster can pull from;
- a rehearsal of the manifests on a local Kubernetes cluster, which catches StatefulSet, DNS, probe and volume
  mistakes and not OpenShift's security constraints or its network;
- the account's spend limit and region policy checked: the installer and ROSA call EC2 and IAM actions the policy may
  not allow.

**Cluster:** ROSA with a hosted control plane, four workers of 4 vCPU and 16 GiB (a 3-CPU pod fits a 4 vCPU worker with
little to spare once the node's reservations are taken), three of them for the members and one for the load. Self-managed
OpenShift costs about the same an hour, takes longer to create, and needs a Route53 domain.

| Step | Time | Running total |
|---|---|---|
| Create the cluster | 20 min | 0:20 |
| Deploy; DNS, election and readiness | 20 min | 0:40 |
| One 5-minute CPU cell at 3 CPUs, order checked | 10 min | 0:50 |
| Kill the leader pod, then a follower pod | 10 min | 1:00 |
| Node loss: stop the leader's node, time until the member rejoins | 15 min | 1:15 |
| Drain a node under the disruption budget | 10 min | 1:25 |
| Rolling restart, one member at a time | 15 min | 1:40 |
| Exit 70: the alert fires and the pod restarts | 10 min | 1:50 |
| NetworkPolicy: the UDP block open, nothing else | 10 min | 2:00 |
| Slack and a repeat of one failure | 20 min | 2:20 |
| Teardown | 15 min | 2:35 |

Pass: the order check passes in every scenario, no two members are ever down at once under a drain or rolling restart,
and the CPU cell shows no throttling.

**The node-loss time depends on the remediation timeout.** A Machine Health Check commonly waits 300 s before it
replaces a node. Shorten it to 60 to 120 s for the session, record the value used, and state the time at the production
setting separately: the two give different outages, and the cluster has no tolerance for a second fault during either.

Not repeated here: the soak, the log-size recovery and the CPU and memory sweeps.

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

Step 5 covers them. The manifests it needs are not written yet.

## 5. Cost

Prices are `ec2-tests.md`'s, us-east-1 on-demand.

| Part | Host | Time | Cost |
|---|---|---|---|
| Steps 1 to 4 | 1 × c6in.4xlarge, $0.91/hr | about 1.9 h measured | about $1.7 measured |
| Step 5 | ROSA, 4 × m6i.xlarge, about $2/hr with the platform fee, NAT and volumes | about 2.5 to 3 h | about $5 to 7 (estimate) |

The ROSA hourly figure is from recalled list prices and is not checked. Left running, the cluster costs about $48 a day.

The 4 CPU cells put 12 of the host's 16 vCPUs under three members, with the dockerd and the probes beside them: read
that cell as an upper bound, not a floor.

## 6. Open questions

- The latency target. At `backoff` the first frame after a quiet period pays the idle strategy's wake-up; the
  hosted-façade run measured 61 µs at p50 (`ec2-results-recorded.md`). If the production target is in milliseconds,
  this does not matter.
- Whether production sets a `<snapshots>` policy. Step 3 gives the number to decide it on.
- Whether `SequencerServer` runs its shutdown barrier on SIGTERM, which the pod's termination relies on.

## 7. Results

One c6in.4xlarge (16 vCPU, Ubuntu 24.04, docker with cgroup v2, gp3 root volume), three members as containers with
the limits below, 14 November 2026. Each cell is one run. The members share one host and one bridge network, so these
are floors for the software under a CPU quota, not figures for a small instance with its own network and disk.

**Step 1, CPU** (512m heap, 1g `/dev/shm`, 100 msg/s for 300 s, then 2,000 msg/s for 30 s):

| CPUs | Order | Elections | Throttled periods | Throttled ms | Average cores | Peak cores | Working set |
|---|---|---|---|---|---|---|---|
| 1 | exact | 0 | 247 | 6,794 | 0.23 | 0.25 | 218 MiB |
| 2 | exact | 0 | 3 | 94 | 0.24 | 0.28 | 219 MiB |
| 3 | exact | 0 | 0 | 0 | 0.24 | 0.26 | 211 MiB |
| 4 | exact | 0 | 0 | 0 | 0.24 | 0.26 | 213 MiB |

3 CPUs is the smallest that passes with no throttling. One CPU throttled the busiest members in about 4% of 100 ms
periods during the run and caused no election. Steady use is a quarter of a core at every size, so the limit covers
bursts, not the rate.

**Step 1, memory** (2 CPUs): heap 256m, 512m and 1g against `/dev/shm` 512m and 1g. Every cell kept the order exact
with no election, a working set of 206 to 237 MiB, and a longest GC pause of 5 ms or less. No setting failed, so the
floor is below 256m heap with 512m of `/dev/shm`, the smallest tried (a 1,792 MiB limit). The cells' throttled
periods (4 to 7) are the 2-CPU result above.

**Step 2, failures** (3 CPUs, 100 msg/s):

| Scenario | Failover | Rejoin | Order |
|---|---|---|---|
| `kill-leader` | 4.5 s | 12.4 s | exact, 328 frames resent |
| `kill-follower` | n/a | 5.7 s | exact |
| `wipe-follower` | n/a | 5.9 s | exact |
| `throttle-leader` (0.3 CPU for 20 s) | n/a | n/a | exact, no election |

The failover is a little over the 3 to 4 s of `ops.md`, from one sample. A restart within 10 s of a SIGKILL fails on
the active mark file (`ops.md`), so the scripts wait 12 s first.

**Step 3, recovery** (3 CPUs, 100-byte payload, about 219 bytes of log a frame):

| Frames | Log | Queue current | CPU | Peak memory | Anon |
|---|---|---|---|---|---|
| 5M | 1.04 GiB | 5.9 s | 4.4 s | 378 MiB | 83 MiB |
| 20M | 4.17 GiB | 18.4 s | 13.5 s | 2,132 MiB | 93 MiB |

Time to a current queue is 1.7 s plus 0.84 s a million frames. A year at 10 msg/s is about 315M frames and 69 GB, which
is about 4.4 minutes at that rate. The log had just been written and was in the page cache, so that is a lower bound:
reading 69 GB cold from a gp3 volume at its default 125 MB/s takes about 9 minutes by itself. The peak memory is page
cache, which the kernel reclaims; the working set is `anon` plus `shmem`.

**Step 4, soak** (3 CPUs, 10 msg/s for 30 minutes): order exact, no election, no throttling, 0.08 cores on average,
a working set of at most 156 MiB, longest GC pause 3.2 ms. `anon` was flat after warm-up (89 to 91 MiB on two
members, 98 MiB on the one carrying the load). The log grew 8.1 MB an hour on each member, within 0.2% of each other,
which is about 68 GiB a year. `shmem` grew from 46 to 57 to 64 MiB, and 30 minutes does not show where it stops.

**Open after this run:** repeat the 2-CPU and 3-CPU cells to separate the floor from noise (the 2-CPU cell's throttled
periods ranged from 3 to 7 over seven runs); the failover time over several kills; a soak long enough for `/dev/shm`
to level off; and all of it on production-sized instances, one member per host.
