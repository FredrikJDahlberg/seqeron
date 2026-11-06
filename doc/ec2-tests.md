# Tests on EC2

Three measurements that a laptop, a Docker bridge or a shared 4-vCPU CI runner cannot make: they need separate hosts
on a real network, dedicated cores, or both. This document says what each one answers, what it costs, how long it
runs and how far its result carries to bare metal, and how to run it with the scripts in
`seqeron-service/src/test/scripts/multihost`.

Prices are us-east-1 on-demand Linux, October 2026. c6in prices scale linearly from c6in.xlarge's $0.2268/hr; check
them before a session.

## 1. The tests

### Test 1: the hosted-façade hop on a tuned cluster

[`hosted-facade.md`](hosted-facade.md) §2 question 2 and the unmeasured items of §3.4 wait on it, and its result decides
whether the hosted C++ client stays. The hop has been measured on a laptop (§3.1, §3.2) and in an aarch64 VM (§3.6,
§3.7) with a bench client on the prototype wire, where a whole loop of 233–389 µs at p50 hides it. No figure comes from the
real stub, schema 218 at protocol version 2, bare metal or a cloud instance. A pinned, busy-spinning cluster on three
hosts should commit in about 100 µs (Aeron's own three-node result on c6in.16xlarge is 95 µs p50 at 100k msg/s), so the
same comparison shows more of the hop and of the host's pauses.

**The question.** On a quiet machine of the deployment's kind, with the host spinning on a core of its own, how much do the
hosted C++ `Application` and `ClientProxy` add to the whole loop (`publish` until the application sees its own frame
sequenced), how many cores do they take, and what do they do at a gate edge?

**The rule.** A hosted arm passes at a rate when, over at least five interleaved runs, each percentile of the whole loop
(p50, p99, p99.9) is within the range the direct arm's own runs span plus a margin; when no command is lost at a gate edge
(spec H-17); and when the host takes no more than the two cores of §4. The margins are **p50 25 µs, p99 250 µs, p99.9 2 ms:
placeholders that the owner sets before the first run**. A difference smaller than the direct arm's own run-to-run spread is
not a difference, as in `hosted-facade.md` §3. The rule is applied to each environment on its own.

**Arms.** All publish the same open-loop stream against the same cluster on the leader's host, interleaved within a rate,
against a fresh cluster per rate. The cluster protocol is unchanged, so one jar serves them all.

| arm | client | host |
| --- | --- | --- |
| `direct` | C++ `app::Application` built from the tag `cpp-direct-client-final` | none |
| `hosted-spin` | `hosted::Application` of this tree | `ClientProxy`, `proxy.spin=true` |
| `hosted-backoff` | as `hosted-spin` | `ClientProxy`, default idle strategy (5k and 20k msg/s only) |
| `java-direct` | Java `Application`, optional | none; separates the language from the hosting |

**Two sets.** Test 1a is the short matrix: 3 rates × 3 modes × 3 repeats of 30 s, to prove the scripts and the instances and
to see the size of the effect. Test 1b is the decision matrix, timed from the intended send time so that a stall counts
against the arm, each run with a 30 s warm-up that is discarded and 120 s measured:

| set | payload | rate (msg/s) | arms | runs |
| --- | --- | --- | --- | --- |
| rate sweep | 18 B | 5k, 20k, 50k, 100k, 200k | `direct`, `hosted-spin` | 5 |
| idle strategy | 18 B | 5k, 20k | `hosted-backoff` | 5 |
| payload sweep | 256 B, 1 KiB, 8 KiB | 20k; 1 KiB also 100k | `direct`, `hosted-spin` | 5 |
| knee | 18 B, 1 KiB | ramp to saturation | `direct`, `hosted-spin` | 2 |
| soak | 256 B | 20k for 1 h | `direct`, `hosted-spin` | 1 |
| gate edge | 18 B | flood, leader paused 4.5 s | `direct`, `hosted-spin` | 5 |

The gate-edge set records, per run, the commands dropped, the time from the new leader's `LeadershipChanged` until every
offered command is sequenced, and the number lost; the hosted arm must show 0. It follows `HostedCppStubTest`'s resend
scenario. Run order within a rate is randomised.

**What a run records.** The whole-loop histogram (HdrHistogram, 1 µs resolution), the achieved rate and the stub's window
declines; the host's `queue:` line (placed, `epochDrops`, `headDeclines`, `peakQueued`), its hosted-lag counter once a second and
the stub's retained count; CPU per process (`pidstat 1`), `mpstat -P ALL` and context switches; the GC and safepoint logs of the
host and the members (`-Xlog:gc,safepoint`, `-Xms` equal to `-Xmx`, `-XX:+AlwaysPreTouch`, the default collector first, and
`-XX:+UseZGC` on the host only if its pauses appear in the p99.9); `lscpu`, `uname -a`, the git sha, the Aeron version and the instance
and image ids. **The stub's allocation in the measured window** is counted in the bench, from the first published marker to the last: the
host's is shown to be zero (`hosted-facade.md` §3.5), the stub's is not. `hosted_hop_bench` hooks `operator new`; hooking `malloc` as well is not done.

**Tooling.** Test 1a runs on what exists. The scripts are in `seqeron-service/src/test/scripts/multihost`.

- `hosted_hop_bench` (`seqeron-client/src/test/cpp-bench/HostedHopBench.cpp`, CMake target of the same name): the open-loop generator over the
  real `hosted::Application`. `--payload` sets the marker's length (18 to 8875 B) and `--warmup-ms` the discarded warm-up (default 20% of
  the run). It prints the whole loop's p50, p99, p99.9 and max, the declines, the stub's window declines and retained count, and the
  allocations.
- The `direct` arm is `hop_bench` of the tag `cpp-direct-client-final`, built in a separate checkout against the same Aeron version
  (§4) with `direct-hop-bench-payload.patch` applied, which gives it `--payload` and a `payload=` field in its line;
  `MULTIHOST_DIRECT_BENCH` names it.
- `hop-matrix.sh` and `node.sh` start `ClientProxy` with `proxy.*` properties and run the arms and modes above, one fresh cluster per
  rate, interleaved within it. `HOP_RATES`, `HOP_REPEATS`, `HOP_SECONDS`, `HOP_MODES` and `HOP_PAYLOADS` select the set. Both
  benches drop the first 20% of a run, so `HOP_SECONDS=150` is 30 s of warm-up and 120 s measured.
- `hop-knee.sh` ramps the rate on one fresh cluster per arm, payload and run, in steps of `KNEE_SECONDS`, and ends each ramp in a
  `knee` line: the last rate below the first step whose p99 exceeds `KNEE_P99_US` (default 10 ms), that loses a marker or whose
  leader moved. The limit is a choice, not a measurement; the step lines are in `hop-matrix.sh`'s format.
- `hop-analyze.py <results>…` applies the rule's latency part: per rate and hosted mode, the hosted median of each percentile
  must lie within the direct runs' range plus the margin (`--margins`, `--min-runs`), and no run may fail or lose a marker.

The rate sweep, the idle-strategy set, the payload sweep and the knee of Test 1b run with these settings; the 1 KiB run at 100k msg/s is a
second invocation with `HOP_PAYLOADS=1024 HOP_RATES=100000`. Not built yet:

- the soak's histogram, since each bench keeps every sample;
- the gate-edge set: a flood mode that counts offered, sequenced and lost commands in both benches, and a driver that pauses the leader;
- a manifest per run and the recordings under "What a run records" other than the bench's own line;
- the rule's other criteria in `hop-analyze.py` (commands lost at a gate edge, the host's cores), and a randomised run order.

The tooling is validated on localhost first (`MULTIHOST_MEMBERS=localhost,localhost,localhost`): the direct arm and the
hosted arm must reproduce `hosted-facade.md` §3.6 within its spread before an instance is launched. A one-instance variant
of the matrix (all members, the host and the client on one instance, cores apart) takes the network out; a difference from the
three-instance run is the network.

**Threats.** Steal, frequency scaling and noisy neighbours on the virtualised instance: record the clock source and the `steal`
column of `/proc/stat` per run, and repeat a run with steal above 1%. No burstable (`t`) families. The first run after a cluster start is
the worst (JIT, warm-up), so each run discards a warm-up and each rate gets a fresh cluster. A faster commit than the laptop's 250 µs
makes the hop a larger share of the loop; that is the question, so the cluster is not slowed to match. Time is slewed by `chrony`, never
stepped: a step back of more than 10 s terminates a member (`ops.md`).

### Test 2: the network-fault suite without CPU steal

`failover.yml` runs `networkFaultTest` with `TIMEOUT_SCALE: '3'` to cover the CPU steal of a shared 4-vCPU runner,
and locally the faulteron tests need a Linux VM for eBPF. On one 16-vCPU Linux instance the suite runs unchanged,
natively and without the scale, and the failover soak can run far more rounds than CI's 15. No script: the commands
are in §5.

### Test 3: replay across a network, and the cost of a forced archive

Every replay in the suites is served over loopback or IPC, which loses nothing and has no bandwidth limit.
`replay-remote.sh` repeats `ReplayColdStartTest`'s measurement twice against one preloaded cluster: a cold
`ClusterProbe follow` on member 1, replayed over `aeron:ipc`, and a gateway host on a fourth host, whose relay copies
the queue recording over UDP before a cold follow replays it there. Each phase checks the replica delivered every
preloaded frame.

The members run the archive with Aeron's default `aeron.archive.file.sync.level` of 0: nothing is forced to disk.
Running test 1 again with `MULTIHOST_MEMBER_OPTS="-Daeron.archive.file.sync.level=1
-Daeron.archive.catalog.file.sync.level=1"` measures what forcing it would cost on the instance's storage.

## 2. Instances and cost

| Role | Instance | vCPU / cores | Network | $/hr |
| --- | --- | --- | --- | --- |
| Member (tests 1, 3) | c6in.8xlarge, ×3 | 32 / 16 | 50 Gbps | 1.81 each |
| Member on bare metal (tests 1, 3) | c6id.metal, ×3 | 128 / 64, two sockets | 50 Gbps | 6.45 each |
| Driver and gateway host (tests 1, 3) | c6in.4xlarge | 16 / 8 | up to 50 Gbps | 0.91 |
| Fault suite (test 2) | c7i.4xlarge, ×3, on-demand | 16 / 8 | up to 12.5 Gbps | 0.71 each |
| Failover soak (test 2) | c7i.4xlarge, spot | 16 / 8 | up to 12.5 Gbps | about 0.25 |
| Image build | c6in.large | 2 / 1 | up to 25 Gbps | 0.11 |

A member busy-spins about nine threads with the driver and archive dedicated (driver conductor, sender and receiver;
archive conductor, recorder and replayer; consensus module; service; Replayer). c6in.8xlarge is the smallest size that
gives each one a core with two to spare. c6in.16xlarge, Aeron's choice, holds a whole socket and its L3 to itself and
costs twice as much; it narrows the tail, not the median. c6in.4xlarge forces shared threading, which is a different
configuration from the one being measured.

Medium instances (c7g.medium, t3.medium, about $0.04/hr) cannot run a busy-spinning member, and t3 throttles a
spinning thread once its CPU credits are spent. Use them, or localhost, only to rehearse the scripts.

### Network traffic

Traffic between instances in one Availability Zone over private IPs is free, which is the only configuration these
tests use: one cluster placement group, and private addresses in `MULTIHOST_MEMBERS` and `MULTIHOST_CLIENT`. At 100k
msg/s of 288-byte messages, about four streams cross instance boundaries at 33 MB/s each, about 0.5 TB/hr. Across
Availability Zones that is $0.02/GB, about $10/hr, and over public addresses in one zone half that. Other charges:
$0.005/hr per public IPv4 address, a NAT gateway's $0.045/hr and $0.045/GB if the instances sit in a private subnet,
and $0.09/GB to the internet past the first 100 GB a month. Seqeron uses no multicast, which a VPC has no native
support for.

## 3. Runtimes

The times assume the image of §4 and scripts already rehearsed on localhost. A run of `hop-matrix.sh` costs its
`HOP_SECONDS` plus about 2 s (direct) to 7 s (hosted) of startup, measured in the localhost rehearsal; a fresh cluster
per rate adds about 3 s.

| Step | Wall clock | Instances | Cost |
| --- | --- | --- | --- |
| Image build, beforehand | about 1 h, unattended | c6in.large | 0.11 |
| Launch from the image, check `cluster.sh up` | 10 min | 3 × c6in.8xlarge + c6in.4xlarge | 1.05 |
| Test 1a: 3 rates × 3 modes × 3 repeats of 30 s | 25 min | the same | 2.65 |
| Sync comparison: test 1a at one rate, sync on and off | 10 min | the same | 1.05 |
| Test 3: preloads of 400,000 and 4,000,000, quiet and under load | 15 min | the same | 1.60 |
| Test 2: the fault suite, three copies in parallel | 15 min, alongside | 3 × c7i.4xlarge | 0.55 |
| Test 2: the failover soak, 200 rounds of about 35 s | 2 h, alongside, unattended | c7i.4xlarge spot | 0.50 |
| **Session** | **about 1 h attended** | | **about 7.50** |

EBS for a day (four 100 GB gp3 volumes) and the public addresses add about $1. Every hour the four test 1 and 3
instances run past the plan costs $6.35, so stop them between attempts and launch them with a shutdown timer (§4).
Budget twice the session for a first attempt.

With the members on c6id.metal the four hosts cost $20.26/hr. Allow about 10 minutes more for a metal launch, so the
launch and check take 20 minutes: tests 1 and 3 cost about $23.60, the session about $24.80, and every hour past the
plan $20.26. Test 1a alone on metal, after the session above, takes 45 minutes and costs about $15.20, for a direct
comparison of the same CPU with and without the hypervisor.

Test 1b is a session of its own. Its runs come to about 100 of 150 s (4.2 h), the knee ramps (0.7 h), the soak and its warm-up
(2.1 h), the gate-edge runs (0.3 h) and setup (0.3 h): about 7.5 h on the four Test 1 hosts. That costs about $48 on
c6in.8xlarge ($6.35/hr) and about $152 on c6id.metal ($20.26/hr), from the instance prices above. Neither includes the
rehearsals, so allow the first attempt twice the figure; the soak and the knee ramps run unattended.

## 4. Image

One Ubuntu 24.04 x86_64 image serves every host. Build it on a c6in.large and save it as an AMI:

```bash
sudo apt-get install -y openjdk-21-jdk-headless cmake g++ git perl docker.io
git clone <this repository> ~/seqeron && cd ~/seqeron && git checkout <branch>
./gradlew uberJar :seqeron-service:compileTestJava
cmake -B cmake-build-release -DCMAKE_BUILD_TYPE=Release
cmake --build cmake-build-release --target hosted_hop_bench
git worktree add ../seqeron-direct cpp-direct-client-final    # the direct arm: hop_bench of the tag
git -C ../seqeron-direct apply "$PWD/seqeron-service/src/test/scripts/multihost/direct-hop-bench-payload.patch"
cmake -S ../seqeron-direct -B ../seqeron-direct/cmake-build-release -DCMAKE_BUILD_TYPE=Release
cmake --build ../seqeron-direct/cmake-build-release --target hop_bench
```

Tuning, all of it applied at boot so the image carries it:

- **Hyper-threading off at launch**: `--cpu-options CoreCount=16,ThreadsPerCore=1` on c6in.8xlarge, so vCPUs 0–15
  are 16 physical cores. A metal instance takes no `--cpu-options`: put `echo off >
  /sys/devices/system/cpu/smt/control` in the metal members' user data instead. Not `nosmt` in the image, which would
  halve test 2's hosts.
- **One socket on metal**: c6id.metal has two. Keep `MULTIHOST_MEMBER_CPUS`, `MULTIHOST_HOST_CPUS` and
  `MULTIHOST_BENCH_CPUS` within one NUMA node's CPUs, as `lscpu` lists them, so the hop never crosses the socket link.
- **Interrupts on CPUs 0–1**: `irqaffinity=0-1` on the kernel command line. The drivers keep the measured processes
  apart with `taskset` (§5). Not `isolcpus`: the kernel does not balance load across isolated CPUs, so a JVM pinned to
  a set of them would run every thread on the first.
- **Socket buffers**: `net.core.rmem_max` and `net.core.wmem_max` at 16777216 in `/etc/sysctl.d`.

Launch the members and the driver host into one cluster placement group, with
`--instance-initiated-shutdown-behavior terminate` and a user-data `shutdown -h +180`, so a forgotten session ends
itself. The driver host needs an ssh key that every host, itself included, accepts.

## 5. Running

The drivers run on the driver host (c6in.4xlarge) and reach the others by ssh at the addresses the members bind:

```bash
cd ~/seqeron/seqeron-service/src/test/scripts/multihost
export MULTIHOST_MEMBERS=10.0.0.11,10.0.0.12,10.0.0.13   # private addresses, member-id order
export MULTIHOST_CLIENT=10.0.0.14                         # this host
export MULTIHOST_IDLE=busyspin MULTIHOST_MEMBER_CPUS=2-11 MULTIHOST_HOST_CPUS=12-13 MULTIHOST_BENCH_CPUS=14-15
export MULTIHOST_DIRECT_BENCH=$HOME/seqeron-direct/cmake-build-release/hop_bench

./cluster.sh up && ./cluster.sh leader && ./cluster.sh down    # the check after launch
./hop-matrix.sh                                                # test 1
./hop-analyze.py ../../../../../build/multihost/hop-*.txt      # the rule's latency part
HOP_RATES=20000 MULTIHOST_MEMBER_OPTS="-Daeron.archive.file.sync.level=1 -Daeron.archive.catalog.file.sync.level=1" \
    ./hop-matrix.sh                                            # the sync comparison; run it again without the option
REPLAY_PRELOAD=4000000 ./replay-remote.sh                      # test 3, and again with REPLAY_LOAD_DURING=1
```

Each script's header lists its variables. Results and every host's logs land under `build/multihost` on the driver
host. A hosted run that cannot attach, or a matrix whose leader moves, fails rather than measuring an election.

Rehearse on one machine with `MULTIHOST_MEMBERS=localhost,localhost,localhost`, short runs (`HOP_RATES=2000
HOP_REPEATS=1 HOP_SECONDS=5`, `REPLAY_PRELOAD=20000`) and `MULTIHOST_HOP_BENCH` naming a local build. Every
`localhost` runs here without ssh. A wipe there stops every local seqeron process and removes nodes 0–3's Aeron
directories, as `stop-cluster.sh` does.

Test 2 is `failover.yml`'s steps on one instance with Docker and a faulteron checkout:

```bash
export FAULTERON_DIR=$HOME/faulteron      # at the commit failover.yml pins
./gradlew operatorDist && "$FAULTERON_DIR"/test/faulteron-rules.sh
./gradlew :seqeron-service:networkFaultTest      # without TIMEOUT_SCALE; REPAIR, FAULT and DIRECTION as failover.yml sets them
ROUNDS=200 ./gradlew :seqeron-service:networkFaultTest --tests '*DockerFailoverSoakTest'   # on the spot instance
```

## 6. How far the results carry to bare metal

| Test | Carries | Does not |
| --- | --- | --- |
| 1 | The hop is IPC inside one host. With pinned, spinning threads it is CPU and memory, which Nitro passes through nearly unchanged, so the direct-against-hosted comparison carries; the rule of Test 1 is applied to each environment on its own. | The commit: a UDP round trip in a placement group (about 20 µs in Aeron's c6in result) is slower than a tuned bare-metal LAN with kernel sockets, so EC2 hides the hop a little more than bare metal would. Tails are worse: hypervisor interrupts, and on c6in.8xlarge a neighbour on the same socket. BIOS settings are out of reach. |
| 2 | Its faults are timed against the 200 ms heartbeat and the 2 s election timeout, far above anything virtualization adds. | — |
| 3 | The relay's flow control and NAK repair over a real network card. | A single flow is capped at about 10 Gbps in a placement group, and one replay is one flow. EBS is network storage: a forced write costs about 0.5–1 ms, against tens of µs on local NVMe. For local disk, run the sync comparison on an instance with instance storage (c6id, i4i). |

c6id.metal (§2) removes the hypervisor from test 1's CPU path and puts the sync comparison on local NVMe; its network
is still ENA. Its CPU is c6in's, so its results compare with the c6in.8xlarge run's.

## 7. Order of work

| step | done when |
| --- | --- |
| decisions (§8) | the open decisions are answered and the rule's margins are set |
| tooling | the four items of Test 1 built; localhost reproduces `hosted-facade.md` §3.6 within its spread; the stub's allocation is known |
| smoke | one virtualised instance, one rate, two runs: scripts, teardown and cost confirmed |
| Test 1a, then 1b | c6in.8xlarge first (cheaper), then c6id.metal |
| report | `hosted-facade.md` gains the tables in a §3.8; §2 question 2 is answered; keep or reverse is decided |
| teardown | no instance, volume or address is left |

## 8. Decisions needed

1. The rule's margins (Test 1). Until they are set the result decides nothing.
2. Who launches the instances: nothing in this tree does, and the machine this was planned on has no AWS credentials.
3. A budget cap in instance-hours; Test 1b is about 7.5 h on four hosts.
4. The architecture: this plan is x86_64 (c6in, c6id); the figures in `hosted-facade.md` are aarch64, and Graviton is not covered.
5. The deployment's hypervisor and region, if the virtualised run is to match it; the prices above are us-east-1.
6. The deployment's real payload sizes and target rate, to replace the sweep where it differs.
7. Shared or dedicated tenancy for the virtualised instance.
