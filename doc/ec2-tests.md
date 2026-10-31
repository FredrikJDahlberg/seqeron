# Tests on EC2

Three measurements that a laptop, a Docker bridge or a shared 4-vCPU CI runner cannot make: they need separate hosts
on a real network, dedicated cores, or both. This document says what each one answers, what it costs, how long it
runs and how far its result carries to bare metal, and how to run it with the scripts in
`seqeron-service/src/test/scripts/multihost`.

Prices are us-east-1 on-demand Linux, October 2026. c6in prices scale linearly from c6in.xlarge's $0.2268/hr; check
them before a session.

## 1. The tests

### Test 1: the hosted-façade hop on a tuned cluster

[`hosted-facade-draft.md`](hosted-facade-draft.md) §17.9 question 2 waits on it. The hop has been measured only on a
laptop (§17.10.2) and in an aarch64 VM (§17.10.6), where a whole loop of 233–389 µs at p50 hides it. A pinned,
busy-spinning cluster on three hosts should commit in about 100 µs (Aeron's own three-node result on c6in.16xlarge is
95 µs p50 at 100k msg/s), so the same comparison shows more of the hop and of the host's pauses.

`hop-matrix.sh` runs `hop_bench` on the leader's host in three modes: `direct` (the C++ `Application`),
`hosted-spin` (`HostedTestApplicationHost` with `host.spin`) and `hosted-backoff`. It runs every mode at each rate,
alternating within a rate, against a fresh cluster per rate. One line per run, with p50/p99/p99.9/max, goes to
`build/multihost/hop-<stamp>.txt`. The same script with `MULTIHOST_MEMBER_OPTS` set runs the disk-sync comparison of
test 3.

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
| Test 1: 3 rates × 3 modes × 3 repeats of 30 s | 25 min | the same | 2.65 |
| Sync comparison: test 1 at one rate, sync on and off | 10 min | the same | 1.05 |
| Test 3: preloads of 400,000 and 4,000,000, quiet and under load | 15 min | the same | 1.60 |
| Test 2: the fault suite, three copies in parallel | 15 min, alongside | 3 × c7i.4xlarge | 0.55 |
| Test 2: the failover soak, 200 rounds of about 35 s | 2 h, alongside, unattended | c7i.4xlarge spot | 0.50 |
| **Session** | **about 1 h attended** | | **about 7.50** |

EBS for a day (four 100 GB gp3 volumes) and the public addresses add about $1. Every hour the four test 1 and 3
instances run past the plan costs $6.35, so stop them between attempts and launch them with a shutdown timer (§4).
Budget twice the session for a first attempt.

## 4. Image

One Ubuntu 24.04 x86_64 image serves every host. Build it on a c6in.large and save it as an AMI:

```bash
sudo apt-get install -y openjdk-21-jdk-headless cmake g++ git perl docker.io
git clone <this repository> ~/seqeron && cd ~/seqeron && git checkout <branch>
./gradlew uberJar :seqeron-service:compileTestJava
cmake -B cmake-build-release -DCMAKE_BUILD_TYPE=Release
cmake --build cmake-build-release --target hop_bench
```

Tuning, all of it applied at boot so the image carries it:

- **Hyper-threading off at launch**: `--cpu-options CoreCount=16,ThreadsPerCore=1` on c6in.8xlarge, so vCPUs 0–15
  are 16 physical cores.
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

./cluster.sh up && ./cluster.sh leader && ./cluster.sh down    # the check after launch
./hop-matrix.sh                                                # test 1
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
| 1 | The hop is IPC inside one host. With pinned, spinning threads it is CPU and memory, which Nitro passes through nearly unchanged, so the direct-against-hosted comparison carries. | The commit: a UDP round trip in a placement group (about 20 µs in Aeron's c6in result) is slower than a tuned bare-metal LAN with kernel sockets, so EC2 hides the hop a little more than bare metal would. Tails are worse: hypervisor interrupts, and on c6in.8xlarge a neighbour on the same socket. BIOS settings are out of reach. |
| 2 | Its faults are timed against the 200 ms heartbeat and the 2 s election timeout, far above anything virtualization adds. | — |
| 3 | The relay's flow control and NAK repair over a real network card. | A single flow is capped at about 10 Gbps in a placement group, and one replay is one flow. EBS is network storage: a forced write costs about 0.5–1 ms, against tens of µs on local NVMe. For local disk, run the sync comparison on an instance with instance storage (c6id, i4i). |

c6in.metal (about $7.26/hr) removes the hypervisor from test 1's CPU path; its network is still ENA.
