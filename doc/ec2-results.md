# Benchmark results: three-member cluster on EC2

What a three-member seqeron cluster sustains on AWS, what limits it, and what the measurements leave open. Each member
derives its queues from the Raft log and records nothing but the log. The sessions ran on 11 and 13 November 2026.

## 1. Summary

| Question | Result | Evidence |
| --- | --- | --- |
| What rate does one producer sustain? | **909k msg/s** of 100-byte messages held in all four ramps that reached it. **1.14M msg/s saturated** in both ramps that tried it. The ceiling lies between the two. | Six runs, one launch. |
| What latency, below that? | From 62k to 909k msg/s: p50 83–104 µs; p99 0.12–0.14 ms in every step but one (8.0 ms); p99.9 0.13–0.23 ms in 22 of 26 steps, and 0.8–22 ms in the other four, which are not traced. | Two ramps, one launch. |
| What does a log volume at gp3's default 125 MB/s cost? | The cluster holds 582k msg/s and saturates at 727k, where the log needs 163 MB/s and single writes take 37–44 ms. | Two ramps, one launch, earlier code (§2.2). |
| What do the kernel's default writeback timers cost? | In one of two ramps the 909k step sequenced 680k msg/s, with a 2.7 s tail. The cause is not established. | Two ramps, one launch, earlier code (§2.2). |
| How long does a cold replay of 4,000,000 messages take? | 5.3 s on a member, or 10.8 s while the cluster takes more load. A gateway host on a fourth machine is serving after 1.5–1.6 s; its end-to-end time was not measured. | One run each, earlier code (§2.3). |
| Does the disk stall commits at a moderate rate? | No interval of 10 ms or more without a commit in two 600 s runs at 49k msg/s. | Two runs, three members on one host. |

**For sizing.** One producer of 100-byte messages has been shown to hold 909k msg/s, on a log volume provisioned well above
its write rate and with the writeback timers of §2. That figure is the highest rate shown to hold, not a capacity. It
comes from one launch and one producer, and the steps nearest it carry the untraced tails of §2.1. The log costs about
224 bytes of disk per 100-byte message, so a volume must sustain at least the message rate × 224 B/s: 204 MB/s at
909k msg/s, against 125 MB/s for gp3 by default.

## 2. Measurements

### Terms

| Term | Meaning |
| --- | --- |
| Ramp | A fresh three-member cluster, then a series of steps at rising offered rates, each 25% above the last: 50k, 62k, 78k, 98k, 122k, 153k, 191k, 238k, 298k, 373k, 466k, 582k, 727k, 909k and 1.14M msg/s. |
| Step | 10 s at one offered rate. The first 2 s are warm-up; latency is reported over the other 8 s. |
| Held, saturated | A step is held when its p99 is at most 10 ms, no message is lost and the leader does not change. Otherwise it saturates and the ramp stops. Held does not mean every percentile is low: a held step can have a p99.9 of 22 ms. |
| Latency | Open-loop: from the moment a message was due to be sent, at the offered rate, until the producer receives that message back on its queue, sequenced. A stall delays every message due during it, and the delay counts. |
| Declined | The leader's ingress refused an offer for back-pressure. The producer retries it at once, and its latency still runs from when it was due. |
| p50, p99, p99.9 | The latency below which 50%, 99% and 99.9% of a step's messages fall. |
| Launch | One set of EC2 instances. Placement and neighbours vary between launches more than between runs on one launch. |

### What one message goes through

The producer runs on the leader's host and submits each message over shared memory. The leader appends it to the
Raft log, and it is replicated over UDP to the two followers. Each member's archive writes the log through the page
cache to EBS, with no forced write. The message commits once the leader and one follower have written it. Every member
then sequences it and publishes it on its queue in shared memory, where the producer reads the leader's copy.
Measured latency therefore covers one network round trip, two page-cache writes and the sequencer, and a slow write on
the leader holds every commit. A slow write on one follower holds only that follower, since the other can complete the
majority.

### Environment

| Item | Setting |
| --- | --- |
| Members | 3 × c6in.8xlarge in one cluster placement group, one member per host (§2.4 excepted). |
| CPU | Intel Xeon Scalable (Ice Lake), up to 3.5 GHz all-core turbo, hyper-threading off: 16 cores per host. Interrupts on cores 0–1, the member on 2–11, the producer on 14–15. |
| Memory | 64 GiB per host. |
| Member process | One JVM: Aeron Cluster with its media driver and archive embedded, the sequencer and the replayer. About nine busy-spinning agent threads on ten pinned cores: the driver's conductor, sender and receiver, the archive's conductor, recorder and replayer, the consensus module, the sequencer and the replayer. G1, with the JVM's default sizing, 1 GB committed. |
| Shared memory | The driver's directory on tmpfs (`/dev/shm`). Each queue is an IPC stream with 16 MiB terms; the Raft log's terms are 64 MiB. |
| Disk | The archive and cluster directories on a 100 GB gp3 root volume, ext4, provisioned at 1,000 MB/s and 4,000 IOPS. The small-volume arm of §2.2 uses a second 100 GB gp3 at its defaults, 125 MB/s and 3,000 IOPS. |
| Kernel writeback | `vm.dirty_expire_centisecs=50` and `vm.dirty_writeback_centisecs=20`, so the kernel flushes a few MB at a time, unless a row says otherwise. Under these the members' dirty pages peaked at about 130 MB; under the kernel's defaults at 3.1–3.4 GB. |
| Network | 50 Gbps per host, one Availability Zone, 16 MiB socket buffers. At 909k msg/s the leader sends each follower about 204 MB/s (1.6 Gbps), computed from the log's size. |
| Producer | One C++ client submitting directly to the cluster, one message at a time, at the offered rate. |
| Gateway host | A fourth host, c6in.4xlarge (8 cores, 32 GiB), used only by the replay test of §2.3. |
| Software | Aeron 1.53.2. |
| Sampling | Each member's counters (commit position, the archive recorder's write times) every 10 ms, and the host's dirty and writeback pages every 100 ms. |
| Not recorded | CPU and network utilisation, and whether the producer or the cluster sets the ceiling between 909k and 1.14M msg/s. |

### 2.1 Saturation (13 November, the current code)

**Method.** 100-byte messages. Two ramps from 50k msg/s, two that start at 466k and two single steps at 909k, each on
a freshly started cluster.

| Start | Highest step held | Saturated at | p99 at 909k |
| --- | --- | --- | --- |
| 50k | 909k, in both ramps | — (the ramp ended at 909k) | 0.14 ms |
| 466k | 909k, in both ramps | 1.14M, in both: p50 of 0.84 and 1.32 s | 1.5 and 3.6 ms |
| 909k | — | 909k, in both: p50 of 116 and 551 µs, p99 of 165 and 287 ms | 165 and 287 ms |

At 1.14M msg/s the median itself passes 0.8 s: the offered rate exceeds what the cluster sequences, and the backlog
grows for the whole step. A cluster that starts cold at 909k falls behind at the start of the step while most of its
messages still take well under a millisecond; its p99 of 165 and 287 ms is that early backlog, and what builds it is not
traced. A ramp reaches 909k without one.

**The ramps from 50k, step by step**, latency in µs:

| Offered, msg/s | Ramp 1: p50 | p99 | p99.9 | Ramp 2: p50 | p99 | p99.9 |
| --- | --- | --- | --- | --- | --- | --- |
| 50k | 81 | 125 | 5,174 | 79 | 126 | 2,047 |
| 62k | 83 | 119 | 132 | 87 | 125 | 133 |
| 78k | 87 | 122 | 133 | 90 | 124 | 135 |
| 98k | 88 | 130 | 228 | 92 | 126 | 138 |
| 122k | 94 | 133 | 3,298 | 93 | 128 | 140 |
| 153k | 93 | 130 | 190 | 94 | 128 | 141 |
| 191k | 96 | 129 | 150 | 95 | 128 | 139 |
| 238k | 96 | 129 | 150 | 99 | 130 | 143 |
| 298k | 96 | 128 | 141 | 97 | 129 | 142 |
| 373k | 96 | 128 | 164 | 99 | 131 | 145 |
| 466k | 96 | 130 | 143 | 100 | 132 | 144 |
| 582k | 97 | 8,025 | 21,742 | 101 | 134 | 170 |
| 727k | 102 | 142 | 1,375 | 100 | 133 | 233 |
| 909k | 101 | 139 | 226 | 104 | 140 | 816 |

The first step of each ramp runs on a cluster started seconds before, and its p99.9 of 2.0–5.2 ms is left out of the
summary's ranges. The 582k step of ramp 1 held by the rule above, with a p99 of 8.0 ms, while 10% of its offers were
declined.

**Garbage collection.** A leader allocates about 3 MB/s, so young-generation pauses are rare. From a member's start to
the end of its ramp, about four minutes, the leader took three, of 2.1–3.3 ms, and each follower one. A pause on the leader holds every
message in flight; one on a follower is covered by the other follower.

### 2.2 The log's volume and the kernel's writeback (13 November, earlier code)

**Method.** The ramp of §2.1 from 50k msg/s, twice per configuration, on a separate launch earlier the same day. Its
ramps stopped at 909k by configuration and tried no higher rate. That code allocated 40–98 MB/s per member, whatever
the rate, from one object built on each duty cycle, so a young-generation pause came every 6–9 s: about 2 ms below
150k msg/s, 0.8 ms above. The baseline
row is that code on the set-up of §2.1, and the other two rows change one thing each against it. Its tails are
therefore higher than §2.1's.

| Configuration | Highest step held | p99, from the second step | p99.9, from the second step | Leader's longest disk write | Dirty pages, peak |
| --- | --- | --- | --- | --- | --- |
| Baseline | 909k in both ramps | 0.10–0.23 ms | 0.12–0.74 ms; 1.8 ms once | 2.3–3.0 ms | 130 MB |
| Kernel's default writeback | 909k, and 727k | 0.10–0.14 ms to 727k; 5.2 ms and 2.7 s at 909k | 0.12–0.16 ms to 466k; 0.15–12 ms above, bar the saturated step | 1.4–2.2 ms | 3.1–3.4 GB |
| Log on gp3 at 125 MB/s | 582k in both ramps | 0.12–0.14 ms to 582k; 22 and 28 ms at 727k | 0.13–0.37 ms to 582k, with 2.6 and 3.7 ms once each; 35 and 42 ms at 727k | 37–44 ms | 240–320 MB |

**The small volume.** At 727k msg/s the log is written at about 163 MB/s, above what a 125 MB/s volume sustains.
Single writes of 37–44 ms held the leader's commit position for up to 44 ms, and both ramps saturated there. At 582k
the log needs about 130 MB/s, and both ramps held. The volume's IOPS limit changed too, from 4,000 to 3,000; that the
ramps saturated where the write rate crossed 125 MB/s points to throughput.

**The default writeback.** In ramp 2 the 909k step sequenced only 680k msg/s of the 909k offered, so its latency grew to
2.7 s and the ramp saturated. In ramp 1 the same step held. During the failure the commit position kept moving, the
leader's longest write was 2.2 ms and no flush coincided with it. The 12 ms p99.9 at 582k in ramp 1 had no slow write on
the leader either. The samples taken do not explain either result; per-CPU time would show whether the kernel's flusher
threads took the member's cores.

### 2.3 Cold replay (13 November, earlier code)

**Method.** 4,000,000 messages of 193 bytes were submitted, which left each member's archive holding 1,152 MB of log.
Before each phase every member's page cache was dropped, so the replay reads from EBS. "Under load" means another
16,000,000 messages were submitted, as fast as ingress took them, while the replay ran. Each time includes the reading
process's JVM start.

| Reader | Quiet | Under load |
| --- | --- | --- |
| A new subscriber on member 1, served by that member's replayer over shared memory, which derives the queue from the log | 5.3 s (216 MB/s) | 10.8 s (107 MB/s) |
| A gateway host on a fourth machine: time until it serves replays | 1.6 s | 1.5 s |
| A new subscriber on that gateway host, after it serves | 1.5 s | 2.1 s |

A gateway host copies a member's log over UDP, derives the queues itself and records them on its own disk; its
subscribers replay from that recording. It reports that it is serving once its recording exists, before the copy
has finished. A subscriber that starts then catches up to the recording's end as it stood, so in all four phases the
first subscriber stopped at 2.6–3.5 million messages, and the test started a second, which received all 4 million. The
table gives the second subscriber's time. Its total from the host's start is therefore longer than 1.5 + 1.5 s and was
not measured. The gateway host's page cache was not dropped, and its subscriber reads a recording just written.

### 2.4 Stalls at a moderate rate (11 November, earlier code)

**Method.** One c6in.8xlarge ran all three members, unpinned, each with its log on its own gp3 volume, under the
kernel's default writeback. A producer sent 18-byte messages at 49k msg/s for 600 s, and each member's commit position
was read every 3 ms. Two runs, on one launch. The three members shared one kernel, so their flushes coincided, which
makes this a harder case than one member per host.

| Run | Intervals of 10 ms or more without a commit, any member | Longest interval | Longest disk write, any member | Leader's longest disk write | Log growth | Dirty pages, peak |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 0 | 7 ms | 3.4 ms | 0.6 ms | 3.7 GB | 619 MB |
| 2 | 0 | 9 ms | 9.0 ms | 1.4 ms | 3.7 GB | 615 MB |

Each run had 67–79 intervals of 5–9 ms without a commit; the 3 ms sampling resolves no finer. At 18 bytes a message
costs about 126 bytes of log (3.7 GB over 29.4 million messages), so these runs wrote about 6 MB/s: they show the
writeback's bursts at a low write rate, not at the rates of §2.1.

## 3. What this does not show

- **A second launch.** Each result comes from one launch. §2.1's 909k held on one launch and §2.2's on another, but with
  different code; the 1.14M ceiling and every tail figure were measured once.
- **Many producers.** A sequencer exists to merge many, and every run had one. The ceiling between 909k and 1.14M msg/s
  may be the producer's.
- **Latency below 50k msg/s, and other clients.** The ramps start at 50k with the direct C++ client. The hosted
  façade and forced writes were not measured on this code.
- **Other payloads at high rates.** The ramps use 100 bytes. The log's size per message, and so the volume a rate needs,
  grows with the payload.
- **Failover.** The time from a leadership change to the next sequenced message was not measured.
- **The volume and writeback results on the current code.** §2.2 ran before the allocation was removed. Its comparison
  is valid within the launch; its absolute tails are higher than the current code's.
- **Bare metal.** A virtualised host has worse tails, so the p99.9 figures are an upper bound for the same CPU. The p50
  should carry.
- **Six untraced results.** The p99.9 of 21.7 ms (with a p99 of 8.0 ms) at 582k and 3.3 ms at 122k in §2.1's ramp 1; the
  p99 of 1.5–3.6 ms at 909k after a start at 466k, against 0.14 ms after the full ramp; the backlog of a cluster that
  starts at 909k; and in §2.2 under default writeback, the 909k step that sequenced 680k and the 12 ms p99.9 at 582k.
- **Raw data.** The result files and logs are not in the repository.

## 4. Reproducing

The scripts are in `seqeron-service/src/test/scripts/multihost`. `disk-session.sh` runs §2.2 and §2.3 in one launch.
§2.1 is `hop-knee.sh` with `KNEE_SECONDS=10`, `KNEE_PAYLOADS=100`, `HOP_MODES=direct`, `MULTIHOST_IDLE=busyspin`,
`MULTIHOST_MEMBER_CPUS=2-11` and `MULTIHOST_BENCH_CPUS=14-15`: from `KNEE_START=50000` with the default `KNEE_MAX`, and
from 465656 and 909483 with `KNEE_MAX=5000000`. The cold replay is `replay-remote.sh` with `REPLAY_PRELOAD=4000000` and `REPLAY_COLD=1`, and
`REPLAY_LOAD_DURING=1` for load.
