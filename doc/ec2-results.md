# Benchmark results: three-member cluster on EC2

Measurements on a three-member cluster of c6in.8xlarge hosts, in two sessions. On 8 November 2026, with members that
recorded their queues: the latency of the sequencing loop at 20k, 50k and 100k msg/s, the rate at which it saturates, the
cost of forcing the archive to disk, and replay over a network (§3). On 11 and 13 November, the disk writes, with members
that derive their queues from the Raft log and record none, as this release does, beside members that record them: stalls
on EBS, saturation and cold replay (§4). The derived runs were made before the release, behind the flag
`replayer.deriveFromLog`. [`ec2-tests.md`](ec2-tests.md) is the plan and the method; this document is what the sessions
measured, and what they do not establish.

## 1. Summary

Derived queues, as released (§4):

| Question | Result | Confidence |
| --- | --- | --- |
| Stalls on EBS, default writeback, 49k msg/s | None of 10 ms or more in two 600 s runs; recorded members had 17, up to 81 ms | Three runs on one host, whose three members share a kernel. |
| Saturation of 100-byte messages, gp3 at 1,000 MB/s | **909k msg/s held**, the top of the ramp; p99 at most 0.23 ms, p99.9 at most 0.74 ms but 1.8 ms once | Two ramps, one launch. The ceiling was not reached; what is left is young GC. |
| The same under the kernel's default writeback | 909k and 727k; one step sequenced 680k of the 909k offered, with a 2.7 s tail | Two ramps. Unexplained. |
| The same with the archive on gp3 at its default 125 MB/s | **582k msg/s**; at 727k, writes of 37–44 ms hold commits | Two ramps. |
| Cold replay of 4,000,000 frames off EBS | 5.3 s on the member, 10.8 s under load; 1.5 and 2.1 s through a gateway host once it serves | One run each. Recorded members: 5.2, 6.8, 1.4 and 2.1 s. |

Plan on this: on a volume provisioned well above the log's write rate and with even writeback, up to about 900k msg/s of
100-byte messages with a sub-millisecond p99.9; on a default gp3 volume, under 580k msg/s.

Recorded queues, 8 November (§3). The release removed that path. The loop latency below was not measured again on derived
members, and its 2–3 ms p99.9 from 50k msg/s is the size §4.2 measured from the recorder's writes on recorded members:

| Question | Result | Confidence |
| --- | --- | --- |
| Loop latency at 20k msg/s, no forced writes | p50 37 µs, p99 49 µs, p99.9 64 µs | Three runs per arm, one launch. |
| Loop latency at 50k and 100k msg/s | p50 63 µs and 78 µs; p99 110 µs and 380 µs; **p99.9 2.6 ms and 3.3 ms** | Eight runs per arm. Every run shows the 2–3 ms tail. |
| Hosted façade against the direct client | No resolvable difference at p50 and p99 at 20k, 50k and 100k msg/s with a spinning host; the default idle strategy adds 61 µs at p50 | Within the direct arm's own run-to-run spread. |
| Forced archive writes (`file.sync.level=1`) | **+6.5 ms at p50, +9 ms at p99**, throughput held | One launch, EBS only. Not repeated on local NVMe. |
| Saturation of 100-byte messages | Not found. One ramp reached 888k msg/s without saturating; the other stopped at 568k on a 30 ms stall | Two ramps that disagree. A stall decides the verdict, not a capacity limit. |
| Replay of 2.3 GB | 1.1–1.4 GB/s local, 0.61 GB/s over the UDP relay | One run each; a fourth run did not complete, cause unconfirmed. |

Plan on this: a few tens of µs through the loop up to about 20k msg/s, and no forced writes unless 6.5 ms is acceptable.

## 2. Set-up, 8 November

| | |
| --- | --- |
| Members | 3 × c6in.8xlarge, hyper-threading off (16 cores), one cluster placement group, one Availability Zone |
| Driver and gateway host | 1 × c6in.4xlarge, same group |
| Storage | 100 GB gp3 root volume per host (3000 IOPS, 125 MB/s); archive and cluster directories on it |
| Kernel | `vm.dirty_expire_centisecs=50`, `vm.dirty_writeback_centisecs=20`; interrupts on CPUs 0–1 |
| Threads | Busy-spinning; members on CPUs 2–11, host on 12–13, bench on 14–15 |
| Software | Aeron 1.53.2, image `seqeron-bench-20261108-2`, built from commit `a5e29e3c` |
| Order check | Run per rate: gap-free `groupSeqNo`, and the three members' queue recordings identical. `PASS` at every rate of §3.1, §3.2 and §3.4; the ramp and the replay runs have none. |

**What is measured.** An open-loop generator publishes one marker per interval and times each from its *intended* send
time until the process sees its own frame sequenced. A declined offer is retried at the same index and its latency still
runs from the intended time, so back-pressure shows as latency. The generator is co-located with the leader member's
host. The marker travels to the leader over IPC, is replicated to a majority over UDP, and comes back through the
sequencer's queue. The first 20% of every run is discarded.

**Arms.** `direct` is the C++ client built into the application. `hosted-spin` is the hosted façade, whose host busy-spins
a core of its own. `hosted-backoff` is the same host with the default backing-off idle strategy. All three publish the same
stream against the same cluster.

**Reading the tables.** Values are medians of the per-run percentiles, with the range over runs in brackets. A run has
480,000 measured markers at 20k msg/s and 1.2 million at 50k msg/s; every run is 30 s.
Payload is 18 bytes unless stated.

## 3. Recorded queues, 8 November

### 3.1 Latency, 20k msg/s, no forced writes

Three runs per arm, interleaved, leader member 2. Latencies in µs.

| Arm | p50 | p99 | p99.9 | max |
| --- | --- | --- | --- | --- |
| `direct` | 36.3 [36.0–37.5] | 46.9 [45.4–52.5] | 63.0 [60.8–80.6] | 915 [191–3371] |
| `hosted-spin` | 37.5 [36.7–37.7] | 49.2 [47.7–50.0] | 64.0 [61.3–77.2] | 754 [691–1539] |
| `hosted-backoff` | 97.8 [92.9–99.6] | 125.2 [121.9–132.3] | 135.3 [133.5–139.3] | 982 [728–3353] |

`hosted-spin` and `direct` are indistinguishable. A host that backs off costs about 60 µs at every percentile: a parked
thread wakes late, once for the command and once for the sequenced frame. A hosted host therefore has to spin, which
takes a core.

### 3.2 Latency, 50k and 100k msg/s, no forced writes

Eight runs per arm, interleaved; leader member 1 at 50k and member 2 at 100k. Latencies in µs.

| Rate | Arm | p50 | p99 | p99.9 | max |
| --- | --- | --- | --- | --- | --- |
| 50k | `direct` | 63.0 [36.9–66.7] | 108 [81–114] | 2567 [1236–3010] | 4626 [3458–7148] |
| 50k | `hosted-spin` | 65.0 [41.1–69.0] | 111 [85–113] | 2664 [2400–3156] | 4962 [4143–5711] |
| 100k | `direct` | 78.3 [76.9–80.1] | 390 [173–659] | 3336 [2995–3702] | 5932 [5527–6956] |
| 100k | `hosted-spin` | 78.4 [77.8–80.0] | 376 [234–906] | 3306 [3047–3562] | 5799 [4898–6690] |

- **The p50 at 50k msg/s is bimodal in both arms**: 37–49 µs in four runs and 61–69 µs in the other twelve. The
  median hides it. The cause is not known; at 100k msg/s the p50 is stable at 77–80 µs.
- **The tail is the finding.** From 50k msg/s the p99.9 is 2.4–3.7 ms in every run of both arms, against 63 µs at 20k
  msg/s. The p99 stays near 110 µs at 50k and rises to 170–900 µs at 100k. The hosted façade neither causes nor hides it:
  the direct client shows the same tail.
- **The archive's writes are the candidate.** An earlier session traced multi-millisecond waits of the archive recorder
  to the kernel's writeback of dirty archive pages (`ec2-tests.md`, "Archive writes on EBS"). The writeback timers set
  here shortened those waits to 5–8 ms, and this p99.9 is what remains. This session did not trace it again; §4.2 measured
  a p99.9 of the same size on recorded members and none on derived ones.

**The hosted façade against the direct client.** The hosted median minus the direct median, per percentile:

| Rate | p50 | p99 | p99.9 |
| --- | --- | --- | --- |
| 20k | +1.2 µs | +2.3 µs | +1.0 µs |
| 50k | +2.0 µs | +2.9 µs | +96 µs |
| 100k | +0.1 µs | −14 µs | −30 µs |

Each difference lies inside the direct arm's own range. The margins `ec2-tests.md` sets for its rule are placeholders, so
this is a measured difference and not a pass.

### 3.3 Saturation

A ramp raises the offered rate by 25% every 20 s on one cluster, 100-byte payload, from 20k msg/s. A step ends the ramp
when its p99 exceeds 10 ms, when it loses a marker, or when the leader moves. Two ramps per arm, each on a fresh cluster.

| Arm | Ramp | Last good step | Ended by |
| --- | --- | --- | --- |
| `direct` | 1 | 568k msg/s | p99 30.5 ms at 710k |
| `direct` | 2 | 888k msg/s | no step failed |
| `hosted-spin` | 1 | 455k msg/s | p99 16.6 ms at 568k |
| `hosted-spin` | 2 | 710k msg/s | p99 15.6 ms at 888k |

**These are not capacity figures.** The same 710k msg/s step had a p99 of 30.5 ms in the first direct ramp and 119 µs
in the second. Steps are either clean or not:

- A clean step has a p99.9 of 125–231 µs and a maximum of 0.3–2.4 ms, at any rate from 455k to 888k msg/s.
- A step with a stall has a p99.9 of 2–75 ms, and its p99 is normal until the stall is long enough to reach it.
- Both clean and stalled steps occur anywhere from 40k to 900k msg/s, in both arms.

A saturated cluster would fail at every rate above its limit, and these ramps do not. The one clean fact is that the
direct arm completed a step at 888k msg/s of 100-byte messages, a payload rate of about 89 MB/s, with a p50 of 91 µs
and p99.9 of 136 µs, on a volume provisioned for 125 MB/s. Whether the ceiling is the volume, the cluster or the
generator has not been measured.

**A message rate is not a packet rate.** Aeron Cluster replicates the log as a byte stream and a follower acknowledges
a position, so one datagram and one acknowledgement cover every entry appended since the last, and the batch grows with
the backlog. A 100-byte payload is a 130-byte ingress frame and, behind the cluster's 32-byte session header and Aeron's
32-byte data header, a 224-byte log entry after alignment. Six fit the 1408-byte MTU, Aeron's default, which these runs
did not change. At 888k msg/s that is at least 148k datagrams/s and 1.6 Gbit/s to each follower. Both figures are
computed from the frame sizes: the runs recorded no packet counts. Compare a system that sends one packet per message
on the datagram rate, and one that forces its writes on §3.4.

The hosted façade's window of 64 KiB back-pressures the offer, so a stall reaches its generator as `window_declines`
(up to 1.05 million in a step); the direct client shows it as declined ingress offers (up to 690,000). Neither the cause
of the stalls nor why they stop in the middle of a ramp is known.

### 3.4 Forced archive writes

The members ran with `aeron.archive.file.sync.level=1` and `aeron.archive.catalog.file.sync.level=1`, then without.
20k msg/s, 18 bytes, three runs per arm per setting, same cluster hosts, leader member 2 in both. Latencies in µs.

| Arm | Sync off p50 | p99 | p99.9 | Sync on p50 | p99 | p99.9 |
| --- | --- | --- | --- | --- | --- | --- |
| `direct` | 36.3 | 46.9 | 63.0 | 6580 | 8947 | 10018 |
| `hosted-spin` | 37.5 | 49.2 | 64.0 | 6467 | 8921 | 10124 |
| `hosted-backoff` | 97.8 | 125.2 | 135.3 | 6540 | 8956 | 9759 |

- **The cost is about 6.5 ms at p50 and 9 ms at p99**, 175 times the unforced median. It swamps the arms: all three are
  within 2% of each other, and the 60 µs idle-strategy difference disappears.
- **The flush is batched.** The load held at 20k msg/s with no declines while the median message waited 6.5 ms, which a
  serial flush per message could not do. The distribution is narrow: p50 6.5 ms, p99 9 ms.
- **The size of the cost is the open question.** `ec2-tests.md` §6 expects a forced EBS write to cost 0.5–1 ms. The
  measured figure is six to thirteen times that. The `fdatasync` trace that would show what one flush costs, and how
  many a message waits for, is not built.
- **EBS is network storage.** The result shows the cost on this volume. It is not the cost on local NVMe with power-loss
  protection, where a flush takes tens of µs, and has not been measured there.
- **The tail grows.** Maxima are 12.8–31 ms with sync on and 0.2–3.4 ms with it off.

What it buys is protection against a simultaneous power loss on a majority of members. Replication already covers one
member's loss, and `overview.md` states the trade.

### 3.5 Replay across a network

`replay-remote.sh` preloads the queue, then replays it twice: a cold follower on member 1 over `aeron:ipc`, and a cold
follower on the gateway host, whose relay copies the queue recording over UDP first. "Serving" is the time before the
relay serves it. Preloads of 400,000 and 4,000,000 messages, quiet and with a producer running.

| Archive | Producer | Local, IPC | Remote, UDP relay |
| --- | --- | --- | --- |
| 230 MB | none | 1615 ms, 143 MB/s | 2506 ms serving, then 1621 ms; 56 MB/s overall |
| 230 MB | running | 1618 ms, 142 MB/s | 1942 ms serving, then 1652 ms; 64 MB/s overall |
| 2304 MB | none | 1619 ms, 1423 MB/s | 1623 ms serving, then 2142 ms; 612 MB/s overall |
| 2304 MB | running | 2125 ms, 1084 MB/s | did not complete, cause unconfirmed |

- **Local replay is about 1.6 s whatever the size** between 230 MB and 2.3 GB, so its rate rises with the archive: the time
  is a fixed cost, not a bandwidth. Do not quote the MB/s of the small archive as a rate.
- **The remote follower waits 1.6–2.5 s before serving, and then catches up in about 1.6–2.1 s.** At 2.3 GB it moved
  about 1.1 GB/s while following, within the roughly 10 Gbps a single flow can take in a placement group, which is
  about 1.25 GB/s. The relay is close to its limit, and one replay is one flow.
- **A producer running costs the local replay 30% on the large archive** (1.6 s to 2.1 s) and the small one nothing.
  One run each does not separate that from noise.
- **The fourth run did not complete, and the cause is unconfirmed.** The follower reported caught up with 2,778,464
  frames delivered in order against the 4 million preloaded. The likely cause is that it caught up at the end of the
  gateway host's recording while the relay was still copying the preload. Its retained-frame buffer (65,536 frames,
  16 MB) also overflowed, but that makes the follower resume rather than report caught up, and the logs do not show it
  to be the cause. Commit `6e938ab4` restarts the remote follow until it has delivered the preload and postdates the
  image, so it may be the fault it fixed. Not rerun.

## 4. Derived queues, 11 and 13 November

### 4.1 Stalls on EBS, 11 November

The set-up of `ec2-tests.md`, "The path is the log's recording": one c6in.8xlarge held three unpinned members, each archive on
its own gp3 volume with the kernel's default writeback, at 49k msg/s for 600 s, three runs in one launch, `CounterSampler` every
3 ms. A derived member's archive holds the Raft log alone:

| run | intervals of 10 ms or more without a new `clusterSeqNo`, all members | longest | recorder's longest write | archive growth | dirty pages, peak |
| --- | --- | --- | --- | --- | --- |
| derived | 0 | 7 ms | 3.4 ms | 3.7 GB | 619 MB |
| recorded | 17, 3 of them on the leader | 81 ms | 79 ms | 7.3 GB | 1270 MB |
| derived, after the recorded run | 0 | 9 ms | 9.0 ms | 3.7 GB | 615 MB |

A blocked write on a follower holds that follower alone; the leader's longest write was 0.6 and 1.4 ms in the derived runs. The
kernel still flushes in bursts, of half the bytes. Intervals of 5–9 ms occur in every run, 67–79 in a derived run and 118 in
the recorded one, at a sampling interval that resolves no finer. Two runs on one host: no stall rate, and no figure for a higher
rate or a larger payload, where the log alone writes what both wrote here.

### 4.2 Saturation, 13 November

`disk-session.sh` on three c6in.8xlarge members, `direct`, 100-byte markers, two ramps per arm from 50k to 909k msg/s in
25% steps of 10 s, `CounterSampler` every 10 ms and `node.sh meminfo-sampler` every 100 ms. Each member's root volume is gp3 at 1,000 MB/s and 4,000 IOPS, and the writeback even,
unless the row says otherwise:

| arm | highest step held | p99 | p99.9, from the second step | max | leader's longest write |
| --- | --- | --- | --- | --- | --- |
| derived | 909k, the top, in both ramps | 0.10–0.23 ms | 0.12–0.74 ms; 1.8 ms once | 0.9–3.7 ms | 2.3–3.0 ms |
| recorded | 909k in both ramps | 0.12–0.20 ms to 582k; 0.4–1.8 ms at 727k and 909k | 2.1–3.7 ms at every step | 3.4–6.2 ms | 6.3–6.4 ms |
| derived, the kernel's default writeback | 909k and 727k | 0.10–0.14 ms to 727k; 5.2 ms and 2.7 s at 909k | 0.12–0.16 ms to 466k; 0.5–12 ms above | 0.9–15 ms; 2.8 s once | 1.4–2.2 ms |
| derived, the archive on gp3 at its default 125 MB/s | 582k in both ramps | 0.12–0.14 ms to 582k; 22–28 ms at 727k | 0.13–0.37 ms; 2.6 and 3.7 ms once each | 1.0–9.0 ms; 37–44 ms at 727k | 37–44 ms |

On recorded members the tail is the recorder's. Both arms write about 0.28 MB per millisecond of write time, but that arm writes
twice the bytes, 379 MB/s against 205 MB/s at 909k, and its leader's longest write passes 5.9 ms by the fourth step.
On derived members the maxima are young GC: a member allocates 40–98 MB/s whatever the rate, so a `G1CollectForAllocation` pause
comes every 6–9 s, about 2 ms below 150k msg/s and 0.8 ms above. A pause on the leader holds every message in flight; one on a
follower is covered by the other. The first step of each ramp, on a fresh cluster, collects more often.

On the default volume, single writes of 37–44 ms at 727k msg/s, about 163 MB/s of log, held the leader's commit position for
44 ms, the one step where a commit stall shows above the sampler's 10 ms. Under the kernel's default writeback, the second ramp's
909k step sequenced 680k msg/s of the 909k offered, so latency grew to 2.7 s; the commit position kept moving, the leader's
longest write was 2.2 ms, no flush coincided, and the same step held 909k in the other ramp. The 12 ms p99.9 at 582k had no slow
write on the leader either. Neither is explained by what the session samples; per-CPU time would show whether the kernel's
flusher took a member's cores.

### 4.3 Cold replay off EBS, 13 November

Test 3 at 4,000,000 frames of 193 bytes, every member's page cache dropped before each phase, in the same launch as §4.2. A
member's archive held 1,152 MB derived and 2,304 MB recorded:

| arm | member's own replay (IPC), quiet | under load | through a gateway host, quiet | under load |
| --- | --- | --- | --- | --- |
| derived | 5.3 s | 10.8 s | 1.5 s | 2.1 s |
| recorded | 5.2 s | 6.8 s | 1.4 s | 2.1 s |

A gateway host's time is its catch-up after it began serving, about 1.5 s in. In all four of its phases, both arms, the host's
first cold follow delivered 2.6–3.5 million of the 4 million frames and the second caught up; `replay-remote.sh` retries, and the
shortfall is not traced.

## 5. What this does not show

- **One launch.** `ec2-tests.md` requires a second launch before a result decides anything, because placement and
  neighbours vary between launches by more than between runs. Nothing here has had one.
- **Derived members' latency below saturation.** §4 ran the ramps from 50k msg/s with 100-byte markers and the direct
  client alone; the loop latency at 20k–100k msg/s, the hosted façade and forced writes were measured on recorded
  members only (§3).
- **One payload for latency.** 18 bytes at 20k–100k msg/s and 100 bytes in the ramps. A 1 KiB or 8 KiB payload, where the
  stub's copy would show, was not run.
- **One producer.** A sequencer exists to merge many, and the producer sweep (1, 4 and 16 sessions) was not run.
- **No failover or gate-edge result this session.** The time from a leadership change to the first sequenced command is
  not measured here.
- **The virtualised instance.** Tails on a hypervisor are worse than on bare metal, so the p99.9 figures are an upper
  bound for the same CPU. The p50 should carry. `ec2-tests.md` §6 says what carries.
- **The 2–3 ms tail and the stalls in the ramps are unexplained.** They decide the saturation figure and the p99.9 at 50k
  msg/s and above, and are the first thing to trace before these results size a deployment: the recorder's maximum
  write time (`CounterSampler`) and an off-CPU trace of the recorder thread, as in the earlier session.

## 6. Data

The raw results are not in the repository, and the stamps of §4's runs are not recorded here. The stamps below identify
§3's, in the `build/multihost` directory of the
driver host and in a copy taken at the end of the session:

| Set | Stamp |
| --- | --- |
| 50k and 100k msg/s, no forced writes (§3.2) | `hop-20261108T123714Z` |
| Ramp (§3.3) | `knee-20261108T130352Z` |
| Replay, four runs (§3.5) | `replay-20261108T134351Z`, `T134445Z`, `T134524Z`, `T134611Z` |
| 20k msg/s, forced writes (§3.4) | `hop-20261108T142106Z` |
| 20k msg/s, no forced writes (§3.1, §3.4) | `hop-20261108T142843Z` |
