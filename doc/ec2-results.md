# Benchmark results: three-member cluster on EC2

Measurements on a three-member cluster of c6in.8xlarge hosts, with members that derive their queues from the Raft log and
record none, as this release does: saturation, cold replay and stalls on EBS (§2), on 11 and 13 November 2026, beside
members that record them. The derived runs were made before the release, behind the flag `replayer.deriveFromLog`. The
earlier design's measurements, of 8 November with members that recorded their queues, are in
[`ec2-results-recorded.md`](ec2-results-recorded.md). [`ec2-tests.md`](ec2-tests.md) is the plan and the method; this
document is what the sessions measured, and what they do not establish.

## 1. Summary

Derived queues, as released:

| Question | Result | Confidence |
| --- | --- | --- |
| Saturation of 100-byte messages, gp3 at 1,000 MB/s | **909k msg/s held**, the top of the ramp; p99 at most 0.23 ms, p99.9 at most 0.74 ms but 1.8 ms once | Two ramps, one launch. The ceiling was not reached; what is left is young GC. |
| The same under the kernel's default writeback | 909k and 727k; one step sequenced 680k of the 909k offered, with a 2.7 s tail | Two ramps. Unexplained. |
| The same with the archive on gp3 at its default 125 MB/s | **582k msg/s**; at 727k, writes of 37–44 ms hold commits | Two ramps. |
| Cold replay of 4,000,000 frames off EBS | 5.3 s on the member, 10.8 s under load; 1.5 and 2.1 s through a gateway host once it serves | One run each. Recorded members: 5.2, 6.8, 1.4 and 2.1 s. |
| Stalls on EBS, default writeback, 49k msg/s | None of 10 ms or more in two 600 s runs; recorded members had 17, up to 81 ms | Three runs on one host, whose three members share a kernel. |

Plan on this: on a volume provisioned well above the log's write rate and with even writeback, up to about 900k msg/s of
100-byte messages with a sub-millisecond p99.9; on a default gp3 volume, under 580k msg/s.

## 2. Derived queues

### 2.1 Saturation, 13 November

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

### 2.2 Cold replay off EBS, 13 November

Test 3 at 4,000,000 frames of 193 bytes, every member's page cache dropped before each phase, in the same launch as §2.1. A
member's archive held 1,152 MB derived and 2,304 MB recorded:

| arm | member's own replay (IPC), quiet | under load | through a gateway host, quiet | under load |
| --- | --- | --- | --- | --- |
| derived | 5.3 s | 10.8 s | 1.5 s | 2.1 s |
| recorded | 5.2 s | 6.8 s | 1.4 s | 2.1 s |

A gateway host's time is its catch-up after it began serving, about 1.5 s in. In all four of its phases, both arms, the host's
first cold follow delivered 2.6–3.5 million of the 4 million frames and the second caught up; `replay-remote.sh` retries, and the
shortfall is not traced.

### 2.3 Stalls on EBS, 11 November

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

## 3. What this does not show

- **One launch.** `ec2-tests.md` requires a second launch before a result decides anything, because placement and
  neighbours vary between launches by more than between runs. Nothing here has had one.
- **Derived members' latency below saturation.** §2 ran the ramps from 50k msg/s with 100-byte markers and the direct
  client alone; the loop latency at 20k–100k msg/s, the hosted façade and forced writes were measured on recorded
  members only ([`ec2-results-recorded.md`](ec2-results-recorded.md)).
- **One payload.** 100 bytes in the ramps. A 1 KiB or 8 KiB payload was not run, and the stalls of §2.3 were measured at
  49k msg/s, where the log alone writes half of what both wrote.
- **One producer.** A sequencer exists to merge many, and the producer sweep (1, 4 and 16 sessions) was not run.
- **No failover or gate-edge result.** The time from a leadership change to the first sequenced command is not measured.
- **The virtualised instance.** Tails on a hypervisor are worse than on bare metal, so the p99.9 figures are an upper
  bound for the same CPU. The p50 should carry. `ec2-tests.md` §6 says what carries.
- **The 909k step that sequenced 680k and the 12 ms p99.9 at 582k are unexplained** (§2.1); per-CPU time would show
  whether the kernel's flusher took a member's cores.
- **Data.** The raw results are not in the repository, and the stamps of §2's runs are not recorded.
