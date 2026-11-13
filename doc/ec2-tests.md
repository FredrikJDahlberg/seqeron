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
| payload sweep | 1 KiB, 8 KiB (256 B only if the 1 KiB cell fails) | 20k; 1 KiB also 100k | `direct`, `hosted-spin` | 5 |
| knee | 18 B, 1 KiB | ramp to saturation | `direct`, `hosted-spin` | 2 |
| soak | 256 B | 20k for 1 h | `direct`, `hosted-spin` | 1 |
| gate edge | 18 B | flood, leader paused 4.5 s | `hosted` and `direct`, both off the cluster on the fourth host | 5 |

The gate-edge set records, per run, the commands offered, sequenced and lost, any sequenced twice or out of order, the
commands the host dropped at the edge, what the stub retains at the end, how long the gate was shut and how long after it
reopened the last command offered before the edge was sequenced; the hosted arm must show 0 lost. It follows
`HostedCppStubTest`'s resend scenario, so the host runs inside a gateway host on the fourth host: a host on the leader's
member does not lead again, and the gate would never reopen. That host has too few cores to spin, so it runs with the default
idle strategy, and the set's times are not comparable with the rate sweep's. The `direct` arm is `hop_bench` of the tag as an
`app::Application` on the same gateway host's driver (`--off-cluster 1`, whose gate opens whoever leads), so both arms pay the
gateway host's queue relay and UDP ingress and differ by the stub and the host alone. It does not resend; the set records what
it loses and how often the cluster declines it, as the baseline the hosted arm's drops are read against, and does not judge
them. Recovery times are an off-cluster client's, not a co-located one's, and the matrix's latency cells, which run
co-located over IPC, say nothing about them. The runs of the two arms alternate in order from one repeat to the next.
Run order within a rate is randomised.

**Stages.** Test 1b runs in stages, each applied with `hop-analyze.py` before the next starts. Five runs per cell is the
floor in every stage: the direct arm's range of fewer runs is narrower, so a hosted arm would fail by chance.

| stage | set | runs | wall clock |
| --- | --- | --- | --- |
| 1 | rate sweep | 50 | 2.1 h |
| 2 | payload sweep: 1 KiB and 8 KiB at 20k, 1 KiB at 100k | 30 | 1.3 h |
| 3 | idle strategy | 10 | 0.4 h |
| 4 | knee, gate edge, soak | | 3.1 h, the soak and the knee unattended |

After stage 1 the owner decides whether stages 2 and 3 are needed: if `hosted-spin` is inside the rule at every rate, they
check the result and do not decide it. Stage 3 matters only once `hosted-spin` has passed, since it asks whether the default
idle strategy is enough. The 8 KiB cell stays in stage 2 because the stub copies the payload, so its cost is the one most
likely to grow with size; 256 B lies between the rate sweep's 18 B and 1 KiB and is run only to locate a failure there.

**What a run records.** The whole-loop histogram (3%-wide buckets), the achieved rate and the stub's window
declines; the host's `queue:` line (placed, `epochDrops`, `headDeclines`, `peakQueued`), its hosted-lag counter once a second and
the stub's retained count; CPU per process (`pidstat 1`), `mpstat -P ALL` and context switches; the GC and safepoint logs of the
host and the members (`-Xlog:gc,safepoint`, `-Xms` equal to `-Xmx`, `-XX:+AlwaysPreTouch`, the default collector first, and
`-XX:+UseZGC` on the host only if its pauses appear in the p99.9); `lscpu`, `uname -a`, the git sha, the Aeron version and the instance
and image ids. **The stub's allocation in the measured window** is counted in the bench, from the first published marker to the last: the
host's is shown to be zero (`hosted-facade.md` §3.5), the stub's is not. `hosted_hop_bench` hooks `operator new`; hooking `malloc` as well is not done.

**Also recorded, and what each one guards.**

| measurement | per | why |
| --- | --- | --- |
| Order check: the sequence numbers the bench sees are gap-free and unique, and at the end of the run the three members' queue recordings are identical (length and hash up to the common commit position) | run | A faster path that mis-orders or diverges is not a pass, and a client's `seen == n` does not show it. A run that fails it is discarded, whatever its latency. |
| ENA counters before and after (`ethtool -S`: `bw_in_allowance_exceeded`, `bw_out_allowance_exceeded`, `pps_allowance_exceeded`), NIC packets, bytes and drops | run, per host | EC2 shapes traffic without saying so; a tail at 200k msg/s or 8 KiB may be the allowance, not the software. A run in which one moved is flagged, not averaged in. |
| Calibration: Aeron's own cluster echo benchmark on the same three hosts and settings | session, before the matrix | Shows the tuned cluster commits near the 95 µs the plan expects. Without it a slow cluster looks like a slow arm and the hop's share of the loop is unknown. |
| Stage split of the loop: client to leader ingress, ingress to commit, commit to the callback | one run per rate | The rule judges the whole loop; the split says which stage a failed percentile came from. From timestamps in the marker and the sequencer's counters, not a second histogram on the hot path. |
| Merged histogram of a cell's runs, and its worst run, beside the median of the per-run percentiles | cell | The median of five p99.9 values hides a bad run. |
| Achieved rate and the declines, with the rule for them: a declined command is retried at the same index and its latency runs from the intended send time (`HostedHopBench.cpp`, `Run::due`), so it counts; the achieved rate is reported with every percentile | run | At 50k msg/s hosted-backoff declined 8–10% of the offered commands in the runs where it declined; a percentile over completed commands alone would not show it. |
| Leader member, and whether the leader's host also runs the bench | run | Placement is a variable; the writeback fix was shown on one leader. |
| A run's stall flag: p99.9 ≥ 5 ms, or the recorder's max write time ≥ 10 ms | run | A flagged run is rerun and reported, not entered in the rule's comparison, so the rule measures the hop and not the volume. |
| Per-minute percentiles, RSS of the bench, the host and the members, the stub's retained count, archive bytes and heap after GC | soak | One hour-long percentile cannot show a slope. Pass: none in RSS or retained count. |
| Time from `LeadershipChanged` to the first sequenced command, and the election's duration, under load | gate edge | The figure an operator plans failover around; the set records loss and catch-up but not this. |
| `fdatasync` latency (`bpftrace`) beside the sequencer's p99.9 | sync comparison | The comparison reports the rate's latency, not what the forced write itself costs. |
| Producers: 1, 4 and 16 sessions at the same total rate, `direct` and `hosted-spin` | 20k and 100k msg/s | Every set has one producer; the sequencer exists to merge many. Reported, not judged by the rule. |

Test 1b is repeated on a second launch of the same instances before a result decides anything: placement and neighbours vary
between launches by more than between runs on one.

**What test 1a left open.** It ran 204 unique runs, 18 B only, 10k–50k msg/s, at 3 to 10 runs per cell. At 50k msg/s 12 of 47
`direct`, 16 of 46 `hosted-spin` and 6 of 45 `hosted-backoff` runs stalled (the archive write above). At 40k `hosted-spin`
stalled in 2 of 3 runs and `direct` in 0 of 3, and at 20k `hosted-spin` and `hosted-backoff` once each in 10: the writeback
setting was shown at 50k only, so 1b reruns 20k and 40k with it. Every hosted run carries a fixed number of declines that
are not the window's, proportional to the rate (27, 54, 82, 109 and 137 at 10k to 50k msg/s, in both hosted modes, 135 runs,
whatever the p99.9); the cause is not found, the stub's offer declines for a shut gate, a full retained queue or a
back-pressured stream 304 and the bench does not say which. `hop-analyze.py` therefore reads `window_declines`, the real
back-pressure; the bench now counts declines after its warm-up (`declines=`, `window_declines=`, and `warmup_declines=` for the rest), so the next runs show whether the constant is the start-up's. No run lost a marker, by the client's count; the order check above is what would show a divergence.

**Test 1a at five runs per cell.** One launch of the refreshed image (3 × c6in.8xlarge and a c6in.4xlarge driver), 18 B, 5 runs per arm at 10k, 20k and 50k msg/s, 45 runs; the order check passed at all three rates (4,500,710, 9,000,721 and 22,500,740 frames). `hop-analyze.py` found no stalled run and applied the placeholder margins (25 µs p50, 250 µs p99, 2 ms p99.9); the figures are the hosted median minus the direct median.

| Rate | Mode | p50 | p99 | p99.9 | Rule |
| --- | --- | --- | --- | --- | --- |
| 10k | `hosted-spin` | −1.1 µs | −0.9 µs | +1.8 µs | pass |
| 10k | `hosted-backoff` | +55.4 µs | +64.0 µs | +73.3 µs | fail (p50) |
| 20k | `hosted-spin` | +0.1 µs | +3.3 µs | +2.8 µs | pass |
| 20k | `hosted-backoff` | +52.8 µs | +81.3 µs | +85.2 µs | fail (p50) |
| 50k | `hosted-spin` | +3.4 µs | +2.5 µs | +227 µs | pass |
| 50k | `hosted-backoff` | +30.1 µs | +37.9 µs | +275 µs | pass |

`hosted-spin` matches `direct` at p50 and p99 at every rate (merged p50 and p99 at 10k and 20k are equal to the printed digit). At 50k all three arms have a merged p99.9 of about 2.1–2.4 ms, the archive-write stall. `hosted-backoff` costs about 55 µs at p50 at 10k and 20k; it passes at 50k only because `direct`'s own p50 spread there is wide (44.6–73.9 µs), and its p50 is still about 30 µs higher. One 20k `hosted-backoff` run had a p99.9 of 3.27 ms, under the 5 ms stall cutoff, so it counted. The margins are placeholders until the owner sets them, so the verdicts are provisional.

**Tooling.** Test 1a runs on what exists. The scripts are in `seqeron-service/src/test/scripts/multihost`.

- `hosted_hop_bench` (`seqeron-client/src/test/cpp-bench/HostedHopBench.cpp`, CMake target of the same name): the open-loop generator over the
  real `hosted::Application`. `--payload` sets the marker's length (18 to 8875 B) and `--warmup-ms` the discarded warm-up (default 20% of
  the run). It prints the reported loop's p50, p99, p99.9 and max, the declines after the warm-up (all of them, and the stub's window
  declines among them; the warm-up's own as `warmup_declines=`), the retained count, the allocations, and `hist=`, the reported latencies
  as nanosecond buckets, 32 to each power of two (3% wide). `hop-analyze.py` merges a cell's histograms and prints the merged p50 to p99.99
  and max beside the medians; they read up to 3% low.
- The `direct` arm is `hop_bench` of the tag `cpp-direct-client-final`, built in a separate checkout against the same Aeron version
  (§4) with `direct-hop-bench-payload.patch`, `direct-hop-bench-histogram.patch` and `direct-hop-bench-edge.patch` applied, which give it
  `--payload`, a `payload=` field, `--flood-ms` with the gate-edge line, `--off-cluster` and `--egress-host`,
  `warmup_declines=` and `hist=` in its line;
  `MULTIHOST_DIRECT_BENCH` names it.
- `hop-matrix.sh` and `node.sh` start `ClientProxy` with `proxy.*` properties and run the arms and modes above, one fresh cluster per
  rate, interleaved within it. `HOP_RATES`, `HOP_REPEATS`, `HOP_SECONDS`, `HOP_MODES` and `HOP_PAYLOADS` select the set. Both
  benches drop the first 20% of a run, so `HOP_SECONDS=150` is 30 s of warm-up and 120 s measured.
- Every JVM `node.sh` starts logs its safepoints with wall-clock time (`-Xlog:safepoint`) beside its GC, and `hop-matrix.sh` takes
  `node.sh sample` on each member before and after every run into `hop-<stamp>.samples`: per-CPU steal, context switches and the
  members' java ticks. A difference between the two lines is that run's, and the safepoint times, in UTC, place a stall in a run
  through the sample's `t` (epoch ms).
- `hop-matrix.sh` and `hop-knee.sh` also start `CounterSampler` on each member (`node.sh counter-sampler`, on CPUs 0-1,
  `MULTIHOST_COUNTER_MS` default 10, 0 for none): every counter of the member's driver, read off its CnC file, as the changed
  ones with a wall-clock `t`, in `counter-sampler.log` under the rate's or the ramp's logs. It shows which counter, commit
  position or agent's maximum cycle time moves or stops in a stall. Each rate's or ramp's logs are fetched when it ends, and a
  knee step's line gives its start (`started`, epoch s).
- `hop-knee.sh` also starts `node.sh meminfo-sampler` on each member host: the kernel's dirty and writeback pages from
  `/proc/meminfo`, every `MULTIHOST_MEMINFO_MS` (default 100), with a wall-clock `t`, in `meminfo-sampler.log` under the
  ramp's logs. Beside `CounterSampler`'s recorder write times, it tells a stall on the kernel's writeback from one in the process.
- `MULTIHOST_ARCHIVE_DIR` puts each member's `archive-<id>` there, linked into `data/`, and `node.sh wipe` removes it; the
  cluster directory stays under `MULTIHOST_BASE_DIR`. Set it to `/dev/shm/…` to take the archive's disk out of a run.
- `hop-knee.sh` ramps the rate on one fresh cluster per arm, payload and run, in steps of `KNEE_SECONDS`, and ends each ramp in a
  `knee` line: the last rate below the first step whose p99 exceeds `KNEE_P99_US` (default 10 ms), that loses a marker or whose
  leader moved. The limit is a choice, not a measurement; the step lines are in `hop-matrix.sh`'s format.
- `hop-matrix.sh` runs the order check once per rate, before the cluster goes down: `node.sh queue-digest` (`QueueDigest`) reads each
  member's recordings of group 1's queue off its archive, fails on a gap in `groupSeqNo`, and prints a CRC-32C over the frames with
  a checkpoint every 65536; `hop-order-check.py` compares the three, and when the members stopped at different frames (the 1 Hz
  heartbeat keeps adding them) they are digested again up to the last frame all hold. The verdict is a `# order` line of the results,
  and `hop-analyze.py` fails a result set whose line is not `order=PASS`. It covers the rate's runs together, not each one.
- `hop-gate-edge.sh` runs the gate-edge set: a fresh cluster per arm and repeat, a gateway host on `MULTIHOST_CLIENT` (with the
  stub's host inside it for `hosted`), `hosted_hop_bench` or `hop_bench direct` with `--flood-ms` offering markers as fast as
  they are taken, the leader's member stopped (`node.sh pause`) for `HOP_EDGE_PAUSE_S`, and a line per run with a verdict.
  Hosted passes with nothing lost, duplicated or out of order, the stub retaining nothing, an edge seen and at least one
  command dropped by the host (else the edge proved nothing); direct with nothing duplicated or out of order and an edge seen;
  both with `hop-order-check.py`'s comparison of the members. It exits 1 if any run fails. The flood ends at `HOP_EDGE_FLOOD_MS`;
  a run that offers `HOP_EDGE_COUNT` markers (default 60 million) fails, since the pause may then come after the last one. A marker's
  index is 28 bits in both benches, so a run holds 268 million, and a larger `--count` is refused.
  On c6in.8xlarge members, five runs of each arm, twice (two launches): hosted lost, duplicated and reordered nothing, the host
  dropped 1024–3072 commands at the edges (1024 per pass over a full window), the stub retained none, and the gate was shut 0.2–0.7 ms
  and recovered in 8.9–22.5 ms; direct lost, duplicated and reordered nothing either, saw 1–3 edges, and recovered in 1.7–20.6 ms,
  the client's offers being declined (44–59 million) while the stopped leader back-pressures ingress. In the second launch two direct
  runs held the gate shut for 1426 and 1876 ms, and a pause gave 1–3 edges; the member logs of that launch were not kept, so neither is
  explained. The direct arm offered 4–8 million more markers in the same flood (17.6–22.3 million against 13.9–16.5 million in the
  second launch). Offers are declined at whatever rate each arm drains, so that is not a throughput figure; the knee of Test 1b is.
- `node.sh sample` also records the NIC's ENA allowance and drop counters (`ethtool -S`) in each `.samples` line.
- `hop-analyze.py <results>…` applies the rule's latency part: per rate and hosted mode, the hosted median of each percentile
  must lie within the direct runs' range plus the margin (`--margins`, `--min-runs`), and no run may fail or lose a marker. A run with p99.9
  at or above `--stall-us` (5 ms) is a stall: it is listed and left out of the comparison and of `--min-runs`, and each arm's worst
  p99.9 is printed.

The rate sweep, the idle-strategy set, the payload sweep and the knee of Test 1b run with these settings; the 1 KiB run at 100k msg/s is a
second invocation with `HOP_PAYLOADS=1024 HOP_RATES=100000`. Not built yet:

- the soak's histogram, since each bench keeps every sample;
- a manifest per run, and of the recordings under "What a run records" the per-second `mpstat` and `pidstat` and the CPU of the
  host and the bench, which are gone when the run's second sample is taken; the samples cover each member's processes;
- the rule's other criteria in `hop-analyze.py` (commands lost at a gate edge, the host's cores), and a randomised run order;
- the calibration run, the stage split, the producer sweep and the `fdatasync` trace.

The tooling is validated on localhost first (`MULTIHOST_MEMBERS=localhost,localhost,localhost`): the direct arm and the
hosted arm must reproduce `hosted-facade.md` §3.6 within its spread before an instance is launched. A one-instance variant
of the matrix (all members, the host and the client on one instance, cores apart) takes the network out; a difference from the
three-instance run is the network.

**Threats.** Steal, frequency scaling and noisy neighbours on the virtualised instance: record the clock source and the `steal`
column of `/proc/stat` per run, and repeat a run with steal above 1%. No burstable (`t`) families. The first run after a cluster start is
the worst (JIT, warm-up), so each run discards a warm-up and each rate gets a fresh cluster. A faster commit than the laptop's 250 µs
makes the hop a larger share of the loop; that is the question, so the cluster is not slowed to match. Time is slewed by `chrony`, never
stepped: a step back of more than 10 s terminates a member (`ops.md`).

**Archive writes on EBS.** One recorder thread writes the Raft log's recording and every queue's, so a blocked `write()` of it
stops the log's recording position. The leader commits no further than its own log recording
(`ConsensusModuleAgent.quorumPositionBoundedByLeaderLog`) and a follower's service reads its log no further than its own, so the
sequencer stalls for as long, and every consumer with it. On the members' root volume (gp3, 3000 IOPS,
125 MB/s) with the kernel's default writeback, 6 of 24 runs at 50k msg/s stalled, in every arm and on any leader (a run counts
as a stall at p99.9 ≥ 5 ms; maxima 37–70 ms, none at 30k msg/s). The recorder's `archive-recorder max write time` counter
(`CounterSampler`) read 36 and then 60 ms, and an off-CPU trace of the thread (`bpftrace`, `sched:sched_switch` on its name,
`archive-recorde`) showed it waiting 24 and 37.5 ms, inside one run whose maximum was 37.6 ms, in
`pwrite64 → ext4_da_write_begin → __filemap_get_folio → folio_wait_bit_common → io_schedule`. A 20 ms sample of `/proc/diskstats`
and `/proc/meminfo` showed the cause: the kernel writes about 30 s of dirty archive data at once, 150–230 MB in under a second
every 20–35 s, and the recorder waits behind it when it appends to a page in that flush. One burst in seven blocked it.

| archive and cluster directories | kernel writeback | stalled runs | worst max |
| --- | --- | --- | --- |
| EBS | default, with and without `discard` | 6 of 24 | 70 ms |
| tmpfs | default | 0 of 9 | 7.1 ms |
| archive on tmpfs, cluster on EBS | default | 0 of 9 | 5.5 ms |
| EBS, `nodiscard` | `vm.dirty_bytes` 64 MiB, `vm.dirty_background_bytes` 16 MiB | p99.9 above 4.8 ms in each of 8 runs | 13 ms |
| EBS, `nodiscard` | `vm.dirty_expire_centisecs=50`, `vm.dirty_writeback_centisecs=20` | 0 of 15 | 7.8 ms |

The byte limits flush small amounts often and hit more runs. The shorter timers keep each flush to a few MB, so the waits are
5–8 ms: p99.9 is 2.4–2.8 ms in every run, p99 unchanged. A latency target under that needs the waits gone, not shortened:
higher provisioned gp3 throughput or an instance-store volume, neither tried. Not a deployment setting: tmpfs loses the recording
on a reboot, and without a `<snapshots>` policy the archive grows by 190 MB per 30 s run at this rate.

**The path is the log's recording, not the queue's window.** One c6in.8xlarge held three unpinned members, each archive on its own
gp3 volume with default writeback, at 49k msg/s for 600 s, and `CounterSampler` read each driver every 3 ms. The leader's
`clusterSeqNo` stood still five times, for 18–54 ms. In the three of 47–54 ms the commit position and the log's `RecordingPos`
held one value while the log publication ran 285–320 KiB ahead of it, and the recorder's written bytes did not change. A follower
stalled the same way on its own recorder, for 63 ms while the other two sequenced 2,965 frames. On every member
`queueBackPressureAlerts` stayed 0 and the queue kept at least 7.0 MiB of its 8 MiB window. In the leader's two shorter stalls
(18 and 25 ms) its own recording advanced while a follower's recorder was blocked; this run does not explain them. The three
archives shared one kernel, so their flushes coincided: the run shows the mechanism, not a stall rate.

**With no queue recording the stall is gone at this rate.** The same host, volumes, rate and length, three runs in one launch,
`CounterSampler` every 3 ms. With `replayer.deriveFromLog` (`disk-load-plan.md`, step 2a) a member's archive holds the Raft log
alone:

| run | intervals of 10 ms or more without a new `clusterSeqNo`, all members | longest | recorder's longest write | archive growth | dirty pages, peak |
| --- | --- | --- | --- | --- | --- |
| derived | 0 | 7 ms | 3.4 ms | 3.7 GB | 619 MB |
| recorded | 17, 3 of them on the leader | 81 ms | 79 ms | 7.3 GB | 1270 MB |
| derived, after the recorded run | 0 | 9 ms | 9.0 ms | 3.7 GB | 615 MB |

A blocked write on a follower holds that follower alone; the leader's longest write was 0.6 and 1.4 ms in the derived runs. The
kernel still flushes in bursts, of half the bytes. Intervals of 5–9 ms occur in every run, 67–79 in a derived run and 118 in
the recorded one, at a sampling interval that resolves no finer. Two runs on one host: no stall rate, and no figure for a higher
rate or a larger payload, where the log alone writes what both wrote here.

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
preloaded frame, and with `REPLAY_COLD=1` starts on an empty page cache.

The members run the archive with Aeron's default `aeron.archive.file.sync.level` of 0: nothing is forced to disk.
Running test 1 again with `MULTIHOST_MEMBER_OPTS="-Daeron.archive.file.sync.level=1
-Daeron.archive.catalog.file.sync.level=1"` measures what forcing it would cost on the instance's storage.

On c6in.8xlarge members with gp3 root volumes, at 20k msg/s, 18-byte payload, three runs of each arm per setting (one launch,
leader member 2, order check `PASS` in both), latency in µs:

| arm | sync level 0: p50 | p99 | sync level 1: p50 | p99 |
| --- | --- | --- | --- | --- |
| direct | 36.0–37.5 | 45.4–52.5 | 6537–6599 | 8920–9014 |
| hosted-spin | 36.7–37.7 | 47.7–50.0 | 6458–6507 | 8910–8983 |
| hosted-backoff | 92.9–99.6 | 121.9–132.3 | 6496–6572 | 8948–9018 |

Forcing the archive and catalog adds about 6.5 ms at the median and 9 ms at p99, 175 times the unforced median, against the
0.5–1 ms a single forced write is expected to cost on EBS (§6); the cause was not traced (the `fdatasync` trace is not built).
The three arms agree within 2% with sync on, so the idle strategy's 60 µs does not show. The p99.9 is 9.5–12.6 ms and the maxima
12.8–31 ms; with sync off the maxima are 0.2–3.4 ms.

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

Test 1b is a session of its own. Its runs come to about 90 of 150 s (3.75 h), the knee ramps (0.7 h), the soak and its warm-up
(2.1 h), the gate-edge runs (0.3 h) and setup (0.3 h): about 7.2 h on the four Test 1 hosts. That costs about $45 on
c6in.8xlarge ($6.35/hr) and about $145 on c6id.metal ($20.26/hr), from the instance prices above; stage 1 alone is 2.1 h and
about $13 on c6in.8xlarge. Neither includes the
rehearsals, so allow the first attempt twice the figure; the soak and the knee ramps run unattended.

The disk session (`disk-session.sh`) measures `disk-load-plan.md`, step 2a, in one launch: two knee ramps (`direct`, 100-byte
markers, from 50k msg/s in 10 s steps) with the flag and two without, two with it under the kernel's default writeback and
two with the archive on a default gp3 volume, then test 3's cold replay of 4,000,000 frames, quiet and under load, both ways.
The ramps take about 26 min, the replays 15–20 and the launch and setup about 25: about 1.2 h on the four Test 1 hosts,
about $7.50 with the volumes, $15 for a first attempt. Each member's root volume is gp3 at 1,000 MB/s and 4,000 IOPS, above
the 400 MB/s the recorded arm writes at the top step, and a second 100 GB gp3 at its default 125 MB/s is mounted for
`DISK_DEFAULT_VOLUME`; an EBS volume's settings change once in 6 hours, so both are set at launch. The members need
passwordless `sudo`, for the writeback and the cache drops.

## 4. Image

One Ubuntu 24.04 x86_64 image serves every host. Build it on a c6in.large and save it as an AMI:

```bash
sudo apt-get install -y openjdk-21-jdk-headless g++ git perl docker.io
sudo snap install cmake --classic    # the repo needs 3.30; apt has 3.28
export PATH=$PATH:/snap/bin          # non-login ssh commands lack it
git clone <this repository> ~/seqeron && cd ~/seqeron && git checkout <branch>
./gradlew uberJar :seqeron-service:compileTestJava
cmake -B cmake-build-release -DCMAKE_BUILD_TYPE=Release
cmake --build cmake-build-release --target hosted_hop_bench
git worktree add ../seqeron-direct cpp-direct-client-final    # the direct arm: hop_bench of the tag
git -C ../seqeron-direct apply "$PWD/seqeron-service/src/test/scripts/multihost/direct-hop-bench-payload.patch" \
    "$PWD/seqeron-service/src/test/scripts/multihost/direct-hop-bench-histogram.patch" \
    "$PWD/seqeron-service/src/test/scripts/multihost/direct-hop-bench-edge.patch"
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
- **Even writeback**: `vm.dirty_expire_centisecs=50` and `vm.dirty_writeback_centisecs=20`, so the kernel flushes the
  archive every 0.2 s instead of in a burst every 30 s ("Archive writes on EBS", test 1). Leave the `vm.dirty_*_bytes`
  and `*_ratio` limits at their defaults.

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
export MULTIHOST_SSH="ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new"   # BatchMode refuses an unseen host key

./cluster.sh up && ./cluster.sh leader && ./cluster.sh down    # the check after launch
./hop-matrix.sh                                                # test 1
./hop-analyze.py ../../../../../build/multihost/hop-*.txt      # the rule's latency part
HOP_RATES=20000 MULTIHOST_MEMBER_OPTS="-Daeron.archive.file.sync.level=1 -Daeron.archive.catalog.file.sync.level=1" \
    ./hop-matrix.sh                                            # the sync comparison; run it again without the option
REPLAY_PRELOAD=4000000 ./replay-remote.sh                      # test 3, and again with REPLAY_LOAD_DURING=1
DISK_DEFAULT_VOLUME=/mnt/gp3-default ./disk-session.sh         # the disk session
```

To run either arm on members that record no queue (`disk-load-plan.md`, step 2a), set
`MULTIHOST_MEMBER_OPTS=-Dreplayer.deriveFromLog=true`, and the same in `MULTIHOST_HOST_OPTS` for a gateway host
(`replay-remote.sh`, `hop-gate-edge.sh`), which then relays a member's Raft log. The order check reads each such member's
queue through its Replayer. `REPLAY_COLD=1` drops every member's page cache before each replay phase, so the replay reads
its log or recording from EBS; it needs passwordless `sudo` on the members.

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
2. Who launches the instances: nothing in this tree does but `ec2-smoke.sh`, and the machine this was planned on has no AWS credentials. [`account.md`](account.md) sets the account up and gives the launch commands.
3. A budget cap in instance-hours; Test 1b is about 7.5 h on four hosts.
4. The architecture: this plan is x86_64 (c6in, c6id); the figures in `hosted-facade.md` are aarch64, and Graviton is not covered.
5. The deployment's hypervisor and region, if the virtualised run is to match it; the prices above are us-east-1.
6. The deployment's real payload sizes and target rate, to replace the sweep where it differs.
7. Shared or dedicated tenancy for the virtualised instance.
