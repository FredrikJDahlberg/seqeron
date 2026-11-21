# Low-rate sizing: results

What a member needs at 10 msg/s on average and 100 msg/s at peak, from the run of 14 November 2026. The method,
scripts and full tables are in `doc/plans/low-rate-sizing-plan.md`; this is the conclusion and how far it carries.

## What to provision

Per member, three members, `backoff` idle strategy and `SHARED` driver and archive threading:

| Resource | Provision | Basis |
|---|---|---|
| CPU | 3 CPUs, request equal to limit | The smallest quota with no throttling. 2 CPUs throttled 3 to 7 periods in each of seven runs; 1 CPU throttled about 4% of periods on the busiest members. No setting caused an election or lost a frame. |
| Heap | 512 MiB, `-Xms` equal to `-Xmx` | 256 MiB, 512 MiB and 1 GiB behaved alike: longest GC pause 5 ms or less. The floor is below 256 MiB and was not found. |
| `/dev/shm` | 1 GiB | 512 MiB behaved the same. Its use grew 30 MB an hour over 30 minutes and had not levelled off. |
| Pod memory | 3 GiB | The working set (`anon` plus `shmem`) was 90 to 240 MiB. Limits down to 1,792 MiB passed. Page cache from reading the log reaches 2.1 GiB on a restart over 4 GiB of log, and the kernel reclaims it, so alert on the working set, not `memory.current`. |
| Log volume | 100 GB at a 100-byte payload, per member | 8.1 MB an hour at 10 msg/s, which is 68 GiB a year, about 219 bytes a frame. A 200-byte payload is about 320 bytes a frame, so about 100 GiB. Without a `<snapshots>` policy the log is never trimmed. |

The CPU in use is small: 0.24 cores averaged over 100 msg/s with a 2,000 msg/s burst, and 0.08 cores at 10 msg/s. The
quota exists for bursts, elections and recovery, not for the rate.

## Failure behaviour

At 100 msg/s with 3 CPUs, all four scenarios kept every frame once and in order:

| Scenario | Failover | Rejoin |
|---|---|---|
| Kill the leader | 4.5 s | 12.4 s |
| Kill a follower | none | 5.7 s |
| Wipe a follower's volume, restart | none | 5.9 s |
| Cap the leader at 0.3 CPU for 20 s | none | none |

The leader failover is a little over the 3 to 4 s `doc/ops.md` gives. It is one sample.

A restart replays the whole log: 1.7 s plus 0.84 s per million frames, 5.9 s for 5M frames and 18.4 s for 20M. A year
at 10 msg/s is about 315M frames, which is about 4.4 minutes with the log in page cache. Read cold from a gp3 volume at
its default 125 MB/s, 69 GB takes about 9 minutes by itself, so plan for 5 to 10 minutes. That is the longest outage a
member has, and it decides whether a snapshot policy is worth its cost (`doc/snapshot.md`).

A member restarted within 10 s of a SIGKILL exits with "active mark file detected" (`doc/ops.md`). A pod restarts onto
the same `/dev/shm` and volume, so the first start after a crash can fail once.

## What this does not show

- **One run per cell.** The 3-CPU pass is evidence, not proof. The 2-CPU result is marginal and was not repeated enough
  to call it a failure.
- **A shared host.** The three members ran as containers on one c6in.4xlarge and one bridge network, on a gp3 root
  volume. A small instance has its own network, its own disk and CPU steal.
- **A quota is not a vCPU.** A 2 vCPU instance is one physical core with two hyper-threads. A 2-CPU quota on a large
  host is two vCPUs that need not share a core. Whether a 2 vCPU instance suffices is not established. A 2-CPU quota
  was marginal, and an OpenShift worker also reserves CPU for the node, so the smallest worker worth trying is 4 vCPU.
  That is an inference, not a measurement.
- **A short soak.** Thirty minutes gives growth rates. It does not show whether `/dev/shm` levels off or a slow leak.
- **No network partition, node loss or fencing.** Those need the faulteron injector and a real cluster
  (`doc/fault-tolerance.md`, `doc/plans/low-rate-sizing-plan.md` section 4). Nothing was run on OpenShift.
- **One client.** The load was one probe client on one member's driver, 100-byte payloads, order-checked.

## Open

1. Repeat the 2-CPU and 3-CPU cells to settle the floor.
2. Run the failover several times for a distribution.
3. Run a soak long enough for `/dev/shm` to level off.
4. Repeat all of it on production-sized instances, one member per host.
5. Decide on a snapshot policy from the restart time.
6. Run the node-loss, fencing and drain tests on OpenShift.
