# Operations

What an operator watches on a running seqeron deployment, and what the platform settings beneath it mean.
It covers the metrics stack (a per-member exporter, Prometheus and a Grafana dashboard), the ports and
Replayer client ids a deployment allocates, the Aeron term lengths, MTU and subscriber timeouts that bound
the tap, what it means when a member or a gateway stops itself, and the clock every frame is stamped with.
[`running-a-cluster.md`](running-a-cluster.md) covers starting and configuring the cluster itself.

## Monitoring

### Architecture

Metrics are pulled, not pushed, from a static list of targets rather than through service discovery.

- **One exporter per member.** `metrics-exporter.sh` (`MetricsExporter`) runs beside a `SequencerServer`,
  as `clusterctl` does, sharing that member's Aeron directory. It reads the member's counters live from the
  CnC file (`CountersReader.forEach`) and serves them as `/metrics` in Prometheus text format.
- **Prometheus scrapes every exporter directly**, one target per member
  (`seqeron-service/src/main/ops/prometheus/prometheus.yml`). Its own `up{job="seqeron",member="N"}` is the
  member's reachability: a member that is down reads 0.
- **The exporter trusts its network.** It does not touch cluster ingress and does not authenticate
  callers; reachability is the access control, as for `clusterctl`. Put it, and Prometheus, inside the
  same network boundary as the members.

### Running it

**The exporter**, one per member, beside that member's `SequencerServer`. It needs no cluster session,
and the script sets the `--add-opens` flags every seqeron process that touches Agrona needs.

```
metrics-exporter.sh          # serve /metrics on port 9400 + memberId
```

| environment variable | property | default |
|---|---|---|
| `METRICS_EXPORTER_MEMBER_ID` | `metricsExporter.memberId` | `0` |
| `METRICS_EXPORTER_AERON_DIR` | `metricsExporter.aeronDir` | `$TMPDIR/seqeron-seq-aeron-<memberId>` |
| `METRICS_EXPORTER_PORT` | `metricsExporter.port` | `9400 + memberId` |

The paths below are this checkout's; in an installed distribution (`./gradlew operatorDist`) the same tree
is `ops/`, beside `bin/` and `lib/`.

**Prometheus** takes `seqeron-service/src/main/ops/prometheus/prometheus.yml`: one job, one target per
member's exporter.

```
prometheus --config.file=seqeron-service/src/main/ops/prometheus/prometheus.yml
```

Its targets are the local three-member cluster's (`localhost:9400`, `9401` and `9402`, as
`start-three-node-cluster.sh` runs it); a deployment names its real hosts. Each target carries a `member`
label so `up` names the member, and `honor_labels: true` keeps the `member` label the exporter already
puts on every sample.

**Grafana** takes `seqeron-service/src/main/ops/grafana/provisioning/`: a Prometheus datasource
(`http://localhost:9090`) and one dashboard (`seqeron.json`, uid `seqeron`), both provisioned from files.
Mount the whole tree at Grafana's provisioning root, which in the standard Docker image is
`/etc/grafana/provisioning`:

```
docker run -p 3000:3000 -v "$(pwd)/seqeron-service/src/main/ops/grafana/provisioning:/etc/grafana/provisioning" grafana/grafana
```

Run natively, point `GF_PATHS_PROVISIONING` at the same directory. The dashboard provider resolves its
path from that variable (`$GF_PATHS_PROVISIONING/dashboards`) rather than from a fixed container path, so
both work:

```
GF_PATHS_PROVISIONING="$(pwd)/seqeron-service/src/main/ops/grafana/provisioning" grafana server ...
```

The dashboard has one panel per metric family below, and one derived panel. **Node apply lag (ms)** is
`time() * 1000 - seqeron_sequencer_last_tick_timestamp_ms`: how far behind the cluster each member's
`SequencerService` is. It is the one place a member that publishes a contiguous but stale tap shows up;
neither the tap-stall nor the recovery-stall fence can see that state.

## Metrics reference

Every metric carries a `member="N"` label, read from the counter's key (`SeqeronCounters.addCounter`,
`KEY_MEMBER_ID_OFFSET`).

### Counter type ids

The exporter maps a counter to a metric by its Aeron type id. Aeron reserves 0–999 for itself.

| type ids | published by |
|---|---|
| 5000–5099 | `SequencerService` |
| 5100–5199 | `ReplayerService` |
| 5200 | `ReplayerStreamReceiver`, inside every client: `seqeron_app_recovery_stalled` |
| 5201–5299 | an application's own counters, allocated by the deployment |

The ids are defined in `SeqeronCounters` in each language, and the copies must stay in step. Counters
outside these ranges are not exported.

The application range (5200–5299) is published by clients, several per member, so its counters carry a
second label, `client="M"`, the client's Replayer client id; `member` alone would merge them into one
series. The exporter names only seqeron's own counters. An application's counter is exported under the
first token of its label, so an application labels its counters `<app>.<area>.<metric> member=…
client=…`: `myapp.fix.sessionsUp member=0 client=3` scrapes as `myapp_fix_sessionsUp{member="0",client="3"}`.
Two applications that use the same type id appear as one metric, named by whichever was scraped first.

### seqeron's metrics

| metric | type | meaning |
|---|---|---|
| `up` | gauge | Prometheus's own, not a counter: 1 if its last scrape of that member's exporter succeeded, else 0 |
| `seqeron_sequencer_global_seq_no` | gauge | the last `globalSeqNo` on this member's tap |
| `seqeron_sequencer_tap_backpressure_alerts_total` | counter | times the tap's back-pressure alert has fired |
| `seqeron_sequencer_rejected_ingress_total` | counter | malformed ingress frames skipped by `Sequencer.sequenceMessage` |
| `seqeron_sequencer_leadership_change_total` | counter | leadership changes this member has observed and sequenced |
| `seqeron_sequencer_current_leader_member_id` | gauge | the leader this member's sequencer last recorded |
| `seqeron_sequencer_last_tick_timestamp_ms` | gauge | the consensus timestamp of the last `ClusterHeartbeat`, in ms (the frame carries ns) |
| `seqeron_sequencer_gateway_promotion_total` | counter | `GatewayActive` frames promoting a standby: on a gateway session closing, or on a designated instance not publishing `GatewayStarted` within 5 s (`GATEWAY_ACTIVATION_TIMEOUT_MS`) |
| `seqeron_sequencer_bootstrap_activated` | gauge | 1 once the bootstrap `GatewayActive` is in this cluster's log, else 0 |
| `seqeron_sequencer_tap_stalled` | gauge | 1 while the tap recording has made no progress for more than 200 ms under back-pressure, else 0; stays 1 when the member terminates for a tap it cannot record ([below](#a-node-that-terminates-itself)) |
| `seqeron_replayer_stalled` | gauge | 1 while the local archive refuses to serve a replay, else 0. Set by every path that asks the archive for one, the startup check included, and cleared by a bounded probe replay the member runs once a second while stalled, so it returns to 0 even when no client is asking for history |
| `seqeron_replayer_ready` | gauge | 1 once the tap recording is visible and replay requests are served |
| `seqeron_replayer_active_slots` | gauge | replays in progress |
| `seqeron_replayer_pending_requests` | gauge | replay requests waiting for a free slot |
| `seqeron_replayer_replays_served_total` | counter | replays started since this member came up |
| `seqeron_replayer_idle_ttl_reclaimed_total` | counter | replay slots reclaimed after sitting idle |
| `seqeron_replayer_integrity_failure` | gauge | 1 once the active tap recording failed the startup check that it begins at `globalSeqNo` 1, else 0. Only that recording is served, so one that begins later cannot cover the log. Latched: `ready` never returns to 1 in that process |
| `seqeron_replayer_control_replies_dropped_total` | counter | control replies dropped, rather than retried indefinitely, because a client stopped reading. Each costs that client one resend interval, so the rate, not the total, identifies a stuck client |
| `seqeron_replayer_client_id_collision` | gauge | 1 once two clients on this member were seen sharing a Replayer client id, else 0. They cancel each other's replays, so the Replayer tells both and both fail (spec R-4) |
| `seqeron_replayer_snapshot_round` | gauge | the newest round whose `SnapshotEnd` this member's Replayer has indexed, per source, labelled `source` as well as `member` ([`snapshot.md`](snapshot.md) §5). Every participating source should follow the latest round; one that lags is missing rounds. Rebuilt from the recording after a restart, so it reappears once the index has caught up |
| `seqeron_replayer_throttled_requests_total` | counter | replay requests answered `ReplayPending` only because the tap recording trailed the tap by more than a quarter of the window while a replay was running ([Term lengths](#term-lengths)). A held client resends about every 500 ms, so the rate is about twice the number of clients held. A sustained rate means the recorder is falling behind the tap |
| `seqeron_app_recovery_stalled` | gauge | 1 while this client's recovery has dispatched nothing for 30 s while not caught up, else 0; also labelled `client`. The client is holding, which is safe, but not serving, and nothing else says so. The causes are a `ReplayUnavailable` refusal, a Replayer that never answers, and a gap this member's recording cannot cover; the client's own log line names which |

## Ports

seqeron's processes bind these ports, and an application's own must stay clear of them.

| port | used by |
|---|---|
| `base + memberId*10 + 1` | the member's archive |
| `base + memberId*10 + 2` | cluster ingress |
| `base + memberId*10 + 3` | consensus between members |
| `base + memberId*10 + 4` | the Raft log |
| `base + memberId*10 + 5` | catch-up transfer |
| `9400 + memberId` | `metrics-exporter.sh`'s `/metrics` (TCP) |
| 9200, 9201 | `TestGateway`'s TCP listeners (test harnesses only) |
| `9202 + memberId` | `seqeron-examples`' cluster egress (UDP) |
| 9205, 9206 | `seqeron-examples`' C++ gateway pair's cluster egress (UDP) |

- **`base`** is 9300 unless `SEQERON_PORT_BASE` sets it, to a value from 1024 to 65466; a process given an
  invalid value fails at startup. Moving the base does not move the other ports in the table.
- **The cluster block**, `base` to `base + 69`, is seven members wide, so a cluster has at most seven
  members: Raft's 3, 5 or 7.
- **Set it everywhere alike.** `SEQERON_PORT_BASE` must be identical for every seqeron process on every
  host; a member and a client that disagree bind and dial different ports, and the symptom is a connection
  that never completes. `SEQERON_HOSTS` (`h0,h1,h2`, member `i` at entry `i`) is set the same way. A member
  builds its `clusterMembers` from it, and a client's default ingress endpoints, `clusterctl`'s egress host
  and a gateway host's member archives follow from it.
- **Four copies of the formula** exist: `seqeron-service/src/main/scripts/ports.sh` and `PortLayout` in Java,
  C++ and C#, pinned to the same values by their `PortLayoutTest`s and `SequencerServerTest`. Change all
  four together. An application checks its own ports against the cluster block with
  `PortLayout.isClusterPort()`.
- **Every co-located process needs its own ports**, since two media drivers on one host cannot bind the same
  UDP port. `clusterctl` binds none: its egress uses an ephemeral port, as the C# examples' does.
- **A gateway host binds no fixed port either.** Its relay reaches each member's archive port, and the member
  replies and replays to ephemeral UDP ports at the name `SEQERON_HOST` gives, so a firewall between them
  must let the members reach those.

## Replayer client ids

Every client on a node — façade, `ReplayerStreamReceiver`, probe — takes a `clientId` that the node's
Replayer keys its replays by, and it must be unique among the clients on that node. Two that share one
cancel each other's replays and neither catches up. The Replayer detects the collision within a few
seconds, logs it, sets `seqeron_replayer_client_id_collision` (type id 5108) and tells both clients (spec
**R-4**). It cannot tell which was there first, so both stop: their next `poll()` or `doWork()` throws
`IllegalStateException` (Java), `std::runtime_error` (C++) or `InvalidOperationException` (C#).

This repository's processes use the ids below. An application's should start at 23 and skip 31–36 and
41–43.

| `clientId` | used by |
|---|---|
| 1 | `start-three-node-cluster.sh` per-member probe; `docker-failover-test.sh` observer |
| 7 | `replay-bench.sh` probe; `seqeron-examples` (Java) |
| 8 | `seqeron-examples` (C++) |
| 9 | `ClusterProbe follow`/`confirm` default |
| 10 | `TestGateway serve` default; `chaos-runner.sh` second consumer |
| 11, 12 | `failover-test.sh` producers |
| 13, 14 | `seqeron-examples` `ColocatedApp`, Java, C++ |
| 15, 16 | `seqeron-examples` `GatewayApp`, Java or C++, GW-EX-A, GW-EX-B |
| 17, 18 | `seqeron-examples` `SnapshotApp`, Java, C++ |
| 19 | `seqeron-examples` `FollowStream` (C#) |
| 20 | `seqeron-examples` `ColocatedApp` (C#) |
| 21, 22 | `seqeron-examples` `GatewayApp` (C#), GW-EX-CS-A, GW-EX-CS-B |
| 31–36 | `csharp-client-test.sh` C# probes |
| 41–43 | `csharp-windows-test.sh` C# probes |

## Term lengths

Every Aeron stream is a log buffer of three terms, and its term length sets three things: its largest
message (term / 8); its publication window, how far a publisher may run ahead of the consumer it waits for
before `offer` back-pressures (term / 2); and its memory, 3 × term, mapped sparse but resident once the
stream has cycled through all three terms. A UDP stream costs another 3 × term per receiving image.

| stream | channel | term length | set by |
|---|---|---|---|
| the tap (stream 205), every co-located producer's ingress, replay control | `aeron:ipc` | 16 MiB | `aeron.ipc.term.buffer.length` on the member's or gateway host's driver |
| ingress from a producer not beside the leader, egress, consensus between members | `aeron:udp` | 16 MiB | `aeron.term.buffer.length` on the sending driver |
| the Raft log | `aeron:udp` | 64 MiB | `aeron.cluster.log.channel`, default `aeron:udp?term-length=64m` |
| a replay: recovery, the snapshot index, a gateway host's relay | IPC or UDP | the recording's | the stream the recording was made from |

16 MiB is seqeron's IPC default; Aeron's own is 64 MiB. The other rows are Aeron's defaults. A client
attached to a member's driver uses that driver's IPC term length and sets none of its own.

**Choosing one.** A term length is a power of two from 64 KiB to 1 GiB, and must be at least:

- 8 × the largest frame, so a frame fits Aeron's message limit: 128 KiB, for an 8,944-byte message on the
  Raft log;
- 2 × peak bytes per second × the longest stall to absorb. The window is the room a stalled consumer has.
  The archive is the tap's one tethered consumer, so a recorder stall longer than window / rate
  back-pressures the sequencer; an untethered tap subscriber that falls a window behind is dropped and
  heals through replay. At 16 MiB the tap's window is 8 MiB: 0.4 s at 20 MB/s.

Two things keep the recorder inside the window. Every log buffer is a memory-mapped file in the driver's
Aeron directory, so on Linux it belongs on tmpfs, such as `/dev/shm`: on a disk, writing back its dirty pages
stalls the threads that write them, the sequencer's included. A member or gateway host whose directory is not
on tmpfs warns at start. And while the recording trails the tap by more than a quarter of the window, the
Replayer starts a new replay only when none is running, since replays compete with the recorder for the disk.

Beyond that, a larger term costs memory and cache. At 16 MiB a member maps 48 MiB for the tap, 48 MiB per
co-located producer's IPC ingress and 48 MiB per replay in progress, besides the Raft log's 192 MiB.

**Setting one.** Pass `-Daeron.ipc.term.buffer.length=32m` (or `aeron.term.buffer.length` for UDP) to the
`java` command that runs `SequencerServer` or `ReplayerServer`; the scripts and the Docker image read it
from `JAVA_TOOL_OPTIONS`. Set the IPC term length identically on every member and gateway host. A
recording's positions depend on it, since a term ends in padding, and a gateway host's relay resumes on
the next member at the same position only when both recordings agree; otherwise it falls back to that
member's recording start.

A recording keeps the term length it was made with, and so do its replays. A member's tap is recorded
afresh at every start, so a new IPC term length takes effect at its next start. The Raft log's cannot
change on a cluster with history: the cluster extends one log recording, and the archive refuses to extend
it with a different term length.

## MTU

A frame longer than its stream's MTU less 32 bytes is fragmented, and every seqeron consumer reassembles
it, so the MTU decides packet sizes, not correctness. Aeron's default of 1408 keeps payloads up to 1,316
bytes whole everywhere, and suits a deployment whose payloads stay below that.

Larger payloads, up to `MAX_PAYLOAD_LENGTH` (8,884 bytes), stay whole on the tap with
`-Daeron.ipc.mtu.length=8960`, passed as the term length is. A replay carries its recording's MTU, so every
UDP replay of the tap — a gateway host's relay, a `ClusterStreamClient` following a member on another host
— then sends datagrams of up to 8,960 bytes, with small frames batched into them. The network between them
needs a 9000-byte MTU; otherwise the kernel fragments each datagram, and a lost piece loses all of it.

Set the IPC MTU identically on every member and gateway host, for the reason the term length must be:
once frames fragment, a recording's positions depend on it, and a relay resumes at the same position on
the next member only when both recordings agree. Change it on all of them in one restart window; a rolling
change leaves them disagreeing until the last one restarts.

A recording keeps the MTU it was made with. A member's tap is recorded afresh at every start, so a new IPC
MTU takes effect at its next start. The Raft log's UDP MTU, `aeron.mtu.length` unless the log channel
names one, cannot change on a cluster with history: the archive refuses to extend the log recording with a
different MTU.

## Untethered subscribers

Every application subscribes to the tap untethered, so one that stops polling — a GC pause, a long
snapshot — is dropped and heals through replay rather than holding the sequencer back. Its driver drops it
on a timer: once it has been three quarters of a window behind for `aeron.untethered.window.limit.timeout`,
and then lingered for `aeron.untethered.linger.timeout`, each checked every `aeron.timer.interval`. Until
then it still holds the window, and a full window behind it back-pressures the tap.

The sequencer and a gateway host's relay terminate after 1 s of back-pressure with no recording progress,
so seqeron's drivers set those three to 10 ms, 100 ms and 100 ms. At Aeron's defaults of 1 s, 5 s and 5 s,
a subscriber that stops for a window's worth of traffic would take its member down. When overriding them,
keep both timeouts plus two timer intervals well inside that second. A dropped subscriber rejoins at the
live position after `aeron.untethered.resting.timeout`, 10 s.

## A node that terminates itself

A member's archive is its copy of the sequenced history, so a member that cannot record would accumulate
silent holes in it. Rather than do that, `SequencerServer` exits with code **70** when its archive stops
recording the tap: no progress for 1 s under back-pressure, or the recording gone. This is deliberate, not
a crash.

- **What you see.** A `[SequencerService/N] FATAL: … terminating this node` line naming the reason, and
  `seqeron_sequencer_tap_stalled{member="N"}` at 1 until the process goes; then `up{job="seqeron",member="N"}`
  falls to 0.
- **The cluster carries on** with the remaining members. Every member holds an identical, complete
  recording, so nothing is lost with this one, and an election moves leadership if it led. **A second member
  down is a loss of quorum**: treat it as an emergency, not a repeat.
- **Restart it** once its storage is healthy. Recovery is the usual full-log replay, which rebuilds the
  recording from scratch. Under a supervisor this is automatic: exit 70, against the 0 of an orderly
  `clusterctl shutdown`, is the signal to restart.
- **If it exits 70 again immediately**, the archive is still broken: start-up has the same bound, and the
  recording must go live within 5 s. Fix the storage first.

A member also exits 70 when its Replayer's duty cycle dies (a `[ReplayerService/N]` error names it), and 71
when its shutdown gave up waiting for that duty cycle. Both mean restart it.

## A gateway that fences itself

A gateway built on the client tier's `Gateway` stops itself when it can no longer trust its view of the log
([`fault-tolerance.md`](fault-tolerance.md) §2.1). The façade reports the reason once, through
`onFenced(ClusterError, detail)`, having released its cluster session, and the application normally exits.
`TestGateway` exits **70**; a production gateway chooses its own code, and 70 is this repository's
convention for "fenced, restart me". A gateway is not a cluster member, so this is never a quorum question.

| `ClusterError` | usual cause |
|---|---|
| `CLUSTER_SESSION_LOST` | the cluster closed the session, or no new leader arrived after a failover, on an instance that had announced its activation on it; on any other, no new session opened within 20 s and the attempt then under way failed |
| `TAP_STALLED` | no `ClusterHeartbeat` on the co-located tap for 20 s, usually because that member's `SequencerServer` terminated (above); they share the tap. A standby instance or a replica not on the leader's member logs `TapStalled` instead and keeps running |
| `RECOVERY_STALLED` | recovery delivered nothing for 60 s after the instance had been caught up; see `seqeron_app_recovery_stalled`. A standby or non-leader replica logs `RecoveryStalled` instead and keeps running |
| `SNAPSHOT_DIVERGED` | the instance's snapshot of a round differs from the one its source sequenced: its state is not the log's (spec §16 A-7). A restart rebuilds the state; an instance that diverges again has broken determinism (A-6) |
| `SNAPSHOT_UNRESTORABLE` | the instance cannot restore its newest confirmed snapshot: a `formatVersion` or header version its build does not read, or records that fail the file's trailer, which means a damaged file ([`snapshot.md`](snapshot.md) §7). The log line `SnapshotRestoreFailed` names the round. A restart repeats it until the build is fixed or the file, `<round>.snapshot` in the instance's snapshot directory, is removed; the instance then restores an older file or replays from `globalSeqNo` 1 |
| `INGRESS_CONFIRM_FAULTED` | an own frame on the tap did not match the oldest pending one, most often because the sequencer rejected one (`seqeron_sequencer_rejected_ingress_total`) |

- **The standby takes over by itself**, if one is running. A fenced instance looks to the cluster like a
  process that died, and the sequencer promotes the standby when its session closes
  (`seqeron_sequencer_gateway_promotion_total`). The new instance opens new external connections; nothing
  moves across live.
- **Restart it** under supervision. Recovery is the usual full-log replay, and the restarted instance
  returns as a standby, active only when named.
- **For `TAP_STALLED` and `RECOVERY_STALLED`, check the co-located member first.** A stopped
  `SequencerServer` or an unavailable Replayer (`seqeron_replayer_ready`,
  `seqeron_replayer_integrity_failure`) explains both.
- **A gateway publishes one seqeron metric**, `seqeron_app_recovery_stalled`, as every client does. Its
  fences appear only in its log.

## The consensus clock

Every frame's `timestamp` is epoch nanoseconds read from the leader host's clock when it appends the entry
(spec §9.3). The unit is fixed; the precision and accuracy are the host's, and seqeron does nothing to
discipline them.

- **Discipline every member, not only the leader.** Any member can be elected, and an offset between
  members shows in the timestamp at the leadership change. Where UTC traceability is required (MiFID II
  RTS 25), that means PTP or an equivalent traceable source on every member, with its offset monitored
  outside seqeron.
- **It is commit time, not event time.** It stamps when the cluster ordered a message, after ingress
  transit and Raft replication. A regulatory event timestamp — an order's receipt, an execution — belongs
  to the application that saw the event, taken at its own edge and carried in its payload.
- **Clock skew is the time daemon's to report.** Node apply lag reads the leader's clock against
  Prometheus's, so a gross offset shows there as a lag that is implausibly large or negative; anything
  finer comes from the host's own clock monitoring.

## Not provided

- **No clock-offset metric.** `/metrics` reports no member's offset from UTC or from its peers; that comes
  from the host's time daemon.
- **No authentication on `/metrics`.** It is meant to sit inside the members' network boundary
  ([Architecture](#architecture)).
- **No alerting rules.** Neither Prometheus alerts nor Grafana alert provisioning: dashboard only.
- **No service discovery.** `prometheus.yml`'s target list is maintained by hand; for a cluster whose
  membership changes, keep it in step with `SEQERON_HOSTS`.
- **No gateway fence counters.** Beyond `seqeron_app_recovery_stalled`, a gateway exports nothing of
  seqeron's: no fence counts, no connection state. `seqeron_sequencer_gateway_promotion_total` shows that
  a promotion happened, not why; the gateway's log does. An application can export its own counters in
  the application range ([Counter type ids](#counter-type-ids)).
