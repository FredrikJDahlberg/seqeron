# Ops — Prometheus/Grafana monitoring stack

Node-local metrics exporter, scraped by Prometheus and feeding Grafana. Covers
`org.limitless.seqeron.protocol.SeqeronCounters` — every `SequencerService`/`ReplayerService` operator
counter — end to end from a running node to a dashboard panel.

## Architecture

Pull, not push, and a **static target list**, not service discovery:

- Each node runs `metrics-exporter.sh` (`MetricsExporter`), co-located with a `SequencerServer`/
  `ReplayerServer` the same way `clusterctl` is — sharing that node's Aeron directory — and serves
  `/metrics` in Prometheus text exposition format, read live off the CnC file via
  `CountersReader.forEach`.
- Prometheus scrapes every node's exporter directly, one target per member
  (`seqeron-service/src/main/ops/prometheus/prometheus.yml`). Its own `up{job="seqeron",member="N"}` is the per-node
  reachability gauge: a node that's down reads 0.
- The exporter doesn't touch cluster ingress or authenticate callers — same trust model as
  `clusterctl`: reachability is the access control. Put it behind the same network boundary as the
  nodes themselves, and Prometheus inside it.

## Running it

### Per node — metrics-exporter.sh

```
metrics-exporter.sh          # serve /metrics on port 9400 + memberId
```

| Env var | Property | Default |
|---|---|---|
| `METRICS_EXPORTER_MEMBER_ID` | `metricsExporter.memberId` | `0` |
| `METRICS_EXPORTER_AERON_DIR` | `metricsExporter.aeronDir` | `$TMPDIR/seqeron-seq-aeron-<memberId>` |
| `METRICS_EXPORTER_PORT` | `metricsExporter.port` | `9400 + memberId` |

Run one per node, co-located with that node's `SequencerServer`/`ReplayerServer` (mirrors
`clusterctl.sh`'s co-location — same Aeron directory, no cluster connection needed). Needs the same
`--add-opens` JVM flags as every other seqeron Java process that touches Agrona; the script sets
them.

### Prometheus

The paths below are this checkout's. In an installed distribution (`./gradlew operatorDist`) the same
tree is `ops/`, beside `bin/` and `lib/`.

`seqeron-service/src/main/ops/prometheus/prometheus.yml` — one job, one target per member's exporter:

```
prometheus --config.file=seqeron-service/src/main/ops/prometheus/prometheus.yml
```

The targets are the 3-node dev cluster's (`localhost:9400`/`9401`/`9402`, from
`start-three-node-cluster.sh`); name the real hosts for a deployment. Each target carries a `member`
label so `up` names the node, and `honor_labels: true` keeps the `member` label the exporter already puts
on every sample.

### Grafana

`seqeron-service/src/main/ops/grafana/provisioning/` — a `Prometheus` datasource (`http://localhost:9090`) and one dashboard
(`seqeron.json`, uid `seqeron`), both provisioned by file. The dashboard has one panel per exported
metric family — see the reference below. One panel plots a derived value rather than the counter
itself: **Node apply lag (ms)** is `time() * 1000 - seqeron_sequencer_last_tick_timestamp_ms`, since
the raw consensus timestamp is an epoch value no operator can read. Per member, that difference is how
far behind the cluster that node's `SequencerService` is — the one place a node publishing a
contiguous but *stale* tap becomes visible (neither the tap-stall silence watchdog nor the
recovery-stall watchdog can see that state).

Deploy by mounting the whole `seqeron-service/src/main/ops/grafana/provisioning` tree at Grafana's own provisioning root.
Under the standard Grafana Docker image that's already `/etc/grafana/provisioning` by default:

```
docker run -p 3000:3000 -v "$(pwd)/seqeron-service/src/main/ops/grafana/provisioning:/etc/grafana/provisioning" grafana/grafana
```

Running Grafana natively instead (no container), point `GF_PATHS_PROVISIONING` at the directory
yourself — the dashboard file provider resolves its `path` from that same env var
(`$GF_PATHS_PROVISIONING/dashboards`, via Grafana's own provisioning-file env-var expansion) rather
than a hardcoded container path, so both cases resolve correctly:

```
GF_PATHS_PROVISIONING="$(pwd)/seqeron-service/src/main/ops/grafana/provisioning" grafana server ...
```

## Metrics reference

Every metric carries a `member="N"` label (the memberId, read from the counter's structured key
buffer — see `SeqeronCounters.addCounter`/`KEY_MEMBER_ID_OFFSET`).

### Counter type ids

The exporter maps a counter to a metric by its Aeron type id. Aeron reserves 0–999 for itself.

| type ids | published by |
|---|---|
| 5000–5099 | `SequencerService` |
| 5100–5199 | `ReplayerService` |
| 5200 | `ReplayerStreamReceiver`, inside every client: `seqeron_app_recovery_stalled` |
| 5201–5299 | an application's own counters, allocated by the deployment |

The ids are defined in `SeqeronCounters.java` and its C++ twin `protocol/SeqeronCounters.hpp`, which must
stay in step. Counters outside these ranges are not exported.

The app range (5200–5299) is published by client replicas, several per node, so its counters carry a
second label, `client="M"` (the replayer client id); `member` alone would merge them into one series.
The exporter knows names only for seqeron's own counters. An application counter is exported under the
first token of its own label, so an application labels its counters `<app>.<area>.<metric> member=…
client=…`: `myapp.fix.sessionsUp member=0 client=3` scrapes as
`myapp_fix_sessionsUp{member="0",client="3"}`. Two applications that use the same type id appear as one
metric, named by whichever was scraped first.

The table below lists seqeron's own metrics.

| Metric | Type | Meaning |
|---|---|---|
| `up` | gauge | Prometheus's own, not read from a counter: 1 if its last scrape of that member's exporter succeeded, else 0 |
| `seqeron_sequencer_global_seq_no` | gauge | Last globalSeqNo emitted on this node's tap |
| `seqeron_sequencer_tap_backpressure_alerts_total` | counter | Count of times the tap-emit back-pressure alert threshold has fired |
| `seqeron_sequencer_rejected_ingress_total` | counter | Count of malformed ingress messages skipped by `Sequencer.sequenceMessage` |
| `seqeron_sequencer_leadership_change_total` | counter | Count of leadership changes this node has observed and sequenced |
| `seqeron_sequencer_current_leader_member_id` | gauge | memberId of the leader last recorded by this node's Sequencer |
| `seqeron_sequencer_last_tick_timestamp_ms` | gauge | Consensus timestamp of the last 1Hz ClusterHeartbeat emitted, in ms (the frame carries ns) |
| `seqeron_sequencer_gateway_promotion_total` | counter | Count of standby-promotion GatewayActive frames emitted — on a gateway session close, or on a designated instance failing to publish `GatewayStarted` within 5 s of being named (`GATEWAY_ACTIVATION_TIMEOUT_MS`) |
| `seqeron_sequencer_bootstrap_activated` | gauge | 1 once the bootstrap GatewayActive has been emitted in this cluster's log, else 0 |
| `seqeron_sequencer_tap_stalled` | gauge | 1 while the tap recording has made no progress for longer than the stall threshold (200ms) under back-pressure, else 0. Latches at 1 when the node terminates for an unrecordable tap — see below |
| `seqeron_replayer_stalled` | gauge | 1 while the local archive is refusing to serve a replay, else 0. Set from every path that asks the archive for one — the startup self-check included — and cleared by a bounded probe replay the node runs itself once a second while stalled, so it reads 0 again even on a Replayer no app is asking for history |
| `seqeron_replayer_ready` | gauge | 1 once the co-located tap recording is visible and replay requests are being served |
| `seqeron_replayer_active_slots` | gauge | Current count of in-flight replays |
| `seqeron_replayer_pending_requests` | gauge | Current count of replay requests waiting for a free slot |
| `seqeron_replayer_replays_served_total` | counter | Count of replays started since this node came up |
| `seqeron_replayer_idle_ttl_reclaimed_total` | counter | Count of replay slots reclaimed by the idle-TTL backstop |
| `seqeron_replayer_integrity_failure` | gauge | 1 once a tap recording failed the startup gseq-1 integrity check, else 0. Every recording in the node's chain is checked, not just the oldest: one that begins above 1 resumed mid-history, which is a hole at its join. Latched: `ready` never becomes 1 again for that process |
| `seqeron_replayer_control_replies_dropped_total` | counter | Count of control replies dropped rather than spun on because an app stopped draining the control stream. Each costs that app one resend interval, so the **rate** identifies a wedged replica — the absolute value does not |
| `seqeron_replayer_client_id_collision` | gauge | 1 once two co-located apps were seen sharing one `SEQERON_REPLAYER_CLIENT_ID`, else 0. They stop each other's replays and neither catches up until the launch configuration is corrected |
| `seqeron_app_recovery_stalled` | gauge | 1 while this replica's recovery has dispatched nothing for 30s while not caught up, else 0. Also labelled `client`. It is holding, which is correct and safe — but it is not serving, and nothing else says so: the causes are a `ReplayUnavailable` refusal, a Replayer that never answers, and a hole this node's recording chain cannot cover. The replica's own fault line names which |

## Ports

seqeron's processes bind these ports; an application's own ports must stay outside them.

| port | used by |
|---|---|
| `base + memberId*10 + 1` | the member's archive |
| `base + memberId*10 + 2` | cluster ingress |
| `base + memberId*10 + 3` | consensus between members |
| `base + memberId*10 + 4` | the Raft log |
| `base + memberId*10 + 5` | catch-up transfer |
| `9400 + memberId` | `metrics-exporter.sh` `/metrics` (TCP) |
| 9200, 9201 | `TestGateway` TCP listeners (test harnesses only) |
| `9202 + memberId` | `seqeron-examples` cluster egress (UDP) |
| 9205, 9206 | `seqeron-examples` C++ gateway pair cluster egress (UDP) |

`base` is 9300 unless `SEQERON_PORT_BASE` is set. The cluster block, `base` to `base + 29`, is three
members wide, so **a cluster has at most three members**; a fourth would need the block widened. Set
`SEQERON_PORT_BASE` identically for every seqeron process on every host: a node and a client that
disagree bind and dial different ports, and the symptom is a connection that never completes. It must
be between 1024 and 65506; a process with an invalid value fails at start-up. Moving the base does not
move the other ports in the table.

The formula has three copies, `protocol/PortLayout.java`, `protocol/PortLayout.hpp` and
`seqeron-service/src/main/scripts/ports.sh`, pinned to the same values by `PortLayoutTest` and
`SequencerServerTest`; change all three together. An application can check its own ports against the
cluster block with `PortLayout.isClusterPort()`.

Two Aeron media drivers on one host cannot bind the same UDP port, so every co-located process needs
ports of its own. `clusterctl` binds none: its egress uses an ephemeral port.

## A node that terminates itself

`SequencerServer` exits **70** when its local archive stops recording the node's tap (stalled with no
progress for 1s under back-pressure, or the recording gone outright). This is deliberate, not a crash:
that node's archive is its copy of the sequenced history, so one that cannot record can only accumulate
silent holes in it. What to expect and what to do:

- **In the log:** a `[SequencerService/N] FATAL: … terminating this node` line naming the reason, and
  `seqeron_sequencer_tap_stalled{member="N"}` at 1 until the process (and its counters) go away.
  `up{job="seqeron",member="N"}` then drops to 0.
- **The cluster keeps going** on the remaining members — every node holds an identical, complete
  recording, so nothing is lost with the node itself, and an election moves leadership if it held it.
  **Two nodes down is a quorum loss**, so treat a second one as an emergency rather than a repeat.
- **Restart it** once the storage is healthy: recovery is the usual full-log replay from `globalSeqNo`
  1, which rebuilds the node's tap recording from scratch. Under process supervision this is automatic
  — exit 70 vs the 0 of an orderly `clusterctl shutdown` is exactly the "restart me" signal.
- **If it exits 70 immediately on restart**, the archive is still broken (the same check bounds
  start-up: the recording must go live within 5s). Fix the storage before restarting again.

## A gateway that fences itself

A gateway built on the client tier's `app/Gateway` stops itself when it can no longer trust its view of
the log (`doc/fault-tolerance.md` §2.1). The façade reports the reason once, through
`Listener.onFenced(ClusterError, detail)`, and the application releases its cluster session, normally
by exiting. `TestGateway` exits **70**; a production gateway chooses its own code, and 70 is the
convention here for "fenced, restart me". It is not a cluster member, so this is not a quorum question.

| `ClusterError` | usual cause |
|---|---|
| `CLUSTER_SESSION_LOST` | the cluster closed the session, or no new leader arrived after a failover |
| `TAP_STALLED` | no `ClusterHeartbeat` on the co-located tap for 20 s, usually because that node's `SequencerServer` terminated (above); they share the tap |
| `RECOVERY_STALLED` | recovery delivered nothing for 60 s after the instance had been caught up; see `seqeron_app_recovery_stalled` |
| `INGRESS_CONFIRM_FAULTED` | an own frame on the tap did not match the oldest pending one, most often because the sequencer rejected one (`seqeron_sequencer_rejected_ingress_total`) |

- **The standby takes over by itself** if one is running: a fenced instance looks to the cluster like a
  process that died, and the sequencer promotes the standby when its session closes
  (`seqeron_sequencer_gateway_promotion_total`). The handover opens new external connections; nothing
  moves live.
- **Restart it** under supervision. Recovery is the usual full-log replay, and a restarted instance
  comes back as a standby, active only when named.
- **Check the co-located node first** for `TAP_STALLED` and `RECOVERY_STALLED`: a stopped
  `SequencerServer` or an unavailable replayer (`seqeron_replayer_ready`, `seqeron_replayer_integrity_failure`)
  explains both.
- **Metrics:** a gateway publishes `seqeron_app_recovery_stalled` (every client does) and nothing else of
  seqeron's; its fences appear only in its log.

## The consensus clock

Every frame's `timestamp` is epoch nanoseconds read from the **leader host's** clock when it appends
the entry (spec §9.3). The unit is fixed; the precision and accuracy are the host's, and seqeron does
nothing to discipline them.

- **Discipline every member, not just the current leader.** Any member can be elected, and an offset
  between members shows up in the timestamp at the leadership change. Where UTC traceability is
  required (MiFID II RTS 25), that is PTP or an equivalent traceable source on all three hosts,
  with its offset monitored outside seqeron.
- **It is commit time, not event time.** It stamps when the cluster ordered a message, after ingress
  transit and Raft replication. A regulatory event timestamp — order receipt, execution — belongs to
  the application that saw the event, taken at its own edge and carried in its payload.
- **Skew is the time daemon's to report.** Node apply lag reads the leader's clock against
  Prometheus's, so a gross offset shows there as a lag that is implausibly large or negative; anything
  finer comes from the host's own clock monitoring.

## Non-goals / open items

- No clock-offset metric. The `/metrics` endpoint doesn't report a member's offset from UTC or from its
  peers; that comes from the host's time daemon.

- No authentication on the `/metrics` endpoint — see "Architecture" above; it is meant to sit behind the
  same network boundary as the nodes.
- No Prometheus alerting rules or Grafana alert provisioning — dashboard only.
- `prometheus.yml`'s target list is fixed and hand-maintained — no service discovery. For a cluster
  whose membership changes, keep it in sync with `clusterMembers`.
- **Gateways publish no fence counters.** Beyond `seqeron_app_recovery_stalled`, a gateway exports
  nothing of seqeron's: no fence counts, no connection state. `seqeron_sequencer_gateway_promotion_total`
  shows that a promotion happened, not why; the gateway's log does. An application can export its own
  counters in the app range (see "Counter type ids" above).
