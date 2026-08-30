# Ops — Prometheus/Grafana monitoring stack

Node-local metrics exporter + central aggregating ops server, feeding Prometheus + Grafana. Covers
`org.limitless.phixeron.PhixeronCounters` — every `SequencerService`/`ReplayerService` operator
counter — end to end from a running node to a dashboard panel.

## Shape

Pull, not push, and an **aggregating proxy**, not service discovery:

- Each node runs `metrics-exporter.sh` (`MetricsExporter`), co-located with a `SequencerServer`/
  `ReplayerServer` the same way `clusterctl` is — sharing that node's Aeron directory — and serves
  `/metrics` in Prometheus text exposition format, read live off the CnC file via
  `CountersReader.forEach`.
- One central `metrics-aggregator.sh` (`MetricsAggregator`) scrapes every node's exporter over HTTP
  and re-exposes one combined `/metrics`. Prometheus's own scrape config then only ever needs
  network reach to this **one** process rather than to every node — a smaller, more easily
  firewalled surface than opening each node's `/metrics` port to wherever Prometheus runs.
- Trade-off taken deliberately: Prometheus's own built-in `up{}` metric would, under this topology,
  only ever reflect reachability to the aggregator, not to each node. `MetricsAggregator` closes
  that gap itself — it records its own per-node scrape success/failure as
  `phixeron_node_up{member="N"}`, independent of whatever else that scrape returned (a node that's
  down still shows `up=0`; it just drops out of the rest of the combined output).
- Neither exporter nor aggregator touch cluster ingress or authenticate callers — same trust model
  as `clusterctl`: reachability is the access control. Put them behind the same network boundary as
  the nodes themselves.

## Running it

### Per node — metrics-exporter.sh

```
metrics-exporter.sh          # serve /metrics on port 9400 + memberId
```

| Env var | Property | Default |
|---|---|---|
| `METRICS_EXPORTER_MEMBER_ID` | `metricsExporter.memberId` | `0` |
| `METRICS_EXPORTER_AERON_DIR` | `metricsExporter.aeronDir` | `$TMPDIR/phixeron-seq-aeron-<memberId>` |
| `METRICS_EXPORTER_PORT` | `metricsExporter.port` | `9400 + memberId` |

Run one per node, co-located with that node's `SequencerServer`/`ReplayerServer` (mirrors
`clusterctl.sh`'s co-location — same Aeron directory, no cluster connection needed). Needs the same
`--add-opens` JVM flags as every other phixeron Java process that touches Agrona; the script sets
them.

### Central — metrics-aggregator.sh

```
metrics-aggregator.sh        # serve combined /metrics on port 9500
```

| Env var | Property | Default |
|---|---|---|
| `METRICS_AGGREGATOR_PORT` | `metricsAggregator.port` | `9500` |
| `METRICS_AGGREGATOR_TARGETS` | `metricsAggregator.targets` | `0=localhost:9400` |

`targets` is `memberId=host:port`, comma-separated — the same `id=endpoint` shape
`clusterctl.sh`'s `ingressEndpoints` uses. For the 3-node dev cluster
(`start-three-node-cluster.sh`, one exporter per member on `localhost:9400`/`9401`/`9402`):

```
METRICS_AGGREGATOR_TARGETS="0=localhost:9400,1=localhost:9401,2=localhost:9402" metrics-aggregator.sh
```

Pure HTTP client/server — no Aeron dependency, so unlike every other phixeron tool it needs no
`--add-opens` flags and doesn't need to be co-located with any node, only HTTP reach to each
exporter.

### Prometheus

`src/main/ops/prometheus/prometheus.yml` — one job, scraping the aggregator's combined endpoint:

```
prometheus --config.file=src/main/ops/prometheus/prometheus.yml
```

### Grafana

`src/main/ops/grafana/provisioning/` — a `Prometheus` datasource (`http://localhost:9090`) and one dashboard
(`phixeron.json`, uid `phixeron`), both provisioned by file. The dashboard has one panel per exported
metric family — see the reference below. One panel plots a derived value rather than the counter
itself: **Node apply lag (ms)** is `time() * 1000 - phixeron_sequencer_last_tick_timestamp_ms`, since
the raw consensus timestamp is an epoch value no operator can read. Per member, that difference is how
far behind the cluster that node's `SequencerService` is — the one place a node publishing a
contiguous but *stale* tap becomes visible (see `review-2.md` #7; neither the tap-stall silence
watchdog nor the recovery-stall watchdog can see that state).

Deploy by mounting the whole `src/main/ops/grafana/provisioning` tree at Grafana's own provisioning root.
Under the standard Grafana Docker image that's already `/etc/grafana/provisioning` by default:

```
docker run -p 3000:3000 -v "$(pwd)/src/main/ops/grafana/provisioning:/etc/grafana/provisioning" grafana/grafana
```

Running Grafana natively instead (no container), point `GF_PATHS_PROVISIONING` at the directory
yourself — the dashboard file provider resolves its `path` from that same env var
(`$GF_PATHS_PROVISIONING/dashboards`, via Grafana's own provisioning-file env-var expansion) rather
than a hardcoded container path, so both cases resolve correctly:

```
GF_PATHS_PROVISIONING="$(pwd)/src/main/ops/grafana/provisioning" grafana server ...
```

## Metrics reference

Every metric carries a `member="N"` label (the memberId, read from the counter's structured key
buffer — see `PhixeronCounters.addCounter`/`KEY_MEMBER_ID_OFFSET`).

`phixeron_app_*` metrics carry a second label, `client="M"` — the replayer clientId. They are published
by the co-located C++ replicas (`FixGateway`, `OrderExecServer`, `BasicDataServer`) rather than by a
Java process, and several of them run per node publishing the same counter, so `member` alone would
collapse them into one repeated series. The C++ half of the registry is
`org/limitless/phixeron/util/PhixeronCounters.hpp`, which must be kept in step with the Java one.

| Metric | Type | Meaning |
|---|---|---|
| `phixeron_node_up` | gauge | Synthesized by the aggregator, not read from a counter: 1 if its last scrape of that node's exporter succeeded, else 0 |
| `phixeron_sequencer_global_seq_no` | gauge | Last globalSeqNo emitted on this node's tap |
| `phixeron_sequencer_tap_backpressure_alerts_total` | counter | Count of times the tap-emit back-pressure alert threshold has fired |
| `phixeron_sequencer_rejected_ingress_total` | counter | Count of malformed ingress messages skipped by `Sequencer.sequenceMessage` |
| `phixeron_sequencer_leadership_change_total` | counter | Count of leadership changes this node has observed and sequenced |
| `phixeron_sequencer_current_leader_member_id` | gauge | memberId of the leader last recorded by this node's Sequencer |
| `phixeron_sequencer_last_tick_timestamp_ms` | gauge | Consensus timestamp of the last 1Hz ClusterHeartbeat emitted |
| `phixeron_sequencer_gateway_promotion_total` | counter | Count of standby-promotion GatewayActive frames emitted — on a gateway session close, or on a designated instance failing to publish `GatewayStarted` within 60s of being named |
| `phixeron_sequencer_bootstrap_activated` | gauge | 1 once the bootstrap GatewayActive has been emitted for the trading day, else 0 |
| `phixeron_sequencer_tap_stalled` | gauge | 1 while the tap recording has made no progress for longer than the stall threshold (2s) under back-pressure, else 0. Latches at 1 when the node terminates for an unrecordable tap — see below |
| `phixeron_replayer_stalled` | gauge | 1 while the local archive is refusing to serve a replay, else 0. Set from every path that asks the archive for one — the startup self-check included — and cleared by a bounded probe replay the node runs itself once a second while stalled, so it reads 0 again even on a Replayer no app is asking for history |
| `phixeron_replayer_ready` | gauge | 1 once the co-located tap recording is visible and replay requests are being served |
| `phixeron_replayer_active_slots` | gauge | Current count of in-flight replays |
| `phixeron_replayer_pending_requests` | gauge | Current count of replay requests waiting for a free slot |
| `phixeron_replayer_replays_served_total` | counter | Count of replays started since this node came up |
| `phixeron_replayer_idle_ttl_reclaimed_total` | counter | Count of replay slots reclaimed by the idle-TTL backstop |
| `phixeron_replayer_integrity_failure` | gauge | 1 once a tap recording failed the startup gseq-1 integrity check, else 0. Every recording in the node's chain is checked, not just the oldest: one that begins above 1 resumed mid-history, which is a hole at its join. Latched: `ready` never becomes 1 again for that process |
| `phixeron_replayer_control_replies_dropped_total` | counter | Count of control replies dropped rather than spun on because an app stopped draining the control stream. Each costs that app one resend interval, so the **rate** identifies a wedged replica — the absolute value does not |
| `phixeron_replayer_client_id_collision` | gauge | 1 once two co-located apps were seen sharing one `PHIXERON_REPLAYER_CLIENT_ID`, else 0. They stop each other's replays and neither catches up until the launch configuration is corrected |
| `phixeron_app_recovery_stalled` | gauge | 1 while this replica's recovery has dispatched nothing for 30s while not caught up, else 0. Also labelled `client`. It is holding, which is correct and safe — but it is not serving, and nothing else says so: the causes are a `ReplayUnavailable` refusal, a Replayer that never answers, and a hole this node's recording chain cannot cover. The replica's own fault line names which |

## A node that terminates itself

`SequencerServer` exits **70** when its local archive stops recording the node's tap (stalled with no
progress for 30s under back-pressure, or the recording gone outright). This is deliberate, not a crash:
that node's archive is its copy of the sequenced history, so one that cannot record can only accumulate
silent holes in it. What to expect and what to do:

- **In the log:** a `[SequencerService/N] FATAL: … terminating this node` line naming the reason, and
  `phixeron_sequencer_tap_stalled{member="N"}` at 1 until the process (and its counters) go away.
  `phixeron_node_up{member="N"}` then drops to 0.
- **The cluster keeps going** on the remaining members — every node holds an identical, complete
  recording, so nothing is lost with the node itself, and an election moves leadership if it held it.
  **Two nodes down is a quorum loss**, so treat a second one as an emergency rather than a repeat.
- **Restart it** once the storage is healthy: recovery is the usual full-log replay from `globalSeqNo`
  1, which rebuilds the node's tap recording from scratch. Under process supervision this is automatic
  — exit 70 vs the 0 of an orderly `clusterctl shutdown` is exactly the "restart me" signal.
- **If it exits 70 immediately on restart**, the archive is still broken (the same check bounds
  start-up: the recording must go live within 5s). Fix the storage before restarting again.

## A gateway that terminates itself

`ExchangeGateway` exits **70** on the same principle and for the same reason — see
`doc/fault-tolerance.md` §2.5 for the four fences. It is not a cluster member, so nothing here is a
quorum question, but it *is* the venue leg: while it is down, nothing reaches the exchange.

- **In the log:** a `[ExchangeGateway/N] FATAL: …` line naming the fence that fired — a lost cluster
  session, a tap that stopped delivering `ClusterHeartbeat`s, a recovery that stopped converging, or one outbound
  frame back-pressured past 20 s.
- **The passive instance takes over on its own** if one is running: the fences deliberately make this
  look to the cluster like the process dying, which is what the sequencer promotes a standby on. The
  handover is a fresh dial to the venue, not a live session moving, so expect a new logon.
- **Restart it** under supervision: exit 70 is the "restart me" signal, and recovery is the usual
  full-log replay. A restarted instance comes back as a standby and is promoted only when named.
- **A tap stall usually means the co-located node is the problem, not the gateway** — check whether
  that member's `SequencerServer` self-terminated first (above); they share the tap.
- **No metrics yet.** The gateway publishes no `PhixeronCounters`, so it is absent from `/metrics`
  entirely — the log is the only signal. See below.

## A venue that will not accept the logon

`ExchangeGateway` keeps running for this one — the gateway is healthy, the venue is the problem — so
there is no exit code and no failover to wait for. The signal is a single log line:

```
[ExchangeGateway/N] the venue has refused this session's logon 3 times running (LOGOUT) — this gateway
will go on retrying every 300000ms, …
```

structured as `VenueLogonRefused` at `Error`, and emitted **once per run of refusals** (the retries
themselves log at `Warn`). It means the socket came up and the logon never completed, three times in a
row — long enough that the one cause which clears by itself, a venue still holding the previous
instance's socket after a promotion, has been ruled out.

- **What it does not tell you is why**, and that is not a gap in the logging: bad credentials, a comp-id
  the venue does not know, a `MsgSeqNum` the venue disagrees with and a venue that is simply closed are
  the same hang-up on the wire. The bracketed `DisconnectReason` narrows it a little — `LOGOUT` means the
  venue answered before hanging up, anything else means it just dropped the socket.
- **Check the closed case first**, since it is the only one that needs nothing done: the gateway retries
  every 5 minutes indefinitely and will come up on its own when the venue opens. The notification is not
  repeated, so a session that recovers leaves one `Error` line behind and nothing else.
- **Otherwise it needs a human, and not a restart** — the state that is being refused is in the
  replicated log, so a restarted gateway replays straight back into the same refusal. Compare what the
  venue expects against what the log holds (`SbeLogPrinter`), and see `doc/todo.md`, "A venue that
  disagrees with the log is never reconciled with".
- **`phixeron_exchange_gateway_*`: nothing.** As above, the gateway publishes no counters, so this
  cannot be alerted on from Prometheus today — it is a log-scrape signal.

## A gateway that never dials at all

Distinct from the above, and quieter, because there is no venue in it: an instance that caught up and
then sat there. The line to look for is

```
[ExchangeGateway/N] the whole log holds no BasicDataSession row owned by gatewaySourceId=5 …
```

also `VenueSessionError` at `Error`, and emitted once, at catch-up. The gateway's comp-ids are reference
data — the one session row its `gatewaySourceId` owns — so an instance without that row has nothing to log
on as and fails closed rather than guessing. It looks exactly like an ordinary standby otherwise: caught
up, holding its cluster session, answering the keep-alive, and it will accept a `GatewayActive` and still
not dial. Fix it in reference data (`BasicDataConstants.hpp`), reload, and no restart is needed — the row
resolves off the tap like any other frame. The sibling line, `a second venue session … is owned by
gatewaySourceId=N`, is the opposite mistake and is not fatal: the gateway stays on the first row and
refuses the second, because one venue session is all this build serves (`doc/design.md` known gap 12).

## Non-goals / open items

- No authentication on either `/metrics` endpoint — see "Shape" above; both are meant to sit behind
  the same network boundary as the nodes.
- No Prometheus alerting rules or Grafana alert provisioning — dashboard only.
- Single aggregator instance; no HA (a downed aggregator is a downed Prometheus scrape target, not a
  downed node — `phixeron_node_up` simply stops updating rather than misreporting).
- `metricsAggregator.targets` is a fixed, hand-maintained list — no service discovery. For a cluster
  whose membership changes, keep it in sync with `clusterMembers`.
- **Neither FIX gateway is instrumented.** `PhixeronCounters` covers `SequencerService`/
  `ReplayerService` only, so `FixGateway` and `ExchangeGateway` contribute nothing to `/metrics`:
  no session state, no fence counters, nothing per-connection. The nearest signal is second-hand and
  sequencer-side — `phixeron_sequencer_gateway_promotion_total` says that a promotion happened, not how
  either gateway is doing. Monitoring the edges themselves means reading their logs.
