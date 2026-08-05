# Ops — Prometheus/Grafana monitoring stack

Node-local metrics exporter + central aggregating ops server, feeding Prometheus + Grafana. Covers
`org.limitless.phixeron.PhixeronCounters` — every `SequencerService`/`ReplayerService` operator
counter — end to end from a running node to a dashboard panel.

## Shape

Pull, not push, and an **aggregating proxy**, not service discovery:

- Each node runs `metrics-exporter.sh` (`MetricsExporter`), co-located with a `SequencerNode`/
  `ReplayerNode` the same way `clusterctl` is — sharing that node's Aeron directory — and serves
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

Run one per node, co-located with that node's `SequencerNode`/`ReplayerNode` (mirrors
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
metric family — see the reference below.

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

| Metric | Type | Meaning |
|---|---|---|
| `phixeron_node_up` | gauge | Synthesized by the aggregator, not read from a counter: 1 if its last scrape of that node's exporter succeeded, else 0 |
| `phixeron_sequencer_global_seq_no` | gauge | Last globalSeqNo emitted on this node's tap |
| `phixeron_sequencer_tap_backpressure_alerts_total` | counter | Count of times the tap-emit back-pressure alert threshold has fired |
| `phixeron_sequencer_rejected_ingress_total` | counter | Count of malformed ingress messages skipped by `Sequencer.sequenceMessage` |
| `phixeron_sequencer_leadership_change_total` | counter | Count of leadership changes this node has observed and sequenced |
| `phixeron_sequencer_current_leader_member_id` | gauge | memberId of the leader last recorded by this node's Sequencer |
| `phixeron_sequencer_last_tick_timestamp_ms` | gauge | Consensus timestamp of the last 1Hz Tick emitted |
| `phixeron_sequencer_gateway_promotion_total` | counter | Count of standby-promotion GatewayActive frames emitted on a gateway session close |
| `phixeron_sequencer_bootstrap_activated` | gauge | 1 once the bootstrap GatewayActive has been emitted for the trading day, else 0 |
| `phixeron_sequencer_tap_stalled` | gauge | 1 while the tap recording has made no progress for longer than the stall threshold (2s) under back-pressure, else 0. Latches at 1 when the node terminates for an unrecordable tap — see below |
| `phixeron_replayer_stalled` | gauge | 1 while the local archive is unreachable for replay, else 0 |
| `phixeron_replayer_ready` | gauge | 1 once the co-located tap recording is visible and replay requests are being served |
| `phixeron_replayer_active_slots` | gauge | Current count of in-flight replays |
| `phixeron_replayer_pending_requests` | gauge | Current count of replay requests waiting for a free slot |
| `phixeron_replayer_replays_served_total` | counter | Count of replays started since this node came up |
| `phixeron_replayer_idle_ttl_reclaimed_total` | counter | Count of replay slots reclaimed by the idle-TTL backstop |

## A node that terminates itself

`SequencerNode` exits **70** when its local archive stops recording the node's tap (stalled with no
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

## Non-goals / open items

- No authentication on either `/metrics` endpoint — see "Shape" above; both are meant to sit behind
  the same network boundary as the nodes.
- No Prometheus alerting rules or Grafana alert provisioning — dashboard only.
- Single aggregator instance; no HA (a downed aggregator is a downed Prometheus scrape target, not a
  downed node — `phixeron_node_up` simply stops updating rather than misreporting).
- `metricsAggregator.targets` is a fixed, hand-maintained list — no service discovery. For a cluster
  whose membership changes, keep it in sync with `clusterMembers`.
