# Test harnesses

`seqeron-service/src/test/scripts` holds the thirteen harnesses — nine Java-only, plus the containerized
`docker-failover-test.sh`, the two C# ones, `csharp-client-test.sh` and `csharp-windows-test.sh` (Git
Bash on Windows, with its member in WSL1), and the C++ one, `cpp-client-test.sh` — and, in `faulteron/`, the
six network-fault tests that drive the faulteron injector against the docker cluster. The thirteen still resolve
paths relative to the repository root — they were written when this tree sat under `cluster/`, so check the
depth of any `../..` you add.

**Six fault tests drive faulteron** (github.com/FredrikJDahlberg/faulteron, an eBPF injector in its own repository),
in `seqeron-service/src/test/scripts/faulteron` beside a `lib.sh` they share: the faults no kill or pause produces,
against the `docker/compose.yml` cluster. They are this repo's because they assert its guarantees and read its
probe, logs and topology; faulteron is only the tool, built from a checkout (`FAULTERON_DIR`, default `../faulteron`)
that `failover.yml` pins.

**Nine of the thirteen harnesses are Java-only.** Eight drive the cluster through `tools/ClusterProbe`, which
submits `ProbeMarker` payloads at ingress (`submit`), round-trips one through consensus and back off
the queue (`ping`), replays history through the co-located Replayer and then follows the queue live
(`follow`), or streams through `ClusterStreamSender` and `sequencer/client/PendingSends` and checks its own queue shows
every frame exactly once, in order (`confirm`, which `failover-test.sh` runs across the leader kill, and
`gateway-host-test.sh` runs on a gateway host across the loss of the relay's member). The probe attaches to a
member's own embedded driver, or a gateway host's, so six of the eight need no
standalone `aeronmd` at all. The ninth, `session-replace-test.sh`, drives `TestApplication` (below) through the
loss of its cluster session, and fails if a replacement stops its duty cycle. The tenth, `docker-failover-test.sh`,
is the containerized multi-round failover soak (`docker/compose.yml`, `./gradlew operatorDist`, CI's
`failover.yml`). The eleventh,
`csharp-client-test.sh`, drives the C# client tier: `Seqeron.ClusterProbe` (`seqeron-client/src/test/csharp-probe`),
the C# twin of the probe's `confirm` and `follow`, then the C# examples, its gateway pair through a handover.
It needs the .NET SDK beside the uber jar, and CI runs it in `chaos.yml`. The twelfth,
`csharp-windows-test.sh`, runs the same C# client on Windows, where it is deployed: one member in WSL1 (three elect without end there) and
the C# clients beside a gateway host on Windows, on one `windows-latest` runner (CI's `windows.yml`). The
thirteenth, `cpp-client-test.sh`, is `csharp-client-test.sh` for the C++ client tier: `cluster_probe`
(`seqeron-client/src/test/cpp-probe`, a target beside `core_tests`, which the harness builds in the examples' tree
so that Aeron is built once), then the C++ examples
built as an outside consumer builds them, their gateway pair through a handover, and `snapshot_app` restored from a
round it confirmed. It needs CMake beside the uber jar, and CI runs it in `chaos.yml`.
`chaos-runner` needs one more thing the probe cannot supply — a **gateway
pair under the faults** — and `TestGateway` is it: an elected active/standby producer (`GW-T-A`/`GW-T-B`,
`gatewaySourceId` 9, listening on 9200/9201) that speaks no application protocol and holds no session
state, but holds the same fences a real gateway does — including the client tier's recovery-stall
fence, which is why `chaos-runner.sh` can drive a
non-converging recovery to a handover rather than a hang. **It is the reference consumer of `app/Gateway`**,
the client tier's façade for one instance of an elected pair: the election (`app/GatewayLifecycle`), the
connection id space it resumes from its predecessor, the connection lifecycle frames, confirmed ingress
(`sequencer/client/PendingSends` under an `IngressPublisher`, held by its `ClusterStreamSender`) and the `app/ClusterError`
values are all behind it, so what is left in the harness is a socket and a line protocol. A media driver that
goes away raises from `doWork()` rather than as a fence. It is in
**`seqeron-service/src/test/java`** and therefore in no jar: `chaos-runner.sh` and `snapshot-test.sh` put
`seqeron-service/build/classes/java/test` on the classpath beside the uber jar and refuse to start
without it. Its
list is `seqeron-service/src/test/resources/topology-test-gateway.xml`, whose pair takes part in snapshot rounds
(`-Dprobe.snapshot`, `-Dprobe.snapshotDir`) so they run under the faults too. `topology-test-snapshot.xml` beside it adds
**`TestApplication`** (`sourceId` 16, one replica per member), the reference consumer of `app/Application` with a
`SnapshotListener`; `snapshot-test.sh` loads it and drives both through restores, a failover onto a restored instance, the
killed instance's return, an operator-requested round and a cluster leader kill. The two other topology documents here are
`seqeron-examples/topology.xml`, the pair the Java and C++ `GatewayApp` examples run, and `topology-csharp.xml` beside it,
the C# example's own pair, which both C# harnesses load.
