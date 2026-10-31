# Test harnesses

Most end-to-end harnesses are JUnit 6 tests in `seqeron-service/src/systemTest`, not scripts: `systemTest` launches
members, probes and `clusterctl` as child JVMs off the uber jar (`ClusterProcesses`), and the tests tagged `docker`
(`networkFaultTest`) drive the `docker/compose.yml` cluster and the faulteron injector (`DockerCluster`). A test reads
state from the members' CnC counters or `clusterctl counters`, never from a log line; waits on conditions with
deadlines; kills by the PID or container it started; and gives every boot a log of its own under
`seqeron-service/build/system-test/<test>/`. `doc/building.md` lists the tests.

`seqeron-service/src/test/scripts` holds what is left: three bash harnesses for the other client tiers —
`csharp-client-test.sh`, `csharp-windows-test.sh` (Git Bash on Windows, with its member in WSL1) and
`cpp-client-test.sh` — plus `start-three-node-cluster.sh`, the operator-facing local cluster, and `multihost/`, the
drivers that run a three-member cluster across hosts over ssh for `doc/ec2-tests.md` (`node.sh` is the per-host half;
`MULTIHOST_MEMBERS=localhost,localhost,localhost` rehearses them on one machine).
They still resolve paths relative to the repository root — they were written when this tree sat under `cluster/`, so
check the depth of any `../..` you add.

**The faulteron tests** (github.com/FredrikJDahlberg/faulteron, an eBPF injector in its own repository) are
`PartitionLeaderTest`, `AsymmetricPartitionTest`, `UnrepairedFollowerTest`, `JitteryLeaderTest`, `MuteVoterTest`,
`ThrottledLeaderTest`, `RelayLinkTest` and `SlowArchiveControlTest`: the faults no kill or pause produces, against
the `docker/compose.yml` cluster. The two relay tests add a gateway host, `node-3`, behind the compose file's
`gateway-host` profile: `DockerCluster.startGatewayHost` starts it once a leader is known and orders its archive
endpoints followers first, since the host's producer talks to the leader and a `--peer` rule aimed at the relay's
member must miss it. A member's replay streams are IPC, which faulteron cannot reach; the relay is the only replay
traffic on UDP. They are this repo's because they assert its guarantees and read its probe, counters and topology;
faulteron is only the tool, built from a checkout (`FAULTERON_DIR`, default `../faulteron`) that `failover.yml` pins.
Do not give `FAULTERON_DIR` a relative path on the gradle command line.

**The tests drive the cluster through `tools/ClusterProbe`**, which submits `ProbeMarker` payloads at ingress
(`submit`), round-trips one through consensus and back off the queue (`ping`), replays history through the co-located
Replayer and then follows the queue live (`follow`), or streams through `ClusterStreamSender` and
`sequencer/client/PendingSends` and checks its own queue shows every frame exactly once, in order (`confirm`, which
`FailoverProcessTest` runs across the leader kill, and `GatewayHostTest` runs on a gateway host across the loss of the
relay's member). The probe attaches to a member's own embedded driver, or a gateway host's, so no test needs a
standalone `aeronmd`. `SessionReplaceTest` drives `TestApplication` (below) through the loss of its cluster session,
and fails if a replacement stops its duty cycle.

**The C# and C++ harnesses.** `csharp-client-test.sh` drives the C# client tier: `Seqeron.ClusterProbe`
(`seqeron-client/src/test/csharp-probe`), the C# twin of the probe's `confirm` and `follow`, then the C# examples, its
gateway pair through a handover, `SnapshotApp` restored from a round it confirmed, and last a `follow` that must fail
on a member restarted at a cut. It needs the .NET SDK beside the uber jar, and CI runs it in `chaos.yml`.
`csharp-windows-test.sh` runs the same C# client on Windows, where it is deployed: one member in WSL1 (three elect
without end there) and the C# clients beside a gateway host on Windows, on one `windows-latest` runner (CI's
`windows.yml`). `cpp-client-test.sh` is `csharp-client-test.sh` for the C++ client tier: `cluster_probe`
(`seqeron-client/src/test/cpp-probe`, a target beside `core_tests`, which the harness builds in the examples' tree so
that Aeron is built once), then the C++ examples built as an outside consumer builds them, their gateway pair through
a handover, `snapshot_app` restored from a round it confirmed, and `ClusterStreamClient` replaying a member restarted
at a cut (`cluster_probe scan`), where a `follow` must fail. It needs CMake beside the uber jar, and CI runs it in
`chaos.yml`.

`ChaosTest` needs one more thing the probe cannot supply — a **gateway pair under the faults** — and `TestGateway` is
it: an elected active/standby producer (`GW-T-A`/`GW-T-B`, `gatewaySourceId` 9, listening on 9200/9201) that speaks no
application protocol and holds no session state, but holds the same fences a real gateway does — including the client
tier's recovery-stall fence, which is why `ChaosTest` can drive a non-converging recovery to a handover rather than a
hang. **It is the reference consumer of `app/Gateway`**, the client tier's façade for one instance of an elected
pair: the election (`app/GatewayLifecycle`), the connection id space it resumes from its predecessor, the connection
lifecycle frames, confirmed ingress (`sequencer/client/PendingSends` under an `IngressPublisher`, held by its
`ClusterStreamSender`) and the `app/ClusterError` values are all behind it, so what is left in the harness is a socket
and a line protocol. A media driver that goes away raises from `doWork()` rather than as a fence. It is in
**`seqeron-service/src/test/java`** and therefore in no jar: `systemTest` puts the compiled test classes on the
children's classpath beside the uber jar (`seqeron.testClasses`). Its list is
`seqeron-service/src/test/resources/topology-test-gateway.xml`, whose pair takes part in snapshot rounds
(`-Dprobe.snapshot`, `-Dprobe.snapshotDir`). `ChaosTest` loads it without its `<snapshots>` element, since a member
restarted from a cluster snapshot starts its queue at a cut, which its plain `ClusterProbe follow` consumers cannot
follow; rounds under member kills are `SnapshotTest`'s and `ClusterSnapshotTest`'s. `topology-test-snapshot.xml`
beside it adds **`TestApplication`** (`sourceId` 16, one replica per member), the reference consumer of
`app/Application` with a `SnapshotListener`; `SnapshotTest` loads it and drives both through restores, a failover onto
a restored instance, the killed instance's return, an operator-requested round and a cluster leader kill. The two
other topology documents here are `seqeron-examples/topology.xml`, the pair the Java and C++ `GatewayApp` examples
run, and `topology-csharp.xml` beside it, the C# example's own pair, which both C# harnesses load.
