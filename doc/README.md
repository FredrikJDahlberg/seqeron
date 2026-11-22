# seqeron documentation

Each folder holds one kind of document. Everything outside `records/` describes the code as it is; `records/` holds
dated plans, reviews and measurements, which describe the code as it was when they were written.

## Evaluating seqeron: read in this order

1. [`../README.md`](../README.md): what seqeron is, its features and its limitations.
2. [`architecture/overview.md`](architecture/overview.md): the design, the life of a message, the failure model and
   the limits.
3. [`architecture/fault-tolerance.md`](architecture/fault-tolerance.md) §0–§2: recovery, leader failover, gateways.
4. [`performance/ec2-results.md`](performance/ec2-results.md) §1: what a three-member cluster sustains, and the
   sizing rule.
5. [`architecture/snapshot.md`](architecture/snapshot.md), Overview and §10: what bounds a restart and the archive.
6. [`architecture/upgrades.md`](architecture/upgrades.md) §1–§2: how seqeron and its applications change versions.
7. [`clients/client-api.md`](clients/client-api.md), up to "Façades": the model, guarantees and contract an
   application team programs against.

The architecture deck, [`presentation/seqeron-architecture.pptx`](presentation/seqeron-architecture.pptx), covers
the same ground in about 30 minutes.

## [`architecture/`](architecture/): how it works

| document | contents |
| --- | --- |
| [`overview.md`](architecture/overview.md) | the design: each part, what it does and why, the life of a message, failures, limits |
| [`fault-tolerance.md`](architecture/fault-tolerance.md) | each failure and how each part recovers from it |
| [`snapshot.md`](architecture/snapshot.md) | application snapshots, and cluster snapshots under a `<snapshots>` policy |
| [`upgrades.md`](architecture/upgrades.md) | the versioning policy that holds today; §6 says which parts of the proposed upgrade model are built |
| [`seqeron-protocol-spec.md`](architecture/seqeron-protocol-spec.md) | normative: frames, system messages, replay protocol, topology document, hosted façade protocol. Look things up here rather than read it through |
| [`versioning-prior-art.md`](architecture/versioning-prior-art.md) | background to `upgrades.md`: how exchanges and log-based systems version their protocols |

## [`clients/`](clients/): building on it

| document | contents |
| --- | --- |
| [`getting-started.md`](clients/getting-started.md) | a one-member cluster from a release, a consumer and a producer, in Java and C# |
| [`client-api.md`](clients/client-api.md) | the client tier's API in Java, C++ and C#, and what in it is not API |

The examples are in [`../seqeron-examples`](../seqeron-examples/README.md); the API reference is at
[fredrikjdahlberg.github.io/seqeron](https://fredrikjdahlberg.github.io/seqeron/).

## [`operations/`](operations/): running it

| document | contents |
| --- | --- |
| [`running-a-cluster.md`](operations/running-a-cluster.md) | the processes, one member or three, ports, properties, restart, the scripts |
| [`clusterctl.md`](operations/clusterctl.md) | the operator tool: start, shutdown, activate, load-topology, snapshots, trim-log |
| [`ops.md`](operations/ops.md) | metrics, ports, client ids, timeouts, term lengths, and what to do when a member or gateway stops itself |
| [`log-printer.md`](operations/log-printer.md) | reading an archive as JSON |

## [`performance/`](performance/): measurements

| document | contents |
| --- | --- |
| [`ec2-results.md`](performance/ec2-results.md) | the current measurements: saturation, latency, cold replay, disk |
| [`ec2-tests.md`](performance/ec2-tests.md) | the tests that need separate hosts: what each answers, its cost, how to run it |
| [`account.md`](performance/account.md) | the AWS account those tests run in |

[`presentation/seqeron-architecture-benchmarks-2026-11-14.pptx`](presentation/seqeron-architecture-benchmarks-2026-11-14.pptx)
presents the measurements of 11 and 13 November.

## [`development/`](development/): working on seqeron

| document | contents |
| --- | --- |
| [`building.md`](development/building.md) | each language's build, the unit, integration and system tests, the examples |
| [`releasing.md`](development/releasing.md) | cutting a release |

## [`records/`](records/): dated, not current

| record | status |
| --- | --- |
| [`plans/disk-load-plan.md`](records/plans/disk-load-plan.md) | step 2a done (queues derived from the log), 3 and 4 in, 2b retired, 5 not implemented |
| [`plans/queue-off-duty-cycle-plan.md`](records/plans/queue-off-duty-cycle-plan.md) | steps 1–5 implemented |
| [`plans/snapshot-offload.md`](records/plans/snapshot-offload.md) | proposed; nothing implemented |
| [`plans/upgrade-model.md`](records/plans/upgrade-model.md) | proposed: specified snapshot records, upcasters, an enforced rollout order, moving consumers to a new source, adding groups; `architecture/upgrades.md` §6 tracks what is built |
| [`reviews/enterprise-review.md`](records/reviews/enterprise-review.md) | 2026-11-15, v0.18.0; issues 4 and 6 answered by the queue-off-duty-cycle plan |
| [`reviews/service-review.md`](records/reviews/service-review.md) | 2026-11-13, v0.18.0; every finding fixed or settled |
| [`reviews/snapshot-review.md`](records/reviews/snapshot-review.md) | 2026-10-24 |
| [`hosted-facade.md`](records/hosted-facade.md) | the hosted façades' design record and measurements; §5, moving the C# client to the stub, is proposed |
| [`ec2-results-2026-11-08-recorded-queues.md`](records/ec2-results-2026-11-08-recorded-queues.md) | measurements of the retired design, in which members recorded their queues |
| [`diagrams/`](records/diagrams/) | the replay sequence as drawn for the retired direct C++ client |
