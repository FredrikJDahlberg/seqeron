<picture>
  <source media="(prefers-color-scheme: dark)" srcset="doc/branding/seqeron-wordmark-dark.svg">
  <img src="doc/branding/seqeron-wordmark.svg" alt="seqeron" width="248" height="60">
</picture>

[![CI](https://github.com/FredrikJDahlberg/seqeron/actions/workflows/ci.yml/badge.svg)](https://github.com/FredrikJDahlberg/seqeron/actions/workflows/ci.yml)
[![JitPack](https://jitpack.io/v/FredrikJDahlberg/seqeron.svg)](https://jitpack.io/#FredrikJDahlberg/seqeron)
[![API reference](https://img.shields.io/badge/docs-API%20reference-blue)](https://fredrikjdahlberg.github.io/seqeron/)
[![License](https://img.shields.io/github/license/FredrikJDahlberg/seqeron)](LICENSE)

A replicated sequencer for systems built on the sequencer architecture. Every message from every producer is
assigned one global, gap-free total order by an Aeron Cluster (Raft) state machine, stamped with the
consensus clock, and recorded on every node. Services downstream of it become deterministic state machines
over a single log rather than peers that coordinate with each other: a replica, a hot standby or an audit
process that reads the same log reaches the same state, and a restart is a replay.

The cluster is protocol-agnostic: it carries no FIX, no order flow and no reference data, and copies every
application payload through unopened. Application protocols are defined by the producers and consumers that
speak them.

To run a node and a first consumer and producer, start with [Getting Started](doc/getting-started.md).

Features
--------

- **Sequence numbering and timestamping.** Every frame gets a cluster-wide `globalSeqNo`, advancing by
  exactly one per frame with no gaps or reuse, and the Raft consensus timestamp in nanoseconds. A 1 Hz
  `ClusterHeartbeat` gives every consumer the same clock, which keeps advancing while producers are silent.
- **Replication.** The order is decided by Raft consensus: a message is sequenced once a majority of members
  hold it. Every member then republishes the sequenced stream onto a node-local tap that its own Aeron
  Archive records, so each member holds a complete, identical copy of history without copying it from a peer.
- **Fault tolerance.** The cluster survives the loss of a minority of its members, and a new leader resumes at
  the next `globalSeqNo`. Producers confirm their messages on the stream and resend what a failover lost, so
  each is sequenced exactly once, in order. Edge producers run as active/hot-standby pairs that the cluster
  elects and hands over, and a member that can no longer record its history stops rather than keep a gap.
- **Determinism.** Every decision the cluster makes is a function of the log alone: the order, the clock,
  gateway promotions, connection lifecycle and snapshot rounds are all sequenced frames, byte-identical on
  every member. Any replica that applies the log reaches the same state, with nothing to ask anyone else.
- **Replay.** A client recovers by replaying, never by state transfer. A Replayer on each host serves cold
  starts and gaps from the local recording, while consumers read the live stream untethered, so a slow
  consumer is dropped and heals by replay instead of holding up the cluster.
- **Application snapshots.** A client can restart from a snapshot of its own state rather than replay from
  `globalSeqNo` 1. The sequencer marks each round's cut in the log; every instance writes its state at that
  cut to a local file, and the publishing instance submits a digest of it to the log. Every instance checks
  its file against the digest and stops if it differs, and a restart restores only from a file the log
  confirms.
- **APIs.** Two façades cover the two kinds of producer: `Gateway`, one instance of an elected pair, and
  `Application`, one replica per member that publishes from the leader's. Each assembles the session,
  the stream, confirmed ingress and the failure fences into one duty cycle. Beneath them, the receiver,
  sender and confirmed-ingress classes serve a client that runs its own.
- **Language support.** The cluster is Java. Clients are available for Java, C++ (header-only) and C# (.NET,
  on Aeron.NET), written as ports of one another, with their core state machines tested case for case in
  every language. All three exchange the same frames, encoded with Simple Binary Encoding (SBE).

What it costs
-------------

- **No cluster snapshots.** A node's recovery is a full replay of the Raft log from `globalSeqNo` 1. That
  is what keeps every node's recording complete, but recovery time and archive size grow with uptime; the
  heartbeat alone is about 86,400 frames a day. Application snapshots shorten a client's restart, not a
  node's.
- **At most seven members.** The cluster's port block is 70 ports wide.
- **C# trails by one Aeron version.** Aeron.NET is a release behind Aeron. Spec **V-1** records the
  exception and the evidence that the two interoperate; it is re-verified on every Aeron upgrade.

How do I use seqeron?
---------------------

1. [Getting Started](doc/getting-started.md)
2. [Client API: Java, C++ and C#](doc/client-api.md)
3. [Examples](seqeron-examples/README.md)
4. [API Reference](https://fredrikjdahlberg.github.io/seqeron/)
5. [Running a Cluster](doc/running-a-cluster.md)
6. [Operator Tooling](doc/clusterctl.md)
7. [Monitoring](doc/ops.md)
8. [Log Printer](doc/log-printer.md)

How does seqeron work?
----------------------

1. [Design Overview](doc/overview.md)
2. [Protocol Specification](doc/seqeron-protocol-spec.md)
3. [Fault Tolerance](doc/fault-tolerance.md)
4. [Application Snapshots](doc/snapshot.md)

How do I hack on seqeron?
-------------------------

1. [Building and Testing](doc/building.md)
2. [Releasing](doc/releasing.md)

License (See LICENSE file for full license)
-------------------------------------------

Copyright 2026 Fredrik Dahlberg

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[NOTICE](NOTICE) records the copyright, and §4(d) of the License obliges anyone redistributing seqeron to
carry it forward. Aeron, Agrona and SBE, which the uber jar redistributes, are under the same License, and
both files ship inside the jar under `META-INF/`.
