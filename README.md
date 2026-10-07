<picture>
  <source media="(prefers-color-scheme: dark)" srcset="doc/branding/seqeron-wordmark-dark.svg">
  <img src="doc/branding/seqeron-wordmark.svg" alt="seqeron" width="248" height="60">
</picture>

[![ci](https://github.com/FredrikJDahlberg/seqeron/actions/workflows/ci.yml/badge.svg)](https://github.com/FredrikJDahlberg/seqeron/actions/workflows/ci.yml)
[![windows](https://github.com/FredrikJDahlberg/seqeron/actions/workflows/windows.yml/badge.svg)](https://github.com/FredrikJDahlberg/seqeron/actions/workflows/windows.yml)
[![chaos](https://github.com/FredrikJDahlberg/seqeron/actions/workflows/chaos.yml/badge.svg)](https://github.com/FredrikJDahlberg/seqeron/actions/workflows/chaos.yml)
[![failover](https://github.com/FredrikJDahlberg/seqeron/actions/workflows/failover.yml/badge.svg)](https://github.com/FredrikJDahlberg/seqeron/actions/workflows/failover.yml)
[![JitPack](https://jitpack.io/v/FredrikJDahlberg/seqeron.svg)](https://jitpack.io/#FredrikJDahlberg/seqeron)
[![NuGet](https://img.shields.io/nuget/v/Org.Limitless.Seqeron)](https://www.nuget.org/packages/Org.Limitless.Seqeron)
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

- **Sequence numbering and timestamping.** Every message gets a cluster-wide `globalSeqNo`, advancing by
  exactly one per message with no gaps or reuse, and the Raft consensus timestamp in nanoseconds. A 1 Hz
  `ClusterHeartbeat` gives every consumer the same clock, which keeps advancing while producers are silent.
- **Replication.** The order is decided by Raft consensus: a message is sequenced once a majority of members
  hold it. Every member then republishes the sequenced stream onto a node-local tap that its own Aeron
  Archive records, so each member holds a complete, identical copy of history without copying it from a peer.
- **Fault tolerance.** The cluster survives the loss of a minority of its members, and a new leader resumes at
  the next `globalSeqNo`. Producers confirm their messages on the stream and resend what a failover lost, so
  each is sequenced exactly once, in order. Edge producers run as active/hot-standby pairs that the cluster
  elects and hands over, and a member that can no longer record its history stops rather than keep a gap.
- **Determinism.** Every decision the cluster makes is a function of the log alone: the order, the clock,
  gateway promotions, connection lifecycle and snapshot rounds are all sequenced messages, byte-identical on
  every member. Any replica that applies the log reaches the same state, with nothing to ask anyone else.
- **Replay and snapshots.** A client recovers by replaying the log, never by state transfer. A Replayer on
  each host serves cold starts and gaps from the local recording, while consumers read the live stream
  untethered, so a slow consumer is dropped and heals by replay instead of holding up the cluster. To restart
  from a known point rather than `globalSeqNo` 1, a client takes snapshots of its own state: at each round's
  cut in the log every instance writes its state to a local file, and the log carries a digest every
  instance checks its file against. The API hides all of it: an application supplies only a listener that
  writes and reads its records, and a directory; the façade takes, confirms and restores the snapshots, and
  replays what follows.
- **Independent application upgrades.** The cluster copies payloads through unopened, so an application
  versions its own protocol and upgrades without touching the cluster. Whether its producers and consumers
  must move together depends on its encoding: a format with schema evolution, such as SBE's `sinceVersion`
  or Protocol Buffers, lets old and new builds run side by side. A new build can also start from a snapshot
  and publish a new payload version from there, reading only what was recorded after the cut, provided it
  can read the snapshot. These rules are the application's; seqeron does not yet specify them
  ([Upgrades and Versioning](doc/upgrades.md)).
- **APIs.** Two façades cover the two kinds of producer: `Gateway`, one instance of an elected pair, and
  `Application`, one replica per member that publishes from the leader's. Each assembles the session,
  the stream, confirmed ingress and the failure fences into one duty cycle. Beneath them, the receiver,
  sender and confirmed-ingress classes serve a client that runs its own.
- **Language support.** The cluster is Java. Clients are available for Java, C++ (header-only) and C# (.NET,
  on Aeron.NET), written as ports of one another, with their core state machines tested case for case in
  every language. All three exchange the same messages, encoded with Simple Binary Encoding (SBE).

Limitations
-----------

- **No cluster snapshots.** Cluster node snapshots are not yet implemented
  ([design](doc/snapshot.md#10-cluster-snapshots)). A node's recovery is a full replay of the Raft log from
  `globalSeqNo` 1. That is what keeps every node's recording complete, but recovery time and archive size
  grow with uptime; the heartbeat alone is about 86,400 messages a day. Application snapshots shorten a
  client's restart, not a node's.
- **No authentication or encryption.** Anyone who reaches the cluster's ports can submit messages, operator
  events included, and read or delete recordings. Run it inside a trusted network.
- **seqeron protocol changes are not rolling.** seqeron's own protocol — the message envelope and system
  messages — changes rarely, but a change cannot be rolled out across a running cluster: every member,
  gateway host and client moves together, and the recorded history is purged. seqeron is pre-1.0, and minor releases break the API.
- **One site.** The members belong on one low-latency network; there is no multi-site deployment.
- **No performance figures.** Latency, throughput and capacity have not been measured on production
  hardware, so a deployment is sized by measuring on its own.
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
5. [Upgrades and Versioning](doc/upgrades.md)

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
