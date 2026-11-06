# Offloaded snapshots (proposed)

A proposal; none of it is implemented. [`snapshot.md`](snapshot.md) describes what is. Section references
without a file name are to this document; `snapshot.md §n` is to that one, and `spec` is
[`seqeron-protocol-spec.md`](seqeron-protocol-spec.md).

Every instance of a participating source serializes on dispatching `SnapshotStarted`, and its dispatch waits
for the encoding, the CRC and the page-cache write (snapshot.md §4 step 1, §12). On the instance that may
publish, that wait delays the source's output, and it grows with the state. Under this proposal that instance
serializes nothing. The source's other instances do — the replicas on the followers, a gateway's hot standby,
and optionally instances on a snapshot host (§3) — and it fetches the file one of them wrote.

What it changes:

- **The publishing instance never pauses for a round.** Its cost is a fetch on its store thread.
- **Two producers check each other.** On three members the followers' replicas both serialize each round and
  are compared against the same `SnapshotEnd` (A-7).
- **An instance with no usable file fetches one from a peer**, in every mode, where today an operator copies it
  in (snapshot.md §10.8, "A replica that is down").

The cut is unchanged: a round is still Aeron's `SNAPSHOT` action, taken on every member, the leader included
(snapshot.md §10.8). The `Sequencer`'s state is small, and its write is the only hold on producers.

## 1 Modes

`snapshot` on a topology row (snapshot.md §1) takes a third value:

| value | the instance that may publish at `R` | every other instance |
| --- | --- | --- |
| `true` | serializes and submits the end (snapshot.md §4) | serializes and compares |
| `offload` | fetches the file (§2) | serializes, submits its end and compares (§2) |

`GatewayRegistered.snapshot` and `ApplicationRegistered.snapshot` carry 2 for `offload`; builds that do not
know the value are not mixed with ones that do (spec V-3). The loader requires one value on every row of a
gateway `sourceId`, as it does today, and `snapshot="false"` stays refused with `<snapshots>` (snapshot.md
§10.6).

`offload` needs an instance that does not publish at the cut: for an application, a cluster of two or more
members; for a gateway, a running standby. A snapshot host (§3) provides one for either.

## 2 A round

**Producers.** Every instance of an `offload` source that may not publish at `R`, on dispatching
`SnapshotStarted`:

1. **Serializes** as snapshot.md §4 step 1. The pause is harmless: it publishes nothing, the queue absorbs what
   arrives behind it, and an instance dropped from the live feed heals by replay.
2. **Forces the file to disk, then submits `SnapshotEnd`.** Unlike snapshot.md §4 step 2, every producer
   submits, unless it has already dispatched another producer's end for the round, and only once its `.tmp` is
   durable, so a sequenced end implies a durable file on the producer's node. A producer places the end of
   every round it serialized and abandons none on a later `SnapshotStarted`; the sequencer accepts a late end
   (snapshot.md §3).
3. **Compares and confirms** as snapshot.md §4 step 4 and §5, against the round's first sequenced end. A
   producer whose figures differ is fenced with `SNAPSHOT_DIVERGED` (A-7). Later ends for the round change
   nothing.

**The publisher.** The instance that may publish at `R` — an `Application` whose gate is open, or the active
`Gateway` once it has placed its `GatewayStarted` (snapshot.md §4 step 2):

1. **Joins** the round (snapshot.md §10.8) and serializes nothing. Dispatch continues.
2. **Fetches** on dispatching the round's first end: its Replayer fetches the file from a peer (§5), and the
   instance writes the records through its store (snapshot.md §4.1), checks their count, length and CRC
   against that end, forces and renames the file, and sends `SnapshotConfirmed`.
3. **Refuses a file that fails the check**, and the Replayer asks the next peer. With none left the instance
   keeps its previous files, and the round stays unsafe on its node (§4).

Each instance decides its role from its own state; nothing needs to agree. A publisher that loses the role
after `R` still fetches; one that gains it after `R` has already serialized. An instance catching up through
old rounds is not publishing, so it serializes them, as snapshot.md §12 says.

The publisher's file is compared with nothing while it publishes. It is compared again at the first round
after it loses the role.

## 3 The snapshot host

A gateway host ([`fault-tolerance.md`](fault-tolerance.md) §3.3) whose instances are producers only. Both
façades gain a mode, `snapshotOnly`: the instance follows its source's queues, restores and recovers as any
instance does, serializes at every round, and publishes nothing but its `SnapshotEnd`. It sends no
`GatewayStarted`, opens no connection and has no gate. It holds a cluster session for the end. A hosted
client's `ClientProxy` takes the same mode.

It adds:

- **Producers that share no host with a member**, where serializing a large state would compete with the
  member's archive (§6).
- **A producer for a gateway whose standby is down.**
- **One host for every recovery input**, with Aeron's `ClusterBackup` beside it: the Raft log, the cluster
  snapshots, and every `offload` source's files.

Its relay holds the purge of the member it reads (snapshot.md §10.6). One that falls behind another member's
purge after a failover stops as any gateway host's relay does. Its restart relays from that member's recording
start, and its instances, whose files are then cut below that start, fetch the newest file from a peer (§5).

It runs the application builds the live instances run (A-6).

## 4 Restart-safe

snapshot.md §10.8 stands: a node's cut is restart-safe once every instance on it that joined the round has
confirmed its file, and `restart-safe.log` and the launcher are unchanged. A confirmation means that the
instance holds the round's file durably under its name, whether it serialized the file or fetched it, which
is what a restart needs either way.

**A member adds one condition**: for every `offload` source, an end for the round has been sequenced.
`SequencerService` hands each sequenced `SnapshotEnd` to the Replayer in-process, as it hands cuts, with each
source's mode from its row. Without the condition, a member with no instance of a source would mark the cut
safe at once and purge below it, while that source's producers on other hosts have not reached it. With it,
no member purges below a cut before a durable file of every `offload` source exists (§2 step 2). A gateway
host needs no such condition: its instances confirm only after the end.

**A round in which an `offload` source has no producer** has no end for that source, and is not restart-safe
on any member. Members keep their previous safe cut, and their queues and Raft logs grow until a round
completes. Nothing is lost, and `seqeron_app_snapshot_round` shows the lag ([`ops.md`](ops.md)).

**A producer that stops after its end is sequenced and before it confirms** leaves its own node's cut unsafe, so
that node keeps the history below `R`. Its restart restores its previous file, replays through `R`, serializes
the round again and confirms it against the end. Its durable `.tmp` is deleted at that confirmation
(snapshot.md §4.1). A publisher that asks for the file meanwhile finds no confirmed copy and asks again.

## 5 Fetching a file

Files stay in each instance's directory (snapshot.md §4.1). The Replayers move them between hosts.

- **Serving.** An instance tells its node's Replayer its snapshot directory when it attaches. The Replayer
  serves the confirmed files in it, read-only.
- **Endpoint.** A member's Replayer serves at `base + memberId*10 + 6`, unused in the member's stride, so the
  cluster block keeps its width ([`ops.md`](ops.md), "Ports"). A gateway host or a snapshot host serves only
  with `replayer.snapshotEndpoint`. Without one it can fetch but not serve, so a host running a gateway's
  standby needs one for the active instance to fetch from it.
- **Request.** An instance asks its Replayer for a source's round, or for the newest round that is not cut
  below the node's recording start. The Replayer asks the members' Replayers, from `SEQERON_HOSTS`, and those
  in `replayer.snapshotPeers`, in turn, and moves on from one that has nothing or fails. The serving Replayer
  sends the file's records to the requester, which passes them to the instance over IPC; the instance writes
  them through its store and checks them, against the sequenced end at a round and against the file's trailer
  at a restore.
- **Messages.** The directory report, `SnapshotFetch` and `SnapshotFetched` join `sbe-replay.xml` (spec §10).
  Their ids, and the streams, are allocated in [`ops.md`](ops.md) when this is built. The part that crosses
  hosts runs between Replayers, which are built and released together.
- **Deletion while serving.** An instance deletes its older files after a confirmation (snapshot.md §4.1),
  possibly during a transfer. The Replayer opens a file with delete sharing, which Java NIO does by default on
  Windows, so the delete succeeds; a transfer whose file is deleted ends, and the requester asks the next peer.

A fetch runs in two places:

1. **The publisher, at each round** (§2).
2. **A restore** (snapshot.md §7 step 1): an instance whose newest confirmed file is missing, damaged, or cut
   below its recording's start fetches the newest file a peer holds, before it falls back to a walk. This
   applies in either mode.

A restart reads local files whenever it has a usable one. A node whose peers are all unreachable restores as it
does today.

## 6 Costs

- **The publisher** pays a fetch per round on its store thread: the file's bytes over the network and onto its
  disk.
- **Producers on members.** A cluster's followers serialize at almost the same moment, and their archives hold
  a quorum's copy of the Raft log. A snapshot directory on the device holding the member's archive slows both
  followers' log appends, and with them the commit position. The publisher's fetched file lands on the leader's
  host the same way. On every member, give snapshot directories a device of their own, keep the serializing
  threads on cores apart from the member's agents, or produce on a snapshot host.
- **Log volume**: one `SnapshotEnd` per producer per round, rather than one per source.
- **Ports**: one UDP port per member host, and one per gateway host or snapshot host that serves.
- **Disk**: as snapshot.md §12, on every instance, the publisher included.

## 7 Determinism

snapshot.md §10.8 already rules that a file holds no role. Offloading exposes one case: state an application
changes only while it may publish would differ between the publisher and the producers, and with the
publisher's file no longer compared, nothing would detect it until the publisher lost the role. Before
deployment, the test of snapshot.md §8 runs each `offload` source in both roles over one frame sequence — one
instance that may publish, one that may not — and compares their serializations byte for byte. A source that
needs the runtime check on its publisher stays `true`.

Every instance of a source, a snapshot host's included, runs one build (A-6).

## 8 What changes elsewhere

| where | change |
| --- | --- |
| spec S-6 case 2 | exempts `SnapshotEnd`: a producer's session is never bound (a standby, a snapshot host). The sequencer acts on no `SnapshotEnd` (snapshot.md §3), so admitting one from any session changes no state |
| spec §6.4, topology schema, loader | `snapshot="offload"`; the registered rows carry 2 |
| snapshot.md §1, §10.2, `Sequencer` | the `Sequencer` decodes each row's `snapshot`, which it does not today, for the member's condition (§4); the modes join its snapshot's contents and `SequencerSnapshotTest` |
| spec §10, `sbe-replay.xml` | the directory report, `SnapshotFetch`, `SnapshotFetched` |
| snapshot.md §4 | the two roles of §2 |
| snapshot.md §7 step 1 | a fetch before a walk |
| snapshot.md §10.8 | confirmation by fetch; the member's added condition (§4) |
| snapshot.md §12 | the costs of §6 |
| façades, Java and C# | the `offload` roles, `snapshotOnly`, and fetched records written through `SnapshotStore`; C++ through `ClientProxy` |
| [`client-api.md`](client-api.md) | `snapshotOnly` and the mode, in every column |
| `PortLayout` (Java, C++, C#), `ports.sh`, [`ops.md`](ops.md) | the `+ 6` port, `replayer.snapshotEndpoint`, `replayer.snapshotPeers` |
| scripts | `start-gateway-host.sh` starts a snapshot host |
| tests | `ClusterSnapshotTest`: a round whose publisher fetches; two producers that diverge; a member with no instance of an `offload` source keeping its purge floor until the end; a gateway with its standby down. `ReplayerRecoveryTest`, in every language: a restore that fetches from a peer |

## 9 Open questions

1. **Whether `true` stays.** It is the only mode for a single-member cluster with no snapshot host, and the only
   runtime check on a publisher's state.
2. **The transport.** Replayer to Replayer, as in §5, keeps the files and the store as they are. The
   alternative is to record producers' files in an Aeron Archive and copy them with `replicate`, which brings
   fragment checksums and archive-managed transfer, but moves storage out of the client tier in every
   language.
3. **Exempting `SnapshotEnd` from S-6, or binding producers' sessions** with a registration of their own, which
   would let the sequencer refuse an end from a session that is not a producer.
