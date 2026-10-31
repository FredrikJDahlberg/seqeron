# The snapshot server (proposed)

A proposal, moved out of [`snapshot.md`](snapshot.md), which describes what is implemented. Section
references without a file name are to this document; `snapshot.md §n` is to that one, and `spec` is
[`seqeron-protocol-spec.md`](seqeron-protocol-spec.md).

Each instance keeps its snapshots in a directory of its own (snapshot.md §4.1), and an instance with no file replays
from `clusterSeqNo` 1. This is the proposed design that replaces the directory
with a snapshot server per host; none of it is implemented. It was written when each Replayer confirmed
snapshots from an index of the log's ends. That index, `SnapshotQuery` and the per-source round counter that
§2 and §4 rely on no longer exist (snapshot.md §5): how the server learns that an instance confirmed a round is open,
and §2, §4 and §5 are to be revised for it before any of this is built.

What it changes: a damaged snapshot falls back to an older round rather than stopping the instance, and a host with no snapshot of a source fetches one from a
peer. Storage leaves the client tier. The façades stream records to the server and read them back, in
every language, and only the server, in Java, writes to disk.

## 1 The process

`SnapshotServer` (service tier) runs on every member host and every gateway host, in a process of its own.
It attaches to the host's media driver, the member's embedded driver or the gateway host's
`ReplayerServer` driver, as the host's clients do, and runs two things on it:

- **An Aeron Archive of its own**, beside the member's: its own directory (`snapshot.archiveDir`, default
  `<base>/snapshot-archive-<nodeId>`, best on a device of its own), its own `archiveId` (Aeron's default is
  unique per driver) and its own local control stream. Not the member's archive: that one also records the
  Raft log and the queue, a durability setting there would fsync the sequencing path, and a large snapshot
  would share the recorder thread with the queue, whose recording making no progress under back-pressure for the
  stall timeout terminates the member ([`ops.md`](ops.md), "A node that terminates itself").
- **The agent**, `SnapshotService`: which recordings are confirmed, retention, restores and peer fetches,
  none of which the archive decides.

A process of its own because a member must never fail because of a snapshot. It ends when its driver goes
away, as every client on the host does, and is restarted with the member.

| archive setting | value | |
| --- | --- | --- |
| `fileSyncLevel` | 1 | data is forced after each block written, at most 1 MiB (`fileIoMaxLength`); segment files are preallocated, so data is all that changes |
| `catalogFileSyncLevel` | 1 | a recording's stop position survives a crash |
| `recordChecksum`, `replayChecksum` | `Checksums.crc32c()` | every fragment is checked when it is read back |
| `controlChannel` | member host: `aeron:udp?endpoint=<host>:<base + memberId*10 + 6>`; gateway host: `snapshot.endpoint`, or none | what peers replicate from (§5) |

`base + memberId*10 + 6` is unused in the member's stride, so the cluster block keeps its width
([`ops.md`](ops.md), "Ports").

## 2 Streams and messages

On `aeron:ipc`, on the host's driver:

| stream | carries |
| --- | --- |
| 208 | the snapshot archive's local control |
| 209 | requests, client → server |
| 210 | responses, server → client |
| 211 | one round's records: an exclusive publication per instance and round |
| 212 | one restore's records, replayed from the snapshot archive |

Three messages join the replay control protocol (`sbe-replay.xml`, schema 212, spec snapshot.md §10):

| message | id | direction | fields |
| --- | --- | --- | --- |
| `SnapshotOpen` | 25 | client → server | `clientId`, `requestId`, `sourceId`, `round`, `sessionId` |
| `SnapshotFetch` | 26 | client → server | `clientId`, `requestId`, `sourceId` |
| `SnapshotFetched` | 27 | server → client | `clientId`, `requestId`, `round`, `asOfClusterSeqNo`, `asOfPosition`, `formatVersion`, `recordCount`, `length`, `crc32c`, `replaySessionId`; `round` = −1 for none |

`clientId` is the instance's Replayer client id, so one id names an instance to both servers. The server
asks the Replayer for ends with `SnapshotQuery` (snapshot.md §5), under a client id of its own; clients no longer do.

## 3 Taking a snapshot

snapshot.md §4 holds, except where the records go:

1. **Open.** On `SnapshotStarted`, the façade adds an exclusive publication on stream 211, sends
   `SnapshotOpen` with its session id, and waits for the publication to connect. The server records
   `aeron:ipc?alias=seqeron-snapshot.<sourceId>.<round>.|session-id=<sessionId>`, which only that
   publication matches, so a connected publication is one the archive records. The alias names the
   recording in the catalog. It ends in a dot because a catalog search matches a substring of the channel:
   `seqeron-snapshot.3.4.` finds round 4 of source 3 and not round 41.
2. **Stream.** Each record is one message, offered as the listener produces it and retried on
   back-pressure with the session kept alive, then the trailer of snapshot.md §4.1 as the last message, and the
   publication is closed. A record of at most 65,535 bytes is within the IPC message limit, an eighth of the
   term length. The façade still keeps the count, length and CRC, publishes the end if it may, and compares
   (A-7).
3. **Fail open.** A publication that does not connect within `SNAPSHOT_OPEN_TIMEOUT_MS` (proposed 1,000), or
   that loses its subscriber mid-round, stores nothing for the round. The instance still compares, the
   server keeps what it had, and a counter records it.

Dispatch pauses for the serialization and the IPC copy; the server forces the data behind it. A snapshot
larger than the publication window, half the term length, proceeds at the archive's write rate.

## 4 Confirmation and retention

The server confirms a recording when it has stopped, its last message is a trailer naming its round, and the
trailer's `recordCount`, `length` and `crc32c` equal the source's sequenced end. The trailer's figures are
the writing instance's, which stops if they differ from the end (A-7), and the fragment checksums cover the
bytes from the publication to the disk, so the server reads only the trailer: a bounded replay of the last
message. Data is forced block by block as it is recorded, so a confirmed recording is durable.

The server learns of a source's new rounds from the Replayer's per-source round counter (snapshot.md §5) on the shared
driver, and asks `SnapshotQuery` for each round it holds. It stores no confirmation: on start it confirms
what its catalog holds the same way. An answer of none is not final, since the Replayer may still be
indexing (snapshot.md §5), so the server asks again and deletes nothing on it.

Per source, the server keeps:

- the two newest confirmed rounds, so a newest one that fails its read at a restore (§6) has a fallback;
- any recording of a newer round, whose end may not be in the log yet.

It purges the rest (`purgeRecording`), and only once a newer round is confirmed, so a write that fails or a
crash that tears one never costs the last good snapshot. Two instances of a source on one host record the
same round twice; the first confirmed is kept.

## 5 Fetching from a peer

A server fetches a round it has no confirmed recording of, for a source a client has opened or fetched:

- **in the background**, when the Replayer indexes the source's newest round and no local recording of it
  is confirmed within `SNAPSHOT_FETCH_DELAY_MS` (proposed 5,000): a write that failed, an instance that was
  down at the cut;
- **on a `SnapshotFetch`** it has nothing confirmed for, before answering.

Its peers are the member hosts' servers, from `SEQERON_HOSTS` and the port above, and those named in
`snapshot.peers`. It lists a peer's catalog with `listRecordingsForUri` for the round's alias on stream
211, replicates the newest match into its own archive with `replicate`, and confirms the copy as its own
(§4); the copy keeps the original channel, so its alias finds it. It asks the peers in turn, and moves
on from one that has none or fails.

Replication is pulled by the destination archive, which must reach the source's control channel. A gateway
host's server is therefore a peer only with a `snapshot.endpoint`, and a source whose instances all run on
gateway hosts needs one there. The source archive replays to an ephemeral port on the fetching host, as a
member's archive does to a gateway host's relay.

## 6 Restore

snapshot.md §7 steps 1 to 3 become one request:

1. **Ask.** The client sends `SnapshotFetch` for its source, resent until answered. The server takes its
   newest confirmed round and reads the whole recording once, fragment checksums on and the records checked
   against the trailer. A recording that fails is purged and counted, and the next older confirmed round is
   tried. With none, after a fetch (§5), it answers `round` −1 and the client walks from `clusterSeqNo` 1.
2. **Check the format**, as snapshot.md §7 step 2.
3. **Read the records** from the replay on stream 212 named by `replaySessionId`, as snapshot.md §7 step 4. A replay
   that ends early, because the server stopped, fences the instance with `SNAPSHOT_UNRESTORABLE`, since the
   listener holds part of the state; a restart retries it.
4. **Resume** and **go live**, as snapshot.md §7 steps 5 and 6.

A server that does not answer holds the restore under the recovery-stall timeout (spec snapshot.md §10.1), after which
the instance is fenced, as for an unavailable Replayer. A walk from `clusterSeqNo` 1 would be correct, but
can take far longer than a gateway's activation deadline.

## 7 What changes elsewhere

| where | change |
| --- | --- |
| snapshot.md §4 step 1, snapshot.md §4.1 | records go to the server; `snapshotDirectory` and `SnapshotStore`'s files go, in every language |
| snapshot.md §5 | the server learns of a confirmed round (open, above) |
| snapshot.md §7 | steps 1 to 3 as §6 |
| snapshot.md §10.6 | an instance with no snapshot fetches one through its server (§5) |
| snapshot.md §12 | "No file means a full replay" goes; a process and an archive per host, and a UDP port per member host, are added |
| spec snapshot.md §10 | the three messages and streams 208 to 212 |
| [`ops.md`](ops.md) | the port, the server's counters, its process in the runbooks |
| scripts | `start-cluster.sh` and `start-three-node-cluster.sh` start a server per member after READY, `stop-cluster.sh` stops it, and `purgelog.sh` removes its archive with the log's, since a purged log's rounds start again at 1 |
| tests | the agent's unit tests, in Java, replace `SnapshotStoreTest`; the restore cases of `ReplayerRecoveryTest` change in every language; `SnapshotTest` adds a server restart, a round with no server, and a fetch from a peer |
