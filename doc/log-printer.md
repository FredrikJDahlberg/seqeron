# Log printer

Every member's archive holds the complete sequenced history, and its Raft log holds every ingress frame
the cluster accepted. `SbeLogPrinter` reads either as JSON, decoded against the SBE schemas. It is how
an operator inspects what the cluster sequenced, how an engineer debugs a consumer against the frames it
was given, and how an application's own payloads are exported to a decoder that understands them.

It reads an archive directory (`archive.catalog` and the segment files under `archive-<id>`), and works
on a running cluster: a recording still being written is printed up to its current end.

```bash
./seqeron-service/src/main/scripts/sbe-log-printer.sh "${TMPDIR:-/tmp}/seqeron-seq/archive-0" --stream 205 --oneline
```

Gradle runs it too, taking the same options as `-P` properties:

```bash
./gradlew sbeLogPrinter -PlogDir="${TMPDIR:-/tmp}/seqeron-seq/archive-0" -Pstream=205 -Poneline
```

## Schemas

Four schemas ship inside the jar, and all four are loaded by default. Each frame is decoded against the
schema its own header names, so one run reads an archive directory end to end, whatever mix of
recordings it holds.

| name | schema | what it decodes |
| --- | --- | --- |
| `frame` | 210 | the frame envelopes and the system vocabulary |
| `replay` | 212 | the node-local replay control messages |
| `probe` | 214 | `ClusterProbe`'s own payload |
| `cluster` | 111 | the Raft log's session and consensus messages |

```
[Catalog] Recording ID: 0 | Stream ID: 205 | ...    → frames  (schema 210)
[Catalog] Recording ID: 1 | Stream ID: 100 | ...    → cluster (schema 111)
```

| option | effect |
| --- | --- |
| `--schema <name>` | load one schema only; frames of the others print as `<schema N not loaded>` and are skipped |
| `--list-schemas` | print the bundled schema names |
| `--spec <file.sbeir>` | decode against an IR file outside the jar instead, which is how an application's own schema reaches the tool; not combined with `--schema` |

The `cluster` schema mirrors part of `io.aeron.cluster.codecs`: the session messages a cluster client
exchanges, and the entries `io.aeron.cluster.LogPublisher` appends to the Raft log (`TimerEvent`,
`SessionOpenEvent`, `SessionCloseEvent`, `ClusterActionRequest`, `NewLeadershipTermEvent`). A frame whose
template it does not define prints as `<not in schema>` with its template id, and the scan continues. That
is what an entry added by a later Aeron version looks like.

## Selecting a recording

An archive directory holds several recordings, and by default the printer prints them all. Stream 100 is
the Raft log. Each recording on stream 205 is one generation of the tap: a member that restarts replays
its whole log onto a new tap recording, which starts again at `globalSeqNo` 1, so every earlier
generation is a prefix of the newest.

`--stream 205` prints only the newest recording on the stream: one complete copy of the sequenced
history, with nothing repeated. The selector is a stream rather than a recording id because recording
ids change across restarts. Without `--stream` the older generations are printed as well. The printer
exits non-zero when the stream names no recording.

## Output

Each message is preceded by a line naming it, since the JSON carries field values only, and a message
that is all header, such as `ClusterHeartbeat`, would otherwise be indistinguishable from any other.
`--oneline` prints each message on one line, which suits `grep` and `diff` better than the default
indented form:

```
LeadershipChanged = { "header": { "sourceId": -1, "connectionId": -1, "sessionId": -1, "systemEventType": 5, "globalSeqNo": 1, "timestamp": 1789409115713932000 }, "newLeaderMemberId": 0, "leadershipTermId": 0 }
ClusterHeartbeat = { "header": { "sourceId": -1, "connectionId": -1, "sessionId": -1, "systemEventType": 16, "globalSeqNo": 2, "timestamp": 1789409116715140000 } }
```

The output as a whole is not a JSON document, because the `[Catalog]` and separator lines sit between
the objects. With `--oneline`, each message line parses on its own.

## Piping payloads to another decoder

The cluster decodes no application payload, so the printer cannot either. `-o <payloadId>` hands them to
something that can: it writes that protocol's payloads to standard output, raw and back to back, for a
decoder that holds their schema ([`seqeron-protocol-spec.md`](seqeron-protocol-spec.md) §13.1).

```bash
./seqeron-service/src/main/scripts/sbe-log-printer.sh "${TMPDIR:-/tmp}/seqeron-seq/archive-0" --stream 205 \
    -o 2 2>frames.log | order-decode
```

- **Standard output carries the payloads alone.** Every text line moves to standard error: the
  `[Catalog]` lines, the frame dump and the errors. Redirected as above, the frames sit beside the
  payloads in the same order, and each frame line carries its `globalSeqNo`.
- **The payload stream has no framing of its own.** An SBE payload declares its own block and var-data
  lengths, so the decoder that holds the schema delimits it.
- **It reads the Raft log too.** With `--stream 100` it exports the same payloads as they arrived on
  ingress.
- **Not through Gradle.** Gradle re-encodes a child process's standard output, which corrupts the bytes.
  Use the script or the jar.

## Payloads it cannot decode

A payload whose schema is not loaded prints as its identifiers rather than its fields. Where the topology
document registered the `payloadId` ([`seqeron-protocol-spec.md`](seqeron-protocol-spec.md) §6.3), the
printer labels it with the registered name, read from the `PayloadIdRegistered` rows in the same
recording; an unregistered one prints under its number:

```
<undecodable payload 2 (order v1): schema 220, templateId 1>
<undecodable payload 7: schema 900, templateId 3>
```

Registration only labels. The sequencer never decodes those rows, and they admit or refuse no frame.
