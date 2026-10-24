# Replay test review: what the suites check of the replayer and its client

Reviewed 2026-10-24, at `46169ec` on `main`. "R-n" and "§n" refer to
[`seqeron-protocol-spec.md`](seqeron-protocol-spec.md).

**Scope.** The server (`replayer.server`) and the client's recovery (`replayer.client`): the unit suites in Java, C++
and C#, the integration suite and the scripts. Measured: `./gradlew test` under JaCoCo, unit tests only;
`integrationTest` runs under the JaCoCo agent, but no report is generated from it. Not measured: C++ and C# coverage. The four Aeron adapters (`AeronReplayer`,
`AeronQueueRelay`, `ReplayerStreamReceiver`, `ReplayerServer`) are at 0% by design, so they were judged by what the
integration suite and the scripts assert.

**Verdict.** The pure seams are well covered: `ReplayerService` 413/437 lines, `ReplayerRecovery` 386/390,
`CutLookup` 49/51, `QueueRelay` 70/71, `ReplaySlotAllocator` 41/41. The gaps are where a seam meets Aeron, between the
two seams, and in the C++ port's parity.

## Highest risk

**1. A latched client fault reaching the application is untested in every language.**

- `ReplayerRecovery` latches the first-frame fault, and only `ReplayerStreamReceiver.poll()`
  (`ReplayerStreamReceiver.java:220`) raises it. No unit test, integration test or script covers that line.
- It is the class of `replay-review.md` finding 1: correct at the seam, silent at the adapter.
- Test: a client with no snapshot file, on a member restarted at a cut, whose `poll()` throws. The same check in the
  C++ and C# harnesses.

**2. The slot cap and the backlog throttle are never checked against a real driver.**

- `AeronReplayer.isReplayLive` (`:199`) frees a slot (R-3); `AeronReplayer.queueBacklog` (`:77`) engages the
  one-replay throttle. Both match driver counters, and the unit suite uses `FakeReplayer`'s answers.
- A `queueBacklog` that always returned null, or an `isReplayLive` that always returned false, passes every test: the
  throttle never engages, or every slot frees at once.
- No script reads the `activeSlots`, `pendingRequests` or `throttledRequests` counters.
- Test: five clients against one member; one is answered `ReplayPending` and served once another closes its replay.

**3. No test runs the client against the server.**

- `ReplayerRecoveryPropertyTest` drives the client against a model of the Replayer. The model drops a reply to a
  superseded request (`ReplayerRecoveryPropertyTest.java:339-340`), so a late reply is never exercised under random
  faults; two example cases cover R-1 ordering.
- The model's edge answers are not checked against `ReplayerService`'s, such as `ReplayPending` against
  `NO_REPLAY_NEEDED` at the tip (`ReplayerService.java:684-690`).
- Both are pure Java: a `seqeron-service` test can drive `ReplayerRecovery` against `ReplayerService` and
  `FakeReplayer`, through a test-side bridge, since neither the class nor its transport is public.

**4. The C++ `ReplayerRecoveryTest` is not case for case with Java.**

| port | cases |
| --- | --- |
| Java | 72 |
| C# | 72, same names and order |
| C++ | 69 |

- Missing in C++: "cold start walks the recording from its start", "a live queue frame ahead of an in-flight walk is
  retained", "a queue gap while the walk is still in flight does not supersede it", "retained queue frames hand
  straight over when the replay reaches them". C++ asserts no retained-frame count.
- Merged: Java's two de-dupe cases are one. C++'s one in-flight case covers a resume, not a cold-start walk.
- Split: "a replay image closing at its bound completes the replay; short of it re-requests" is two.
- C++ only: `AConnectionFrameReachesOnSequencedWhenNoLifecycleCallbackIsGiven`.
- Order drifts around Java's cases 32, 38 and 40.

## Medium

**5. Parts of `1bec6fd` are untested.**

- `ReplayerService.java:648-649`, no active recording answered `ReplayPending` and not queued. Its sibling, no tip
  yet, is tested.
- `AeronReplayer.scan`'s 2 s deadline (`:123-153`), connect and delivery halves. It reads `System.currentTimeMillis()`,
  so it cannot be tested without Aeron.
- `FakeReplayer.stallScans()` stalls every scan, so a stalled hint followed by a successful scan from the start is not
  exercised.

**6. The retained FIFO's overflow is tested only with a frame that cannot occur.** `deliverQueueTooBigToRetain`
exceeds the 16 KiB `RetainBlock`; the largest frame is about 8.9 KB. The caps load reaches, 65 536 frames and 16 MiB
(`ReplayerRecovery.java:786`), are hit by neither suite.

**7. A restore's leadership callback is untested** (`ReplayerRecovery.java:653-654`). A leadership handler is never
shown told the snapshot's leader.

**8. Two replayer scripts run in no workflow:** `replayer-restart-test.sh` and `gateway-host-test.sh`. The first is the
only test of a client's own member restarting under it: the client exits on driver loss, and a fresh one recovers from
the new recording.

**9. Two `ReplayerStreamReceiver` decisions were thought untested.** Routing `SnapshotRound` (`:159-171`) is covered
by `ClusterSnapshotTest`'s gateway cases: misrouting it fails them. Dropping a superseded replay's remaining batch
(`:313-318`) cannot be reached in Java or C#: their Aeron clients close the image on the closing thread, and `poll`
stops at a closed image. Only the C++ client closes asynchronously, so only its guard is load-bearing. No end-to-end
test restored a C# client.

## Low

- `ReplayerService`: the archive-error catches at 344, 367-369, 405-407 (a purge that throws, as against one refused),
  720-721 and 770; the `MAX_POSITION_EXCEEDED` offer result; a wrong schema id or an unknown template ignored (468,
  500); `SnapshotJoined` and `SnapshotConfirmed` at a Replayer that tracks no cuts (480, 491).
- `SnapshotStore`: a failed write. Of the structural damage on read, a length past the records is tested (`damagedFiles`'
  `oversized`); see Not gaps for the rest.
- `CutLookup`: line 96, an empty recording held; line 102, the archive throwing on a hint that is no frame boundary.
  `FakeReplayer` answers `UNREADABLE` there instead; the integration `CutLookupTest` covers the archive's behaviour.
- `replayer-restart-test.sh:7` names `ReplayerStreamReceiverTest.cpp`, now `ReplayerRecoveryTest.cpp`.

## Not gaps

- `ReplayerService.java:305`'s other branch is unreachable: `RestartSafety` tracks one round, which `openRound` sets
  together with `inFlightCut`.
- The adapters' 0% unit coverage, by design.
- `SnapshotStore.Reader`'s bound on a length prefix: a prefix past the records puts its record past them too, which the
  record's own bound catches, since the trailer always follows. A record over `MAX_RECORD_LENGTH` cannot be read: the
  prefix is a uint16 and the maximum 65535.
- §14 without a §10 row: §14 is normative and scoped to framing, and a row would oblige a C++ and C# conformance case.

## Status

| finding | test |
| --- | --- |
| 1 | `ClusterSnapshotTest`, a gateway with no snapshot file on a member restarted at a cut; `cpp-client-test.sh` H and `csharp-client-test.sh` H, a `follow` that must fail |
| 2 | `ReplaySlotsTest`: a slot held until its subscriber lets go, each queue's backlog matched to its own recording, and a second replay held while a gateway host's recording trails its queue by more than a quarter of the window |
| 3 | `ReplaySeamTest`: `ReplayerRecovery` against `ReplayerService` and `FakeReplayer`, through `ReplayerRecoveryBridge` |
| 4 | the C++ `ReplayerRecoveryTest`, case for case with Java |
| 5 | `ReplayerServiceTest`, a queue with no active recording and a stalled hint; `tools.CutLookupTest`, a scan past the recording stalls at its deadline and one still growing runs past it; `ScanDeadlineTest`, a replay that never connects |
| 6 | a case per cap in each port's `ReplayerRecoveryTest` |
| 7 | a case in each port's `ReplayerRecoveryTest` |
| 8 | `chaos.yml` jobs `replayer-restart` and `gateway-host` |
| 9 | `ClusterSnapshotTest`, a gateway restored on its member; `cpp-client-test.sh` G, which fails without the C++ guard; `csharp-client-test.sh` G, `SnapshotApp` restored |

| Low | `ReplayerServiceTest` and `QueuePurgeTest`, each `ReplayerService` path listed; each port's `SnapshotStoreTest`, a round whose file cannot be created; `replayer.server.CutLookupTest`, lines 96 and 102; `replayer-restart-test.sh:7` renamed |
