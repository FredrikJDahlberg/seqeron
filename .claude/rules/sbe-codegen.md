---
paths:
  - "**/sbe/**"
  - "seqeron-client/src/main/generated/**"
  - "**/build.gradle"
  - "CMakeLists.txt"
---

# SBE code generation

Four schemas, split by who speaks them — `sbe-frame`, `sbe-replay` and `sbe-cluster` under
`seqeron-client/src/main/sbe`, `sbe-probe` under `seqeron-service/src/main/sbe` — each generating into
a distinct namespace so one include path
covers all of them:

- `sbe-frame.xml` (schema 210) — the nine top-level templates, their four header composites, and the
  twelve submitted **system** payloads (the connection lifecycle events, the cluster markers, the gateway
  list/election frames, `GatewayActivationRequested`, `ApplicationRegistered`, the snapshot frames). No system message carries
  a `header` field — the frame's is the only one. Seqeron's own, and the only thing this tier decodes.
- `sbe-replay.xml` (schema 212) — the three **replay control** messages, node-local between a
  `ReplayerService` and its co-located app replicas, never sequenced and never recorded.
- `sbe-probe.xml` (schema 214) — core's own application payload (`payloadId` 5): one message,
  `ProbeMarker`, carrying a submitter-side `seqNo` and variable-length filler. What `tools/ClusterProbe`
  speaks, so the e2e scripts can drive a cluster with no product binary built. A payload rather than a
  system event precisely because the sequencer decodes no `payloadId` at all — a probe frame exercises the
  real copy-through path and needs no change to `sbe-frame.xml`. **Java only**, which keeps the C++
  `generated/sbe/core` root free of any application codec. `payloadId` 5 is private (one publisher, one
  consumer, both the probe), so it needs no `PayloadIdRegistered` row; its `sourceId` is 8
  (`ClusterProbe.PROBE_SOURCE_ID`).
- `sbe-cluster.xml` (schema 111) — trimmed mirror of `io.aeron.cluster.codecs` (SessionConnectRequest,
  SessionEvent, SessionKeepAlive, …), replacing a hand-written `ClusterProtocol.hpp`. Its last section is
  decode-only — the consensus-module log entries (`TimerEvent`, `SessionOpenEvent`, …) that
  `SbeLogPrinter --schema cluster` reads out of a Raft-log recording; the client never sends them. Java
  side is **IR-only** (`generateClusterSbeIr`, no codecs), since the Java side speaks the cluster protocol
  through `io.aeron.cluster.codecs` and the stubs would be dead classes.

A module's `sbe` directory is a codegen input, not a resource one, so no jar ships an XML. The one
file a
jar does need is `seqeron-service/src/main/resources/topology.xsd`.

Every language regenerates independently from the same XML — keep them in sync when editing a schema: the C++
and C# codecs are committed, so a schema edit is `RegenerateSbeCodecs` and `generateCSharp*Sbe` too, then a
commit of what they wrote (`CheckSbeCodecsCurrent` and `checkCSharpSbeCurrent` fail otherwise). SBE
never deletes generated files for messages you removed, so **each schema owns a disjoint output directory
and each codegen step wipes its own before running**; a regeneration is a replacement, and no manual purge
of `cmake-build-*/generated/sbe` or `build/generated/sources/sbe` is needed. Keep that property when
adding a schema: give it its own package/namespace directory, declare only that as the task's output, and
wipe it in the same step. `collectSbeIr` stages each schema's `.sbeir` into the jar under
**`META-INF/seqeron/sbeir/`** for `SbeLogPrinter`, which **discovers whatever `.sbeir` resources are on
its classpath rather than naming schemas** — so a product jar beside this one lets the same tool name an
application payload, and this jar alone names none, which is the point. The staging directory is
`META-INF`, not a package dir, because a consumer contributing its IR must not have to write into
seqeron's own package namespace; `collectSbeIr` wipes its destination first, like every codegen step.
