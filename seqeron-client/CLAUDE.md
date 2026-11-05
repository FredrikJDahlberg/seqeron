# Client tier

**The producer side is a Java/C# port pair.** Java's `ClusterStreamSender`/`IngressPublisher` carry
the names and semantics of the retired C++ files (branch `cpp-direct-client`) — `connectColocated` (IPC ingress on the co-located member, UDP
endpoints when it is not leading), a `send` that spins through back-pressure and an election rather than
dropping the frame — unless a new leader arrives mid-spin while its `IngressTracker` holds, when it places
nothing and returns false — a self-throttling `keepAlive`, and the three-valued `Publish`. They are far smaller
than their twins because `AeronCluster` already is the cluster protocol that the retired
`ClusterStreamSender.hpp` implemented by hand; what the Java side adds is only what that client does not do. Two divergences are
deliberate and documented in the class: the payload arrives pre-encoded rather than through a `Fill` over an
encoder (Java's SBE codecs share no interface), and leadership moving off the co-located member costs a
session rather than a publication swap (`AeronCluster` owns its publication). `IngressSender` exists so
`IngressPublisher` has a seam the Java suite can drive without an Aeron runtime — the retired C++ transport seam
had no Java equivalent, so the state machine its 868 test lines covered is Aeron's here, not ours.
What is left of ours that is easy to get wrong is split off and unit-tested: **`IngressStallPolicy`**
(which offer results are terminal — `CLOSED` is not, it is an election in progress). `ClusterStreamSender`
itself is then the Aeron adapter, whose one other decision — replacing an IPC session whose leader moved
away — is a single comparison, so its low line coverage is the same statement `SequencerService`'s is.
C#'s are Java's on Aeron.NET's `AeronCluster`, divergences included, and set every timeout Java sets
explicitly: Aeron.NET's defaults are Aeron's, whose new-leader timeout is shorter than an election.

**A send that succeeds is not a frame sequenced.** Nothing confirms ingress on egress, and a leader
failover silently loses whatever the old leader had not committed — the session survives it.
`sequencer/client/PendingSends` is the confirm-on-queue tracker (spec §16 A-4, A-5): a producer gives it to
`IngressPublisher` as its `IngressTracker` (which tracks what it places and declines while it holds or is
full) and to the sender with `setIngressHold`, feeds it its own queue and each `LeadershipChanged`'s term, and
resends what a term change or a closed session lost. Java and C#,
case for case, with a property test asserting exactly-once, in-order delivery across random failovers;
`FailoverProcessTest` proves the same across a real leader kill, `csharp-client-test.sh` for C#.

**The C++ half is the hosted client stubs (`hosted/`), a library and not a program.** `seqeron_core` is a
header-only INTERFACE target and the only binaries the build produces are `core_tests` and the probes
(`hosted_probe`, `hosted_gateway_probe`, `hosted_consumer_probe`) that the `Hosted*Test` system tests drive when
`HOSTED_PROBE` names `hosted_probe`. The direct C++ client is retired: branch `cpp-direct-client`, tag
`cpp-direct-client-final`. There is **no C++ replay server** — the server side of the replay protocol is Java only.

`seqeron-service/src/main/ops/` holds the Prometheus and Grafana provisioning the metrics scripts feed
(`doc/ops.md`); it is deployment configuration, not code.

## C++ build

C++23, and fetches Aeron 1.53.2 (unless an installed one is found) and GoogleTest from source. **The core SBE codecs are generated and
committed**, under `seqeron-client/src/main/generated/sbe/core` — the git tag is the C++ artifact, so
shipping them
with it is what lets a consumer build with no SBE tool and no JDK of seqeron's asking (Aeron's own
build still wants a JDK 17+). `find_package(Java)` is therefore `QUIET`, not `REQUIRED`, and is used
only by `RegenerateSbeCodecs` (rewrites the committed tree — run it when a schema changes, then commit
what it produced) and `CheckSbeCodecsCurrent`, which regenerates into the build tree and compares.
SBE's C++ output is deterministic, so that comparison is exact; `run_tests` depends on it, which is
what keeps the committed copy from drifting away from `seqeron-client/src/main/sbe`. GoogleTest and
`core_tests` are gated on `SEQERON_BUILD_TESTS`, which defaults to
`PROJECT_IS_TOP_LEVEL` — a build that adds this one gets neither unless it asks. `-DSEQERON_COVERAGE=ON` adds instrumentation. `--target docs` renders the C++ API reference
(`<build>/docs/html`, `detail/` and the codecs left out, as the javadoc leaves them out), and exists only
when `find_package(Doxygen)` finds one. No SSH remote is needed.

**Consumable two ways, under the same target name.** `add_subdirectory`/`FetchContent` over the
checkout (`seqeron-examples`), or `find_package(seqeron)` against a `cmake --install`ed prefix —
`SEQERON_INSTALL` gates the install rules and defaults to `PROJECT_IS_TOP_LEVEL`. The exported target
names no Aeron target, because FetchContent leaves Aeron's in no export set and an `install(EXPORT)`
naming one fails at generate time; they are `$<BUILD_INTERFACE:>`-wrapped and
`cmake/seqeronConfig.cmake.in` re-attaches them under `aeron::`, so an installed consumer brings its
own installed Aeron. CI's `installed` job is the only thing that exercises this path.

## C# build

.NET 10, on Aeron.NET and the SBE C# runtime from NuGet. The frame and replay codecs are generated and committed
under `seqeron-client/src/main/generated/sbe/csharp`, as the C++ ones are, so a C# build needs no JDK; Gradle
owns only their generation, since the SBE jar is on its classpath, and `checkCSharpSbeCurrent` regenerates and
compares. CI's `csharp` job runs on Linux and on Windows, where the client is deployed; keep the tests free of
POSIX-only assumptions. `Directory.Build.props` at the root reads `versions.properties` and `VERSION`, so the package is
versioned with the jars. It is `Org.Limitless.Seqeron` on nuget.org, pushed by `release.yml` with its symbols
and XML docs, and it names its dependencies exactly. The C# examples (`seqeron-examples/src/csharp`) resolve
it from `build/nuget` rather than the source tree; NuGet serves a cached copy of a version it has seen, so
delete `~/.nuget/packages/org.limitless.seqeron` after re-packing an unchanged one. The API reference is
Doxygen over the public types, `doc/site/csharp.Doxyfile`, which `pages.yml` renders beside the Java and C++
ones.

## `ReplayerRecovery` parity

`ReplayerStreamReceiver` is the Aeron adapter only — subscriptions, the replay image, the clocks; every
decision it makes about them lives in **`ReplayerRecovery`**, which holds none of them and is where the
unit suite drives the walk/resume/gap state machine — `ReplayerRecoveryTest` names one situation per case,
`ReplayerRecoveryPropertyTest` drives seeded fault sequences against a model of the archive/queue/Replayer and
asserts gap-freedom and convergence over whatever comes out; both tests exist in Java and C#. What the model
assumes of the Replayer is held to `ReplayerService` itself by `seqeron-service`'s `ReplaySeamTest`, Java only, as
the server is, through the test-side `ReplayerRecoveryBridge`, since neither the class nor its transport is public. The Java
and C# classes are faithful ports of each other: same protocol, same state machine, same adapter/seam
split. Keep all four files and all four tests in step — the two `ReplayerRecoveryTest`s are case for
case in the same order precisely so a divergence is visible as a missing case rather than as a
runtime decode failure on a live queue, and the two property tests draw from `SplitMix64` in the same order, so
a seed makes the same fault sequence in each.
