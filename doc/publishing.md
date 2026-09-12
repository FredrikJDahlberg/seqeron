# Publishing

What a consumer can get today, and what stands between that and a coordinate someone else can
resolve. This is a **backlog**, not a runbook: nothing here is required to build, run or test this
repo, and every item is a cost paid only when someone outside this machine wants to depend on it.

The two halves are independent and in different states. Java has an artifact that is not published
anywhere; C++ has one — the git tag, consumable two ways — that no registry indexes.

## What exists

| | Java | C++ |
| --- | --- | --- |
| artifact | two, by audience: `org.limitless:seqeron:0.1.0` (the client tier) and `org.limitless:seqeron-node:0.1.0` (the cluster), the second depending on the first | the git tag: the headers plus a CMake package, `seqeron::seqeron_core` |
| how a consumer gets it | `./gradlew publishToMavenLocal`, then `mavenLocal()` | `FetchContent` over the checkout (`add_subdirectory` under the hood), or `cmake --install` then `find_package(seqeron)` |
| proof it works | [`examples/java`](../examples/java) — resolves `seqeron` alone, no source dependency and no node classes | [`examples/cpp`](../examples/cpp) — `SEQERON_SOURCE_DIR`, swappable for `GIT_REPOSITORY`/`GIT_TAG` |
| reach | this machine's `~/.m2` | anyone who can clone the repo |

Two decisions are already made and are **not** open items:

- **The uber jar is not published.** It is the runnable artifact every script resolves out of
  `build/libs`, and it repacks Aeron and Agrona — publishing it would put a shaded copy of both on a
  consumer's classpath beside the real ones.
- **The C++ codecs are generated and committed**, under `src/main/generated/sbe/core`. The git tag is
  the C++ artifact, so a codec that only exists in a build tree is not shipped at all: a consumer would
  have to own the SBE tool, the JDK and the codegen step to use headers whose schema is core's and
  which it has no business regenerating. Committed, `FetchContent` over the tag plus a C++23 compiler
  is the whole prerequisite list on seqeron's side. (Aeron's own build still requires a JDK 17+ —
  `aeron-archive/src/main/c` does `find_package(Java 17 REQUIRED)` — so a from-source Aeron keeps one
  on the machine; what this removes is seqeron's *own* demand for one.) The cost is a second copy of
  the schemas, and `CheckSbeCodecsCurrent` is what stops it drifting: SBE's C++ output is
  deterministic, so it regenerates into the build tree and compares exactly, and `run_tests` depends
  on it. `RegenerateSbeCodecs` is the only thing that writes into the source tree.
- **Aeron, Agrona and the two Aeron modules are `api` dependencies**, because they appear in this
  repo's own public signatures (`DirectBuffer` in `SequencedFrameDecoder`/`SystemFrame`, `Aeron` and
  `Image` in `ReplayerStreamReceiver`, `AeronArchive` in `ReplayerService`, `ClusteredService` in
  `SequencerService`). `aeron-driver` and `sbe-tool` stay `implementation`. So each POM carries its
  own at compile scope and a consumer declares one dependency.
- **The jar is split by audience, and the split is by package.** `seqeron` is what a process that
  merely talks to a cluster needs — the frame and replay codecs, `replayer.client`, the ingress client,
  `util.Logger`, `SeqeronCounters`, and `sequencer`'s `FrameLayer`, `SystemFrame` and `PortLayout` —
  and `seqeron-node` is the cluster: the sequencer, the Replayer, the tools, the metrics exporter, the
  probe codecs and the `.sbeir` resources. A gateway is not a node, so it takes the first alone and the
  archive and the media driver stay off its classpath. `aeron-cluster` is the exception and not ours to
  fix: the cluster *client* and the consensus module ship in one upstream artifact.

  The tiers are packages rather than source sets because the line runs through `sequencer`, so
  `checkTierSeparation` (wired into `check`) scans the compiled client classes and fails on a reference
  to a node type or to `io.aeron.{archive,driver}` — a same-package reference needs no import, so
  imports are not enough to check. Keeping the client tier closed is what put the constants a client
  needs into client-tier classes: the tap's identity and the cluster clock in `FrameLayer`, the port
  block in `PortLayout`, the replay protocol's addresses in `ReplayerStreamReceiver`.

## Java — open items

### 1. No remote repository

`publishToMavenLocal` is the whole workflow. Nobody else can resolve the coordinate. Three ways
out, cheapest first:

- **JitPack.** Builds a git tag on demand, runs `publishToMavenLocal` in its own container, serves
  the result. No credentials on either side, and no publish step in this repo. The coordinate
  changes to `com.github.<Owner>:seqeron:<tag>`, and it needs a `jitpack.yml` naming JDK 21 —
  JitPack's default is older and the build will not compile without it.
- **GitHub Packages.** Keeps the `org.limitless` coordinate and needs a `repositories` block plus
  credentials. The catch is on the **consumer** side: GitHub Packages requires a token to *read*,
  so every consumer must configure authentication for a public artifact.
- **Maven Central.** The only registry a consumer needs nothing for — and the most work. See items
  2–5, which are all Central's entry requirements.

### 2. The POM carries no metadata

No `<name>`, `<description>`, `<url>`, `<licenses>`, `<developers>` or `<scm>`. Nothing local reads
any of it, and JitPack does not care. Central rejects a POM without it. This is the first thing to
add if the answer to item 1 is ever Central, and it is the cheapest of these to add early.

### 3. The `org.limitless` groupId is not claimable on Central

Central verifies the **coordinate**, not the Java package name — so this is a `build.gradle`
question, not a rename. `org.limitless` would require proving DNS control of `limitless.org`.
`io.github.<owner>` requires only owning the GitHub repository of that name.

**The Java package stays `org.limitless.seqeron` either way.** A groupId and a package name are
allowed to differ, nothing in the tooling ties them, and CLAUDE.md's "every artifact says seqeron"
rule is about the *artifact* name, which does not change.

### 4. No PGP signing

Central requires every artifact signed. That means the `signing` plugin, a key, and the key's
private half reachable from wherever the publish runs — a real secret-management question if it
ever runs in CI. JitPack and GitHub Packages require none of it.

### 5. No sources or javadoc jar

`withSourcesJar()` and `withJavadocJar()` are not enabled. Central requires both. Sources is free.
Javadoc runs clean today (`./gradlew javadoc` succeeds) but emits ~100 `no comment` warnings, all
from the generated SBE codecs, and would publish a jar that is mostly generated accessors — so
enabling it wants an exclusion for the generated source roots rather than needing one.

### 6. No tags, and no release process

`git tag` is empty. Both JitPack (which resolves a tag) and a C++ consumer's
`FetchContent GIT_TAG` need one, so this blocks item 1's cheapest option and the C++ side's only
version-pinning story at once. The version is a hand-typed `0.1.0` in `build.gradle` with nothing
tying it to a tag, so the first release also has to decide who owns that number.

## C++ — the one open item

### 7. An installed consumer supplies its own Aeron

`find_package(seqeron)` works. `cmake --install` writes the hand-written headers, the committed SBE
ones, `seqeronTargets.cmake` and `seqeronConfig.cmake` under the prefix, and a consumer links
`seqeron::seqeron_core` — the same target name the `FetchContent` path gives it — with no source
dependency on this repo. What the package cannot do is bring Aeron with it.

Every Aeron target in `seqeron_core`'s link interface is wrapped in `$<BUILD_INTERFACE:>`, and that is
not tidiness. This build pulls Aeron in with `FetchContent`, where `AERON_INSTALL_TARGETS` defaults to
`${STANDALONE_BUILD}` — off — so none of those targets is in any export set, and an `install(EXPORT)`
naming one fails **at generate time**: `requires target "aeron_client_wrapper" that is not in any
export set`. The wrapper strips them from the exported target, and `seqeronConfig.cmake` re-attaches
the same four under `aeron::` behind a `find_dependency(aeron 1.51.0)` configured from
`SEQERON_AERON_VERSION`, so the floor is stated in one place.

The cost therefore lands on the consumer: an Aeron built with `-DAERON_INSTALL_TARGETS=ON` and
installed, on `CMAKE_PREFIX_PATH` beside seqeron. That is the honest shape — seqeron does not own
Aeron's packaging, and vendoring its install gating would mean maintaining someone else's.
`FetchContent` stays the simpler path for a consumer that builds Aeron from source anyway, which is
why `examples/cpp` uses it.

Two details bite silently if this is ever rewritten:

- **`seqeron_core` declares four Aeron targets, not one.** `Aeron.h` reaches for `aeronc.h` in the C
  client, and `ClusterStreamClient.hpp` for `client/archive/AeronArchive.h` and the C archive client
  behind it. Declaring `aeron_client_wrapper` alone left every consumer to work the rest out — which
  `examples/cpp` duly did, by adding `aeron` to its own link line.
- **`INTERFACE_SYSTEM_INCLUDE_DIRECTORIES` is not prefix-rewritten on export.** CMake copies the
  `$<INSTALL_INTERFACE:>` text through verbatim, so the exported entry is a relative path matching no
  include root and the SYSTEM marking quietly does nothing. `seqeronConfig.cmake` restates it
  absolutely; without that a `find_package` consumer gets every `-Wtype-limits` the SBE codecs emit.

The install rules are gated on `SEQERON_INSTALL`, defaulting to `PROJECT_IS_TOP_LEVEL` as
`SEQERON_BUILD_TESTS` does: a parent that vendors this build should not find seqeron's headers landing
in its own `install` target.

Nothing exercises the installed path automatically — `examples/cpp` takes the `FetchContent` one, and
the CI `examples` job with it — so verifying a change to the package means installing Aeron and seqeron
to a prefix by hand.

## Cross-cutting

### 8. Nothing in CI builds the examples

`.github/workflows/ci.yml` builds and tests the repo; it never configures `examples/cpp` or runs
`publishToMavenLocal` and builds `examples/java`. The examples are the only thing that exercises
the consumable surface — a change that breaks the artifact (a dependency re-scoped back to
`implementation`, an include root that stops resolving out-of-tree) passes CI today and is found
by whoever tries to consume it.

### 9. A consumer inherits this repo's Aeron pin

A C++ consumer that adds this build gets seqeron's `FetchContent` of Aeron 1.51.0 and its
`SBE_VERSION` 1.38.1. A consumer with its own Aeron pin gets whichever `FetchContent_Declare` wins,
silently. Java has the same shape but the mechanism handles it: the POM declares versions and
Gradle resolves conflicts visibly.

## Smallest useful next step

Item 6 then item 1's JitPack option: tag a release, add `jitpack.yml` pinning JDK 21, and the same
tag serves both the Java coordinate and a C++ consumer's `GIT_TAG`. Total cost is one file and one
tag, no credentials on either side, and it does not foreclose Central later — items 2–5 are
additive.
