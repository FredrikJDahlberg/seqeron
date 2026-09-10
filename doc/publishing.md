# Publishing

What a consumer can get today, and what stands between that and a coordinate someone else can
resolve. This is a **backlog**, not a runbook: nothing here is required to build, run or test this
repo, and every item is a cost paid only when someone outside this machine wants to depend on it.

The two halves are independent and in different states. Java has an artifact that is not published
anywhere; C++ has no artifact at all and cannot have one without owning Aeron's packaging.

## What exists

| | Java | C++ |
| --- | --- | --- |
| artifact | `org.limitless:seqeron:0.1.0` — the plain jar, `components.java` | none |
| how a consumer gets it | `./gradlew publishToMavenLocal`, then `mavenLocal()` | `FetchContent` over the checkout, `add_subdirectory` under the hood |
| proof it works | [`examples/java`](../examples/java) — resolves the coordinate, no source dependency | [`examples/cpp`](../examples/cpp) — `SEQERON_SOURCE_DIR`, swappable for `GIT_REPOSITORY`/`GIT_TAG` |
| reach | this machine's `~/.m2` | anyone who can clone the repo |

Two decisions are already made and are **not** open items:

- **The uber jar is not published.** It is the runnable artifact every script resolves out of
  `build/libs`, and it repacks Aeron and Agrona — publishing it would put a shaded copy of both on a
  consumer's classpath beside the real ones.
- **Aeron, Agrona and the two Aeron modules are `api` dependencies**, because they appear in this
  repo's own public signatures (`DirectBuffer` in `SequencedFrameDecoder`/`SystemFrame`, `Aeron` and
  `Image` in `ReplayerStreamReceiver`, `AeronArchive` in `ReplayerService`, `ClusteredService` in
  `SequencerService`). `aeron-driver` and `sbe-tool` stay `implementation`. So the POM carries them
  at compile scope and a consumer declares one dependency.

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

## C++ — the one structural item

### 7. `seqeron_core` cannot be installed or exported

`find_package(seqeron)` cannot work, and this is not a matter of writing the missing `install()`
rules. `seqeron_core` interface-links `aeron_client_wrapper`, whose own `install()`/`export()` is
gated on Aeron's `AERON_INSTALL_TARGETS`, defaulting to `${STANDALONE_BUILD}` — off when Aeron is
pulled in with `FetchContent`, which is how this build pulls it. An `install(EXPORT)` of
`seqeron_core` therefore fails **at generate time**, on a dependency that is in no export set.

Getting past it means one of:

- requiring a **pre-installed Aeron** and `find_package`ing it rather than fetching it, which moves
  a build-from-source cost onto every consumer;
- vendoring or patching Aeron's install gating, which means owning someone else's packaging;
- exporting a target that does *not* carry `aeron_client_wrapper` and making the consumer supply
  Aeron itself — an interface that no longer describes what the header-only library needs.

None of the three is small, and `FetchContent` works today. This is recorded so the absence reads
as a decision rather than an oversight.

A second, smaller consequence: the generated SBE headers live in the build tree, so
`seqeron_core`'s include roots are `$<BUILD_INTERFACE:>` only. Any install would have to install
generated output, which is a second reason the export is not a five-line addition.

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
