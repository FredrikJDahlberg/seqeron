# Packaging improvements

Review of how seqeron is packaged, and what would make it easier to use. The biggest problems are that
nobody outside the build machine can download seqeron yet, and the version number is typed by hand in 12
places. Checked against the built client, node and uber jars and the generated POMs.

## Java artifacts

1. **Publish it somewhere.** Right now the only way to get `org.limitless:seqeron` is
   `publishToMavenLocal` on your own machine. Pick the coordinate before anyone
   depends on it: JitPack changes it to `com.github.<owner>:seqeron`, while GitHub Packages keeps
   `org.limitless` but makes every user set up a token just to download.

   **Done in v0.2.0.** JitPack, via `jitpack.yml`: `com.github.FredrikJDahlberg.seqeron:{seqeron,seqeron-node}:<tag>`.

2. **The published jars are bare.** `seqeron-client-0.1.0.jar` and `seqeron-node-0.1.0.jar` have:
   - no `LICENSE` or `NOTICE`, although the uber jar carries both for the Apache licence's sake;
   - an empty manifest: no `Implementation-Version` and no `Automatic-Module-Name`, so a modular (JPMS)
     project can't `requires` them cleanly;
   - no sources jar, so IDEs show users decompiled classes.

   **Done in v0.2.0.** Both jars carry `LICENSE`, `NOTICE` and `Implementation-Title`/`-Version`, the client
   jar an `Automatic-Module-Name`, and both are published with a sources jar.

3. **The POMs are written by hand and get scopes wrong.** `pom.withXml` marks every dependency `compile`.
   For `seqeron-node` that puts `sbe-tool` and `aeron-driver` on users' compile classpath, even though
   `build.gradle` makes them `implementation`. Gradle's normal `from components.java` publishing would get
   scopes right and add Gradle module metadata.

   **Done in v0.2.0.** The POMs stay hand-written, since the tiers are packages and `from components.java`
   cannot split them, but each dependency now carries its scope: `aeron-driver` and `sbe-tool` are `runtime`.

4. **Every launch needs four `--add-opens` flags.** They are copied into 14 files (scripts, the Docker
   entrypoint, the example, the README).
   - Adding `Add-Opens` to the uber jar's manifest fixes this for `java -jar`, which is how
     `SequencerServer` starts.
   - The `-cp` launches still need one shared `JAVA_OPTS` defined in `seqeron-home.sh`.

   **Done in v0.2.0.** The uber jar's manifest has `Add-Opens`, and every `-cp` launch uses
   `SEQERON_JAVA_OPTS` from `seqeron-home.sh`.

5. **Nothing aligns Aeron versions for users.** A project that brings its own Aeron has Gradle quietly
   pick one version. A small `seqeron-bom` (a Gradle platform), or a documented `enforcedPlatform`, would
   make a mismatch visible. That matters more here than usual, because a version mismatch fails only at
   runtime, when frames don't decode.

   **Done in v0.2.2.** `seqeron-bom` pins seqeron, Aeron, Agrona and SBE; the README shows it as an
   `enforcedPlatform`, and `examples/java` resolves through it.

## Versions

6. **`0.1.0` is typed in 12 places, and nothing ties it to the `v0.1.0` tag.**
   - The biggest offenders are `docker/Dockerfile:10` and the six test harnesses (`failover-test.sh:22`,
     `chaos-runner.sh:56`/`:796`, etc.). They hard-code the jar path instead of using
     `seqeron_require_jar`, so bumping the version breaks them.
   - Fix: keep the version in one place (a `VERSION` file or the git tag), read it from both
     `build.gradle` and `CMakeLists.txt`, and have the harnesses source `seqeron-home.sh`.

   **Done in v0.2.0.** `VERSION` is read by both builds, and the harnesses and the Dockerfile glob the jar.
   The v0.2.0 tag itself was cut before `VERSION` was bumped, so its C++ package reports 0.1.0; v0.2.1
   corrected that, and since v0.2.2 `release.yml` refuses a tag that does not match `VERSION`.

7. **Aeron and SBE versions are pinned twice.** They live in `build.gradle`'s `ext` block and again in
   `CMakeLists.txt`. One `versions.properties` file, read by Gradle and by CMake's `file(STRINGS)`, would
   turn the "change both together" rule into something the build enforces.

   **Done in v0.2.0.** `versions.properties`, loaded by `build.gradle` and parsed by `CMakeLists.txt`.

## C++ package

8. **Let users bring their own Aeron.** Adding `FIND_PACKAGE_ARGS` to the Aeron `FetchContent_Declare`
   means an installed or already-declared Aeron is used when present, and fetched only when absent. That
   closes the case where today a user's own Aeron pin silently loses.

   **Done in v0.2.0.** `FIND_PACKAGE_ARGS ${SEQERON_AERON_VERSION} GLOBAL`, and seqeron links Aeron only by
   its `aeron::` names.

9. **The CMake minimum version is wrong.** `CMakeLists.txt` asks for CMake 3.28, but the CI file notes
   Aeron 1.51.0 needs 3.30 or newer. A user on 3.28 gets past seqeron's check and then hits an error deep
   inside Aeron's build. Raising the minimum to 3.30 makes the failure clear and early.

   **Done in v0.2.0.** `CMakeLists.txt` and `examples/cpp` require 3.30.

10. **Test the installed package in CI.** CI builds `examples/cpp` only through `FetchContent`. Nothing
    runs `cmake --install` followed by `find_package(seqeron)`, so a break in `seqeronConfig.cmake` goes
    unnoticed.

    **Done in v0.2.0.** CI's `installed` job installs Aeron and seqeron, checks Aeron was found rather than
    fetched, and builds `examples/cpp` with `-DSEQERON_FIND_PACKAGE=ON`.

## Releases and Docker

11. **Add a release workflow.** There are only `ci`, `chaos` and `failover` workflows. A workflow
    triggered by a tag could attach `operatorDistZip` and the jars to a GitHub Release, and publish the
    artifacts too. That gives operators a download instead of "clone and build".

    **Done in v0.2.2.** `release.yml` fails unless the tag is `v` + `VERSION`, waits for JitPack, pushes
    the node image, and creates a GitHub Release with the zip and the uber, client and node jars.

12. **Make the Docker image usable on its own.** Today it needs the jar built on the host first,
    hard-codes the version, and is tagged `seqeron/node:local` without being pushed anywhere. It also
    copies in only `ports.sh`, so `clusterctl.sh` isn't available inside a node container. Options: build
    from `operatorDist` so `bin/` comes along, and push to GHCR on each tag.

    **Done in v0.2.2.** The image is built from `operatorDist` and pushed as
    `ghcr.io/fredrikjdahlberg/seqeron-node:<version>`. `clusterctl` is on the `PATH` in each container,
    set to that container's member, and `clusterctl.egressHost` lets it run on a follower.

## Docs that mislead users

13. **`doc/publishing.md` §8 is out of date.** It says "Nothing in CI builds the examples", but `ci.yml`
    has an `examples` job. The "smallest next step" section and item 2's JitPack wording should be checked
    at the same time.

    **Done in v0.2.0.** Corrected then; `doc/publishing.md` itself was removed in v0.2.1, and the references
    to it were removed in v0.2.2.

14. **Smaller inconsistencies:**
    - Three comments still cite the removed `doc/future-arch.md`, which `CLAUDE.md` forbids:
      `build.gradle:393`, `CMakeLists.txt:88`, `PortLayout.java:5`.
    - `build.gradle:477` says "see 'Split by audience' below", but that section is above it.
    - Test counts disagree: `CLAUDE.md` says both 310 and 286 JUnit tests; `CLAUDE.md` says 126 C++ tests
      while `README.md` says 132.

    **Done in v0.2.0.** The citations are gone, the comment says "above", and both files give 317 JUnit and
    130 C++ tests.

## Suggested order

1. Items 13–14 and 9 (small fixes).
2. Items 6 and 4, which remove the most repeated values.
3. Items 2 and 3.
4. Items 1 and 11 together, since the publish target decides what the release workflow does.
