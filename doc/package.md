# Packaging improvements

Review of how seqeron is packaged, and what would make it easier to use. The biggest problems are that
nobody outside the build machine can download seqeron yet, and the version number is typed by hand in 12
places. Checked against the built client, node and uber jars and the generated POMs.

## Java artifacts

1. **Publish it somewhere.** Right now the only way to get `org.limitless:seqeron` is
   `publishToMavenLocal` on your own machine (`doc/publishing.md` §1). Pick the coordinate before anyone
   depends on it: JitPack changes it to `com.github.<owner>:seqeron`, while GitHub Packages keeps
   `org.limitless` but makes every user set up a token just to download.

2. **The published jars are bare.** `seqeron-client-0.1.0.jar` and `seqeron-node-0.1.0.jar` have:
   - no `LICENSE` or `NOTICE`, although the uber jar carries both for the Apache licence's sake;
   - an empty manifest: no `Implementation-Version` and no `Automatic-Module-Name`, so a modular (JPMS)
     project can't `requires` them cleanly;
   - no sources jar, so IDEs show users decompiled classes.

3. **The POMs are written by hand and get scopes wrong.** `pom.withXml` marks every dependency `compile`.
   For `seqeron-node` that puts `sbe-tool` and `aeron-driver` on users' compile classpath, even though
   `build.gradle` makes them `implementation`. Gradle's normal `from components.java` publishing would get
   scopes right and add Gradle module metadata.

4. **Every launch needs four `--add-opens` flags.** They are copied into 14 files (scripts, the Docker
   entrypoint, the example, the README).
   - Adding `Add-Opens` to the uber jar's manifest fixes this for `java -jar`, which is how
     `SequencerServer` starts.
   - The `-cp` launches still need one shared `JAVA_OPTS` defined in `seqeron-home.sh`.

5. **Nothing aligns Aeron versions for users.** A project that brings its own Aeron has Gradle quietly
   pick one version. A small `seqeron-bom` (a Gradle platform), or a documented `enforcedPlatform`, would
   make a mismatch visible. That matters more here than usual, because a version mismatch fails only at
   runtime, when frames don't decode.

## Versions

6. **`0.1.0` is typed in 12 places, and nothing ties it to the `v0.1.0` tag.**
   - The biggest offenders are `docker/Dockerfile:10` and the six test harnesses (`failover-test.sh:22`,
     `chaos-runner.sh:56`/`:796`, etc.). They hard-code the jar path instead of using
     `seqeron_require_jar`, so bumping the version breaks them.
   - Fix: keep the version in one place (a `VERSION` file or the git tag), read it from both
     `build.gradle` and `CMakeLists.txt`, and have the harnesses source `seqeron-home.sh`.

7. **Aeron and SBE versions are pinned twice.** They live in `build.gradle`'s `ext` block and again in
   `CMakeLists.txt`. One `versions.properties` file, read by Gradle and by CMake's `file(STRINGS)`, would
   turn the "change both together" rule into something the build enforces.

## C++ package

8. **Let users bring their own Aeron.** Adding `FIND_PACKAGE_ARGS` to the Aeron `FetchContent_Declare`
   means an installed or already-declared Aeron is used when present, and fetched only when absent. That
   closes `publishing.md` §9, where today a user's own Aeron pin silently loses.

9. **The CMake minimum version is wrong.** `CMakeLists.txt` asks for CMake 3.28, but the CI file notes
   Aeron 1.51.0 needs 3.30 or newer. A user on 3.28 gets past seqeron's check and then hits an error deep
   inside Aeron's build. Raising the minimum to 3.30 makes the failure clear and early.

10. **Test the installed package in CI.** CI builds `examples/cpp` only through `FetchContent`. Nothing
    runs `cmake --install` followed by `find_package(seqeron)`, so a break in `seqeronConfig.cmake` goes
    unnoticed (`publishing.md` §7 says so itself).

## Releases and Docker

11. **Add a release workflow.** There are only `ci`, `chaos` and `failover` workflows. A workflow
    triggered by a tag could attach `operatorDistZip` and the jars to a GitHub Release, and publish the
    artifacts too. That gives operators a download instead of "clone and build".

12. **Make the Docker image usable on its own.** Today it needs the jar built on the host first,
    hard-codes the version, and is tagged `seqeron/node:local` without being pushed anywhere. It also
    copies in only `ports.sh`, so `clusterctl.sh` isn't available inside a node container. Options: build
    from `operatorDist` so `bin/` comes along, and push to GHCR on each tag.

## Docs that mislead users

13. **`doc/publishing.md` §8 is out of date.** It says "Nothing in CI builds the examples", but `ci.yml`
    has an `examples` job. The "smallest next step" section and item 2's JitPack wording should be checked
    at the same time.

14. **Smaller inconsistencies:**
    - Three comments still cite the removed `doc/future-arch.md`, which `CLAUDE.md` forbids:
      `build.gradle:393`, `CMakeLists.txt:88`, `PortLayout.java:5`.
    - `build.gradle:477` says "see 'Split by audience' below", but that section is above it.
    - Test counts disagree: `CLAUDE.md` says both 310 and 286 JUnit tests; `CLAUDE.md` says 126 C++ tests
      while `README.md` says 132.

## Suggested order

1. Items 13–14 and 9 (small fixes).
2. Items 6 and 4, which remove the most repeated values.
3. Items 2 and 3.
4. Items 1 and 11 together, since the publish target decides what the release workflow does.
