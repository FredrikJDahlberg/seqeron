---
name: release
description: How a seqeron release is tagged, built by JitPack and published (Maven coordinates, BOM, release.yml, the GHCR node image). Use when cutting or debugging a release.
---

# Releasing seqeron

**JitPack is the Java release channel.** `jitpack.yml` builds a tag with `check publishToMavenLocal`,
and it is served as `com.github.FredrikJDahlberg.seqeron:{seqeron,seqeron-node}:<tag>`. Under
`JITPACK=true`, `build.gradle` publishes with that group and the tag as version, so `seqeron-node`'s
POM dependency on `seqeron` resolves there; everywhere else the group stays `org.limitless` and the
version `VERSION`'s. A third publication, the pom-only `seqeron-bom`, pins
Aeron, Agrona and SBE at `versions.properties`. A `v*` tag runs `release.yml`: it fails unless the tag
is `v` + `VERSION`, waits for JitPack's build, pushes the node image to GHCR, and creates a GitHub Release
with the operator distribution. The image is built from `operatorDist`, so a node container has
`bin/` and a `clusterctl` on the `PATH` set to its own member (`docker/clusterctl`).
