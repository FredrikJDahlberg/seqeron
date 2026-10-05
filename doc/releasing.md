# Releasing

A tag `v<version>` runs `.github/workflows/release.yml`, which fails unless the tag matches `VERSION`.
It publishes:

- the Java artifacts on JitPack (`seqeron`, `seqeron-service`, `seqeron-bom`);
- the C# package, `Org.Limitless.Seqeron`, on nuget.org, with its symbols package;
- the node image, `ghcr.io/fredrikjdahlberg/seqeron-service:<version>` — the image `docker/compose.yml`
  builds locally as `seqeron/node:local`;
- a GitHub Release with the operator distribution, `seqeron-<version>.zip` (`bin/`, `lib/`, `ops/`), the
  uber, client and node jars, and the C# package.

The NuGet push needs the repository secret `NUGET_API_KEY`, an API key for the account that owns the
package id. nuget.org takes a version once: a package pushed in error can be unlisted, never replaced.

To cut one:

1. Start from `main` with CI green, and commit the release's notes to `.github/release-notes/`, named
   after the tag (`v0.6.3.md`). The GitHub Release uses them, followed by the changelog link; without
   them it has the link alone. A release that moves Aeron also needs `csharp-client-test.sh` to pass
   against the new version first — CI's `chaos.yml` runs it on the upgrade's pull request — with
   `aeronDotnet` in `versions.properties` naming the Aeron.NET it passed with (spec **V-1**).
2. Run `.github/tag-release.sh <major.minor.patch>`, which is the whole tagging step:
   ```bash
   .github/tag-release.sh 0.6.3
   ```
   It writes the number to `VERSION` — the only place to change it, since every build reads it from
   there — commits that as `Release <version>`, pushes `main`, then tags the commit `v<version>` and
   pushes the tag. Pushing the commit and the tag together is what keeps the two equal, the one thing
   `release.yml` refuses to proceed without. The argument must be three dot-separated numbers;
   anything else is rejected before the script writes anything.
3. Watch the `release` workflow. It fails when the tag does not match `VERSION`, when the C# suite or
   codec check fails, and when JitPack has not built the tag within about ten minutes (it prints the
   tail of JitPack's build log).
4. Check the GitHub Release lists the zip, the three jars and the package. It is created last, so it
   exists only when the JitPack build, the image push and the NuGet push have succeeded.
5. Bump the release pinned in [`getting-started.md`](getting-started.md) when the API its snippets use
   has changed.
