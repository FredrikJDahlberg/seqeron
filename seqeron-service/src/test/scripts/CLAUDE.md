# Test harnesses

`seqeron-service/src/test/scripts` holds the ten harnesses — eight Java-only, plus the containerized
`docker-failover-test.sh` and `csharp-client-test.sh`, which needs the .NET SDK — and those still resolve
paths relative to the repository root — they were written when this tree sat under `cluster/`, so check the
depth of any `../..` you add.
