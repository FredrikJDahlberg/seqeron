# Test harnesses

`seqeron-service/src/test/scripts` holds the six harnesses — five Java-only, plus the containerized
`docker-failover-test.sh` — and those still resolve paths relative to the repository
root — they were written when this tree sat under `cluster/`, so check the depth of any `../..` you add.
