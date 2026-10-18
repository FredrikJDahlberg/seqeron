# Test harnesses

`seqeron-service/src/test/scripts` holds the thirteen harnesses — nine Java-only, plus the containerized
`docker-failover-test.sh`, the two C# ones, `csharp-client-test.sh` and `csharp-windows-test.sh` (Git
Bash on Windows, with its member in WSL1), and the C++ one, `cpp-client-test.sh` — and, in `faulteron/`, the
six network-fault tests that drive the faulteron injector against the docker cluster. The thirteen still resolve
paths relative to the repository root — they were written when this tree sat under `cluster/`, so check the
depth of any `../..` you add.
