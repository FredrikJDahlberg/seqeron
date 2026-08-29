# Linux portability — RHEL 9 / RHEL 10

> Written 2026-08-23 against the tree as it stands. phixeron is developed on macOS/arm64 with
> Apple clang and libc++; nothing has ever been built or run on Linux. This records what a first
> RHEL bring-up has to fix, in the order it will hit.
>
> **The C++ itself is not the problem.** There is no Darwin API anywhere in `src/main/cpp` or
> `src/test/cpp` — no kqueue, no `mach_*`, no `SO_NOSIGPIPE`, no `sysctlbyname`, no
> `__APPLE__`/`TargetConditionals` guards. The socket layer is already written to the Linux
> spelling (`MSG_NOSIGNAL` at `fix/FixIngressHandler.hpp:83`, `POLLIN|POLLHUP|POLLERR` at
> `fix/FixGateway.cpp:440`, an explicit `O_NONBLOCK` on the accepted fd rather than relying on
> BSD inheritance, `fix/TcpServer.hpp:66`). What breaks is the **toolchain flags**, the
> **`$TMPDIR` path idiom**, and the **Aeron directory defaults** — plus a handful of includes
> that libc++ is currently hiding.
>
> Updated the same day: §1a is **fixed** — the build no longer requires clang. §1b and §1c were
> re-examined and stand as they were, for reasons now recorded there; a first-pass claim that the
> C++23 setting was gratuitous was wrong and is corrected in §1c.
>
> Updated 2026-08-29: §2 and §4 are **fixed** — path joining now goes through
> `src/main/scripts/paths.sh` (`TMP_DIR`, `aeron_default_dir`) on the shell side, `Env.hpp`'s
> `joinPath` on the C++ side and `new File(parent, child)` in `BasicDataLoader`; the missing
> includes and the two non-standard header spellings are in. What remains untried is a real
> libstdc++ compile, which is where anything §4 missed will surface. §3 is still open.

Platform baselines assumed below:

| | RHEL 9 | RHEL 10 |
|---|---|---|
| Default compiler | GCC 11 (`gcc-toolset-13`/`-14` in AppStream) | GCC 14 |
| clang | `llvm-toolset` (AppStream, not installed by default) | `llvm-toolset` |
| CMake | 3.26 | 3.30 |
| glibc | 2.34 | 2.39 |
| bash | 5.1 | 5.2 |
| JDK 21 | `java-21-openjdk` | `java-21-openjdk` |
| Arch baseline | x86-64-v2 | x86-64-v3 |

## 1. Toolchain

Nothing here stops a RHEL build outright. What it does is **pin RHEL 9 to a non-default toolchain**
on two independent axes — CMake and the compiler — while RHEL 10 handles both from stock packages.

### 1a. Clang-only coverage flags reached every phixeron target — **fixed**

`SimdFix::SimdFix` is an INTERFACE dependency, so simdfix's own `target_compile_options`
(`CMakeLists.txt:89-96` in its tree) propagated into every phixeron TU. In Debug that was:

```
-Wall -Wextra -g -O0 -fsanitize=address -fno-omit-frame-pointer -fprofile-instr-generate -fcoverage-mapping
```

plus `-O3 -march=native` in Release, and `-fsanitize=address -fprofile-instr-generate` at link.
`-fprofile-instr-generate` / `-fcoverage-mapping` are clang spellings — GCC hard-errors on them and
wants `--coverage`. `-march=native` is accepted by both compilers but produces binaries that SIGILL
on a host older than the one that built them.

**This was never a RHEL blocker**: clang ships in both RHEL 9 and RHEL 10 AppStream
(`llvm-toolset`). It was a *GCC* constraint — you could not build Debug with the distro-default
compiler.

Fixed in `CMakeLists.txt` by taking simdfix's headers and owning the flags locally: its
`INTERFACE_COMPILE_OPTIONS` / `INTERFACE_LINK_OPTIONS` are cleared after
`FetchContent_MakeAvailable`, and a `phixeron_flags` INTERFACE target re-establishes `-Wall -Wextra`,
the Debug/Release flags, and simdfix's probed `-msse4.1` (load-bearing — `Uint8x16_sse.inl` needs
`_mm_blendv_epi8`). Coverage became an opt-in `PHIXERON_COVERAGE` option that forks on
`CMAKE_CXX_COMPILER_ID`, and `-march=native` is gone. `-Wno-character-conversion` is now applied only
where the compiler knows the warning; the probe tests the **positive** form, since clang accepts any
unknown `-Wno-*` silently and would false-positive on the negative one.

Two notes that outlive the fix. First, the coverage *artifacts* differ too — clang writes `.profraw`
(`llvm-profdata merge` → `llvm-cov`), GCC writes `.gcno`/`.gcda` (`gcov`/`lcov`) — so any report step
has to fork the same way. Second, simdfix's own test targets still carry the clang flags, but they
are `EXCLUDE_FROM_ALL` and never built here; `Generator`, which phixeron *does* build, does not.

### 1b. `cmake_minimum_required(VERSION 3.28)` — a real floor

`CMakeLists.txt:1`. RHEL 9 ships CMake 3.26, so configure fails before anything else is evaluated.
RHEL 10 (3.30) is fine.

3.28 is genuinely required, not incidental: `EXCLUDE_FROM_ALL` on `FetchContent_Declare` — which is
what keeps simdfix's own tests and benchmarks (and their dependency on simdfix's `test.xml` /
`session.xml` fixtures, absent here) out of the default build — was **added in CMake 3.28**.
`SYSTEM`, used on the Aeron declaration, was added in 3.25.

Lowering the floor therefore means replacing `EXCLUDE_FROM_ALL` with the pre-3.28
`FetchContent_Populate` + `add_subdirectory(… EXCLUDE_FROM_ALL)` idiom, which CMake 3.30+ deprecates
under `CMP0169` — trading a RHEL 9 blocker for a deprecation warning on RHEL 10. Not worth it:
**RHEL 9 needs CMake from Kitware's repo or `pip install cmake`.**

### 1c. `CMAKE_CXX_STANDARD 23` — required by exactly one TU

`CMakeLists.txt`. RHEL 9's default GCC 11 has no usable C++23, so RHEL 9 needs `gcc-toolset-14`
(or clang). RHEL 10's GCC 14 is fine.

A first pass at this doc claimed the setting was gratuitous. It is not — that sweep used a
hand-built include set rather than the real one. Re-run faithfully against every command in
`compile_commands.json`, **33 of 34 TUs compile clean at `-std=c++20` and one does not**:

`src/test/cpp/.../sequencer/FrameStartPositionTest.cpp` includes Aeron's **C** header `aeron_image.h`
(deliberately — it fabricates an `aeron_header_t` by hand, since the C++ suite runs no media driver)
alongside Aeron's C++ headers. `aeron_image.h` pulls `<stdatomic.h>`, whose C11 macros collide with
`std::atomic_thread_fence` in `Atomic64_gcc_cpp11.h`:

```
Atomic64_gcc_cpp11.h:31:10: error: no type named '__c11_atomic_thread_fence' in namespace 'std'
Atomic64_gcc_cpp11.h:31:35: error: definition or redeclaration of 'memory_order_acq_rel'
                                  not allowed inside a function
```

C++23 fixes this by making `<stdatomic.h>` C++-aware (P0943, *Support C atomics in C++*). No C++23
*feature* is used anywhere in the tree — no `std::expected`, `std::print`, `std::flat_map`,
`std::stacktrace`, `std::to_underlying`, deducing `this` — so the standard is being paid for a header
interaction, in a test.

Two ways out if RHEL 9's stock GCC ever matters more than this does: compile that one file at
`-std=c++23` via `set_source_files_properties`, or reorder so Aeron's C++ headers are included first.
Both are workarounds against a third-party C header's include graph. The reason to leave it alone is
§1b — RHEL 9 already needs a non-stock CMake, so it is a non-stock-toolchain target either way, and
`gcc-toolset-14` is no additional class of burden. The rationale is pinned in a comment above the
`set(CMAKE_CXX_STANDARD 23)` line so it does not get "simplified" later.

## 2. Builds, won't run

All four of these are the same root cause: **macOS's `$TMPDIR` ends in a slash and Linux's `/tmp`
does not**, and the tree has absorbed that in three different places, each with a different failure
mode.

### 2a. Bare `${TMPDIR}` under `set -euo pipefail`

35 occurrences across 12 scripts. The load-bearing ones:

| Site | |
|---|---|
| `src/main/scripts/start-cluster.sh:54,60` | `AERON_DIR`, `SEQ_AERON_DIR` |
| `src/main/scripts/start-three-node-cluster.sh:86,91,264` | `AERON_DIR`, `SEQ_AERON_DIR`, per-member `MDIR` |
| `src/test/scripts/{chaos-runner,failover-test,gap-recovery-test,replayer-restart-test,replay-bench}.sh` | base dirs and per-member Aeron dirs |

RHEL leaves `TMPDIR` unset in a normal login shell. Every one of these scripts runs `set -euo
pipefail`, so the first bare `${TMPDIR}` aborts with `TMPDIR: unbound variable` — `start-cluster.sh`
dies at line 54, before it has launched anything.

This one is loud, which makes it the least dangerous of the four.

### 2b. `${TMPDIR:-/tmp}phixeron-…` — the silent half

19 occurrences. The default has no separator because on macOS the variable supplies one:

```bash
# src/main/scripts/purgelog.sh:35
BASE_DIRS=("${TMPDIR:-/tmp}phixeron-seq" "${TMPDIR:-/tmp}phixeron-seq3")   # RHEL → /tmpphixeron-seq
```

while the cluster itself writes to `java.io.tmpdir + "/phixeron-seq"` (`SequencerServer.java:105`)
→ `/tmp/phixeron-seq`. On RHEL the launchers, `sbe-log-printer.sh`, and the e2e scripts all address
a directory the cluster never touches, and **`purgelog.sh` reports success having deleted nothing**
— its `ls "${BASE_DIR}"/archive-* >/dev/null 2>&1` guard just fails quietly.

That failure mode matters more than it looks: stale per-tenure recordings surviving a purge is
exactly what produces the deterministic-assertion e2e failures (first-Logon timeout) that
`purgelog.sh`'s own header comment and the run-before-every-e2e rule exist to prevent. On RHEL the
rule would be followed and the purge would still not happen.

Also affected: `start-three-node-cluster.sh:78` (`BASE_DIR`), `order-gateway-test.sh:60,61,144,321`,
`exchange-gateway-test.sh:55,56,148,265,446`, `gateway-failover-test.sh:47`.

### 2c. `BasicDataLoader.java:79` — missing separator

```java
System.getProperty("basicdata.aeronDir",
                   System.getProperty("java.io.tmpdir") + "phixeron-seq-aeron-0");
```

Every other Java site uses `new File(tmpdir, …)` (`OrderGatewayConfig.java:59,69`,
`ExchangeGatewayConfig.java:58,70`) or `+ "/…"` (`SequencerServer.java:105-107`,
`ReplayerServer.java:96`, `ClusterCtl.java:77,79`, `MetricsExporter.java:31`). This one line does
neither, so on Linux it resolves to `/tmpphixeron-seq-aeron-0` and the loader attaches to a media
driver directory that does not exist. On macOS it happens to be correct.

The `+ "/…"` sites are the mirror image — harmless on Linux, producing a cosmetic `//` on macOS.
Only `BasicDataLoader` is actually wrong, and it is wrong only on Linux.

### 2d. `Env.hpp:resolveAeronDir` — a trailing slash baked into the fallback

```cpp
// src/main/cpp/org/limitless/phixeron/util/Env.hpp:49-51
const char* tmpDir = std::getenv("TMPDIR");
return std::string(tmpDir != nullptr && *tmpDir != '\0' ? tmpDir : "/tmp/") + "phixeron-seq-aeron-" +
       std::to_string(memberId);
```

The literal fallback carries the slash, so this is correct on RHEL **only while `TMPDIR` is unset**.
Set it — systemd, a batch scheduler, a site profile, `TMPDIR=/var/tmp` — and every C++ binary that
does not get an explicit `PHIXERON_*_AERON_DIR` resolves `/var/tmpphixeron-seq-aeron-0` and silently
attaches to nothing. The scripts do set the override on every launch, so this only bites a
hand-started process, which is precisely the debugging session where it will cost the most.

The fix on all four of the above is the same: join paths, never concatenate. Shell
`"${TMPDIR:-/tmp}"` + an explicit `/`; Java `new File(parent, child)`; C++ a small `joinPath` helper
that normalises the separator.

### 2e. Aeron's default directory is `/dev/shm` on Linux

`aeron_fileutil.c:910` (`"/dev/shm/aeron-%s"`) and `CommonContext.java:582-585` — on Linux Aeron
defaults its directory to `/dev/shm/aeron-<user>` when `/dev/shm` exists, falling back to
`$TMPDIR/aeron-<user>` only where it does not (i.e. macOS).

So `AERON_DIR="${TMPDIR}aeron-$(whoami)"` (`start-cluster.sh:54`,
`start-three-node-cluster.sh:86`) names a path nothing on RHEL will use by default. The standalone
`aeronmd` is launched with that value in its environment so it will honour it, but:

- any C++ client falling back to the Aeron default looks in `/dev/shm/aeron-$USER` instead, and
- the `rm -rf … "$AERON_DIR"` cleanups in `failover-test.sh:36`, `gap-recovery-test.sh:91`,
  `replayer-restart-test.sh:110` and `chaos-runner.sh:199` leave `/dev/shm` state behind between
  runs — the same class of cross-run pollution as 2b.

Either point `AERON_DIR` at the platform default explicitly, or keep overriding it everywhere and
purge the real default too.

## 3. RHEL deployment environment

None of these are code defects; they are things a macOS-only history has never had to answer.

**Arch baseline.** `-march=native` is gone from phixeron's Release build (§1a), so binaries no longer
carry the build host's ISA. Nothing replaces it yet: Release is plain `-O3` plus the probed
`-msse4.1` that `_mm_blendv_epi8` needs. If Release builds move to a dedicated build host, pin the
platform baseline explicitly — `-march=x86-64-v2` for RHEL 9, `-march=x86-64-v3` for RHEL 10 — via
`check_cxx_compiler_flag`, which also fails cleanly on arm64.

**systemd `PrivateTmp=yes`** — common in hardened unit files — gives each unit its own `/tmp`
namespace. That silently breaks the co-location contract: `FixGateway`, `OrderExecServer` and
`BasicDataServer` must share the sequencer's Aeron directory to reach the node tap, the Replayer and
cluster ingress over `aeron:ipc`. Each in its own private `/tmp` sees an empty directory and never
connects.

**`/tmp` is not a durable location for the transaction log.** `systemd-tmpfiles` ages `/tmp` out
(10 days by default on RHEL), and the archive under `baseDir` *is* the transaction log — with no
snapshots, recovery is always full-log replay from `globalSeqNo` 1, so losing archive segments is
unrecoverable, not merely slow. The `java.io.tmpdir` default is a dev-box convenience; a real
deployment needs `sequencer.baseDir` on managed storage.

**`/dev/shm` sizing.** Default is half of RAM on a host, but **64 MB in a container** — well under
Aeron's term buffers. Needs an explicit `--shm-size` / tmpfs mount wherever this runs containerised.

**firewalld is enabled by default.** The cluster block (9300–9325 for three nodes, per
`src/main/scripts/ports.sh`) plus 9000 (C++ FIX gateway), 9010 (mock exchange), 9020 (OrderGateway),
9400+ (metrics exporters) and 9500 (aggregator) need opening for anything multi-host. Localhost-only
runs are unaffected.

**SELinux** is enforcing by default. Unconfined processes are fine; a confined systemd service
mmapping `/dev/shm` and binding these ports will need policy.

**Tools not installed on a minimal RHEL.** `nc` (`fix-test-server.sh:48`,
`start-three-node-cluster.sh:312`, `chaos-runner.sh:277,389,446`) and `lsof`
(`chaos-runner.sh:201`). Both are in AppStream — `nmap-ncat` supports the `-z` form used here.
`caffeinate` (`chaos-runner.sh:38-40`) is macOS-only but already correctly guarded by
`command -v`, so it is a no-op on Linux rather than a failure.

**Build-host egress.** Configure-time network is required in three places: `git@github.com:` over
**SSH** for the simdfix `FetchContent` clone (needs a key and `known_hosts` on the build host),
`https://github.com` for Aeron and googletest, and `repo1.maven.org` + `services.gradle.org` for the
SBE jar and the Gradle distribution. Worth knowing before pointing this at an air-gapped build host.

## 4. Latent — masked by libc++ today

These compile on macOS only because libc++'s headers pull them in transitively. libstdc++ is the
classic place they surface, and GCC 13 pruned a large number of transitive `<cstdint>` / `<cstring>`
includes specifically.

**Choosing clang on RHEL does not avoid this.** clang on Linux defaults to `-stdlib=libstdc++` —
libc++ needs `libcxx-devel` and an explicit flag. Compiler and standard library are separate axes,
and it is the standard library that changes here. Expect these regardless of §1a's outcome.

| File | Missing include | Used at |
|---|---|---|
| `fix/TcpServer.hpp` | `<cerrno>`, `<cstring>`, `<stdexcept>`, `<string>` | `errno`, `std::strerror`, `std::runtime_error`, `std::string` — lines 29, 42, 46 |
| `fix/FixIngressHandler.hpp` | `<algorithm>`, `<cstdint>` | `std::min` at 605, 639 |
| `fix/ClientSession.hpp`, `fix/ServerSession.hpp` | `<cstdint>` | fixed-width types throughout |
| `util/PhixeronCounters.hpp` | `<cstring>`, `<memory>` | `std::memcpy` at 40-41, `std::shared_ptr` at 36 |

`TcpServer.hpp` is the clearest case: it includes only `<arpa/inet.h>`, `<netinet/in.h>`,
`<sys/fcntl.h>`, `<unistd.h>` and `Logger.hpp`, and `Logger.hpp` supplies only
`<array> <cstdarg> <cstdint> <cstdio>`. `socket()` still resolves on glibc (via
`<netinet/in.h>` → `<sys/socket.h>`), but the four standard-library names do not.

Two non-standard spellings, both of which glibc keeps compatibility wrappers for, so they are
cosmetic rather than breaking: `<sys/fcntl.h>` (`fix/TcpServer.hpp:9`) should be `<fcntl.h>`, and
`<sys/poll.h>` should be `<poll.h>`.

**Format specifiers.** `%lld` / `%llu` are used against `std::int64_t` and SBE `sbePosition()`
values, which are `long` / `unsigned long` on LP64 Linux and `long long` on macOS. Same width, so
this works at runtime on both — and most sites already cast explicitly to `long long`
(`order/VenueExecId.hpp:43`, `fix/Session.hpp:993`, `replayer/client/ReplayerRecovery.hpp:401-406`,
`fix/FixConnection.hpp:647`), which is the right habit. But `util/Logger.hpp`'s variadic entry
points (`log`/`info`/`warn`/`error` at 194-222) carry no
`__attribute__((format(printf, N, N+1)))`, so **nothing checks any of it** on either platform.
Adding the attribute is the cheap way to find the sites that were missed — such as
`fix/FixIngressHandler.hpp:561`'s `%llu` — before a `-Wformat` sweep under GCC does.

## What a RHEL build needs

| | RHEL 9 | RHEL 10 |
|---|---|---|
| CMake ≥ 3.28 (§1b) | Kitware repo or `pip install cmake` — stock 3.26 is too old | stock 3.30 ✓ |
| C++23 compiler (§1c) | `gcc-toolset-14` or `llvm-toolset` — stock GCC 11 is too old | stock GCC 14 ✓ |
| ASan runtime | `libasan` (GCC) / `compiler-rt` (clang) | same |
| `nc`, `lsof` for the e2e scripts (§3) | AppStream | AppStream |

## Suggested order of work

1. **Done** — the compiler fork (§1a). Debug now builds with either GCC or clang; coverage is opt-in
   and forks on `CMAKE_CXX_COMPILER_ID`; `-march=native` is gone.
2. **Done** — path joining, all four sites (§2a–§2d), via `src/main/scripts/paths.sh` on the
   shell side and `Env.hpp::joinPath` on the C++ side.
3. **Done** — `AERON_DIR` against the platform default (§2e): `aeron_default_dir` in `paths.sh`,
   which every launcher and cleanup now calls.
4. **Done, as far as macOS can tell** — the missing includes (§4). They still have to be confirmed
   by an actual libstdc++ compile, so treat the list as a head start on "whatever the first RHEL
   build reports", not a completed sweep. The `format(printf)` attribute on `util/Logger` is
   deliberately **not** done: it is a diagnostics change with a `-Wformat` sweep behind it, not a
   portability break.
5. Deployment questions (§3) when there is a target host to answer them against.

§1b and §1c are deliberately **not** on this list: both were examined and left alone, for the
reasons recorded there. RHEL 9 is a non-stock-toolchain target; RHEL 10 is not.
