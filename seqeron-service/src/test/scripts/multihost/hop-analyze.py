#!/usr/bin/env python3
"""hop-analyze.py — applies the latency part of Test 1's rule (doc/ec2-tests.md) to hop-matrix.sh's result files.

    hop-analyze.py [--margins P50,P99,P999] [--min-runs N] [--stall-us US] hop-*.txt

For each rate and each hosted mode, over the runs of both arms, a percentile passes when the hosted arm's median across
its runs lies within [min(direct) - margin, max(direct) + margin], the range the direct arm's own runs span. A pair
passes when all three do, no run failed or lost a marker, and each arm has at least --min-runs runs. Margins are in
microseconds (default 25,250,2000: the placeholders of the rule, which the owner sets before the first run).

A run whose p99.9 is at least --stall-us (default 5000) is a stall: the archive's write on a slow volume freezes the
sequencer for tens of ms (doc/ec2-tests.md, "Archive writes on EBS"), which is the volume's and not the arm's. A
stalled run is listed and left out of the comparison and of --min-runs, so a cell with stalls needs more runs, and
each arm's worst run is printed beside the median.

A "# order" line of the results that does not say order=PASS (the members' queue recordings were not one log) fails
the run set.

A run's hist= field (the bench's latencies as bucket:count pairs, 32 buckets to each power of two of the nanoseconds)
is merged across each arm's runs outside stalls, and the merged p50, p99, p99.9, p99.99 and max are printed beside the
medians of the runs' percentiles, which cannot show a tail rarer than one run's samples. They are reported, not
judged; a bucket's lower edge is the value, so they read up to 3% low.

Not evaluated here, since hop-matrix.sh does not record them: commands lost at a gate edge (H-17) and the host's
cores. Exits 1 when any pair fails, 2 on unreadable input.
"""

import re
import statistics
import sys
from collections import defaultdict

PERCENTILES = ("p50", "p99", "p99.9")
FIELD = re.compile(r"\b(n|seen|declines|window_declines|allocs|p50|p99|p99\.9|max)=([\d.]+)")


def parse(paths):
    """Returns {(rate, payload, mode): [run, ...]} and the list of lines that were not a run. A run's expected
    markers are its rate times the seconds the file's header names."""
    runs = defaultdict(list)
    failed = []
    for path in paths:
        seconds = 0
        with open(path) as file:
            for line in file:
                line = line.rstrip("\n")
                header = re.search(r"\bseconds=(\d+)", line) if line.startswith("# hop-matrix") else None
                if header:
                    seconds = int(header.group(1))
                if line.startswith("# order ") and "order=PASS" not in line:
                    failed.append(line)
                if not line or line.startswith("#"):
                    continue
                head = re.match(r"^mode=(\S+) repeat=\d+ leader=\d+ arm=(\S+) rate=(\d+)(?: payload=(\d+))?", line)
                values = dict(FIELD.findall(line))
                if head is None or "p50" not in values:
                    failed.append(line)
                    continue
                mode, _, rate, payload = head.groups()
                run = {key: float(value) for key, value in values.items()}
                hist = re.search(r"\bhist=(\S+)", line)
                if hist:
                    run["hist"] = {int(b): int(c) for b, c in (pair.split(":") for pair in hist.group(1).split(","))}
                run["expected"] = float(int(rate) * seconds)
                runs[(int(rate), int(payload or 18), mode)].append(run)
    return runs, failed


def bucket_floor_ns(bucket):
    if bucket < 32:
        return bucket
    log = bucket // 32 + 4
    return (32 + bucket % 32) << (log - 5)


def merged(arm_runs):
    """The runs' histograms summed, as {bucket: count}; empty when a run has none."""
    total = defaultdict(int)
    for run in arm_runs:
        for bucket, count in run.get("hist", {}).items():
            total[bucket] += count
    return total


def merged_line(arm_runs):
    total = merged(arm_runs)
    samples = sum(total.values())
    if not samples:
        return "no histograms"
    ordered = sorted(total.items())
    values = []
    for quantile in (0.5, 0.99, 0.999, 0.9999):
        rank, seen = quantile * samples, 0
        for bucket, count in ordered:
            seen += count
            if seen >= rank:
                values.append(bucket_floor_ns(bucket) / 1000.0)
                break
    values.append(bucket_floor_ns(ordered[-1][0]) / 1000.0)
    return "n={} p50 {:.1f} p99 {:.1f} p99.9 {:.1f} p99.99 {:.1f} max {:.1f}".format(samples, *values)


def span(values):
    return f"{min(values):.1f}-{max(values):.1f}"


def main(argv):
    margins = [25.0, 250.0, 2000.0]
    min_runs = 5
    stall_us = 5000.0
    paths = []
    args = iter(argv)
    for arg in args:
        if arg == "--margins":
            margins = [float(part) for part in next(args).split(",")]
        elif arg == "--min-runs":
            min_runs = int(next(args))
        elif arg == "--stall-us":
            stall_us = float(next(args))
        else:
            paths.append(arg)
    if len(margins) != 3 or not paths:
        print(__doc__, file=sys.stderr)
        return 2
    try:
        runs, failed = parse(paths)
    except OSError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2

    ok = not failed
    for line in failed:
        print(f"FAIL  run without a result: {line}")
    for (rate, payload, mode) in sorted(runs):
        if mode == "direct":
            continue
        direct = runs.get((rate, payload, "direct"), [])
        hosted = runs[(rate, payload, mode)]
        label = f"rate={rate} payload={payload} {mode}"
        if not direct:
            print(f"FAIL  {label}: no direct runs at this rate")
            ok = False
            continue
        problems = []
        stalled = {}
        worst = {}
        for arm, arm_runs in (("direct", direct), (mode, hosted)):
            lost = sum(run["seen"] < run["expected"] for run in arm_runs)
            if lost:
                problems.append(f"{arm}: {lost} run(s) lost markers")
            worst[arm] = max(run["p99.9"] for run in arm_runs)
            stalled[arm] = [run for run in arm_runs if run["p99.9"] >= stall_us]
        all_direct, all_hosted = direct, hosted
        direct = [run for run in direct if run["p99.9"] < stall_us]
        hosted = [run for run in hosted if run["p99.9"] < stall_us]
        for arm, arm_runs in (("direct", direct), (mode, hosted)):
            if len(arm_runs) < min_runs:
                problems.append(f"{arm} has {len(arm_runs)} runs outside stalls, needs {min_runs}")
        if not direct or not hosted:
            print(f"FAIL  {label}  (no run outside a stall: {len(stalled['direct'])} direct, "
                  f"{len(stalled[mode])} hosted stalled)")
            for problem in problems:
                print(f"        ! {problem}")
            ok = False
            continue
        rows = []
        for percentile, margin in zip(PERCENTILES, margins):
            low = min(run[percentile] for run in direct) - margin
            high = max(run[percentile] for run in direct) + margin
            median = statistics.median(run[percentile] for run in hosted)
            inside = low <= median <= high
            if not inside:
                problems.append(f"{percentile} {median:.1f} outside {low:.1f}..{high:.1f}")
            rows.append(
                f"{percentile} direct {span([run[percentile] for run in direct])} "
                f"hosted {span([run[percentile] for run in hosted])} "
                f"diff {median - statistics.median(run[percentile] for run in direct):+.1f}"
            )
        declines = sum(run.get("window_declines", 0) for run in all_hosted)
        print(f"{'PASS' if not problems else 'FAIL'}  {label}  ({len(direct)} direct, {len(hosted)} hosted runs "
              f"outside stalls; stalled {len(stalled['direct'])} and {len(stalled[mode])}; "
              f"{declines:.0f} hosted window declines)")
        print(f"        worst p99.9 direct {worst['direct']:.1f} hosted {worst[mode]:.1f}")
        for row in rows:
            print(f"        {row}")
        print(f"        merged direct {merged_line(direct)}")
        print(f"        merged hosted {merged_line(hosted)}")
        for problem in problems:
            print(f"        ! {problem}")
        ok = ok and not problems
    if not any(mode != "direct" for (_, _, mode) in runs):
        print("FAIL  no hosted runs")
        ok = False
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
