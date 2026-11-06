#!/usr/bin/env python3
"""hop-analyze.py — applies the latency part of Test 1's rule (doc/ec2-tests.md) to hop-matrix.sh's result files.

    hop-analyze.py [--margins P50,P99,P999] [--min-runs N] hop-*.txt

For each rate and each hosted mode, over the runs of both arms, a percentile passes when the hosted arm's median across
its runs lies within [min(direct) - margin, max(direct) + margin], the range the direct arm's own runs span. A pair
passes when all three do, no run failed or lost a marker, and each arm has at least --min-runs runs. Margins are in
microseconds (default 25,250,2000: the placeholders of the rule, which the owner sets before the first run).

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
                if not line or line.startswith("#"):
                    continue
                head = re.match(r"^mode=(\S+) repeat=\d+ leader=\d+ arm=(\S+) rate=(\d+)(?: payload=(\d+))?", line)
                values = dict(FIELD.findall(line))
                if head is None or "p50" not in values:
                    failed.append(line)
                    continue
                mode, _, rate, payload = head.groups()
                run = {key: float(value) for key, value in values.items()}
                run["expected"] = float(int(rate) * seconds)
                runs[(int(rate), int(payload or 18), mode)].append(run)
    return runs, failed


def span(values):
    return f"{min(values):.1f}-{max(values):.1f}"


def main(argv):
    margins = [25.0, 250.0, 2000.0]
    min_runs = 5
    paths = []
    args = iter(argv)
    for arg in args:
        if arg == "--margins":
            margins = [float(part) for part in next(args).split(",")]
        elif arg == "--min-runs":
            min_runs = int(next(args))
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
        for arm, arm_runs in (("direct", direct), (mode, hosted)):
            if len(arm_runs) < min_runs:
                problems.append(f"{arm} has {len(arm_runs)} runs, needs {min_runs}")
            lost = sum(run["seen"] < run["expected"] for run in arm_runs)
            if lost:
                problems.append(f"{arm}: {lost} run(s) lost markers")
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
        declines = sum(run.get("declines", 0) + run.get("window_declines", 0) for run in hosted)
        print(f"{'PASS' if not problems else 'FAIL'}  {label}  ({len(direct)} direct, {len(hosted)} hosted runs, "
              f"{declines:.0f} hosted declines)")
        for row in rows:
            print(f"        {row}")
        for problem in problems:
            print(f"        ! {problem}")
        ok = ok and not problems
    if not any(mode != "direct" for (_, _, mode) in runs):
        print("FAIL  no hosted runs")
        ok = False
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
