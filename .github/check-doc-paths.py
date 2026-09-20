#!/usr/bin/env python3
# Fails when a doc or comment names a repo path that does not exist.
#
# The module split moved every source path in the tree and the prose kept the old ones, so the
# README's quickstart pointed at a script nobody had. Nothing caught it: the build does not read
# prose. This does, and runs in CI.
#
# Two rules. A path starting with a tracked top-level directory must resolve. A path starting with
# `src/` or `cluster/src/` is a pre-split leftover — those trees live under seqeron-client/ and
# seqeron-service/ now. Paths relative to a package (`protocol/PortLayout.hpp`) are shorthand for a
# class, not a claim about the filesystem, and are left alone.
#
# Run: python3 .github/check-doc-paths.py

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCAN_SUFFIXES = {".md", ".java", ".hpp", ".cpp", ".sh"}

# Cited on purpose: they name the product repo's files, which are not in this tree.
FOREIGN = ("aeron-archive/", "aeron-cluster/", "aeron-client/")

# A review log of resolved items: it names files by the version that removed them, on purpose.
SKIP_FILES = {"doc/package.md"}

PRE_SPLIT = re.compile(r"(?:cluster/)?src/(?:main|test)/")
# A path-shaped token: bare, or fenced in backticks/braces. Trailing punctuation and :line suffixes
# are trimmed by strip_token.
TOKEN = re.compile(r"[A-Za-z0-9_.][A-Za-z0-9_./-]*/[A-Za-z0-9_./-]+")


def tracked():
    out = subprocess.run(
        ["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.split()
    return [f for f in out if "generated" not in f]


def strip_token(tok):
    tok = re.sub(r":[0-9]+(-[0-9]+)?$", "", tok)  # file.cpp:668-696
    if tok.startswith("./"):  # `./src/main/scripts/x.sh` as typed at a shell
        tok = tok[2:]
    return tok.rstrip(".,;:)»\"'")


def main():
    files = tracked()
    top = {f.split("/")[0] for f in files}
    failures = []

    for rel in files:
        if Path(rel).suffix not in SCAN_SUFFIXES or rel in SKIP_FILES:
            continue
        for lineno, line in enumerate(
            (ROOT / rel).read_text(encoding="utf-8").splitlines(), 1
        ):
            for m in TOKEN.finditer(line):
                tok = strip_token(m.group(0))
                if tok.startswith(FOREIGN) or "phixeron" in tok:
                    continue
                # `<module>/src/main/...` is a template for both modules, not a path.
                placeholder = line[max(0, m.start() - 2) : m.start()] in (">/", "}/")
                if PRE_SPLIT.match(tok) and not placeholder:
                    failures.append(
                        (rel, lineno, tok, "pre-split path — prefix the module directory")
                    )
                elif tok.split("/")[0] in top and not (ROOT / tok).exists():
                    failures.append((rel, lineno, tok, "no such file or directory"))

    for rel, lineno, tok, why in failures:
        print(f"{rel}:{lineno}: {tok} — {why}")
    if failures:
        print(f"\n{len(failures)} broken path reference(s).", file=sys.stderr)
        return 1
    print(f"OK — every repo path named in {len(files)} tracked files resolves.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
