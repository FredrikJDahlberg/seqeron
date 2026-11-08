#!/usr/bin/env python3
"""hop-order-check.py — compares the members' queue digests (QueueDigest, node.sh queue-digest).

    hop-order-check.py <digest-file>...

Each file holds one member's lines. Compares each member's active recording: the same first groupSeqNo, the same CRC at
every checkpoint they all hold, and, when they hold the same last groupSeqNo, the same frame count and CRC. Prints one
line and exits 0 when the members hold one log to the last frame, 3 when they agree to the last common checkpoint but
stopped at different frames (the line's min= is the last frame all hold: digest again up to it), and 1 on a divergence
or an unreadable digest.
"""

import re
import sys


def parse(path):
    with open(path) as file:
        text = file.read()
    if "ERROR" in text:
        return None, text.strip().replace("\n", " | ")
    active = [line for line in text.splitlines() if " active=true " in line]
    if not active:
        return None, "no active recording"
    fields = dict(re.findall(r"(\w+)=(\S*)", active[-1]))
    fields["ckpt"] = dict(part.split(":") for part in fields["ckpt"].split(",") if part)
    return fields, None


def main(paths):
    if len(paths) < 2:
        print(__doc__, file=sys.stderr)
        return 1
    digests = []
    for path in paths:
        digest, error = parse(path)
        if digest is None:
            print(f"order=FAIL {path}: {error}")
            return 1
        digests.append(digest)
    if len({digest["first"] for digest in digests}) != 1:
        print("order=FAIL first groupSeqNo differs: " + ",".join(digest["first"] for digest in digests))
        return 1
    common = set.intersection(*(set(digest["ckpt"]) for digest in digests))
    for seq in sorted(common, key=int):
        if len({digest["ckpt"][seq] for digest in digests}) != 1:
            print(f"order=FAIL members hold different frames at or before groupSeqNo {seq}")
            return 1
    lasts = [digest["last"] for digest in digests]
    if len(set(lasts)) == 1:
        if len({digest["crc"] for digest in digests}) != 1:
            print(f"order=FAIL members hold different frames at or before groupSeqNo {lasts[0]}")
            return 1
        print(f"order=PASS frames={digests[0]['frames']} last={lasts[0]} crc={digests[0]['crc']}")
        return 0
    print(f"order=LAGGING last={','.join(lasts)} min={min(map(int, lasts))} "
          f"agreed to checkpoint {max(common, key=int) if common else 'none'}")
    return 3


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
