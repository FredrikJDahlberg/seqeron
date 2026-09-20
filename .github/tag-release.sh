#!/usr/bin/env bash
# tag-release.sh <version> — write VERSION, commit it, and push the v<version> tag.
#
#   tag-release.sh 0.6.1

set -euo pipefail

RELEASE="${1:-}"

if [[ ! "$RELEASE" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "usage: $0 <major.minor.patch>" >&2
  exit 1
fi

echo "$RELEASE" > VERSION
git commit -am "Release $RELEASE"
git push origin main
git tag "v$RELEASE"
git push origin "v$RELEASE"
