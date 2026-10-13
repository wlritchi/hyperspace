#!/bin/sh
# Print the next version for a tag stream, from Conventional Commits.
#
#   next-version.sh <tag prefix> [path ...]
#
# The prefix is "" for X.Y.Z, or for example "protocol-v". With paths, only
# commits that change those paths count. Print nothing if there is nothing to
# release. A `feat` commit bumps the minor version, `!` or `BREAKING CHANGE`
# the major version (the minor version before 1.0.0), and other commits the
# patch version. The first release of a stream is 0.1.0.
set -eu
PREFIX="$1"
shift
LAST="$(git tag -l --merged HEAD "${PREFIX}*" | sed "s/^${PREFIX}//" \
  | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' | sort -V | tail -1 || true)"
if [ -n "${LAST}" ]; then
  RANGE="${PREFIX}${LAST}..HEAD"
else
  LAST=""
  RANGE="HEAD"
fi
SUBJECTS="$(git log --format=%s "${RANGE}" -- "$@")"
BODIES="$(git log --format=%B "${RANGE}" -- "$@")"
if [ -z "${SUBJECTS}" ]; then
  exit 0
fi
if [ -z "${LAST}" ]; then
  echo "0.1.0"
  exit 0
fi
MAJOR="${LAST%%.*}"; REST="${LAST#*.}"; MINOR="${REST%%.*}"; PATCH="${LAST##*.}"
BUMP="patch"
if printf '%s\n' "${SUBJECTS}" | grep -qE '^feat(\([^)]*\))?!?:'; then BUMP="minor"; fi
if printf '%s\n' "${SUBJECTS}" | grep -qE '^[a-z]+(\([^)]*\))?!:' \
    || printf '%s\n' "${BODIES}" | grep -q '^BREAKING CHANGE'; then
  BUMP="major"
fi
if [ "${MAJOR}" = 0 ] && [ "${BUMP}" = major ]; then BUMP="minor"; fi
case "${BUMP}" in
  major) echo "$((MAJOR + 1)).0.0" ;;
  minor) echo "${MAJOR}.$((MINOR + 1)).0" ;;
  patch) echo "${MAJOR}.${MINOR}.$((PATCH + 1))" ;;
esac
