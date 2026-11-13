#!/bin/sh
# SPDX-FileCopyrightText: 2026 Nawer Kezal
# SPDX-License-Identifier: AGPL-3.0-only

# Phase 0 milestone, second half: 10 000 messages with a mix of outcomes,
# while ingest and then dispatch are killed (SIGKILL) and restarted.
#
# Needs the test bench to be up (make bench-up). Usage:
#
#   CONTAINER=podman hack/milestone.sh [messages]

set -eu

CONTAINER="${CONTAINER:-docker}"
PROJECT="${COMPOSE_PROJECT_NAME:-postier}"
N="${1:-10000}"
# The public development key of compose.yaml.
export POSTIER_API_KEY="${POSTIER_API_KEY:-pst_test_siyd5ddj_E8h8Jtb0LlgMfGZXzBmgyeX5diTL9iQQraCQV9EDJbU}"

name() {
  # docker compose and podman-compose name containers differently.
  for candidate in "${PROJECT}_$1_1" "${PROJECT}-$1-1"; do
    if "$CONTAINER" inspect "$candidate" >/dev/null 2>&1; then
      echo "$candidate"
      return
    fi
  done
  echo "milestone: container of service $1 not found" >&2
  exit 1
}

INGEST="$(name ingest)"
DISPATCH="$(name dispatch)"

chaos() {
  sleep 3
  echo ">>> killing ingest"
  "$CONTAINER" kill "$INGEST" >/dev/null
  sleep 4
  echo ">>> killing dispatch"
  "$CONTAINER" kill "$DISPATCH" >/dev/null
  sleep 8
  echo ">>> restarting ingest"
  "$CONTAINER" start "$INGEST" >/dev/null
  sleep 5
  echo ">>> restarting dispatch"
  "$CONTAINER" start "$DISPATCH" >/dev/null
}

chaos &
CHAOS=$!
status=0
go run ./test/milestone -n "$N" || status=$?
wait "$CHAOS" || true
exit "$status"
