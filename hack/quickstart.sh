#!/bin/sh
# SPDX-FileCopyrightText: 2026 Nawer Kezal
# SPDX-License-Identifier: AGPL-3.0-only

# Postier on a local kind cluster, with Mailpit as the destination: the first
# email is visible in a browser a few minutes later, with no DNS and no
# public IP.
#
#   hack/quickstart.sh          create the cluster and install Postier
#   hack/quickstart.sh delete   remove the cluster
#
# It never touches your kubeconfig: the cluster's credentials are written to
# .quickstart/kubeconfig, and every command below uses that file.
#
# Requirements: go, helm, kubectl, and docker or podman.

set -eu

CLUSTER="postier-quickstart"
NAMESPACE="postier-system"
WORKDIR="$(pwd)/.quickstart"
export KUBECONFIG="$WORKDIR/kubeconfig"

if [ -z "${CONTAINER:-}" ]; then
  if command -v docker >/dev/null 2>&1; then CONTAINER=docker; else CONTAINER=podman; fi
fi
if [ "$CONTAINER" = "podman" ]; then
  export KIND_EXPERIMENTAL_PROVIDER=podman
fi

# Use the installed kind if there is one, the pinned version otherwise.
if command -v kind >/dev/null 2>&1; then
  KIND="kind"
else
  KIND="go run sigs.k8s.io/kind@v0.33.0"
fi

if [ "${1:-}" = "delete" ]; then
  $KIND delete cluster --name "$CLUSTER"
  rm -rf "$WORKDIR"
  exit 0
fi

mkdir -p "$WORKDIR"

# Fully qualified, so that docker and podman load it under the same name.
IMAGE="postier.local/postier:dev"

echo "==> Building the image"
"$CONTAINER" build -q --target postier -t "$IMAGE" . >/dev/null

echo "==> Creating the kind cluster"
if ! $KIND get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  $KIND create cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG" --wait 120s
else
  $KIND export kubeconfig --name "$CLUSTER" --kubeconfig "$KUBECONFIG"
fi

echo "==> Loading the image into the cluster"
rm -f "$WORKDIR/postier.tar"
"$CONTAINER" save "$IMAGE" -o "$WORKDIR/postier.tar"
$KIND load image-archive "$WORKDIR/postier.tar" --name "$CLUSTER"
rm -f "$WORKDIR/postier.tar"

echo "==> Generating the DKIM key of acme.test"
go run ./cmd/postier dkim --domain acme.test --selector pst1 \
  --out "$WORKDIR/acme.test.pst1.key" --if-missing >/dev/null
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NAMESPACE" create secret generic postier-dkim \
  --from-file=acme.test.pst1.key="$WORKDIR/acme.test.pst1.key" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

echo "==> Installing the chart"
helm upgrade --install postier deploy/helm/postier \
  --namespace "$NAMESPACE" -f deploy/helm/postier/values-quickstart.yaml \
  --wait --wait-for-jobs --timeout 10m

API_KEY="$(kubectl -n "$NAMESPACE" logs job/postier-seed -c seed 2>/dev/null | grep '^pst_' | tail -1 || true)"

cat <<MSG

Postier is running. In another terminal:

  export KUBECONFIG=$KUBECONFIG
  kubectl -n $NAMESPACE port-forward svc/postier-api 8080:8080 &
  kubectl -n $NAMESPACE port-forward svc/postier-mailpit 8025:8025 &

Then send your first email and watch it arrive at http://localhost:8025

  curl -X POST http://localhost:8080/v1/emails \\
    -H "Authorization: Bearer ${API_KEY:-<API key: kubectl -n $NAMESPACE logs job/postier-seed>}" \\
    -H "Content-Type: application/json" \\
    -d '{"from":"Acme <hello@acme.test>","to":["you@example.org"],"subject":"Hello","text":"It works."}'

Remove everything with: hack/quickstart.sh delete
MSG
