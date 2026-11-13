# SPDX-FileCopyrightText: 2026 Nawer Kezal
# SPDX-License-Identifier: AGPL-3.0-only

# One image, one static binary, several roles (postier api | dispatch | …).
# The smtpsink target is a test tool and is never published.

FROM --platform=$BUILDPLATFORM docker.io/library/golang:1.27 AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w -X main.version=$VERSION" -o /out/ ./cmd/postier ./test/smtpsink

FROM gcr.io/distroless/static-debian12:nonroot AS smtpsink
COPY --from=build /out/smtpsink /smtpsink
ENTRYPOINT ["/smtpsink"]

# Last stage: the default target.
FROM gcr.io/distroless/static-debian12:nonroot AS postier
COPY --from=build /out/postier /postier
ENTRYPOINT ["/postier"]
