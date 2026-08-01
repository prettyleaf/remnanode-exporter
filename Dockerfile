# The binary is pure Go, so the build stage always runs on the native
# architecture of the runner and cross-compiles. No QEMU, no per-arch runners.
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS build

WORKDIR /src

# Cache the module download layer separately from the sources.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/remnanode-exporter ./cmd/remnanode-exporter

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

COPY --from=build /out/remnanode-exporter /usr/local/bin/remnanode-exporter

EXPOSE 9102
ENTRYPOINT ["/usr/local/bin/remnanode-exporter"]
