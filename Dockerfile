FROM golang:1.24-alpine AS build

WORKDIR /src

# Cache the module download layer separately from the sources.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/remnanode-exporter ./cmd/remnanode-exporter

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

COPY --from=build /out/remnanode-exporter /usr/local/bin/remnanode-exporter

EXPOSE 9102
ENTRYPOINT ["/usr/local/bin/remnanode-exporter"]
