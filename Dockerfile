# Build stage: pure Go, so the binary runs on an empty base image.
FROM golang:1.26-alpine AS build
ARG VERSION=dev
ARG COMMIT=unknown
RUN apk add --no-cache ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w -X github.com/genadigeno/data-feed/internal/buildinfo.Version=${VERSION} -X github.com/genadigeno/data-feed/internal/buildinfo.Commit=${COMMIT}" \
    -o /out/datafeed ./cmd/datafeed
# Scratch has no mkdir: prepare writable directories for the non-root user here.
RUN mkdir -p /out/data /out/spool && chown -R 65532:65532 /out/data /out/spool

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/datafeed /datafeed
COPY --from=build --chown=65532:65532 /out/data /data
COPY --from=build --chown=65532:65532 /out/spool /spool
USER 65532:65532
ENV DATAFEED_DATA_DIR=/data \
    DATAFEED_WORKER__SPOOL_PATH=/spool \
    DATAFEED_WORKER__CREDENTIAL_FILE=/spool/worker-credential
EXPOSE 8080 8081
ENTRYPOINT ["/datafeed"]
CMD ["serve", "--listen", "0.0.0.0:8080", "--worker-listen", "0.0.0.0:8081"]
