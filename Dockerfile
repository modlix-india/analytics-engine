# Static binary, no CGO, distroless base — the same shape as whatsapp-bridge, and for the same
# reason: nothing in the runtime image to patch, and nothing that can differ between the builder
# and the thing that actually runs.
#
# CGO_ENABLED=0 is a design constraint here, not a default. It is why the query layer hand-writes
# its aggregations over Parquet instead of embedding DuckDB, which needs cgo and would forfeit
# this. DuckDB still earns its place as a test-time oracle; see PLAN.md section 7.
# parquet-go ships hand-written assembly per architecture and needs no libc, so it is compatible
# with this and was chosen partly for that.
FROM golang:1.25-alpine AS build

WORKDIR /src

# Dependencies first, so a code change does not re-download the module graph on every build.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Reported on analytics_build_info, so what is running where is observable during a rollout
# rather than inferred from a deploy log.
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/engine ./cmd/engine

FROM gcr.io/distroless/static-debian12:nonroot

# The WAL and the local Parquet tier live here. A deployment mounts one volume at this path;
# the image itself carries no data and the container filesystem stays read-only in practice.
ENV ANALYTICS_DATA_DIR=/data

COPY --from=build /out/engine /engine

EXPOSE 8080

# nonroot, from the base image. The process needs to write only ANALYTICS_DATA_DIR, so the
# mounted volume is the one thing that has to be writable by this uid.
USER nonroot:nonroot

ENTRYPOINT ["/engine"]
