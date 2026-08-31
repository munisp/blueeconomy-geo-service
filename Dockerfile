# syntax=docker/dockerfile:1

# Build stage. The service is pure Go (pgx, kafka-go, go-ais); CGO is
# disabled so the runtime image is distroless static, running as non-root.
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/geo-service ./cmd/mrv-api

# Runtime target: geo-service (distroless, non-root).
FROM gcr.io/distroless/static-debian12:nonroot AS geo-service
COPY --from=build /out/geo-service /geo-service
USER nonroot:nonroot
ENTRYPOINT ["/geo-service"]

# Runtime target: mrv-api (Phase-8 MRV emissions boundary, /v1/mrv/* REST +
# mrv.* outbox publisher). Build with: docker build --target mrv-api .
# Configuration is MRV_* env, documented in README ("MRV API (mrv-api)").
FROM gcr.io/distroless/static-debian12:nonroot AS mrv-api
COPY --from=build /out/mrv-api /mrv-api
USER nonroot:nonroot
ENTRYPOINT ["/mrv-api"]
