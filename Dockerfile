# syntax=docker/dockerfile:1

# Build stage. The service is pure Go (pgx, kafka-go, go-ais); CGO is
# disabled so the runtime image is distroless static, running as non-root.
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/geo-service

# Runtime target (distroless, non-root).
FROM gcr.io/distroless/static-debian12:nonroot AS geo-service
COPY --from=build /out/geo-service /geo-service
USER nonroot:nonroot
ENTRYPOINT ["/geo-service"]
