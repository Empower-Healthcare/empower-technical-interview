# All Go binaries in one reproducible multi-stage build. Compose selects the
# binary per service with `command:`.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata && adduser -S -u 10001 parcellab
COPY --from=build /out/ /usr/local/bin/
USER parcellab
# Default is the API; other services override `command`.
CMD ["shipment-service"]
