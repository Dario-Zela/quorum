# Multi-stage build: static binaries on a distroless-ish base.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/quorum-server ./cmd/quorum-server && \
    CGO_ENABLED=0 go build -o /out/quorum-cli ./cmd/quorum-cli

FROM alpine:3.20
COPY --from=build /out/quorum-server /usr/local/bin/quorum-server
COPY --from=build /out/quorum-cli /usr/local/bin/quorum-cli
ENTRYPOINT ["quorum-server"]
