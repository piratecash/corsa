FROM golang:1.26.1 AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/corsa-node ./cmd/corsa-node

FROM debian:bookworm-slim

RUN useradd --create-home --shell /usr/sbin/nologin corsa

WORKDIR /app

COPY --from=builder /out/corsa-node /usr/local/bin/corsa-node

RUN mkdir -p /app/.corsa && chown -R corsa:corsa /app

USER corsa

ENV CORSA_LISTEN_ADDRESS=:64646
EXPOSE 64646/tcp

ENTRYPOINT ["/usr/local/bin/corsa-node"]
