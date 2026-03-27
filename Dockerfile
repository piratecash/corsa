FROM golang:1.26.1 AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/corsa-node ./cmd/corsa-node

FROM debian:bookworm-slim

RUN useradd --uid 10001 --create-home --shell /usr/sbin/nologin corsa

WORKDIR /home/corsa

COPY --from=builder /out/corsa-node /usr/local/bin/corsa-node

RUN mkdir -p /home/corsa/.corsa && chown -R corsa:corsa /home/corsa

USER corsa

ENV CORSA_LISTEN_ADDRESS=:64646
VOLUME ["/home/corsa/.corsa"]
EXPOSE 64646/tcp
EXPOSE 46464/tcp

ENTRYPOINT ["/usr/local/bin/corsa-node"]
