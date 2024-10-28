FROM rust:1.82-alpine AS builder
RUN apk update && apk upgrade && apk add musl-dev
WORKDIR /app
COPY . /app
RUN cargo build --release

FROM rust:1.82-alpine AS release
RUN apk update && apk upgrade && apk add ca-certificates
RUN adduser -D -h /home/exporter exporter
USER exporter
COPY --from=builder /app/target/release/solana-validator-exporter /usr/local/bin
ENTRYPOINT ["solana-validator-exporter"]