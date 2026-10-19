####################################################################################################
## Base image
####################################################################################################
FROM rust:1.99.0-slim AS chef
USER root
WORKDIR /app

# Install OS dependencies (musl target toolchain; the C compiler builds `ring`)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    musl-tools \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add x86_64-unknown-linux-musl

RUN cargo install cargo-chef --locked

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --target x86_64-unknown-linux-musl --recipe-path recipe.json
COPY . .
RUN cargo build --release --locked --target x86_64-unknown-linux-musl

####################################################################################################
## Final image
####################################################################################################
FROM scratch

WORKDIR /app

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/message-bridge /app/message-bridge

USER 100
EXPOSE 8000
CMD ["/app/message-bridge"]
