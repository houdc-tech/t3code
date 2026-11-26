FROM rust:1.95-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release --bin self-hosted-relay

FROM debian:bookworm-slim
COPY --from=build /src/target/release/self-hosted-relay /usr/local/bin/self-hosted-relay
USER 65532:65532
ENV RELAY_CONFIG=/etc/relay.toml
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/self-hosted-relay"]
