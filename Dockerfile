# precog-node: the broker. Built from the workspace, run as a non-root user.
FROM rust:1.97.0-trixie AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p precog-mesh --bin precog-node

FROM debian:trixie-slim
COPY --from=build /src/target/release/precog-node /usr/local/bin/precog-node
USER 65532:65532
ENTRYPOINT ["precog-node"]
