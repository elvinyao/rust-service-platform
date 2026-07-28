FROM rust:1.86-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --locked --release -p fake-server --bin fake-server

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/fake-server /fake-server
EXPOSE 8090 8091 8092 8093
ENTRYPOINT ["/fake-server"]
