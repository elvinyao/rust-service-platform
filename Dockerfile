FROM rust:1.86-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --locked --release -p service-runtime --bin service-workflow

FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app
COPY --from=build /src/target/release/service-workflow /app/service-workflow
COPY config /app/config
ENV ADMIN_ADDR=0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["/app/service-workflow"]
