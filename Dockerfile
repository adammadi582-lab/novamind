FROM rust:latest AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/target/release/novamind /app/novamind
COPY --from=builder /app/index.html /app/index.html
EXPOSE 8080
CMD ["./novamind"]