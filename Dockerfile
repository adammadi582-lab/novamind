# استخدام أحدث بيئة رسمية للغة Rust للبناء الآمن
FROM rust:1.75 AS builder

WORKDIR /app
COPY . .

# بناء السيرفر للإنتاج بأعلى أداء (Release Mode)
RUN cargo build --release

# استخراج النسخة النهائية في بيئة خفيفة وآمنة جداً
FROM debian:bookworm-slim

WORKDIR /app

# تثبيت حزم النظام الأساسية الضرورية لقاعدة البيانات والتشفير
RUN apt-get update && apt-get install -y \
    libssl-dev \
    ca-certificates \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# نقل السيرفر المبني من مرحلة البناء
COPY --from=builder /app/target/release/novamind-server /app/novamind-server

# فتح البورت 8080 للشبكة العالمية
EXPOSE 8080

# تشغيل السيرفر مباشرة
CMD ["./novamind-server"]