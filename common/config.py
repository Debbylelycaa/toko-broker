"""
Konfigurasi bersama seluruh service.

Berisi parameter koneksi RabbitMQ, nama exchange, routing key, nama queue,
dan aturan binding. Seluruh service mengacu ke modul ini agar penamaan
konsisten di semua komponen.
"""
import os

# Koneksi RabbitMQ (dapat ditimpa melalui environment variable)
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")

# Exchange bertipe topic: pesan dirutekan berdasarkan pola routing key
EXCHANGE_NAME = "toko"
EXCHANGE_TYPE = "topic"

# Routing key
RK_ORDER_CREATED = "order.created"      # dikirim oleh Order service
RK_PAYMENT_SUCCESS = "payment.success"  # dikirim oleh Pembayaran service saat pembayaran berhasil
RK_PAYMENT_FAILED = "payment.failed"    # dikirim oleh Pembayaran service saat pembayaran gagal

# Nama queue, satu untuk setiap consumer
QUEUE_GUDANG = "q_gudang"
QUEUE_NOTIFIKASI = "q_notifikasi"
QUEUE_PEMBAYARAN = "q_pembayaran"

# Aturan binding: queue -> daftar pola routing key yang diterima.
# Wildcard "*" mencocokkan tepat satu kata, misalnya "payment.*" mencocokkan
# "payment.success" dan "payment.failed".
TOPOLOGI = {
    QUEUE_GUDANG: [RK_ORDER_CREATED, RK_PAYMENT_FAILED],
    QUEUE_PEMBAYARAN: [RK_ORDER_CREATED],
    QUEUE_NOTIFIKASI: ["order.*", "payment.*"],
}