"""
Konfigurasi bersama untuk semua service.

File ini adalah "kesepakatan" antar service: alamat broker, nama exchange,
label (routing key), dan nama queue. Semua service membaca dari sini supaya
tidak ada salah ketik nama yang berbeda-beda.
"""
import os

# --- Alamat message broker (RabbitMQ) ---
# Port 5672 adalah port standar protokol AMQP (slide 64).
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")

# --- Exchange: "petugas sortir" di dalam broker ---
# Tipe "topic" = pesan diarahkan berdasarkan topik/label (slide 59).
EXCHANGE_NAME = "toko"
EXCHANGE_TYPE = "topic"

# --- Routing key: "label" yang ditempel di setiap pesan ---
RK_ORDER_CREATED = "order.created"      # dikirim Order service
RK_PAYMENT_SUCCESS = "payment.success"  # dikirim Pembayaran service jika lunas
RK_PAYMENT_FAILED = "payment.failed"    # dikirim Pembayaran service jika gagal

# --- Nama queue: "kotak surat" milik masing-masing consumer ---
QUEUE_GUDANG = "q_gudang"
QUEUE_NOTIFIKASI = "q_notifikasi"
QUEUE_PEMBAYARAN = "q_pembayaran"

# --- Aturan binding (bagian "Rules" pada arsitektur broker, slide 60) ---
# Artinya: queue X menerima pesan yang label-nya cocok dengan pola berikut.
# Tanda * berarti "satu kata apa saja", jadi "order.*" cocok dengan
# "order.created", "order.cancelled", dan seterusnya.
TOPOLOGI = {
    # Gudang: kurangi stok saat ada pesanan, kembalikan stok saat pembayaran gagal
    QUEUE_GUDANG: [RK_ORDER_CREATED, RK_PAYMENT_FAILED],
    QUEUE_PEMBAYARAN: [RK_ORDER_CREATED],
    # Notifikasi: "payment.*" otomatis mencakup payment.success DAN payment.failed
    QUEUE_NOTIFIKASI: ["order.*", "payment.*"],
}
