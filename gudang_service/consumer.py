"""
GUDANG SERVICE  (Consumer 1 / Subscriber)

Tanggung jawab:
  - Menerima pesan dari queue q_gudang, yang berlangganan dua topik:
        "order.created"  -> kurangi stok (barang disisihkan untuk pesanan)
        "payment.failed" -> kembalikan stok (pesanan batal karena gagal bayar)

Urutan pesan aman: "order.created" selalu masuk ke q_gudang lebih dulu,
karena "payment.failed" baru dibuat setelah Pembayaran memproses pesanan itu.
Queue bersifat FIFO (yang masuk duluan, keluar duluan).

Cara menjalankan (dari folder utama proyek):
    python -m gudang_service.consumer
"""
from common import config
from common.broker import buka_koneksi, deklarasi_exchange, deklarasi_queue
from common.message import unmarshal
from common.util import log

NAMA = "GUDANG"

# Stok awal (disimpan di memori; akan kembali ke awal kalau service di-restart).
STOK = {
    "Laptop": 20,
    "Mouse": 50,
    "Keyboard": 30,
}

# Catatan pesanan yang stoknya sudah dikurangi: {order_id: (item, jumlah)}.
# Dipakai supaya stok hanya dikembalikan untuk pesanan yang memang sudah diambil.
SUDAH_DIAMBIL = {}


def kurangi_stok(pesanan: dict):
    order_id, item, jumlah = pesanan["order_id"], pesanan["item"], pesanan["jumlah"]
    stok_sekarang = STOK.get(item, 0)
    if jumlah <= stok_sekarang:
        STOK[item] = stok_sekarang - jumlah
        SUDAH_DIAMBIL[order_id] = (item, jumlah)
        log(NAMA, f"Pesanan #{order_id}: stok {item} berkurang {jumlah}, sisa {STOK[item]}")
    else:
        log(NAMA, f"Pesanan #{order_id}: stok {item} TIDAK CUKUP (diminta {jumlah}, ada {stok_sekarang})")


def kembalikan_stok(data: dict):
    order_id = data["order_id"]
    if order_id not in SUDAH_DIAMBIL:
        log(NAMA, f"Pesanan #{order_id}: pembayaran gagal, tidak ada stok yang perlu dikembalikan")
        return
    item, jumlah = SUDAH_DIAMBIL.pop(order_id)
    STOK[item] = STOK.get(item, 0) + jumlah
    log(NAMA, f"Pesanan #{order_id}: pembayaran gagal, stok {item} dikembalikan {jumlah}, sisa {STOK[item]}")


def proses_pesan(ch, method, properties, body):
    """Callback: dipanggil otomatis oleh broker setiap ada pesan baru (NOTIFY, slide 56)."""
    try:
        data = unmarshal(body)  # JSON bytes -> dict
        topik = method.routing_key  # label pesan: tentang apa pesan ini?

        if topik == config.RK_ORDER_CREATED:
            kurangi_stok(data)
        elif topik == config.RK_PAYMENT_FAILED:
            kembalikan_stok(data)
        else:
            log(NAMA, f"Topik '{topik}' tidak ditangani, diabaikan")
    except (ValueError, KeyError) as err:
        log(NAMA, f"Pesan rusak, dibuang: {err}")
        # Tolak dan jangan dimasukkan lagi ke queue, supaya tidak berulang terus.
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    # ACK: "pesan sudah saya proses, silakan dihapus dari queue" (slide 66: message.ack()).
    # Kalau service mati sebelum baris ini, broker akan mengirim ulang pesannya.
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    koneksi, channel = buka_koneksi()
    deklarasi_exchange(channel)
    deklarasi_queue(channel, config.QUEUE_GUDANG)

    # Ambil 1 pesan dulu, selesaikan, baru ambil berikutnya.
    channel.basic_qos(prefetch_count=1)

    # Daftarkan callback: broker akan "memanggil" proses_pesan saat ada pesan masuk.
    channel.basic_consume(queue=config.QUEUE_GUDANG, on_message_callback=proses_pesan)

    pola = ", ".join(config.TOPOLOGI[config.QUEUE_GUDANG])
    log(NAMA, f"Siap. Berlangganan topik: {pola}")
    log(NAMA, f"Stok awal: {STOK}")
    log(NAMA, f"Menunggu pesan di '{config.QUEUE_GUDANG}'... (Ctrl+C untuk berhenti)")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        koneksi.close()
        log(NAMA, "Berhenti.")


if __name__ == "__main__":
    main()
