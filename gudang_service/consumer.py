"""
Gudang service (consumer).

Mengelola stok barang berdasarkan pesan dari queue q_gudang:
- order.created  : mengurangi stok sesuai pesanan
- payment.failed : mengembalikan stok pesanan yang pembayarannya gagal

Urutan pemrosesan terjaga karena queue bersifat FIFO: untuk pesanan yang sama,
"order.created" selalu masuk ke q_gudang sebelum "payment.failed".

Penggunaan:
    python -m gudang_service.consumer
"""
from common import config
from common.broker import buka_koneksi, deklarasi_exchange, deklarasi_queue
from common.message import unmarshal
from common.util import log

NAMA = "GUDANG"

# Stok awal. Disimpan di memori dan kembali ke nilai awal saat service dijalankan ulang.
STOK = {
    "Laptop": 20,
    "Mouse": 50,
    "Keyboard": 30,
}

# Pesanan yang stoknya telah dikurangi: {order_id: (item, jumlah)}.
# Digunakan agar pengembalian stok hanya dilakukan untuk pesanan yang tercatat.
SUDAH_DIAMBIL = {}


def kurangi_stok(pesanan: dict):
    """Mengurangi stok jika tersedia dan mencatat pesanan yang berhasil dialokasikan."""
    order_id, item, jumlah = pesanan["order_id"], pesanan["item"], pesanan["jumlah"]
    stok_sekarang = STOK.get(item, 0)
    if jumlah <= stok_sekarang:
        STOK[item] = stok_sekarang - jumlah
        SUDAH_DIAMBIL[order_id] = (item, jumlah)
        log(NAMA, f"Pesanan #{order_id}: stok {item} berkurang {jumlah}, sisa {STOK[item]}")
    else:
        log(NAMA, f"Pesanan #{order_id}: stok {item} TIDAK CUKUP (diminta {jumlah}, ada {stok_sekarang})")


def kembalikan_stok(data: dict):
    """Mengembalikan stok untuk pesanan yang pembayarannya gagal."""
    order_id = data["order_id"]
    if order_id not in SUDAH_DIAMBIL:
        log(NAMA, f"Pesanan #{order_id}: pembayaran gagal, tidak ada stok yang perlu dikembalikan")
        return
    item, jumlah = SUDAH_DIAMBIL.pop(order_id)
    STOK[item] = STOK.get(item, 0) + jumlah
    log(NAMA, f"Pesanan #{order_id}: pembayaran gagal, stok {item} dikembalikan {jumlah}, sisa {STOK[item]}")


def proses_pesan(ch, method, properties, body):
    """Callback consumer: memproses pesan berdasarkan routing key."""
    try:
        data = unmarshal(body)
        topik = method.routing_key

        if topik == config.RK_ORDER_CREATED:
            kurangi_stok(data)
        elif topik == config.RK_PAYMENT_FAILED:
            kembalikan_stok(data)
        else:
            log(NAMA, f"Topik '{topik}' tidak ditangani, diabaikan")
    except (ValueError, KeyError) as err:
        log(NAMA, f"Pesan rusak, dibuang: {err}")
        # Pesan tidak valid ditolak tanpa requeue agar tidak diproses berulang
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    # Acknowledge setelah pemrosesan selesai. Jika service berhenti sebelum
    # baris ini, broker akan mengirim ulang pesan tersebut.
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    koneksi, channel = buka_koneksi()
    deklarasi_exchange(channel)
    deklarasi_queue(channel, config.QUEUE_GUDANG)

    # Batasi satu pesan yang belum di-ack per consumer
    channel.basic_qos(prefetch_count=1)
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