"""
NOTIFIKASI SERVICE  (Consumer 2 / Subscriber)

Tanggung jawab:
  - Menerima pesan dari queue q_notifikasi
  - Menampilkan notifikasi untuk pelanggan

Queue ini berlangganan DUA topik sekaligus: "order.*" dan "payment.*".
Jadi notifikasi muncul saat pesanan dibuat, saat pembayaran lunas,
DAN saat pembayaran gagal (payment.success maupun payment.failed cocok dengan "payment.*").
Inilah contoh routing berdasarkan topik / publish-subscribe (slide 59).

Cara menjalankan (dari folder utama proyek):
    python -m notifikasi_service.consumer
"""
from common import config
from common.broker import buka_koneksi, deklarasi_exchange, deklarasi_queue
from common.message import unmarshal
from common.util import log, rupiah

NAMA = "NOTIFIKASI"


def proses_pesan(ch, method, properties, body):
    try:
        data = unmarshal(body)
    except ValueError as err:
        log(NAMA, f"Pesan rusak, dibuang: {err}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    # Label (routing key) memberi tahu pesan ini tentang apa.
    topik = method.routing_key

    if topik == config.RK_ORDER_CREATED:
        log(
            NAMA,
            f"Kepada {data['pelanggan']}: pesanan #{data['order_id']} "
            f"({data['jumlah']} {data['item']}) sudah kami terima.",
        )
    elif topik == config.RK_PAYMENT_SUCCESS:
        log(
            NAMA,
            f"Kepada {data['pelanggan']}: pembayaran pesanan #{data['order_id']} "
            f"sebesar {rupiah(data['total'])} BERHASIL.",
        )
    elif topik == config.RK_PAYMENT_FAILED:
        # Pesan ini sampai ke sini TANPA mengubah aturan binding,
        # karena pola "payment.*" sudah mencakup "payment.failed".
        log(
            NAMA,
            f"Kepada {data['pelanggan']}: pembayaran pesanan #{data['order_id']} "
            f"GAGAL, {data['keterangan']}. Pesanan dibatalkan.",
        )
    else:
        log(NAMA, f"Pesan dengan topik '{topik}': {data}")

    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    koneksi, channel = buka_koneksi()
    deklarasi_exchange(channel)
    deklarasi_queue(channel, config.QUEUE_NOTIFIKASI)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=config.QUEUE_NOTIFIKASI, on_message_callback=proses_pesan)

    pola = ", ".join(config.TOPOLOGI[config.QUEUE_NOTIFIKASI])
    log(NAMA, f"Siap. Berlangganan topik: {pola}")
    log(NAMA, f"Menunggu pesan di '{config.QUEUE_NOTIFIKASI}'... (Ctrl+C untuk berhenti)")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        koneksi.close()
        log(NAMA, "Berhenti.")


if __name__ == "__main__":
    main()
