"""
Pembayaran service (consumer dan producer).

Memproses tagihan untuk setiap pesan "order.created" dari queue q_pembayaran
dengan memotong saldo dompet digital pelanggan, lalu mengirim hasilnya ke
exchange:
- payment.success : saldo mencukupi
- payment.failed  : saldo tidak mencukupi

Penggunaan:
    python -m pembayaran_service.consumer             # jeda pemrosesan 3 detik
    python -m pembayaran_service.consumer --delay 8   # jeda pemrosesan 8 detik
"""
import argparse
import time

from common import config
from common.broker import buka_koneksi, deklarasi_exchange, deklarasi_queue, properti_persisten
from common.message import marshal, unmarshal
from common.util import log, rupiah, waktu_sekarang

NAMA = "PEMBAYARAN"
JEDA_PROSES = 3  # detik; dapat diubah melalui argumen --delay

# Saldo awal dompet digital per pelanggan (simulasi, disimpan di memori)
SALDO = {
    "Inem": 50_000_000,
    "Siti": 20_000_000,
    "Rina": 5_000_000,
    "Budi": 1_000_000,
    "Andi": 500_000,
}
SALDO_PELANGGAN_BARU = 2_000_000  # saldo awal untuk pelanggan yang belum terdaftar


def proses_pembayaran(pelanggan: str, total: int):
    """
    Memotong saldo pelanggan jika mencukupi.

    Mengembalikan tuple (berhasil, keterangan).
    """
    saldo = SALDO.setdefault(pelanggan, SALDO_PELANGGAN_BARU)
    if saldo >= total:
        SALDO[pelanggan] = saldo - total
        return True, f"sisa saldo {rupiah(SALDO[pelanggan])}"
    return False, f"saldo tidak cukup (saldo {rupiah(saldo)}, tagihan {rupiah(total)})"


def proses_pesan(ch, method, properties, body):
    """Callback consumer: memproses tagihan dan mengirim hasil pembayaran."""
    try:
        pesanan = unmarshal(body)
        order_id = pesanan["order_id"]
        pelanggan = pesanan["pelanggan"]
        total = pesanan["harga"] * pesanan["jumlah"]
    except (ValueError, KeyError) as err:
        log(NAMA, f"Pesan rusak, dibuang: {err}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    log(NAMA, f"Pesanan #{order_id}: membuat tagihan {rupiah(total)} untuk {pelanggan}...")

    # Simulasi waktu pemrosesan pembayaran
    time.sleep(JEDA_PROSES)

    berhasil, keterangan = proses_pembayaran(pelanggan, total)
    routing_key = config.RK_PAYMENT_SUCCESS if berhasil else config.RK_PAYMENT_FAILED

    hasil = {
        "order_id": order_id,
        "pelanggan": pelanggan,
        "item": pesanan["item"],       # diperlukan Gudang service untuk pengembalian stok
        "jumlah": pesanan["jumlah"],
        "total": total,
        "status": "LUNAS" if berhasil else "GAGAL",
        "keterangan": keterangan,
        "waktu": waktu_sekarang(),
    }

    # Kirim hasil pembayaran ke exchange; penentuan penerima ditangani broker
    ch.basic_publish(
        exchange=config.EXCHANGE_NAME,
        routing_key=routing_key,
        body=marshal(hasil),
        properties=properti_persisten(),
    )
    log(NAMA, f"Pesanan #{order_id}: {hasil['status']}, {keterangan} -> kirim '{routing_key}'")

    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    global JEDA_PROSES
    parser = argparse.ArgumentParser(description="Pembayaran service (consumer)")
    parser.add_argument("--delay", type=float, default=3, help="lama simulasi proses pembayaran (detik)")
    JEDA_PROSES = parser.parse_args().delay

    koneksi, channel = buka_koneksi()
    deklarasi_exchange(channel)
    deklarasi_queue(channel, config.QUEUE_PEMBAYARAN)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=config.QUEUE_PEMBAYARAN, on_message_callback=proses_pesan)

    log(NAMA, f"Siap (jeda proses {JEDA_PROSES} detik).")
    log(NAMA, "Saldo awal: " + ", ".join(f"{k} {rupiah(v)}" for k, v in SALDO.items()))
    log(NAMA, f"Menunggu pesan di '{config.QUEUE_PEMBAYARAN}'... (Ctrl+C untuk berhenti)")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        koneksi.close()
        log(NAMA, "Berhenti.")


if __name__ == "__main__":
    main()