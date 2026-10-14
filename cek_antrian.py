"""
CEK ANTRIAN - melihat berapa pesan yang sedang menunggu di setiap queue.

Berguna saat demo komunikasi persisten: matikan satu consumer, kirim pesanan,
lalu jalankan script ini untuk membuktikan pesannya tersimpan di broker.

Cara menjalankan:
    python cek_antrian.py
"""
import pika

from common import config
from common.broker import buka_koneksi


def main():
    koneksi, _ = buka_koneksi()
    print(f"{'Queue':<15} {'Pesan menunggu':>15} {'Consumer aktif':>15}")
    print("-" * 47)

    for nama_queue in config.TOPOLOGI:
        # Pakai channel baru untuk tiap queue, karena kalau queue belum ada,
        # broker akan menutup channel yang dipakai.
        channel = koneksi.channel()
        try:
            # passive=True -> hanya mengecek, tidak membuat queue baru
            hasil = channel.queue_declare(queue=nama_queue, passive=True)
            print(
                f"{nama_queue:<15} {hasil.method.message_count:>15} "
                f"{hasil.method.consumer_count:>15}"
            )
            channel.close()
        except pika.exceptions.ChannelClosedByBroker:
            print(f"{nama_queue:<15} belum dibuat (jalankan setup_broker.py)")

    koneksi.close()


if __name__ == "__main__":
    main()
