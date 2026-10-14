"""
Pemantauan queue.

Menampilkan jumlah pesan yang menunggu dan jumlah consumer aktif
untuk setiap queue yang terdaftar di config.TOPOLOGI.

Penggunaan:
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
        # Channel terpisah per queue: deklarasi pasif terhadap queue yang
        # belum ada menyebabkan broker menutup channel tersebut.
        channel = koneksi.channel()
        try:
            # passive=True hanya memeriksa keberadaan queue tanpa membuatnya
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