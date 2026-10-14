"""
SETUP BROKER - jalankan SEKALI sebelum service lain.

Script ini membuat exchange, semua queue, dan aturan binding di RabbitMQ.

Kenapa perlu? Kalau Order service mengirim pesan sebelum queue-nya ada,
exchange tidak punya tujuan dan pesan akan dibuang. Dengan menyiapkan queue
lebih dulu, pesan tetap tersimpan walaupun consumer-nya belum dinyalakan.

Cara menjalankan (dari folder utama proyek):
    python setup_broker.py
"""
from common import config
from common.broker import buka_koneksi, deklarasi_exchange, deklarasi_queue


def main():
    koneksi, channel = buka_koneksi()

    deklarasi_exchange(channel)
    print(f"Exchange '{config.EXCHANGE_NAME}' (tipe {config.EXCHANGE_TYPE}) siap.")

    for nama_queue, daftar_pola in config.TOPOLOGI.items():
        deklarasi_queue(channel, nama_queue)
        print(f"Queue '{nama_queue}' siap, menerima label: {', '.join(daftar_pola)}")

    koneksi.close()
    print("Broker siap dipakai.")


if __name__ == "__main__":
    main()
