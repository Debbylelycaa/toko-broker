"""
Inisialisasi topologi broker.

Mendeklarasikan exchange, seluruh queue, dan binding sesuai config.TOPOLOGI.
Jalankan sekali sebelum service lain, dan jalankan ulang setiap kali
TOPOLOGI berubah.

Queue perlu tersedia sebelum producer mengirim pesan, karena pesan yang
tidak cocok dengan binding mana pun akan dibuang oleh exchange.

Penggunaan:
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