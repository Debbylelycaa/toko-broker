"""
ORDER SERVICE  (Producer / Publisher)

Tanggung jawab:
  - Menerima input pesanan dari user
  - Mengemas pesanan menjadi JSON (marshalling)
  - Mengirim pesanan ke broker dengan label "order.created"

Order service TIDAK tahu siapa saja yang akan menerima pesannya.
Ia hanya tahu alamat broker. Setelah broker menyatakan pesan diterima,
Order service langsung lanjut melayani pesanan berikutnya (asinkron).

Cara menjalankan (dari folder utama proyek):
    python -m order_service.producer            # mode interaktif
    python -m order_service.producer --demo 3   # kirim 3 pesanan contoh otomatis
"""
import argparse
import random
import uuid

import pika

from common import config
from common.broker import buka_koneksi, deklarasi_exchange, properti_persisten
from common.message import marshal
from common.util import log, rupiah, waktu_sekarang

NAMA = "ORDER"

# Daftar barang dan harganya (sederhana, tanpa database)
KATALOG = {
    "Laptop": 15_000_000,
    "Mouse": 150_000,
    "Keyboard": 450_000,
}


def buat_pesanan(pelanggan: str, item: str, jumlah: int) -> dict:
    """Membuat objek pesanan (masih berupa dict Python biasa)."""
    return {
        "order_id": uuid.uuid4().hex[:6].upper(),
        "pelanggan": pelanggan,
        "item": item,
        "jumlah": jumlah,
        "harga": KATALOG[item],
        "waktu": waktu_sekarang(),
    }


def kirim_pesanan(channel, pesanan: dict) -> bool:
    """PUT: mengirim pesan ke broker (operasi PUT pada slide 56)."""
    body = marshal(pesanan)  # dict -> JSON bytes
    try:
        channel.basic_publish(
            exchange=config.EXCHANGE_NAME,
            routing_key=config.RK_ORDER_CREATED,
            body=body,
            properties=properti_persisten(),
            mandatory=True,  # beri tahu kami kalau tidak ada queue yang menampung
        )
    except pika.exceptions.UnroutableError:
        log(NAMA, "GAGAL: belum ada queue yang menampung pesan. Jalankan setup_broker.py dulu.")
        return False
    except pika.exceptions.NackError:
        log(NAMA, "GAGAL: broker menolak pesan.")
        return False

    log(
        NAMA,
        f"Pesanan #{pesanan['order_id']} ({pesanan['jumlah']} {pesanan['item']}, "
        f"{pesanan['pelanggan']}) diterima broker. Lanjut melayani pesanan berikutnya.",
    )
    return True


def cari_item(teks: str):
    """Mencocokkan input user dengan katalog tanpa peduli huruf besar/kecil."""
    for nama in KATALOG:
        if nama.lower() == teks.strip().lower():
            return nama
    return None


def mode_interaktif(channel):
    print("\n=== ORDER SERVICE ===")
    print("Katalog: " + ", ".join(f"{k} ({rupiah(v)})" for k, v in KATALOG.items()))
    print("Ketik 'exit' pada nama pelanggan untuk keluar.\n")

    while True:
        pelanggan = input("Nama pelanggan : ").strip()
        if pelanggan.lower() == "exit":
            break
        pelanggan = pelanggan.title()  # "inem" -> "Inem", supaya cocok dengan data saldo
        if not pelanggan:
            print("  Nama tidak boleh kosong.\n")
            continue

        item = cari_item(input("Nama barang    : "))
        if item is None:
            print(f"  Barang tidak ada di katalog. Pilih: {', '.join(KATALOG)}\n")
            continue

        try:
            jumlah = int(input("Jumlah         : "))
            if jumlah <= 0:
                raise ValueError
        except ValueError:
            print("  Jumlah harus angka lebih dari 0.\n")
            continue

        kirim_pesanan(channel, buat_pesanan(pelanggan, item, jumlah))
        print()


def mode_demo(channel, banyak: int):
    """Mengirim beberapa pesanan contoh sekaligus (praktis untuk demo)."""
    nama_contoh = ["Inem", "Budi", "Siti", "Andi", "Rina"]
    for _ in range(banyak):
        pesanan = buat_pesanan(
            random.choice(nama_contoh),
            random.choice(list(KATALOG)),
            random.randint(1, 3),
        )
        kirim_pesanan(channel, pesanan)


def main():
    parser = argparse.ArgumentParser(description="Order service (producer)")
    parser.add_argument("--demo", type=int, metavar="N", help="kirim N pesanan contoh lalu keluar")
    args = parser.parse_args()

    koneksi, channel = buka_koneksi()
    deklarasi_exchange(channel)

    # Publisher confirm: broker membalas "pesan sudah saya terima & simpan".
    # Ini adalah respons opsional "status penerimaan" pada slide 61 (panah nomor 3).
    channel.confirm_delivery()

    try:
        if args.demo:
            mode_demo(channel, args.demo)
        else:
            mode_interaktif(channel)
    except KeyboardInterrupt:
        print()
    finally:
        koneksi.close()
        log(NAMA, "Koneksi ditutup.")


if __name__ == "__main__":
    main()
