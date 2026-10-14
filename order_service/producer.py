"""
Order service (producer).

Menerima input pesanan, menyusunnya menjadi pesan JSON, lalu mengirimnya ke
exchange dengan routing key "order.created". Service ini tidak bergantung pada
consumer mana pun; distribusi pesan sepenuhnya ditangani oleh broker.

Penggunaan:
    python -m order_service.producer            # mode interaktif
    python -m order_service.producer --demo 3   # mengirim 3 pesanan contoh
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

# Katalog produk dan harga satuan (Rupiah)
KATALOG = {
    "Laptop": 15_000_000,
    "Mouse": 150_000,
    "Keyboard": 450_000,
}


def buat_pesanan(pelanggan: str, item: str, jumlah: int) -> dict:
    """Menyusun data pesanan dengan ID unik dan harga dari katalog."""
    return {
        "order_id": uuid.uuid4().hex[:6].upper(),
        "pelanggan": pelanggan,
        "item": item,
        "jumlah": jumlah,
        "harga": KATALOG[item],
        "waktu": waktu_sekarang(),
    }


def kirim_pesanan(channel, pesanan: dict) -> bool:
    """
    Mengirim pesanan ke exchange sebagai pesan persisten.

    Mengembalikan True jika broker mengonfirmasi penerimaan, atau False jika
    pesan tidak dapat dirutekan ke queue mana pun maupun ditolak broker.
    """
    body = marshal(pesanan)
    try:
        channel.basic_publish(
            exchange=config.EXCHANGE_NAME,
            routing_key=config.RK_ORDER_CREATED,
            body=body,
            properties=properti_persisten(),
            mandatory=True,  # memicu error jika tidak ada queue yang cocok
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
    """Mencari nama produk di katalog tanpa membedakan huruf besar/kecil."""
    for nama in KATALOG:
        if nama.lower() == teks.strip().lower():
            return nama
    return None


def mode_interaktif(channel):
    """Membaca input pesanan dari terminal hingga pengguna mengetik 'exit'."""
    print("\n=== ORDER SERVICE ===")
    print("Katalog: " + ", ".join(f"{k} ({rupiah(v)})" for k, v in KATALOG.items()))
    print("Ketik 'exit' pada nama pelanggan untuk keluar.\n")

    while True:
        pelanggan = input("Nama pelanggan : ").strip()
        if pelanggan.lower() == "exit":
            break
        pelanggan = pelanggan.title()  # normalisasi agar cocok dengan data saldo
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
    """Mengirim sejumlah pesanan dengan data acak."""
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

    # Publisher confirms: broker mengonfirmasi setiap pesan yang berhasil diterima
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