"""Fungsi bantu tampilan di terminal."""
from datetime import datetime


def waktu_sekarang() -> str:
    return datetime.now().strftime("%H:%M:%S")


def log(nama_service: str, pesan: str):
    """Mencetak log dengan jam, supaya saat demo terlihat urutan & jeda waktunya."""
    print(f"[{waktu_sekarang()}] [{nama_service}] {pesan}", flush=True)


def rupiah(angka: int) -> str:
    """15000000 -> 'Rp15.000.000'"""
    return "Rp" + f"{angka:,}".replace(",", ".")
