"""Utilitas format keluaran terminal."""
from datetime import datetime


def waktu_sekarang() -> str:
    """Mengembalikan waktu lokal dalam format HH:MM:SS."""
    return datetime.now().strftime("%H:%M:%S")


def log(nama_service: str, pesan: str):
    """Mencetak log dengan timestamp dan nama service."""
    print(f"[{waktu_sekarang()}] [{nama_service}] {pesan}", flush=True)


def rupiah(angka: int) -> str:
    """Memformat angka sebagai Rupiah, contoh: 15000000 -> 'Rp15.000.000'."""
    return "Rp" + f"{angka:,}".replace(",", ".")