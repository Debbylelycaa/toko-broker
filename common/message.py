"""
Marshalling dan unmarshalling pesan (slide 7-11).

- marshal()   : data Python (dict)  -> JSON (bytes) yang bisa dikirim lewat jaringan
- unmarshal() : JSON (bytes)        -> data Python (dict) yang bisa dipakai program

Semua service sepakat memakai JSON. Inilah "common messaging protocol"
yang disebut di slide 59: semua aplikasi menyepakati format pesan yang sama.
"""
import json


def marshal(data: dict) -> bytes:
    """Mengemas dict menjadi JSON lalu mengubahnya ke bytes (socket/broker hanya mengirim bytes)."""
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


def unmarshal(body: bytes) -> dict:
    """Membuka kembali bytes JSON menjadi dict Python."""
    return json.loads(body.decode("utf-8"))
