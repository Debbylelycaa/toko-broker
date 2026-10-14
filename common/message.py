"""
Serialisasi pesan.

Seluruh pesan antar-service menggunakan format JSON dengan encoding UTF-8.
- marshal()   : dict -> bytes JSON, sebelum pesan dikirim ke broker
- unmarshal() : bytes JSON -> dict, setelah pesan diterima dari broker
"""
import json


def marshal(data: dict) -> bytes:
    """Mengubah dict menjadi bytes JSON (UTF-8)."""
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


def unmarshal(body: bytes) -> dict:
    """Mengubah bytes JSON (UTF-8) menjadi dict."""
    return json.loads(body.decode("utf-8"))