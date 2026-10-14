"""
Utilitas koneksi dan deklarasi topologi RabbitMQ.

Menyediakan fungsi untuk membuka koneksi, mendeklarasikan exchange dan queue,
serta properti standar untuk pesan persisten. Modul ini menggunakan library
pika sebagai klien AMQP 0-9-1.
"""
import sys

import pika

from common import config


def buka_koneksi():
    """
    Membuka koneksi ke RabbitMQ dan membuat satu channel.

    Menghentikan program dengan pesan error jika broker tidak dapat dihubungi.
    Mengembalikan tuple (koneksi, channel).
    """
    kredensial = pika.PlainCredentials(config.RABBITMQ_USER, config.RABBITMQ_PASS)
    parameter = pika.ConnectionParameters(
        host=config.RABBITMQ_HOST,
        port=config.RABBITMQ_PORT,
        credentials=kredensial,
    )
    try:
        koneksi = pika.BlockingConnection(parameter)
    except pika.exceptions.AMQPConnectionError:
        print(
            f"[ERROR] Tidak bisa terhubung ke RabbitMQ di "
            f"{config.RABBITMQ_HOST}:{config.RABBITMQ_PORT}.\n"
            f"        Pastikan RabbitMQ sudah berjalan (misalnya: docker compose up -d)."
        )
        sys.exit(1)

    # Channel adalah jalur komunikasi logis di atas satu koneksi TCP
    channel = koneksi.channel()
    return koneksi, channel


def deklarasi_exchange(channel):
    """Mendeklarasikan exchange utama. Aman dipanggil berulang kali (idempoten)."""
    channel.exchange_declare(
        exchange=config.EXCHANGE_NAME,
        exchange_type=config.EXCHANGE_TYPE,
        durable=True,  # tetap tersedia setelah broker dijalankan ulang
    )


def deklarasi_queue(channel, nama_queue):
    """
    Mendeklarasikan queue dan mengikatnya ke exchange sesuai config.TOPOLOGI.

    Aman dipanggil berulang kali (idempoten).
    """
    # Queue durable disimpan ke disk sehingga tidak hilang saat broker dijalankan ulang
    channel.queue_declare(queue=nama_queue, durable=True)

    for pola in config.TOPOLOGI[nama_queue]:
        channel.queue_bind(
            queue=nama_queue,
            exchange=config.EXCHANGE_NAME,
            routing_key=pola,
        )


def properti_persisten():
    """Properti pesan JSON dengan delivery_mode=2 agar pesan disimpan ke disk."""
    return pika.BasicProperties(
        content_type="application/json",
        delivery_mode=2,
    )