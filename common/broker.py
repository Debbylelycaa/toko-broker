"""
Fungsi bantu untuk berhubungan dengan RabbitMQ.

Langkah-langkahnya sama dengan contoh rabbitpy di slide 65-66:
  1. buat koneksi ke server RabbitMQ
  2. buat channel di atas koneksi itu
  3. deklarasikan exchange
  4. deklarasikan queue lalu bind (ikat) queue ke exchange dengan sebuah key
Bedanya, di sini kita memakai library `pika` (library Python untuk AMQP yang
dipakai di tutorial resmi RabbitMQ). Konsep dan urutannya tetap sama.
"""
import sys

import pika

from common import config


def buka_koneksi():
    """Membuka koneksi ke RabbitMQ dan membuat satu channel."""
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

    # Satu koneksi bisa berisi banyak channel (slide 64).
    channel = koneksi.channel()
    return koneksi, channel


def deklarasi_exchange(channel):
    """Membuat exchange jika belum ada. Aman dipanggil berkali-kali."""
    channel.exchange_declare(
        exchange=config.EXCHANGE_NAME,
        exchange_type=config.EXCHANGE_TYPE,
        durable=True,  # exchange tetap ada walaupun RabbitMQ di-restart
    )


def deklarasi_queue(channel, nama_queue):
    """Membuat queue dan mengikatnya ke exchange sesuai aturan di config.TOPOLOGI."""
    # durable=True -> queue disimpan di disk, tidak hilang saat broker restart.
    # Ini bagian penting dari komunikasi PERSISTEN (slide 21).
    channel.queue_declare(queue=nama_queue, durable=True)

    for pola in config.TOPOLOGI[nama_queue]:
        channel.queue_bind(
            queue=nama_queue,
            exchange=config.EXCHANGE_NAME,
            routing_key=pola,
        )


def properti_persisten():
    """Properti pesan: JSON dan persisten (delivery_mode=2 = disimpan ke disk)."""
    return pika.BasicProperties(
        content_type="application/json",
        delivery_mode=2,
    )
