# Toko Broker

Sistem pemesanan toko online berbasis event dengan RabbitMQ sebagai message broker.
Setiap service berjalan sebagai proses terpisah dan hanya berkomunikasi secara
asinkron melalui broker, tanpa saling memanggil secara langsung.

Alur pemesanan mengikuti pola toko online: pesanan dicatat terlebih dahulu,
pembayaran diproses secara terpisah, lalu hasilnya disampaikan kepada pelanggan
melalui notifikasi.

## Arsitektur

```
Order service ──► [ RabbitMQ: exchange "toko" ] ──► q_gudang      ──► Gudang service
   (producer)                                    ──► q_pembayaran  ──► Pembayaran service ─┐
                                                 ──► q_notifikasi  ──► Notifikasi service   │
                                                          ▲                                 │
                                                          └── payment.success / failed ◄────┘
```

Order service mengirim pesan `order.created` ke exchange `toko` bertipe *topic*.
Exchange meneruskan pesan ke setiap queue yang pola binding-nya cocok. Pembayaran
service memproses tagihan, kemudian mengirim `payment.success` atau `payment.failed`
ke exchange yang sama.

### Routing key

| Routing key | Pengirim | Penerima |
|---|---|---|
| `order.created` | Order service | Gudang (mengurangi stok), Pembayaran (membuat tagihan), Notifikasi |
| `payment.success` | Pembayaran service | Notifikasi |
| `payment.failed` | Pembayaran service | Notifikasi, Gudang (mengembalikan stok) |

### Format pesan

Seluruh pesan menggunakan JSON (UTF-8). Contoh pesan `order.created`:

```json
{
  "order_id": "B3F824",
  "pelanggan": "Inem",
  "item": "Laptop",
  "jumlah": 2,
  "harga": 15000000,
  "waktu": "16:05:32"
}
```

Contoh pesan `payment.failed`:

```json
{
  "order_id": "2BEC2E",
  "pelanggan": "Budi",
  "item": "Laptop",
  "jumlah": 1,
  "total": 15000000,
  "status": "GAGAL",
  "keterangan": "saldo tidak cukup (saldo Rp1.000.000, tagihan Rp15.000.000)",
  "waktu": "16:52:56"
}
```

Format pesan dan routing key merupakan kontrak antar-service. Perubahan pada
keduanya harus disesuaikan di seluruh service yang terdampak.

## Struktur Proyek

```
toko-broker/
├── docker-compose.yml               Konfigurasi container RabbitMQ
├── requirements.txt                 Dependensi Python
├── setup_broker.py                  Inisialisasi exchange, queue, dan binding
├── cek_antrian.py                   Pemantauan jumlah pesan per queue
├── common/
│   ├── config.py                    Konfigurasi koneksi, routing key, dan topologi
│   ├── broker.py                    Utilitas koneksi dan deklarasi RabbitMQ
│   ├── message.py                   Serialisasi pesan JSON
│   └── util.py                      Utilitas log dan format Rupiah
├── order_service/producer.py        Producer: menerima dan mengirim pesanan
├── gudang_service/consumer.py       Consumer: mengelola stok
├── notifikasi_service/consumer.py   Consumer: menampilkan notifikasi
└── pembayaran_service/consumer.py   Consumer dan producer: memproses pembayaran
```

## Prasyarat

- Docker Desktop (atau Docker Engine dengan plugin Docker Compose)
- Python 3.8 atau lebih baru

## Instalasi

1. Clone repository:
   ```
   git clone https://github.com/Debbylelycaa/toko-broker.git
   cd toko-broker
   ```
2. Install dependensi Python:
   ```
   pip install -r requirements.txt
   ```
3. Jalankan RabbitMQ:
   ```
   docker compose up -d
   ```
   Tunggu 10–20 detik hingga RabbitMQ siap. Management UI tersedia di
   http://localhost:15672 (username `guest`, password `guest`).

## Menjalankan Aplikasi

Seluruh perintah dijalankan dari direktori root proyek. Service dijalankan sebagai
modul (`python -m`), sehingga gunakan tanda titik sebagai pemisah dan tanpa ekstensi `.py`.
Pada macOS/Linux, ganti `python` dengan `python3` jika diperlukan.

| Langkah | Terminal | Perintah |
|---|---|---|
| 1 | Terminal 1 | `python setup_broker.py` |
| 2 | Terminal 2 | `python -m gudang_service.consumer` |
| 3 | Terminal 3 | `python -m notifikasi_service.consumer` |
| 4 | Terminal 4 | `python -m pembayaran_service.consumer` |
| 5 | Terminal 1 | `python -m order_service.producer` |

Langkah 1 cukup dijalankan sekali, dan perlu dijalankan ulang setiap kali
`TOPOLOGI` di `common/config.py` berubah. Setiap service dapat dihentikan dengan `Ctrl+C`.

### Opsi tambahan

| Perintah | Keterangan |
|---|---|
| `python -m order_service.producer --demo N` | Mengirim N pesanan dengan data acak, lalu keluar |
| `python -m pembayaran_service.consumer --delay N` | Mengatur lama simulasi pemrosesan pembayaran (detik, bawaan 3) |
| `python cek_antrian.py` | Menampilkan jumlah pesan menunggu dan consumer aktif per queue |

### Menghentikan RabbitMQ

```
docker compose down       # menghentikan container, data queue tetap tersimpan
docker compose down -v    # menghentikan container dan menghapus seluruh data
```

## Konfigurasi

Parameter koneksi dapat diubah melalui environment variable:

| Variabel | Bawaan |
|---|---|
| `RABBITMQ_HOST` | `localhost` |
| `RABBITMQ_PORT` | `5672` |
| `RABBITMQ_USER` | `guest` |
| `RABBITMQ_PASS` | `guest` |

## Data Simulasi

Data berikut disimpan di memori masing-masing service dan kembali ke nilai awal
setiap kali service dijalankan ulang.

**Katalog** (`order_service/producer.py`)

| Produk | Harga |
|---|---|
| Laptop | Rp15.000.000 |
| Mouse | Rp150.000 |
| Keyboard | Rp450.000 |

**Stok awal** (`gudang_service/consumer.py`): Laptop 20, Mouse 50, Keyboard 30.

**Saldo dompet digital** (`pembayaran_service/consumer.py`)

| Pelanggan | Saldo awal |
|---|---|
| Inem | Rp50.000.000 |
| Siti | Rp20.000.000 |
| Rina | Rp5.000.000 |
| Budi | Rp1.000.000 |
| Andi | Rp500.000 |
| Pelanggan lain | Rp2.000.000 |

## Skenario Pengujian

**1. Pemrosesan asinkron.** Jalankan Pembayaran service dengan `--delay 8`, lalu kirim
beberapa pesanan. Order service tetap dapat menerima pesanan baru tanpa menunggu
pembayaran selesai, terlihat dari timestamp pada log.

**2. Persistensi pesan.** Hentikan Gudang service, lalu kirim tiga pesanan. Jalankan
`python cek_antrian.py` atau buka menu *Queues* di Management UI: `q_gudang` berisi
tiga pesan yang menunggu. Saat Gudang service dijalankan kembali, ketiga pesan
langsung diproses.

**3. Acknowledgement dan pengiriman ulang.** Jalankan Pembayaran service dengan
`--delay 10`, kirim satu pesanan, lalu hentikan service sebelum status pembayaran
muncul. Karena pesan belum di-*acknowledge*, broker mempertahankannya dan mengirim
ulang pesan tersebut saat service dijalankan kembali.

**4. Pembayaran gagal.** Kirim pesanan atas nama `Budi` untuk `1` `Laptop`. Pembayaran
service mengirim `payment.failed`, yang diterima oleh dua service sekaligus:
Notifikasi menampilkan pemberitahuan pembayaran gagal, dan Gudang mengembalikan stok
yang sebelumnya dikurangi.

**5. Urutan pesan.** Hentikan Gudang service, lalu ulangi skenario 4. `q_gudang` akan
berisi dua pesan (`order.created`, lalu `payment.failed`). Saat Gudang service dijalankan
kembali, stok dikurangi lalu dikembalikan sesuai urutan.

**6. Penambahan consumer baru.** Tambahkan queue baru beserta polanya ke `TOPOLOGI`
di `common/config.py`, buat service consumer baru, lalu jalankan `setup_broker.py`.
Order service tidak memerlukan perubahan apa pun.

## Pemecahan Masalah

| Gejala | Penyebab dan solusi |
|---|---|
| `ModuleNotFoundError: No module named 'common'` | Perintah tidak dijalankan dari direktori root atau tidak menggunakan `python -m`. Jalankan dari folder `toko-broker` dengan format `python -m nama_service.file`. |
| `Tidak bisa terhubung ke RabbitMQ` | Container belum berjalan atau belum siap. Periksa dengan `docker ps`, jalankan `docker compose up -d`, lalu tunggu beberapa detik. |
| `belum ada queue yang menampung pesan` | Topologi belum diinisialisasi. Jalankan `python setup_broker.py`. |
| Gudang tidak menerima `payment.failed` | Binding lama masih digunakan. Jalankan ulang `python setup_broker.py`. |
| Port 5672 atau 15672 sudah digunakan | Ada instance RabbitMQ lain yang berjalan. Hentikan instance tersebut atau ubah pemetaan port di `docker-compose.yml`. |

## Batasan

- Stok dan saldo disimpan di memori, belum menggunakan database.
- Jika stok tidak mencukupi tetapi pembayaran berhasil, pelanggan tetap ditagih.
  Penanganan ini dapat ditambahkan dengan routing key baru, misalnya
  `stock.unavailable`, yang memicu pengembalian dana di Pembayaran service.
- Koneksi menggunakan kredensial bawaan `guest` dan tanpa TLS, sehingga hanya
  ditujukan untuk lingkungan pengembangan lokal.
