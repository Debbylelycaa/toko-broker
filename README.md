# Toko Broker: Sistem Pemesanan dengan Message Broker

Aplikasi pemesanan toko online sederhana yang menerapkan **message broker (RabbitMQ)**,
mengikuti contoh arsitektur pada slide 61 materi *Chapter 04: Communication*.

Satu **producer** (Order service) mengirim pesanan ke broker, lalu broker meneruskannya ke
tiga **consumer** yang masing-masing punya tanggung jawab sendiri: Gudang, Notifikasi, dan Pembayaran.
Semua service ditulis dengan Python dan berkomunikasi **hanya lewat broker**.

Order service berperan seperti tombol *Checkout* di toko online, bukan kasir: pelanggan memesan,
lalu pembayaran diproses di belakang layar dan hasilnya dikabarkan lewat notifikasi.

```
Order service ──► [ RabbitMQ: exchange "toko" ] ──► q_gudang      ──► Gudang service
   (producer)                                    ──► q_pembayaran  ──► Pembayaran service ─┐
                                                 ──► q_notifikasi  ──► Notifikasi service   │
                                                          ▲                                 │
                                                          └── payment.success / failed ◄────┘
```

Label (routing key) yang dipakai dan siapa penerimanya:

| Label | Dikirim oleh | Diterima oleh |
|---|---|---|
| `order.created` | Order service | Gudang (kurangi stok), Pembayaran (buat tagihan), Notifikasi |
| `payment.success` | Pembayaran service | Notifikasi |
| `payment.failed` | Pembayaran service | Notifikasi, Gudang (kembalikan stok) |

## Struktur folder

```
toko-broker/
├── docker-compose.yml          menjalankan RabbitMQ
├── requirements.txt            library Python yang dibutuhkan (pika)
├── setup_broker.py             menyiapkan exchange, queue, dan binding (jalankan sekali)
├── cek_antrian.py              melihat jumlah pesan yang menunggu di tiap queue
├── common/
│   ├── config.py               alamat broker, nama exchange/queue, aturan binding
│   ├── broker.py               fungsi koneksi & deklarasi exchange/queue
│   ├── message.py              marshal (dict → JSON) dan unmarshal (JSON → dict)
│   └── util.py                 log bertimestamp & format rupiah
├── order_service/producer.py           Producer: menerima input pesanan
├── gudang_service/consumer.py          Consumer 1: kurangi stok, kembalikan stok jika gagal bayar
├── notifikasi_service/consumer.py      Consumer 2: menampilkan notifikasi
└── pembayaran_service/consumer.py      Consumer 3: cek saldo, kirim payment.success / payment.failed
```

## Persiapan

1. Install **Python 3.8+** dan **Docker Desktop**.
2. Buka terminal di folder `toko-broker`, lalu install library:
   ```
   pip install -r requirements.txt
   ```
3. Jalankan RabbitMQ:
   ```
   docker compose up -d
   ```
   Tunggu sekitar 10–20 detik. Dashboard bisa dibuka di http://localhost:15672
   (username `guest`, password `guest`).

   > Tanpa Docker: install Erlang lalu RabbitMQ dari situs resmi rabbitmq.com,
   > kemudian aktifkan dashboard dengan `rabbitmq-plugins enable rabbitmq_management`.

## Cara menjalankan

**Penting:** semua perintah dijalankan dari folder utama `toko-broker`, dan service dijalankan
dengan `python -m ...` (pakai titik, bukan garis miring). Di Windows gunakan `python`,
di Mac/Linux mungkin `python3`.

| Langkah | Terminal | Perintah |
|---|---|---|
| 1 | Terminal 1 | `python setup_broker.py` (cukup sekali) |
| 2 | Terminal 2 | `python -m gudang_service.consumer` |
| 3 | Terminal 3 | `python -m notifikasi_service.consumer` |
| 4 | Terminal 4 | `python -m pembayaran_service.consumer` |
| 5 | Terminal 1 | `python -m order_service.producer` |

Di Order service, isi nama pelanggan, nama barang (`Laptop`, `Mouse`, atau `Keyboard`), dan jumlah.
Ketik `exit` untuk keluar.

Pembayaran disimulasikan dengan saldo dompet digital (atur di `pembayaran_service/consumer.py`):

| Pelanggan | Saldo awal |
|---|---|
| Inem | Rp50.000.000 |
| Siti | Rp20.000.000 |
| Rina | Rp5.000.000 |
| Budi | Rp1.000.000 |
| Andi | Rp500.000 |
| nama lain | Rp2.000.000 |

Harga: Laptop Rp15.000.000, Mouse Rp150.000, Keyboard Rp450.000. Jadi Inem bisa membeli Laptop,
sedangkan Budi akan gagal membeli Laptop tapi berhasil membeli Mouse. Untuk mengirim beberapa pesanan contoh sekaligus:
`python -m order_service.producer --demo 3`.

Hentikan service mana pun dengan **Ctrl+C**.

## Skenario demo

**1. Komunikasi asinkron.** Jalankan Pembayaran dengan jeda lama:
`python -m pembayaran_service.consumer --delay 8`. Kirim beberapa pesanan. Perhatikan jam di log:
Order service langsung selesai dan bisa menerima pesanan baru, sementara Pembayaran
masih memproses satu per satu.

**2. Komunikasi persisten.** Matikan Gudang service (Ctrl+C). Kirim 3 pesanan. Jalankan
`python cek_antrian.py`, maka `q_gudang` berisi 3 pesan menunggu (atau lihat di dashboard,
menu *Queues*). Nyalakan lagi Gudang service, ketiga pesan langsung diproses.
Bandingkan dengan program socket chat (slide 40–48) yang langsung error kalau server mati.

**3. Toleransi kesalahan (ack).** Jalankan Pembayaran dengan `--delay 10`, kirim satu pesanan,
lalu tekan Ctrl+C sebelum muncul tulisan LUNAS. Karena pesan belum di-ack, broker menyimpannya.
Nyalakan lagi Pembayaran, pesanan yang sama diproses ulang dari awal.

**4. Pembayaran gagal dan publish-subscribe.** Pesan atas nama `Budi`, barang `Laptop`, jumlah `1`.
Pembayaran mengirim `payment.failed`. Pesan itu sampai ke dua service sekaligus: Notifikasi
menampilkan "pembayaran GAGAL" (tanpa mengubah aturan binding, karena `payment.*` sudah mencakupnya),
dan Gudang mengembalikan stok Laptop yang tadi sempat dikurangi. Pembayaran sendiri tidak tahu
siapa saja penerimanya; broker yang mengatur.

**5. Urutan pesan tetap terjaga.** Matikan Gudang, lalu ulangi skenario 4. Jalankan `cek_antrian.py`:
`q_gudang` berisi 2 pesan (`order.created` lalu `payment.failed`). Nyalakan Gudang, stok dikurangi
lalu dikembalikan sesuai urutan, karena queue bersifat FIFO.

**6. Decoupling (tambah service tanpa mengubah producer).** Salin `notifikasi_service` menjadi
service baru (misalnya `laporan_service`), ganti nama queue-nya, lalu tambahkan queue itu ke
`TOPOLOGI` di `common/config.py`. Order service tidak perlu diubah sama sekali.

## Pemetaan ke materi

| Konsep di slide | Wujudnya di kode |
|---|---|
| Persistent communication (slide 21) | `queue_declare(durable=True)` dan `delivery_mode=2` di `common/broker.py` |
| Asynchronous communication (slide 21, 23) | Order service tidak menunggu consumer; terlihat di skenario demo 1 |
| Operasi PUT (slide 56) | `channel.basic_publish(...)` di `order_service/producer.py` |
| Operasi NOTIFY (slide 56) | `channel.basic_consume(on_message_callback=...)` di setiap consumer |
| Queue manager (slide 57–58) | Server RabbitMQ |
| Message broker: routing per topik / publish-subscribe (slide 59) | Exchange tipe `topic`; satu pesan `payment.failed` diterima Notifikasi dan Gudang sekaligus; pola `payment.*` mencakup `payment.success` dan `payment.failed` |
| Arsitektur broker: *Rules* dan *Queuing layer* (slide 60) | `TOPOLOGI` di `common/config.py` (rules) dan queue di RabbitMQ (queuing layer) |
| Respons status penerimaan (slide 61, panah 3) | `channel.confirm_delivery()` di Order service |
| Marshalling / unmarshalling (slide 7–11) | `marshal()` dan `unmarshal()` di `common/message.py` |
| Connection & channel AMQP (slide 64–66) | `buka_koneksi()` di `common/broker.py` |
| `message.ack()` (slide 66) | `ch.basic_ack(...)` di setiap consumer |

## Masalah yang sering muncul

- **`ModuleNotFoundError: No module named 'common'`**: jalankan dari folder utama `toko-broker`
  dengan format `python -m nama_service.file` (tanpa `.py`).
- **`Tidak bisa terhubung ke RabbitMQ`**: RabbitMQ belum berjalan. Cek dengan `docker ps`
  atau buka dashboard di http://localhost:15672.
- **Order service menampilkan "belum ada queue yang menampung pesan"**: jalankan
  `python setup_broker.py` terlebih dahulu.
- **Stok atau saldo kembali ke awal**: stok (Gudang) dan saldo (Pembayaran) disimpan di memori,
  jadi kembali ke nilai awal setiap kali service dinyalakan ulang. Untuk pengembangan, keduanya bisa
  dipindah ke database.
- **Sudah pernah menjalankan versi lama?** Jalankan `python setup_broker.py` sekali lagi supaya
  `q_gudang` ikut berlangganan `payment.failed`.

## Batasan yang disengaja

Ini simulasi untuk mempelajari komunikasi lewat broker, jadi beberapa hal dibuat sederhana.
Contohnya, jika stok tidak cukup tetapi pembayaran berhasil, pelanggan tetap ditagih. Di sistem
sungguhan, Gudang bisa mengirim pesan seperti `stock.unavailable` agar Pembayaran mengembalikan dana.
Pola ini bisa ditambahkan dengan cara yang sama seperti `payment.failed`.
