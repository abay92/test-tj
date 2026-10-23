# test-tj
Hasil Tes Teknis: Backend Engineer Transjakarta - Sistem Manajemen Armada

## Menjalankan dengan Docker

```bash
cp .env.example .env      # opsional, semua nilai punya default
docker compose up -d --build
docker compose ps         # semua service harus "healthy"
```

| Service    | Image / Build          | Port host                | Keterangan                          |
| ---------- | ---------------------- | ------------------------ | ----------------------------------- |
| app        | `./Dockerfile`         | `8080`                   | Gin API + subscriber MQTT           |
| postgres   | `postgres:17-alpine`   | `5433`                   | skema otomatis dari `docker/init.sql` |
| rabbitmq   | `rabbitmq:4-management-alpine` | `5672`, `15673`  | UI: http://localhost:15673 (guest/guest) |
| mosquitto  | `eclipse-mosquitto:2`  | `1883`                   | MQTT broker                         |

Endpoint: `GET /health`, `GET /vehicles/:vehicle_id/location`,
`GET /vehicles/:vehicle_id/history?start=<unix>&end=<unix>`.

`/health` mengecek tiga komponen sekaligus (PostgreSQL, RabbitMQ, MQTT) dan
mengembalikan `503` bila ada yang bermasalah:

```json
{"checks":{"database":"ok","mqtt":"ok","rabbitmq":"ok"},"status":"ok"}
```

### Ketahanan koneksi

- **RabbitMQ**: koneksi dibuka ulang otomatis saat broker restart. Publish yang
  gagal karena koneksi mati akan dicoba sekali lagi setelah reconnect, dan
  `/health` ikut memicu reconnect sehingga status pulih sendiri.
- **MQTT**: `auto reconnect` aktif; subscription `fleet/vehicle/+/location`
  dipasang ulang di `OnConnectHandler` setiap kali (re)connect.
- **Pesan geofence** dikirim dengan `DeliveryMode: persistent` agar tidak hilang
  saat broker restart.

### Mock publisher (kirim data tiap 2 detik)

Script mengirim data lokasi kendaraan ke topik
`/fleet/vehicle/{vehicle_id}/location` **setiap 2 detik**:

```bash
./scripts/mock.sh          # build + start + tampilkan log
./scripts/mock.sh stop     # hentikan
```

Atau lewat compose langsung:

```bash
docker compose --profile mock up -d --build
docker compose logs -f mock-publisher
```

Konfigurasi lewat `.env`:

| ENV | Default | Keterangan |
| --- | --- | --- |
| `MQTT_TOPIC_TEMPLATE` | `/fleet/vehicle/{vehicle_id}/location` | Template topik (wajib ada `{vehicle_id}`) |
| `MOCK_VEHICLE_IDS` | `B1234XYZ,B5678ABC` | Daftar ID kendaraan, pisah koma |
| `MOCK_INTERVAL` | `2s` | Interval pengiriman |

Data mock bergerak 15 m → 35 m → 55 m → 120 m dari pusat geofence, sehingga
bergantian masuk/keluar radius dan memicu alert. `vehicle_id` di payload selalu
sama dengan segmen topik (sesuai validasi aplikasi).

> Aplikasi subscribe ke **dua format** topik: `/fleet/vehicle/+/location`
> (sesuai requirement) dan `fleet/vehicle/+/location` (format awal), jadi
> publisher lama tetap kompatibel.

## Konfigurasi geofence

Titik pusat dan radius bisa diubah lewat `.env` (default: Monas Jakarta, 50 m):

```env
GEOFENCE_LAT=-6.1753924
GEOFENCE_LNG=106.8271528
GEOFENCE_RADIUS=50
```

Contoh pindah geofence ke Bandung dengan radius 200 m:

```bash
GEOFENCE_LAT=-6.917464 GEOFENCE_LNG=107.619123 GEOFENCE_RADIUS=200 \
  docker compose up -d
docker compose logs app | grep "Geofence aktif"
```

Nilai ENV yang tidak valid (mis. `GEOFENCE_RADIUS=abc`) akan diabaikan dan
memakai default, dengan peringatan di log.

Uji kirim data lokasi (topik `fleet/vehicle/<id>/location`):

```bash
docker compose exec mosquitto mosquitto_pub \
  -h 127.0.0.1 -t 'fleet/vehicle/B1234XYZ/location' \
  -m "{\"vehicle_id\":\"B1234XYZ\",\"latitude\":-6.1753924,\"longitude\":106.8271528,\"timestamp\":$(date +%s)}"

curl http://localhost:8080/vehicles/B1234XYZ/location
```

Hentikan: `docker compose down` (tambah `-v` bila ingin menghapus data).

> **Catatan port RabbitMQ:** jika `15672` sudah dipakai RabbitMQ lain (mis. install
> `brew services` di macOS), UI container dipublikasikan di **15673**. Ubah lewat
> `RABBITMQ_UI_PORT` di `.env` setelah port tersebut dikosongkan.
