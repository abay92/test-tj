#!/usr/bin/env bash
# Menjalankan mock publisher: kirim data lokasi kendaraan tiap 2 detik ke
# topik /fleet/vehicle/{vehicle_id}/location.
#
# Pemakaian:
#   ./scripts/mock.sh          # start + tampilkan log
#   ./scripts/mock.sh stop     # hentikan
#   ./scripts/mock.sh logs     # lihat log
set -euo pipefail

cd "$(dirname "$0")/.."

case "${1:-start}" in
  start)
    docker compose --profile mock up -d --build mock-publisher
    exec docker compose logs -f mock-publisher
    ;;
  stop)
    docker compose --profile mock stop mock-publisher
    ;;
  logs)
    exec docker compose logs -f mock-publisher
    ;;
  *)
    echo "Usage: $0 [start|stop|logs]" >&2
    exit 1
    ;;
esac
