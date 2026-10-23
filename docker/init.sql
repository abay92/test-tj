-- Skema tabel yang dipakai main.go (dijalankan otomatis oleh image postgres)
CREATE TABLE IF NOT EXISTS vehicles (
    id          BIGSERIAL PRIMARY KEY,
    vehicle_id  TEXT             NOT NULL,
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    timestamp   BIGINT           NOT NULL,
    created_at  TIMESTAMPTZ      NOT NULL DEFAULT now()
);

-- Mempercepat GET /vehicles/:vehicle_id/location (ORDER BY timestamp DESC LIMIT 1)
-- dan GET /vehicles/:vehicle_id/history (range timestamp)
CREATE INDEX IF NOT EXISTS idx_vehicles_vehicle_id_timestamp
    ON vehicles (vehicle_id, timestamp DESC);
