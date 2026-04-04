-- MobyDB Benchmark — PostGIS Setup
-- Run once on Railway PostgreSQL plugin after provisioning:
--   psql $DATABASE_URL -f postgis_setup.sql

-- ── Extensions ────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── Sensor readings table ─────────────────────────────────────────────────────
DROP TABLE IF EXISTS sensor_readings CASCADE;
CREATE TABLE sensor_readings (
  id          BIGSERIAL    PRIMARY KEY,
  sensor_id   UUID         NOT NULL,
  lat         DOUBLE PRECISION NOT NULL,
  lng         DOUBLE PRECISION NOT NULL,
  geom        GEOMETRY(Point, 4326) NOT NULL,
  recorded_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  epoch       INTEGER      NOT NULL,
  value       DOUBLE PRECISION NOT NULL,
  unit        VARCHAR(20)  DEFAULT 'kV'
);

-- ── Indexes — best practice, fairly tuned ─────────────────────────────────────
CREATE INDEX sr_geom_gist  ON sensor_readings USING GIST(geom);
CREATE INDEX sr_epoch      ON sensor_readings(epoch);
CREATE INDEX sr_sensor_id  ON sensor_readings(sensor_id);
CREATE INDEX sr_time_brin  ON sensor_readings USING BRIN(recorded_at);
-- Composite: most benchmark queries filter by both geom + epoch
CREATE INDEX sr_epoch_geom ON sensor_readings(epoch, id);

-- ── Districts table (for Q3 aggregation benchmark) ────────────────────────────
DROP TABLE IF EXISTS districts CASCADE;
CREATE TABLE districts (
  district_id VARCHAR(20) PRIMARY KEY,
  name        VARCHAR(100),
  geom        GEOMETRY(Polygon, 4326)
);
CREATE INDEX districts_geom ON districts USING GIST(geom);

-- ── Synthetic data generator ──────────────────────────────────────────────────
-- Generates N sensor readings inside the Italy bounding box
-- lat: 36.6..47.1 (Sicily to Alps)
-- lng:  6.6..18.5 (West to East coast)
-- Run at three scales:
--   SELECT generate_iot_data(100000, 100);   -- 100K: 100 sensors × 1000 epochs
--   SELECT generate_iot_data(1000000, 1000); -- 1M
--   SELECT generate_iot_data(10000000,1000); -- 10M

CREATE OR REPLACE FUNCTION generate_iot_data(
  total_records INTEGER,
  num_sensors   INTEGER
) RETURNS TEXT AS $$
DECLARE
  lat_min   CONSTANT DOUBLE PRECISION := 36.6;
  lat_max   CONSTANT DOUBLE PRECISION := 47.1;
  lng_min   CONSTANT DOUBLE PRECISION := 6.6;
  lng_max   CONSTANT DOUBLE PRECISION := 18.5;
  epochs    INTEGER := total_records / num_sensors;
  lat       DOUBLE PRECISION;
  lng       DOUBLE PRECISION;
  sid       UUID;
  sensor_lats DOUBLE PRECISION[];
  sensor_lngs DOUBLE PRECISION[];
  i         INTEGER;
  e         INTEGER;
BEGIN
  -- Pre-generate sensor positions (fixed per sensor)
  sensor_lats := ARRAY[]::DOUBLE PRECISION[];
  sensor_lngs := ARRAY[]::DOUBLE PRECISION[];

  FOR i IN 1..num_sensors LOOP
    sensor_lats := array_append(sensor_lats,
      lat_min + random() * (lat_max - lat_min));
    sensor_lngs := array_append(sensor_lngs,
      lng_min + random() * (lng_max - lng_min));
  END LOOP;

  -- Insert readings
  FOR i IN 1..num_sensors LOOP
    sid := uuid_generate_v4();
    lat := sensor_lats[i];
    lng := sensor_lngs[i];

    FOR e IN 1..epochs LOOP
      INSERT INTO sensor_readings
        (sensor_id, lat, lng, geom, recorded_at, epoch, value)
      VALUES (
        sid,
        lat + (random() - 0.5) * 0.001,  -- tiny GPS jitter
        lng + (random() - 0.5) * 0.001,
        ST_SetSRID(ST_MakePoint(
          lng + (random() - 0.5) * 0.001,
          lat + (random() - 0.5) * 0.001
        ), 4326),
        NOW() - (e * INTERVAL '1 hour'),
        e,
        220.0 + (random() - 0.5) * 40.0  -- voltage: 200-240 kV range
      );
    END LOOP;
  END LOOP;

  VACUUM ANALYZE sensor_readings;

  RETURN format('Generated %s records: %s sensors × %s epochs',
    total_records, num_sensors, epochs);
END;
$$ LANGUAGE plpgsql;

-- ── Palermo district polygons (for Q3) ────────────────────────────────────────
-- Approximate H3 Res-5 district polygons around Palermo for aggregation test
INSERT INTO districts (district_id, name, geom) VALUES
('palermo-centro',
 'Palermo Centro',
 ST_SetSRID(ST_MakePolygon(ST_GeomFromText(
   'LINESTRING(13.30 38.08, 13.42 38.08, 13.42 38.16, 13.30 38.16, 13.30 38.08)'
 )), 4326)),
('palermo-nord',
 'Palermo Nord',
 ST_SetSRID(ST_MakePolygon(ST_GeomFromText(
   'LINESTRING(13.28 38.16, 13.44 38.16, 13.44 38.24, 13.28 38.24, 13.28 38.16)'
 )), 4326)),
('palermo-sud',
 'Palermo Sud',
 ST_SetSRID(ST_MakePolygon(ST_GeomFromText(
   'LINESTRING(13.30 37.98, 13.42 37.98, 13.42 38.08, 13.30 38.08, 13.30 37.98)'
 )), 4326)),
('palermo-est',
 'Palermo Est',
 ST_SetSRID(ST_MakePolygon(ST_GeomFromText(
   'LINESTRING(13.42 38.05, 13.55 38.05, 13.55 38.18, 13.42 38.18, 13.42 38.05)'
 )), 4326)),
('palermo-ovest',
 'Palermo Ovest',
 ST_SetSRID(ST_MakePolygon(ST_GeomFromText(
   'LINESTRING(13.15 38.05, 13.30 38.05, 13.30 38.18, 13.15 38.18, 13.15 38.05)'
 )), 4326));

-- ── Quick verify ──────────────────────────────────────────────────────────────
SELECT
  'Setup complete' AS status,
  COUNT(*) AS sensor_readings,
  (SELECT COUNT(*) FROM districts) AS districts
FROM sensor_readings;

-- ── USAGE ─────────────────────────────────────────────────────────────────────
-- After running this file, load data at each scale:
--
-- Scale S (100K):
--   SELECT generate_iot_data(100000, 100);
--
-- Scale M (1M):
--   TRUNCATE sensor_readings;
--   SELECT generate_iot_data(1000000, 1000);
--
-- Scale L (10M):
--   TRUNCATE sensor_readings;
--   SELECT generate_iot_data(10000000, 1000);
--
-- Always run after loading:
--   VACUUM ANALYZE sensor_readings;
