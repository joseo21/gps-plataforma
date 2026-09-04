-- =====================================================================
-- Esquema historico GpsSinergy  -  Aurora PostgreSQL 16
-- Dimensionado para 2049 equipos reportando cada 15 s:
--   11.800.000 registros/dia  |  136 inserciones/s  |  4.300M/anio
--
-- Decision central: los IO que se consultan siempre van en COLUMNAS
-- tipadas, no en jsonb. Guardar {"External Voltage": 27.4} 4.300
-- millones de veces significa guardar el texto "External Voltage"
-- 4.300 millones de veces. La diferencia es ~120 bytes/fila contra
-- ~600, o sea 500 GB contra 2,6 TB al anio.
--
-- Retencion: 90 dias en linea (~134 GB), el resto a Parquet en S3.
-- =====================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ---------------------------------------------------------------------
-- Empresas y equipos
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS tenants (
    id          bigserial PRIMARY KEY,
    nombre      text        NOT NULL,
    activo      boolean     NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS devices (
    id          bigserial PRIMARY KEY,
    tenant_id   bigint      NOT NULL REFERENCES tenants(id),
    imei        text        NOT NULL UNIQUE,
    nombre      text,
    patente     text,
    modelo      text        NOT NULL DEFAULT '*',
    activo      boolean     NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS devices_tenant_idx ON devices (tenant_id) WHERE activo;

-- Diccionario de IO fuera del codigo. modelo '*' aplica a todos.
CREATE TABLE IF NOT EXISTS io_definitions (
    modelo   text    NOT NULL DEFAULT '*',
    avl_id   integer NOT NULL,
    nombre   text    NOT NULL,
    unidad   text,
    escala   double precision NOT NULL DEFAULT 1,
    tipo     text    NOT NULL DEFAULT 'number'
             CHECK (tipo IN ('number','bool','text','hex')),
    columna  text,          -- si no es NULL, va a esa columna de telemetry
    PRIMARY KEY (modelo, avl_id)
);

-- ---------------------------------------------------------------------
-- TELEMETRIA
--
-- lat/lon como integer escalado 1e7: es como lo manda el Teltonika,
-- y ahorra 8 bytes por fila contra double precision (34 GB al anio).
-- Los voltajes en centivolts caben en smallint hasta 327 V.
-- io_extra queda NULL cuando no hay nada raro: un NULL cuesta 1 bit.
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS telemetry (
    device_id       bigint      NOT NULL,
    ts              timestamptz NOT NULL,

    lat_e7          integer,              -- grados x 1e7
    lon_e7          integer,
    speed           smallint,             -- km/h
    angle           smallint,             -- grados
    altitude        smallint,             -- m
    sats            smallint,
    gps_valid       boolean     NOT NULL DEFAULT false,

    ignition        boolean,
    movement        boolean,
    ext_voltage_cv  smallint,             -- voltios x 100
    bat_voltage_cv  smallint,
    odometer_m      integer,              -- metros, techo 2.1M km
    gsm_signal      smallint,
    fuel_level      smallint,
    fuel_used       integer,

    event_id        integer,
    priority        smallint,
    lag_s           integer,              -- segundos entre ts y recepcion:
                                          -- detecta datos con buffer
    io_extra        jsonb,                -- solo lo no promovido

    PRIMARY KEY (device_id, ts)
) PARTITION BY RANGE (ts);

-- Vista con lat/lon en grados, para no dividir en cada consulta
CREATE OR REPLACE VIEW telemetry_v AS
SELECT device_id, ts,
       lat_e7 / 1e7 AS lat,
       lon_e7 / 1e7 AS lon,
       speed, angle, altitude, sats, gps_valid,
       ignition, movement,
       ext_voltage_cv / 100.0 AS ext_voltage,
       bat_voltage_cv / 100.0 AS bat_voltage,
       odometer_m, gsm_signal, fuel_level, fuel_used,
       event_id, priority, lag_s, io_extra
FROM telemetry;

-- ---------------------------------------------------------------------
-- Tramas crudas. Retencion corta: sirven para reprocesar cuando
-- descubras que mapeaste mal un IO, no para guardar historia.
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw_frames (
    id           bigserial,
    imei         text        NOT NULL,
    received_at  timestamptz NOT NULL DEFAULT now(),
    codec        smallint,
    payload      bytea       NOT NULL,     -- bytea, no hex: la mitad de espacio
    PRIMARY KEY (received_at, id)
) PARTITION BY RANGE (received_at);

-- ---------------------------------------------------------------------
-- Estado actual. Redis es la fuente en vivo; esto persiste el ultimo
-- estado para que el mapa cargue en frio.
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS device_state (
    device_id       bigint      PRIMARY KEY REFERENCES devices(id),
    ts              timestamptz NOT NULL,
    lat_e7          integer,
    lon_e7          integer,
    speed           smallint,
    angle           smallint,
    gps_valid       boolean     NOT NULL DEFAULT false,
    ignition        boolean,
    movement        boolean,
    ext_voltage_cv  smallint,
    odometer_m      integer,
    io_extra        jsonb,
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ingest_rejects (
    id           bigserial PRIMARY KEY,
    imei         text,
    motivo       text        NOT NULL,
    detalle      jsonb,
    received_at  timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- Particiones SEMANALES.
-- A 11,8M registros diarios, una particion mensual son 354 millones de
-- filas: pesada de desprender y de escanear. Semanal son 82 millones,
-- unos 10 GB. Con 90 dias de retencion viven ~13 particiones.
-- ---------------------------------------------------------------------

CREATE OR REPLACE FUNCTION ensure_week_partition(
    p_parent text, p_col text, p_dia date
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_ini  date := date_trunc('week', p_dia)::date;
    v_fin  date := (date_trunc('week', p_dia) + interval '1 week')::date;
    v_name text := p_parent || '_' || to_char(v_ini, 'IYYY_IW');
BEGIN
    IF to_regclass('public.' || quote_ident(v_name)) IS NULL THEN
        EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                       v_name, p_parent, v_ini, v_fin);
        EXECUTE format('CREATE INDEX %I ON %I USING brin (%I) WITH (pages_per_range = 32)',
                       v_name || '_brin', v_name, p_col);
        RAISE NOTICE 'Particion creada: %', v_name;
    END IF;
END $$;

-- Crea la semana pasada, la actual y las 4 siguientes.
CREATE OR REPLACE FUNCTION ensure_partitions_ahead() RETURNS void
LANGUAGE plpgsql AS $$
DECLARE d date;
BEGIN
    FOR d IN SELECT generate_series(
                 date_trunc('week', now() - interval '1 week'),
                 date_trunc('week', now() + interval '4 weeks'),
                 interval '1 week')::date
    LOOP
        PERFORM ensure_week_partition('telemetry',  'ts',          d);
        PERFORM ensure_week_partition('raw_frames', 'received_at', d);
    END LOOP;
END $$;

-- Lista particiones fuera de la ventana de retencion, para archivar.
CREATE OR REPLACE FUNCTION particiones_a_archivar(p_dias integer DEFAULT 90)
RETURNS TABLE(particion text, filas bigint, tamano text)
LANGUAGE sql AS $$
    SELECT c.relname::text,
           c.reltuples::bigint,
           pg_size_pretty(pg_total_relation_size(c.oid))
    FROM pg_class c
    JOIN pg_inherits i ON i.inhrelid = c.oid
    JOIN pg_class p ON p.oid = i.inhparent
    WHERE p.relname = 'telemetry'
      AND to_date(substring(c.relname from '(\d{4})_(\d{2})$'), 'IYYY') IS NOT NULL
      AND pg_get_expr(c.relpartbound, c.oid) < format('FOR VALUES FROM (''%s'')',
                                                      (now() - (p_dias || ' days')::interval)::date)
    ORDER BY c.relname;
$$;

SELECT ensure_partitions_ahead();

-- ---------------------------------------------------------------------
-- Diccionario IO. 'columna' indica cual va promovida a columna tipada.
-- Verificar escala y unidad contra la lista AVL del modelo exacto.
-- ---------------------------------------------------------------------

INSERT INTO io_definitions (modelo, avl_id, nombre, unidad, escala, tipo, columna) VALUES
 ('*', 239, 'ignition',         NULL,  1,     'bool',   'ignition'),
 ('*', 240, 'movement',         NULL,  1,     'bool',   'movement'),
 ('*',  66, 'external_voltage', 'V',   0.001, 'number', 'ext_voltage_cv'),
 ('*',  67, 'battery_voltage',  'V',   0.001, 'number', 'bat_voltage_cv'),
 ('*',  16, 'total_odometer',   'm',   1,     'number', 'odometer_m'),
 ('*', 216, 'total_odometer',   'm',   1,     'number', 'odometer_m'),
 ('*',  21, 'gsm_signal',       NULL,  1,     'number', 'gsm_signal'),
 ('*',  87, 'fuel_level',       NULL,  1,     'number', 'fuel_level'),
 ('*',  76, 'fuel_counter',     NULL,  1,     'number', 'fuel_used'),
 -- Los que van a io_extra
 ('*', 199, 'trip_odometer',    'm',   1,     'number', NULL),
 ('*', 200, 'sleep_mode',       NULL,  1,     'number', NULL),
 ('*', 181, 'gnss_pdop',        NULL,  0.1,   'number', NULL),
 ('*', 182, 'gnss_hdop',        NULL,  0.1,   'number', NULL),
 ('*', 241, 'gsm_operator',     NULL,  1,     'number', NULL),
 ('*',   1, 'digital_input_1',  NULL,  1,     'bool',   NULL),
 ('*',   2, 'digital_input_2',  NULL,  1,     'bool',   NULL),
 ('*', 179, 'digital_output_1', NULL,  1,     'bool',   NULL),
 ('*', 180, 'digital_output_2', NULL,  1,     'bool',   NULL),
 ('*',   9, 'analog_input_1',   'V',   0.001, 'number', NULL),
 ('*',  70, 'pcb_temperature',  'C',   0.1,   'number', NULL),
 ('*',  72, 'dallas_temp_1',    'C',   0.1,   'number', NULL),
 ('*',  78, 'ibutton',          NULL,  1,     'hex',    NULL),
 ('*',  88, 'engine_speed',     'rpm', 1,     'number', NULL),
 ('*', 135, 'fuel_rate',        NULL,  1,     'number', NULL)
ON CONFLICT (modelo, avl_id) DO NOTHING;

INSERT INTO tenants (id, nombre) VALUES (1, 'Sinergy Interno')
ON CONFLICT (id) DO NOTHING;
SELECT setval('tenants_id_seq', GREATEST((SELECT max(id) FROM tenants), 1));

-- ---------------------------------------------------------------------
-- Comprobacion de dimensionamiento. Correr cuando lleves unos dias
-- con datos reales: te dice los bytes por fila de TU operacion.
--
--   SELECT pg_size_pretty(pg_total_relation_size('telemetry')) AS total,
--          (SELECT count(*) FROM telemetry) AS filas,
--          pg_total_relation_size('telemetry') /
--            NULLIF((SELECT count(*) FROM telemetry),0) AS bytes_por_fila;
--
-- Objetivo: 120-160 bytes por fila. Si da mas de 250, hay algun IO
-- cayendo en io_extra en cada registro que deberia ser columna.
-- Para encontrarlo:
--
--   SELECT k, count(*) FROM telemetry, jsonb_object_keys(io_extra) k
--   WHERE ts > now() - interval '1 hour' GROUP BY 1 ORDER BY 2 DESC;
-- ---------------------------------------------------------------------
