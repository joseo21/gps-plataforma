-- =====================================================================
-- GpsSinergy — esquema de plataforma
-- Aurora PostgreSQL 17
--
-- Cubre los modulos del frontend que hoy viven en mockDatabase.js:
-- empresas, aplicaciones, usuarios, roles, permisos, accesos, etiquetas
-- de acceso, grupos de vehiculos, activos, geocercas y auditoria.
--
-- Complementa a schema.sql (telemetria). Aplicar DESPUES de ese.
--   psql "$DATABASE_URL" -f db/schema_plataforma.sql
--
-- REGLA DE DOMINIO CENTRAL (del README del frontend):
--
--   activos de la empresa
--     -> permisos y etiquetas de acceso   = QUE PUEDE VER (seguridad)
--     -> filtros de ciudad / grupos       = QUE ELIGE VER (visual)
--     -> activos visibles en pantalla
--
-- Las etiquetas de acceso limitan de verdad. Los grupos de vehiculos y
-- las ciudades solo organizan lo ya autorizado. El backend debe imponer
-- la primera capa: hoy se calcula en el navegador y eso es inseguro.
-- =====================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ---------------------------------------------------------------------
-- EMPRESAS Y APLICACIONES
-- Una empresa tiene una aplicacion (el espacio de trabajo del cliente).
-- ---------------------------------------------------------------------

ALTER TABLE tenants ADD COLUMN IF NOT EXISTS rut          text;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS giro         text;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS direccion    text;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS telefono     text;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS email        text;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS logo_url     text;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS config       jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS updated_at   timestamptz NOT NULL DEFAULT now();

CREATE TABLE IF NOT EXISTS applications (
    id          bigserial PRIMARY KEY,
    tenant_id   bigint      NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    nombre      text        NOT NULL,
    short_name  text        NOT NULL DEFAULT 'APP',
    tipo        text        NOT NULL DEFAULT 'Empresa cliente',
    activo      boolean     NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS applications_tenant_idx ON applications (tenant_id);

-- ---------------------------------------------------------------------
-- USUARIOS Y AUTENTICACION
--
-- La contrasena se guarda con bcrypt (passlib en la API). Nunca en claro,
-- nunca reversible. El campo password_hash no se expone en ningun endpoint.
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS users (
    id              bigserial PRIMARY KEY,
    email           citext,                       -- ver nota de extension
    nombre          text        NOT NULL,
    apellido        text,
    password_hash   text        NOT NULL,
    telefono        text,
    activo          boolean     NOT NULL DEFAULT true,
    es_superadmin   boolean     NOT NULL DEFAULT false,
    ultimo_acceso   timestamptz,
    debe_cambiar_pw boolean     NOT NULL DEFAULT false,
    intentos_fallidos smallint  NOT NULL DEFAULT 0,
    bloqueado_hasta timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);
-- Si citext no esta disponible: usar text y un indice sobre lower(email).
CREATE UNIQUE INDEX IF NOT EXISTS users_email_idx ON users (lower(email::text));

CREATE TABLE IF NOT EXISTS refresh_tokens (
    id          bigserial PRIMARY KEY,
    user_id     bigint      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash  text        NOT NULL UNIQUE,
    expira      timestamptz NOT NULL,
    revocado    boolean     NOT NULL DEFAULT false,
    user_agent  text,
    ip          inet,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS refresh_user_idx ON refresh_tokens (user_id) WHERE NOT revocado;

-- ---------------------------------------------------------------------
-- MODULOS, FUNCIONES, ROLES Y PERMISOS
--
-- Un rol agrupa funciones de modulo. Un acceso conecta usuario + empresa
-- + rol, y ahi se define tambien el alcance operativo.
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS modules (
    id      text PRIMARY KEY,          -- 'assets', 'maintenance', 'users', 'audit'
    nombre  text NOT NULL,
    orden   smallint NOT NULL DEFAULT 0,
    activo  boolean NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS module_functions (
    id          text PRIMARY KEY,      -- 'assets.create', 'reports.export'
    module_id   text NOT NULL REFERENCES modules(id) ON DELETE CASCADE,
    nombre      text NOT NULL,
    descripcion text
);
CREATE INDEX IF NOT EXISTS module_functions_module_idx ON module_functions (module_id);

CREATE TABLE IF NOT EXISTS roles (
    id          bigserial PRIMARY KEY,
    tenant_id   bigint REFERENCES tenants(id) ON DELETE CASCADE,  -- NULL = rol global
    nombre      text        NOT NULL,
    descripcion text,
    sistema     boolean     NOT NULL DEFAULT false,   -- no editable por el usuario
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS roles_nombre_idx
    ON roles (coalesce(tenant_id, 0), lower(nombre));

CREATE TABLE IF NOT EXISTS role_permissions (
    role_id            bigint NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    module_function_id text   NOT NULL REFERENCES module_functions(id) ON DELETE CASCADE,
    PRIMARY KEY (role_id, module_function_id)
);

-- Acceso: un usuario en una empresa, con un rol y un alcance.
CREATE TABLE IF NOT EXISTS accesses (
    id          bigserial PRIMARY KEY,
    user_id     bigint      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tenant_id   bigint      NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    role_id     bigint      NOT NULL REFERENCES roles(id),
    -- Alcance de activos: 'todos' o 'etiquetas'. Con 'etiquetas', solo ve
    -- los activos que tengan alguna de las etiquetas de access_asset_tags.
    alcance     text        NOT NULL DEFAULT 'todos'
                CHECK (alcance IN ('todos','etiquetas','ninguno')),
    activo      boolean     NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (user_id, tenant_id)
);
CREATE INDEX IF NOT EXISTS accesses_user_idx ON accesses (user_id) WHERE activo;

-- ---------------------------------------------------------------------
-- ETIQUETAS DE ACCESO (limitan de verdad)
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS asset_tags (
    id          bigserial PRIMARY KEY,
    tenant_id   bigint      NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    nombre      text        NOT NULL,
    color       text,
    activo      boolean     NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS asset_tags_nombre_idx
    ON asset_tags (tenant_id, lower(nombre));

CREATE TABLE IF NOT EXISTS device_asset_tags (
    device_id     bigint NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    asset_tag_id  bigint NOT NULL REFERENCES asset_tags(id) ON DELETE CASCADE,
    PRIMARY KEY (device_id, asset_tag_id)
);
CREATE INDEX IF NOT EXISTS device_tags_tag_idx ON device_asset_tags (asset_tag_id);

CREATE TABLE IF NOT EXISTS access_asset_tags (
    access_id     bigint NOT NULL REFERENCES accesses(id) ON DELETE CASCADE,
    asset_tag_id  bigint NOT NULL REFERENCES asset_tags(id) ON DELETE CASCADE,
    PRIMARY KEY (access_id, asset_tag_id)
);

-- ---------------------------------------------------------------------
-- GRUPOS DE VEHICULOS (solo organizan, NO son permisos)
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS vehicle_groups (
    id          bigserial PRIMARY KEY,
    tenant_id   bigint      NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    nombre      text        NOT NULL,
    descripcion text,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS vehicle_groups_nombre_idx
    ON vehicle_groups (tenant_id, lower(nombre));

CREATE TABLE IF NOT EXISTS vehicle_group_members (
    group_id  bigint NOT NULL REFERENCES vehicle_groups(id) ON DELETE CASCADE,
    device_id bigint NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    PRIMARY KEY (group_id, device_id)
);

-- ---------------------------------------------------------------------
-- ACTIVOS: ampliacion de devices con lo que el frontend necesita
-- ---------------------------------------------------------------------

ALTER TABLE devices ADD COLUMN IF NOT EXISTS application_id  bigint REFERENCES applications(id);
ALTER TABLE devices ADD COLUMN IF NOT EXISTS asset_type      text;      -- car, pickup, truck, bus, van, motorcycle, machinery, trailer
ALTER TABLE devices ADD COLUMN IF NOT EXISTS map_icon        text;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS marca           text;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS modelo_vehiculo text;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS anio            smallint;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS vin             text;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS color           text;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS ciudad          text;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS sucursal        text;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS conductor       text;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS sim             text;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS perfil_operacional text;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS odometro_offset_m  bigint NOT NULL DEFAULT 0;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS notas           text;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS metadata        jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS updated_at      timestamptz NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS devices_ciudad_idx ON devices (tenant_id, ciudad) WHERE activo;
CREATE INDEX IF NOT EXISTS devices_patente_idx ON devices (tenant_id, patente);

-- ---------------------------------------------------------------------
-- GEOCERCAS
-- geometria en GeoJSON: circulo {tipo, centro, radio} o poligono {tipo, puntos}
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS geofence_groups (
    id          bigserial PRIMARY KEY,
    tenant_id   bigint      NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    nombre      text        NOT NULL,
    color       text,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS geofences (
    id          bigserial PRIMARY KEY,
    tenant_id   bigint      NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    group_id    bigint REFERENCES geofence_groups(id) ON DELETE SET NULL,
    nombre      text        NOT NULL,
    tipo        text        NOT NULL CHECK (tipo IN ('circulo','poligono','ruta')),
    geometria   jsonb       NOT NULL,
    color       text,
    activo      boolean     NOT NULL DEFAULT true,
    -- Caja envolvente para descartar rapido sin evaluar la geometria
    min_lat     double precision,
    max_lat     double precision,
    min_lon     double precision,
    max_lon     double precision,
    created_by  bigint REFERENCES users(id),
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS geofences_tenant_idx ON geofences (tenant_id) WHERE activo;
CREATE INDEX IF NOT EXISTS geofences_bbox_idx ON geofences (min_lat, max_lat, min_lon, max_lon);

CREATE TABLE IF NOT EXISTS geofence_devices (
    geofence_id bigint NOT NULL REFERENCES geofences(id) ON DELETE CASCADE,
    device_id   bigint NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    PRIMARY KEY (geofence_id, device_id)
);

-- Eventos de entrada/salida. Los genera un worker consumiendo el stream.
CREATE TABLE IF NOT EXISTS geofence_events (
    id          bigserial,
    geofence_id bigint      NOT NULL REFERENCES geofences(id) ON DELETE CASCADE,
    device_id   bigint      NOT NULL,
    tipo        text        NOT NULL CHECK (tipo IN ('entrada','salida')),
    ts          timestamptz NOT NULL,
    lat         double precision,
    lon         double precision,
    PRIMARY KEY (ts, id)
) PARTITION BY RANGE (ts);

-- ---------------------------------------------------------------------
-- VISTAS GUARDADAS (workspaces del frontend)
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS workspaces (
    id          bigserial PRIMARY KEY,
    user_id     bigint      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tenant_id   bigint      NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    nombre      text        NOT NULL,
    estado      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    predeterminada boolean  NOT NULL DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS workspaces_user_idx ON workspaces (user_id, tenant_id);

-- ---------------------------------------------------------------------
-- AUDITORIA
-- Hoy vive en localStorage: se pierde y es falsificable. Va al servidor.
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS audit_log (
    id          bigserial,
    ts          timestamptz NOT NULL DEFAULT now(),
    user_id     bigint REFERENCES users(id),
    tenant_id   bigint REFERENCES tenants(id),
    modulo      text        NOT NULL,
    accion      text        NOT NULL,       -- crear, editar, eliminar, exportar, login
    entidad     text,                        -- 'device', 'user', 'geofence'
    entidad_id  text,
    detalle     jsonb,
    ip          inet,
    user_agent  text,
    PRIMARY KEY (ts, id)
) PARTITION BY RANGE (ts);

-- ---------------------------------------------------------------------
-- Particiones mensuales para las tablas de eventos
-- ---------------------------------------------------------------------

CREATE OR REPLACE FUNCTION ensure_month_partition(
    p_parent text, p_col text, p_mes date
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_ini  date := date_trunc('month', p_mes)::date;
    v_fin  date := (date_trunc('month', p_mes) + interval '1 month')::date;
    v_name text := p_parent || '_' || to_char(v_ini, 'YYYY_MM');
BEGIN
    IF to_regclass('public.' || quote_ident(v_name)) IS NULL THEN
        EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                       v_name, p_parent, v_ini, v_fin);
        EXECUTE format('CREATE INDEX %I ON %I USING brin (%I)',
                       v_name || '_brin', v_name, p_col);
    END IF;
END $$;

CREATE OR REPLACE FUNCTION ensure_plataforma_partitions() RETURNS void
LANGUAGE plpgsql AS $$
DECLARE m date;
BEGIN
    FOR m IN SELECT generate_series(date_trunc('month', now() - interval '2 months'),
                                    date_trunc('month', now() + interval '3 months'),
                                    interval '1 month')::date
    LOOP
        PERFORM ensure_month_partition('audit_log',       'ts', m);
        PERFORM ensure_month_partition('geofence_events', 'ts', m);
    END LOOP;
END $$;

SELECT ensure_plataforma_partitions();

-- ---------------------------------------------------------------------
-- VISTA DE ACTIVOS AUTORIZADOS
--
-- Esta es la pieza de seguridad. Toda consulta de activos del backend
-- debe pasar por aca, nunca por un filtro armado en el cliente.
-- ---------------------------------------------------------------------

CREATE OR REPLACE FUNCTION activos_autorizados(p_user_id bigint, p_tenant_id bigint)
RETURNS TABLE (device_id bigint)
LANGUAGE sql STABLE AS $$
    WITH acc AS (
        SELECT a.id, a.alcance
        FROM accesses a
        WHERE a.user_id = p_user_id AND a.tenant_id = p_tenant_id AND a.activo
    )
    SELECT d.id
    FROM devices d
    JOIN acc ON true
    WHERE d.tenant_id = p_tenant_id
      AND d.activo
      AND (
        acc.alcance = 'todos'
        OR (acc.alcance = 'etiquetas' AND EXISTS (
              SELECT 1 FROM device_asset_tags dat
              JOIN access_asset_tags aat ON aat.asset_tag_id = dat.asset_tag_id
              WHERE dat.device_id = d.id AND aat.access_id = acc.id))
      )
    UNION
    SELECT d.id FROM devices d
    WHERE d.tenant_id = p_tenant_id AND d.activo
      AND EXISTS (SELECT 1 FROM users u WHERE u.id = p_user_id AND u.es_superadmin);
$$;

-- ---------------------------------------------------------------------
-- SEMILLA: modulos, funciones y roles base
-- ---------------------------------------------------------------------

INSERT INTO modules (id, nombre, orden) VALUES
 ('assets','Activos',1), ('reports','Reportes',2), ('maintenance','Mantenciones',3),
 ('users','Usuarios',4), ('companies','Empresas',5), ('audit','Auditoria',6)
ON CONFLICT (id) DO NOTHING;

INSERT INTO module_functions (id, module_id, nombre) VALUES
 ('assets.view','assets','Ver activos'),
 ('assets.create','assets','Crear activos'),
 ('assets.edit','assets','Editar activos'),
 ('assets.delete','assets','Eliminar activos'),
 ('assets.command','assets','Enviar comandos al equipo'),
 ('assets.geofence','assets','Gestionar geocercas'),
 ('reports.view','reports','Ver reportes'),
 ('reports.create','reports','Crear plantillas'),
 ('reports.execute','reports','Ejecutar reportes'),
 ('reports.export','reports','Exportar PDF/Excel'),
 ('maintenance.view','maintenance','Ver mantenciones'),
 ('maintenance.manage','maintenance','Gestionar ordenes'),
 ('users.view','users','Ver usuarios'),
 ('users.manage','users','Gestionar usuarios y permisos'),
 ('companies.view','companies','Ver empresas'),
 ('companies.manage','companies','Gestionar empresas'),
 ('audit.view','audit','Ver auditoria')
ON CONFLICT (id) DO NOTHING;

INSERT INTO roles (id, tenant_id, nombre, descripcion, sistema) VALUES
 (1, NULL, 'Administrador', 'Acceso total a la empresa', true),
 (2, NULL, 'Supervisor',    'Operacion y reportes, sin gestion de usuarios', true),
 (3, NULL, 'Operador',      'Operacion diaria', true),
 (4, NULL, 'Visualizador',  'Solo lectura', true)
ON CONFLICT (id) DO NOTHING;
SELECT setval('roles_id_seq', GREATEST((SELECT max(id) FROM roles), 4));

INSERT INTO role_permissions (role_id, module_function_id)
 SELECT 1, id FROM module_functions
ON CONFLICT DO NOTHING;

INSERT INTO role_permissions (role_id, module_function_id) VALUES
 (2,'assets.view'),(2,'assets.edit'),(2,'assets.geofence'),(2,'assets.command'),
 (2,'reports.view'),(2,'reports.create'),(2,'reports.execute'),(2,'reports.export'),
 (2,'maintenance.view'),(2,'maintenance.manage'),(2,'audit.view'),
 (3,'assets.view'),(3,'assets.command'),(3,'reports.view'),(3,'reports.execute'),
 (3,'maintenance.view'),
 (4,'assets.view'),(4,'reports.view')
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------
-- PENDIENTE (fase siguiente): reportes y mantenciones.
--
-- No los modelo todavia a proposito: sus estructuras dependen de
-- mockReportTemplates.js (738 lineas), reportEventRuleConfig.js (301) y
-- useMaintenanceModule.js (1541). Inventar ese modelo sin leerlos
-- garantiza tener que rehacerlo. Se disena cuando se revisen esos
-- archivos.
-- ---------------------------------------------------------------------
