-- =====================================================================
-- GpsSinergy — permisos, usuarios, empresas y accesos
--
-- Corrige el modelo de role_permissions de schema_plataforma.sql, que
-- asumia permisos planos (rol -> funcion). El frontend usa un modelo
-- mas rico, con ACCIONES por funcion y overrides por acceso:
--
--   acceso = {
--     modules:   [{ moduleId, enabled }],
--     functions: [{ functionId, enabled, permissions: {view, create, ...} }],
--     scope:     { assetIds, assetTagIds }
--   }
--
-- Ademas los identificadores del frontend usan guion (users-view) y no
-- punto (users.view). Se migran los existentes.
--
-- Aplicar despues de schema_plataforma.sql:
--   psql "$DATABASE_URL" -f db/schema_permisos.sql
-- =====================================================================

-- ---------------------------------------------------------------------
-- Acciones posibles sobre una funcion
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS permission_actions (
    id     text PRIMARY KEY,
    nombre text NOT NULL,
    orden  smallint NOT NULL DEFAULT 0
);

INSERT INTO permission_actions (id, nombre, orden) VALUES
 ('view','Ver',1), ('create','Crear',2), ('edit','Editar',3),
 ('delete','Eliminar',4), ('export','Exportar',5), ('execute','Ejecutar',6)
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------------------
-- Modulos y funciones con IDs del frontend (guion, no punto)
-- ---------------------------------------------------------------------

ALTER TABLE module_functions ADD COLUMN IF NOT EXISTS acciones text[]
    NOT NULL DEFAULT ARRAY['view'];

DELETE FROM role_permissions WHERE module_function_id LIKE '%.%';
DELETE FROM module_functions WHERE id LIKE '%.%';

INSERT INTO modules (id, nombre, orden) VALUES
 ('assets','Activos',1), ('reports','Reportes',2), ('maintenance','Mantenciones',3),
 ('users','Usuarios',4), ('companies','Empresas',5), ('audit','Auditoria',6),
 ('geofences','Geocercas',7)
ON CONFLICT (id) DO UPDATE SET nombre = EXCLUDED.nombre, orden = EXCLUDED.orden;

INSERT INTO module_functions (id, module_id, nombre, acciones) VALUES
 ('assets-view',        'assets',     'Monitoreo de activos', ARRAY['view']),
 ('assets-manage',      'assets',     'Gestion de activos',   ARRAY['view','create','edit','delete']),
 ('assets-command',     'assets',     'Comandos al equipo',   ARRAY['view','execute']),
 ('assets-itinerary',   'assets',     'Itinerarios',          ARRAY['view','export']),
 ('geofences-view',     'geofences',  'Ver geocercas',        ARRAY['view']),
 ('geofences-manage',   'geofences',  'Gestion de geocercas', ARRAY['view','create','edit','delete']),
 ('reports-view',       'reports',    'Ver reportes',         ARRAY['view']),
 ('reports-templates',  'reports',    'Plantillas',           ARRAY['view','create','edit','delete']),
 ('reports-rules',      'reports',    'Reglas de evento',     ARRAY['view','create','edit','delete']),
 ('reports-execute',    'reports',    'Ejecutar y exportar',  ARRAY['view','execute','export']),
 ('maintenance-view',   'maintenance','Ver mantenciones',     ARRAY['view']),
 ('maintenance-manage', 'maintenance','Planes y ordenes',     ARRAY['view','create','edit','delete']),
 ('users-view',         'users',      'Ver usuarios',         ARRAY['view']),
 ('users-create',       'users',      'Crear usuarios',       ARRAY['view','create']),
 ('users-edit',         'users',      'Editar usuarios',      ARRAY['view','edit']),
 ('users-permissions',  'users',      'Gestionar permisos',   ARRAY['view','edit']),
 ('companies-view',     'companies',  'Ver empresas',         ARRAY['view']),
 ('companies-manage',   'companies',  'Gestion de empresas',  ARRAY['view','create','edit','delete']),
 ('audit-view',         'audit',      'Ver auditoria',        ARRAY['view','export'])
ON CONFLICT (id) DO UPDATE
  SET module_id = EXCLUDED.module_id, nombre = EXCLUDED.nombre,
      acciones = EXCLUDED.acciones;

-- ---------------------------------------------------------------------
-- Permisos del rol: funcion + accion, no solo funcion
-- ---------------------------------------------------------------------

ALTER TABLE role_permissions ADD COLUMN IF NOT EXISTS accion text
    NOT NULL DEFAULT 'view' REFERENCES permission_actions(id);

ALTER TABLE role_permissions DROP CONSTRAINT IF EXISTS role_permissions_pkey;
ALTER TABLE role_permissions ADD PRIMARY KEY (role_id, module_function_id, accion);

-- ---------------------------------------------------------------------
-- Overrides por acceso: un usuario puede tener menos que su rol.
-- Solo restan, nunca suman: lo que el rol no da, el override no puede
-- otorgar. Esa regla la impone la funcion permisos_efectivos().
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS access_module_overrides (
    access_id  bigint  NOT NULL REFERENCES accesses(id) ON DELETE CASCADE,
    module_id  text    NOT NULL REFERENCES modules(id) ON DELETE CASCADE,
    enabled    boolean NOT NULL DEFAULT true,
    PRIMARY KEY (access_id, module_id)
);

CREATE TABLE IF NOT EXISTS access_function_overrides (
    access_id          bigint  NOT NULL REFERENCES accesses(id) ON DELETE CASCADE,
    module_function_id text    NOT NULL REFERENCES module_functions(id) ON DELETE CASCADE,
    accion             text    NOT NULL REFERENCES permission_actions(id),
    enabled            boolean NOT NULL DEFAULT true,
    PRIMARY KEY (access_id, module_function_id, accion)
);

-- Alcance por activos concretos, ademas del alcance por etiquetas
CREATE TABLE IF NOT EXISTS access_devices (
    access_id bigint NOT NULL REFERENCES accesses(id) ON DELETE CASCADE,
    device_id bigint NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    PRIMARY KEY (access_id, device_id)
);

ALTER TABLE accesses DROP CONSTRAINT IF EXISTS accesses_alcance_check;
ALTER TABLE accesses ADD CONSTRAINT accesses_alcance_check
    CHECK (alcance IN ('todos','etiquetas','activos','ninguno'));

-- ---------------------------------------------------------------------
-- Permisos efectivos = permisos del rol MENOS los overrides deshabilitados
-- ---------------------------------------------------------------------

CREATE OR REPLACE FUNCTION permisos_efectivos(p_access_id bigint)
RETURNS TABLE (module_id text, module_function_id text, accion text)
LANGUAGE sql STABLE AS $$
    SELECT mf.module_id, rp.module_function_id, rp.accion
    FROM accesses a
    JOIN role_permissions rp ON rp.role_id = a.role_id
    JOIN module_functions mf ON mf.id = rp.module_function_id
    WHERE a.id = p_access_id
      AND a.activo
      -- el modulo no fue apagado para este acceso
      AND NOT EXISTS (
        SELECT 1 FROM access_module_overrides amo
        WHERE amo.access_id = a.id AND amo.module_id = mf.module_id
          AND amo.enabled = false)
      -- la accion no fue apagada para este acceso
      AND NOT EXISTS (
        SELECT 1 FROM access_function_overrides afo
        WHERE afo.access_id = a.id
          AND afo.module_function_id = rp.module_function_id
          AND afo.accion = rp.accion
          AND afo.enabled = false);
$$;

-- ---------------------------------------------------------------------
-- Activos autorizados, ahora con los tres alcances
-- ---------------------------------------------------------------------

CREATE OR REPLACE FUNCTION activos_autorizados(p_user_id bigint, p_tenant_id bigint)
RETURNS TABLE (device_id bigint)
LANGUAGE sql STABLE AS $$
    SELECT d.id
    FROM devices d
    WHERE d.tenant_id = p_tenant_id AND d.activo
      AND EXISTS (SELECT 1 FROM users u
                  WHERE u.id = p_user_id AND u.activo AND u.es_superadmin)

    UNION

    SELECT d.id
    FROM devices d
    JOIN accesses a ON a.tenant_id = d.tenant_id
                   AND a.user_id = p_user_id
                   AND a.activo
    WHERE d.tenant_id = p_tenant_id AND d.activo
      AND (
        a.alcance = 'todos'
        OR (a.alcance = 'etiquetas' AND EXISTS (
              SELECT 1 FROM device_asset_tags dat
              JOIN access_asset_tags aat ON aat.asset_tag_id = dat.asset_tag_id
              WHERE dat.device_id = d.id AND aat.access_id = a.id))
        OR (a.alcance = 'activos' AND EXISTS (
              SELECT 1 FROM access_devices ad
              WHERE ad.access_id = a.id AND ad.device_id = d.id))
      );
$$;

-- ---------------------------------------------------------------------
-- Roles base con sus acciones
-- ---------------------------------------------------------------------

DELETE FROM role_permissions WHERE role_id IN (1,2,3,4);

-- Administrador: todo
INSERT INTO role_permissions (role_id, module_function_id, accion)
SELECT 1, mf.id, unnest(mf.acciones) FROM module_functions mf
ON CONFLICT DO NOTHING;

-- Supervisor: operacion y reportes, sin usuarios ni empresas
INSERT INTO role_permissions (role_id, module_function_id, accion)
SELECT 2, mf.id, unnest(mf.acciones) FROM module_functions mf
WHERE mf.module_id IN ('assets','geofences','reports','maintenance','audit')
ON CONFLICT DO NOTHING;

-- Operador: ver y operar, sin crear ni borrar
INSERT INTO role_permissions (role_id, module_function_id, accion)
SELECT 3, mf.id, a FROM module_functions mf, unnest(mf.acciones) a
WHERE mf.module_id IN ('assets','geofences','reports','maintenance')
  AND a IN ('view','execute','export')
ON CONFLICT DO NOTHING;

-- Visualizador: solo lectura
INSERT INTO role_permissions (role_id, module_function_id, accion)
SELECT 4, mf.id, 'view' FROM module_functions mf
WHERE mf.module_id IN ('assets','geofences','reports','maintenance')
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------
-- Comprobacion
-- ---------------------------------------------------------------------

SELECT r.nombre AS rol, count(*) AS permisos,
       count(DISTINCT rp.module_function_id) AS funciones
FROM roles r JOIN role_permissions rp ON rp.role_id = r.id
GROUP BY r.id, r.nombre ORDER BY r.id;
