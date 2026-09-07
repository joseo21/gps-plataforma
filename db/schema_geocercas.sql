-- =====================================================================
-- GpsSinergy — geocercas alineadas al modelo del frontend
--
-- El esquema original usaba tipos en espanol (circulo/poligono/ruta) y
-- un grupo por clave foranea. El frontend usa circle/polygon/route y
-- deriva los grupos de un campo de texto en la propia geocerca.
--
-- Se adopta el modelo del frontend: traducir en el medio es una fuente
-- de errores que no aporta nada.
--
-- Aplicar despues de schema_plataforma.sql:
--   psql "$DATABASE_URL" -f db/schema_geocercas.sql
-- =====================================================================

ALTER TABLE geofences DROP CONSTRAINT IF EXISTS geofences_tipo_check;
ALTER TABLE geofences ADD CONSTRAINT geofences_tipo_check
    CHECK (tipo IN ('circle', 'polygon', 'route'));

ALTER TABLE geofences ADD COLUMN IF NOT EXISTS group_name       text;
ALTER TABLE geofences ADD COLUMN IF NOT EXISTS tolerance_meters integer;

-- geometria guarda la forma tal como la maneja el frontend:
--   circle  -> {"center": {"lat": .., "lng": ..}, "radius": 250}
--   polygon -> {"coordinates": [{"lat": .., "lng": ..}, ...]}   (min 3)
--   route   -> {"coordinates": [...]}                            (min 2)

-- La caja envolvente permite descartar geocercas sin evaluar geometria.
-- Con 2000 equipos reportando cada 15 s, esa poda es la diferencia entre
-- un worker viable y uno que no da abasto.
CREATE OR REPLACE FUNCTION calcular_bbox_geocerca() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    v_lat  double precision;
    v_lng  double precision;
    v_rad  double precision;
    v_dlat double precision;
    v_dlng double precision;
BEGIN
    IF NEW.tipo = 'circle' THEN
        v_lat := (NEW.geometria #>> '{center,lat}')::double precision;
        v_lng := (NEW.geometria #>> '{center,lng}')::double precision;
        v_rad := coalesce((NEW.geometria ->> 'radius')::double precision, 0);
        IF v_lat IS NULL OR v_lng IS NULL THEN
            RAISE EXCEPTION 'Geocerca circular sin centro valido';
        END IF;
        v_dlat := v_rad / 111320.0;
        -- Un grado de longitud se acorta con la latitud; sin el coseno la
        -- caja quedaria angosta y descartaria equipos que si estan dentro.
        v_dlng := v_rad / (111320.0 * GREATEST(cos(radians(v_lat)), 0.01));
        NEW.min_lat := v_lat - v_dlat;  NEW.max_lat := v_lat + v_dlat;
        NEW.min_lon := v_lng - v_dlng;  NEW.max_lon := v_lng + v_dlng;
    ELSE
        SELECT min((p ->> 'lat')::double precision),
               max((p ->> 'lat')::double precision),
               min((p ->> 'lng')::double precision),
               max((p ->> 'lng')::double precision)
          INTO NEW.min_lat, NEW.max_lat, NEW.min_lon, NEW.max_lon
          FROM jsonb_array_elements(NEW.geometria -> 'coordinates') p;
        IF NEW.min_lat IS NULL THEN
            RAISE EXCEPTION 'Geocerca sin coordenadas validas';
        END IF;
        -- Una ruta es una linea: se ensancha por su tolerancia.
        IF NEW.tipo = 'route' THEN
            v_dlat := coalesce(NEW.tolerance_meters, 100) / 111320.0;
            NEW.min_lat := NEW.min_lat - v_dlat;  NEW.max_lat := NEW.max_lat + v_dlat;
            NEW.min_lon := NEW.min_lon - v_dlat;  NEW.max_lon := NEW.max_lon + v_dlat;
        END IF;
    END IF;
    NEW.updated_at := now();
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_bbox_geocerca ON geofences;
CREATE TRIGGER trg_bbox_geocerca
    BEFORE INSERT OR UPDATE OF geometria, tipo, tolerance_meters ON geofences
    FOR EACH ROW EXECUTE FUNCTION calcular_bbox_geocerca();

-- Validacion de forma: una geocerca mal armada no debe poder guardarse.
ALTER TABLE geofences DROP CONSTRAINT IF EXISTS geofences_geometria_check;
ALTER TABLE geofences ADD CONSTRAINT geofences_geometria_check CHECK (
    (tipo = 'circle'
     AND geometria #>> '{center,lat}' IS NOT NULL
     AND geometria #>> '{center,lng}' IS NOT NULL
     AND (geometria ->> 'radius')::double precision > 0)
    OR
    (tipo = 'polygon'
     AND jsonb_array_length(geometria -> 'coordinates') >= 3)
    OR
    (tipo = 'route'
     AND jsonb_array_length(geometria -> 'coordinates') >= 2)
);

CREATE INDEX IF NOT EXISTS geofences_grupo_idx
    ON geofences (tenant_id, group_name) WHERE activo;

-- Grupos explicitos: opcionales. El frontend deriva grupos del texto,
-- esta tabla solo guarda color y orden cuando el usuario los personaliza.
ALTER TABLE geofence_groups ADD COLUMN IF NOT EXISTS orden smallint NOT NULL DEFAULT 0;
CREATE UNIQUE INDEX IF NOT EXISTS geofence_groups_nombre_idx
    ON geofence_groups (tenant_id, lower(nombre));

-- ---------------------------------------------------------------------
-- Comprobacion del trigger
-- ---------------------------------------------------------------------

DO $$
DECLARE v_id bigint; v_min double precision; v_max double precision;
BEGIN
    INSERT INTO geofences (tenant_id, nombre, tipo, geometria)
    VALUES (1, '__prueba_bbox__', 'circle',
            '{"center":{"lat":-38.74,"lng":-72.62},"radius":1000}'::jsonb)
    RETURNING id, min_lat, max_lat INTO v_id, v_min, v_max;

    IF round((v_max - v_min)::numeric, 4) <> round((2000.0/111320.0)::numeric, 4) THEN
        RAISE EXCEPTION 'bbox incorrecta: % a %', v_min, v_max;
    END IF;
    RAISE NOTICE 'Trigger de bbox correcto (circulo de 1 km)';

    DELETE FROM geofences WHERE id = v_id;

    BEGIN
        INSERT INTO geofences (tenant_id, nombre, tipo, geometria)
        VALUES (1, '__prueba_mala__', 'polygon',
                '{"coordinates":[{"lat":-38,"lng":-72}]}'::jsonb);
        RAISE EXCEPTION 'La validacion NO esta funcionando';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'Validacion correcta: poligono de 1 punto rechazado';
    END;
END $$;
