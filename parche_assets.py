#!/usr/bin/env python3
"""Agrega a la API los endpoints CRUD de activos.

Los activos dejan de vivir en localStorage y pasan a Aurora:
  - GET    /api/assets            lista con el formato exacto del frontend
  - POST   /api/assets            alta (el IMEI entra solo a la whitelist)
  - PATCH  /api/assets/{id}       edicion
  - DELETE /api/assets/{id}       baja logica

Correr desde /opt/gps:  python3 parche_assets.py
"""
import sys, pathlib, py_compile

API = pathlib.Path("api/main.py")
if not API.exists():
    sys.exit("ERROR: correr desde /opt/gps")

s = API.read_text()
if "/api/assets" in s:
    print("Ya estaba parcheado.")
    sys.exit(0)

BLOQUE = '''

# ---------------------------------------------------------------------
# ACTIVOS
#
# Devuelve el objeto con el formato que consume el frontend, incluidos
# los alias (vehiculo/name/nombrePantalla, patente/patent, imei/deviceId)
# porque distintos componentes leen campos distintos.
#
# El estado de telemetria sale de device_state: es dato real del equipo,
# no calculado a partir del odometro como hacia el mock.
# ---------------------------------------------------------------------

SQL_ASSETS = """
SELECT d.id, d.tenant_id, d.application_id, d.imei, d.nombre, d.patente,
       d.modelo, d.asset_type, d.map_icon, d.conductor, d.ciudad, d.sucursal,
       d.marca, d.modelo_vehiculo, d.anio, d.vin, d.color, d.sim,
       d.perfil_operacional, d.notas, d.activo, d.created_at,
       s.ts, s.lat_e7, s.lon_e7, s.speed, s.angle, s.gps_valid,
       s.ignition, s.movement, s.ext_voltage_cv, s.odometer_m,
       s.sats, s.gsm_signal, s.fuel_level, s.io_extra,
       (now() - s.ts) < interval '10 minutes' AS online,
       coalesce(array_agg(dat.asset_tag_id) FILTER (WHERE dat.asset_tag_id IS NOT NULL), '{}') AS tags
FROM devices d
LEFT JOIN device_state s ON s.device_id = d.id
LEFT JOIN device_asset_tags dat ON dat.device_id = d.id
WHERE d.activo
GROUP BY d.id, s.device_id, s.ts, s.lat_e7, s.lon_e7, s.speed, s.angle,
         s.gps_valid, s.ignition, s.movement, s.ext_voltage_cv, s.odometer_m,
         s.sats, s.gsm_signal, s.fuel_level, s.io_extra
ORDER BY d.nombre NULLS LAST
"""

ETIQUETA_TIPO = {
    "car": "Automovil", "pickup": "Camioneta", "truck": "Camion",
    "bus": "Bus", "van": "Furgon", "motorcycle": "Moto",
    "machinery": "Maquinaria", "trailer": "Remolque",
}


def fila_a_activo(d: Dict[str, Any]) -> Dict[str, Any]:
    """Traduce una fila de devices + device_state al objeto del frontend.
    Recibe un dict por nombre de columna, no una tupla: con 38 columnas,
    los indices numericos son una fuente segura de errores."""
    io = d.get("io_extra") or {}
    online = bool(d.get("online"))
    encendido = d.get("ignition") is True
    vel = int(d.get("speed") or 0)
    movimiento = d.get("movement") is True or vel > 2

    if not online:
        estado = "offline"
    elif movimiento:
        estado = "moving"
    elif encendido:
        estado = "idle"
    else:
        estado = "stopped"

    odo_m = d.get("odometer_m")
    odo_km = round(odo_m / 1000, 1) if odo_m is not None else 0
    comb = d.get("fuel_level")
    volt = round(d["ext_voltage_cv"] / 100, 1) if d.get("ext_voltage_cv") is not None else 0
    senal = int((d.get("gsm_signal") or 0) * 20)
    trabajo = io.get("can_engine_worktime")
    horas = round(trabajo / 60, 1) if trabajo else None
    tipo = d.get("asset_type") or "truck"
    imei = d["imei"]
    nombre = d.get("nombre") or imei
    ts = d.get("ts")
    tenant = d.get("tenant_id") or 1

    return {
        "id": f"asset-{d['id']}",
        "dbId": d["id"],
        "companyId": f"company-{tenant:03d}",
        "applicationId": f"app-{tenant:03d}",
        "sucursalId": d.get("sucursal"),
        "assetTagIds": [str(t) for t in (d.get("tags") or [])],
        "source": "aurora",

        "estado": estado,
        "vehiculo": nombre, "name": nombre, "nombrePantalla": nombre,
        "patente": d.get("patente") or "", "patent": d.get("patente") or "",
        "conductor": d.get("conductor") or "-",
        "datosUlt": ts.strftime("%H:%M:%S") if ts else "-",
        "choque": "-",

        "lat": (d["lat_e7"] / 1e7) if d.get("lat_e7") is not None else None,
        "lng": (d["lon_e7"] / 1e7) if d.get("lon_e7") is not None else None,
        "speed": vel,
        "velocidad": f"{vel} km/h",
        "heading": int(d.get("angle") or 0),

        "combustible": f"{comb}%" if comb is not None else "-",
        "fuelPercent": comb,
        "combustibleNivel": comb,

        "odometro": f"{odo_km:,.1f} km".replace(",", "\u00a7").replace(".", ",").replace("\u00a7", "."),
        "odometer": odo_km,
        "horometroTotal": f"{horas} h" if horas else "-",
        "horometroDiario": "-",
        "engineHours": horas,

        "direccion": f"Ultima ubicacion registrada de {nombre}",
        "imei": imei, "deviceId": imei,
        "protocol": "tcp",
        "trackerModel": (d.get("modelo") or "").lower(),
        "trackerModelLabel": f"Teltonika {d.get('modelo')}" if d.get("modelo") else "Teltonika",
        "trackerManufacturer": "Teltonika",

        "assetType": tipo, "tipoActivo": tipo,
        "assetTypeLabel": ETIQUETA_TIPO.get(tipo, tipo),
        "tipoActivoLabel": ETIQUETA_TIPO.get(tipo, tipo),
        "mapIcon": d.get("map_icon") or f"vehicle-{tipo}",
        "markerIcon": d.get("map_icon") or f"vehicle-{tipo}",

        "marca": d.get("marca"), "modeloVehiculo": d.get("modelo_vehiculo"),
        "anio": d.get("anio"), "vin": d.get("vin"), "color": d.get("color"),
        "sim": d.get("sim"), "ciudad": d.get("ciudad"),
        "perfilOperacional": d.get("perfil_operacional"),
        "descripcion": d.get("notas") or f"Activo operativo {d.get('patente') or imei}",
        "fechaIngreso": d["created_at"].strftime("%Y-%m-%d") if d.get("created_at") else "-",
        "fechaBaja": "-", "fechaSuspension": "-",

        "gpsSignal": senal,
        "gpsSignalLabel": f"{senal}%",
        "gpsSatellites": int(d.get("sats") or 0),
        "gpsFix": "Fix 3D" if d.get("gps_valid") else "Sin fix",

        "canStatus": "OK" if online else "Sin datos",
        "canRpm": io.get("can_engine_rpm") or 0,
        "canEngineTemp": io.get("can_engine_temp") or 0,
        "canBatteryVoltage": volt,
        "canEngineLoad": io.get("can_engine_load") or 0,
        "canThrottle": io.get("can_pedal_position") or 0,
        "canFuelRate": io.get("can_fuel_rate") or 0,
        "canFuelUsed": io.get("can_fuel_consumed") or 0,
        "canOilPressure": io.get("can_oil_pressure") or 0,
        "canAdBlueLevel": io.get("can_adblue_level") or 0,
        "canDtcCount": io.get("dtc_errors") or 0,
        "canSummary": (f"RPM {io.get('can_engine_rpm') or 0} / "
                       f"{io.get('can_engine_temp') or 0} C / "
                       f"{io.get('can_engine_load') or 0}%") if online else "Sin datos",
    }


async def _traer_activos(conn, db_id=None):
    sql = SQL_ASSETS
    args = ()
    if db_id is not None:
        sql = sql.replace("WHERE d.activo", "WHERE d.activo AND d.id = %s")
        args = (db_id,)
    cur = await conn.execute(sql, args)
    cols = [c.name for c in cur.description]
    return [dict(zip(cols, r)) for r in await cur.fetchall()]


class ActivoIn(BaseModel):
    imei: str
    nombre: Optional[str] = None
    vehiculo: Optional[str] = None
    patente: Optional[str] = None
    modelo: Optional[str] = None
    assetType: Optional[str] = None
    mapIcon: Optional[str] = None
    conductor: Optional[str] = None
    ciudad: Optional[str] = None
    sucursalId: Optional[str] = None
    marca: Optional[str] = None
    modeloVehiculo: Optional[str] = None
    anio: Optional[int] = None
    vin: Optional[str] = None
    color: Optional[str] = None
    sim: Optional[str] = None
    notas: Optional[str] = None
    tenantId: int = DEFAULT_TENANT_ID


@app.get("/api/assets")
async def listar_activos():
    async with pool.connection() as conn:
        return [fila_a_activo(d) for d in await _traer_activos(conn)]


@app.post("/api/assets", status_code=201)
async def crear_activo(a: ActivoIn):
    imei = (a.imei or "").strip()
    if not (15 <= len(imei) <= 17) or not imei.isdigit():
        raise HTTPException(400, "IMEI invalido: deben ser 15 a 17 digitos")

    nombre = a.vehiculo or a.nombre or f"Equipo {imei}"
    async with pool.connection() as conn:
        cur = await conn.execute(
            "INSERT INTO devices (tenant_id, imei, nombre, patente, modelo, "
            "  asset_type, map_icon, conductor, ciudad, sucursal, marca, "
            "  modelo_vehiculo, anio, vin, color, sim, notas) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (imei) DO NOTHING RETURNING id",
            (a.tenantId, imei, nombre, a.patente, a.modelo or "*",
             a.assetType, a.mapIcon, a.conductor, a.ciudad, a.sucursalId,
             a.marca, a.modeloVehiculo, a.anio, a.vin, a.color, a.sim, a.notas))
        row = await cur.fetchone()
        if row is None:
            raise HTTPException(409, f"El IMEI {imei} ya esta registrado")
        filas = await _traer_activos(conn, row[0])

    _device_cache.pop(imei, None)
    # El equipo puede conectar de inmediato: la whitelist se refresca sola.
    try:
        await rds.sadd(KEY_ALLOWED, imei)
    except Exception as exc:
        log.warning("No se pudo agregar %s a la whitelist: %s", imei, exc)

    return fila_a_activo(filas[0])


CAMPOS_EDITABLES = {
    "vehiculo": "nombre", "nombre": "nombre", "patente": "patente",
    "modelo": "modelo", "assetType": "asset_type", "mapIcon": "map_icon",
    "conductor": "conductor", "ciudad": "ciudad", "sucursalId": "sucursal",
    "marca": "marca", "modeloVehiculo": "modelo_vehiculo", "anio": "anio",
    "vin": "vin", "color": "color", "sim": "sim", "notas": "notas",
    "imei": "imei",
}


@app.patch("/api/assets/{asset_id}")
async def editar_activo(asset_id: str, cambios: Dict[str, Any]):
    db_id = int(str(asset_id).replace("asset-", ""))

    sets, args = [], []
    for clave, valor in cambios.items():
        col = CAMPOS_EDITABLES.get(clave)
        if col and f"{col} = %s" not in sets:
            sets.append(f"{col} = %s")
            args.append(valor)
    if not sets:
        raise HTTPException(400, "Nada que actualizar")

    sets.append("updated_at = now()")
    args.append(db_id)

    async with pool.connection() as conn:
        cur = await conn.execute(
            f"UPDATE devices SET {', '.join(sets)} WHERE id = %s RETURNING imei", args)
        row = await cur.fetchone()
        if row is None:
            raise HTTPException(404, "Activo no encontrado")
        filas = await _traer_activos(conn, db_id)

    _device_cache.clear()
    try:
        await rds.sadd(KEY_ALLOWED, row[0])
    except Exception:
        pass
    return fila_a_activo(filas[0])


@app.delete("/api/assets/{asset_id}", status_code=204)
async def eliminar_activo(asset_id: str):
    """Baja logica: el historico de telemetria no se toca."""
    db_id = int(str(asset_id).replace("asset-", ""))
    async with pool.connection() as conn:
        cur = await conn.execute(
            "UPDATE devices SET activo = false, updated_at = now() "
            "WHERE id = %s RETURNING imei", (db_id,))
        row = await cur.fetchone()
        if row is None:
            raise HTTPException(404, "Activo no encontrado")
    _device_cache.clear()
    try:
        await rds.srem(KEY_ALLOWED, row[0])
    except Exception:
        pass
    return Response(status_code=204)
'''

# Insertar antes del WebSocket, que tiene que quedar al final
marca = '@app.websocket("/ws/live")'
if s.count(marca) != 1:
    sys.exit(f"ERROR: no se encontro el punto de insercion ({s.count(marca)})")

s = s.replace(marca, BLOQUE.strip() + "\n\n\n" + marca)
API.write_text(s)
py_compile.compile(str(API), doraise=True)
print(f"Endpoints de activos agregados ({len(s)} bytes).")
