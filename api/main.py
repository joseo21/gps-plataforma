#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API GpsSinergy.

Dimensionada para 2049 equipos cada 15 s (136 inserciones/s) y 500
usuarios concurrentes.

Dos decisiones que sostienen esa carga:

1. Los IO calientes van a columnas tipadas, no a jsonb. Ver schema.sql.
2. /api/live NUNCA toca Postgres. Un worker refresca un JSON por empresa
   en Redis cada pocos segundos; la API lo devuelve tal cual. Sin esto,
   500 usuarios refrescando el mapa cada 10 s son 100.000 filas/s
   saliendo de la base para mostrar lo mismo 500 veces.
"""

import asyncio
import json
import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from psycopg_pool import AsyncConnectionPool
from psycopg.types.json import Jsonb
from pydantic import BaseModel

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(),
                    format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("api")

DATABASE_URL = os.environ["DATABASE_URL"]
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
CMD_URL = os.getenv("CMD_URL", "http://ingestor:5028")

TS_MAX_PAST_DAYS = int(os.getenv("TS_MAX_PAST_DAYS", "90"))
TS_MAX_FUTURE_HOURS = int(os.getenv("TS_MAX_FUTURE_HOURS", "24"))
LIVE_REFRESH_S = float(os.getenv("LIVE_REFRESH_S", "3"))
AUTO_CREATE_DEVICES = os.getenv("AUTO_CREATE_DEVICES", "1").lower() in ("1", "true", "yes")
DEFAULT_TENANT_ID = int(os.getenv("DEFAULT_TENANT_ID", "1"))

CANAL_LIVE = "gps:live"
KEY_LIVE = "gps:live:tenant:{tid}"
KEY_ALLOWED = "gps:allowed_imei"

pool: Optional[AsyncConnectionPool] = None
rds: Optional[aioredis.Redis] = None
_device_cache: Dict[str, tuple] = {}
_io_defs: Dict[str, Dict[int, tuple]] = {}

# Nombres que emite server.py -> columna. Lo que no esta aca va a io_extra.
IO_A_COLUMNA = {
    "Ignition":         ("ignition",       "bool"),
    "Movement":         ("movement",       "bool"),
    "External Voltage": ("ext_voltage_cv", "cv"),
    "Battery Voltage":  ("bat_voltage_cv", "cv"),
    "Total Odometer":   ("odometer_m",     "int"),
    "GSM Signal":       ("gsm_signal",     "int"),
    "Fuel Counter":     ("fuel_used",      "int"),
    # El AVL 87 ("Fuel Level" en el mapa generico) es kilometraje total
    # en equipos con CAN. No se promueve: queda en io_extra intacto.
}

# Claves redundantes que el parser genera por duplicado.
DESCARTAR = {"IButton", "IButton_Reverse", "IButton_Connected", "Speed"}

COLS = ["device_id", "ts", "lat_e7", "lon_e7", "speed", "angle", "altitude",
        "sats", "gps_valid", "ignition", "movement", "ext_voltage_cv",
        "bat_voltage_cv", "odometer_m", "gsm_signal", "fuel_level",
        "fuel_used", "event_id", "priority", "lag_s", "io_extra"]

SQL_INSERT = (f"INSERT INTO telemetry ({', '.join(COLS)}) "
              f"VALUES ({', '.join(['%s'] * len(COLS))}) "
              f"ON CONFLICT (device_id, ts) DO NOTHING")


# ---------------------------------------------------------------------
# Tareas de fondo
# ---------------------------------------------------------------------

async def _loop_particiones():
    while True:
        try:
            async with pool.connection() as conn:
                await conn.execute("SELECT ensure_partitions_ahead()")
        except Exception as exc:
            log.error("ensure_partitions_ahead: %s", exc)
        await asyncio.sleep(3600)


async def _loop_live_cache():
    """Refresca el estado de la flota en Redis. Una consulta cada pocos
    segundos alimenta a todos los usuarios conectados."""
    while True:
        try:
            async with pool.connection() as conn:
                cur = await conn.execute(
                    "SELECT d.tenant_id, d.id, d.imei, d.nombre, d.patente, "
                    "       s.ts, (s.lat_e7/1e7)::float8, (s.lon_e7/1e7)::float8, "
                    "       s.speed, s.angle, "
                    "       s.gps_valid, s.ignition, s.movement, "
                    "       (s.ext_voltage_cv/100.0)::float8, s.odometer_m, "
                    "       (now() - s.ts) < interval '10 minutes', "
                    "       s.sats, s.gsm_signal, s.fuel_level, s.io_extra "
                    "FROM devices d LEFT JOIN device_state s ON s.device_id = d.id "
                    "WHERE d.activo")
                por_tenant: Dict[int, List[Dict[str, Any]]] = {}
                for r in await cur.fetchall():
                    por_tenant.setdefault(r[0], []).append({
                        "id": r[1], "imei": r[2], "nombre": r[3], "patente": r[4],
                        "ts": r[5].isoformat() if r[5] else None,
                        "lat": r[6], "lon": r[7], "speed": r[8], "angle": r[9],
                        "gps_valid": r[10], "ignition": r[11], "movement": r[12],
                        "ext_voltage": r[13], "odometer_m": r[14],
                        "online": bool(r[15]),
                        "sats": r[16], "gsm_signal": r[17],
                        "fuel_level": r[18], "io_extra": r[19] or {},
                    })
            pipe = rds.pipeline()
            for tid, lista in por_tenant.items():
                pipe.set(KEY_LIVE.format(tid=tid), json.dumps(lista), ex=60)
            await pipe.execute()
        except Exception as exc:
            log.error("live cache: %s", exc)
        await asyncio.sleep(LIVE_REFRESH_S)


async def _loop_io_defs():
    """Trae io_definitions a memoria. Cambiar un mapeo es un UPDATE."""
    global _io_defs
    while True:
        try:
            async with pool.connection() as conn:
                cur = await conn.execute(
                    "SELECT modelo, avl_id, nombre, escala, tipo, columna, "
                    "       coalesce(signed,false), bits FROM io_definitions")
                nuevo: Dict[str, Dict[int, tuple]] = {}
                for m, aid, nom, esc, tipo, col, sig, bits in await cur.fetchall():
                    nuevo.setdefault(m, {})[aid] = (nom, float(esc), tipo, col, sig, bits)
            _io_defs = nuevo
            _device_cache.clear()
            log.info("io_definitions: %s", {m: len(d) for m, d in nuevo.items()})
        except Exception as exc:
            log.error("io_defs: %s", exc)
        await asyncio.sleep(60)


async def _loop_whitelist():
    """Publica en Redis los IMEI habilitados, para que el ingestor
    rechace el handshake de cualquier otro."""
    while True:
        try:
            async with pool.connection() as conn:
                cur = await conn.execute("SELECT imei FROM devices WHERE activo")
                imeis = [r[0] for r in await cur.fetchall()]
            if imeis:
                pipe = rds.pipeline()
                pipe.delete(KEY_ALLOWED)
                pipe.sadd(KEY_ALLOWED, *imeis)
                await pipe.execute()
        except Exception as exc:
            log.error("whitelist: %s", exc)
        await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool, rds
    pool = AsyncConnectionPool(DATABASE_URL, min_size=4, max_size=20, open=False)
    await pool.open(wait=True)
    rds = aioredis.from_url(REDIS_URL, decode_responses=True)
    tareas = [asyncio.create_task(f()) for f in
              (_loop_particiones, _loop_live_cache, _loop_whitelist, _loop_io_defs)]
    log.info("API lista")
    yield
    for t in tareas:
        t.cancel()
    await pool.close()
    await rds.aclose()


app = FastAPI(title="GpsSinergy API", lifespan=lifespan)
app.add_middleware(CORSMiddleware,
                   allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
                   allow_methods=["*"], allow_headers=["*"])


# ---------------------------------------------------------------------
# Entrada
# ---------------------------------------------------------------------

class Record(BaseModel):
    ts: float
    event_id: int = 0
    priority: int = 0
    gps: Optional[Dict[str, Any]] = None
    io: Dict[str, Any] = {}
    io_raw: Dict[str, Any] = {}


class IngestPayload(BaseModel):
    imei: str
    codec: int = 8
    records: List[Record] = []
    payload_hex: Optional[str] = None


def _num(v):
    if isinstance(v, bool) or v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _bool(v):
    return v if isinstance(v, bool) else (bool(v) if isinstance(v, int) else None)


def _clamp(v, lo, hi):
    """Nunca confiar en el rango de lo que manda un equipo."""
    if v is None or isinstance(v, bool):
        return None
    try:
        v = int(v)
    except (TypeError, ValueError):
        return None
    return v if lo <= v <= hi else None


def _alt_i16(v):
    """La altitud Teltonika es entero de 16 bits CON signo, pero el parser
    la lee sin signo: 65535 significa -1 metro. Sin esto, cualquier
    altitud negativa desborda smallint y tumba el lote entero."""
    if v is None or isinstance(v, bool):
        return None
    try:
        v = int(v)
    except (TypeError, ValueError):
        return None
    if v > 32767:
        v -= 65536
    return _clamp(v, -32768, 32767)


def separar_io(io: Dict[str, Any]):
    """Reparte los IO entre columnas tipadas y el jsonb de sobras."""
    cols: Dict[str, Any] = {}
    extra: Dict[str, Any] = {}
    for k, v in io.items():
        # El parser emite cada valor dos veces: escalado y "_raw". Guardar
        # ambos en cada registro duplica el jsonb para siempre. El crudo se
        # puede recalcular desde el escalado y la trama cruda queda en
        # raw_frames, asi que se descarta.
        if k.endswith("_raw") or k in DESCARTAR:
            continue
        destino = IO_A_COLUMNA.get(k)
        if destino is None:
            extra[k] = v
            continue
        col, tipo = destino
        if tipo == "bool":
            cols[col] = _bool(v)
            continue
        if tipo == "cv":
            n = _num(v)
            val = _clamp(round(n * 100), -32768, 32767) if n is not None else None
        elif col in ("odometer_m", "fuel_used"):
            val = _clamp(_num(v), -2147483648, 2147483647)
        else:
            val = _clamp(_num(v), -32768, 32767)
        if val is None and v is not None:
            # No cabe en la columna: se conserva en io_extra en vez de
            # perderlo. Casi siempre significa mapeo equivocado.
            extra[k] = v
        else:
            cols[col] = val
    return cols, (extra or None)


def _con_signo(v: int, bits: int) -> int:
    mask = (1 << bits) - 1
    v &= mask
    return v - (1 << bits) if (v & (1 << (bits - 1))) else v


def resolver_io(io_raw: Dict[str, Any], modelo: str):
    """Traduce {avl_id: crudo} usando el diccionario del modelo. Un ID sin
    definir se guarda por su numero: nunca se descarta un dato."""
    porm = _io_defs.get(modelo) or {}
    gen = _io_defs.get("*") or {}
    cols: Dict[str, Any] = {}
    extra: Dict[str, Any] = {}
    for k, v in io_raw.items():
        try:
            aid = int(k)
        except (TypeError, ValueError):
            extra[str(k)] = v
            continue
        d = porm.get(aid) or gen.get(aid)
        if d is None:
            extra[str(aid)] = v
            continue
        nombre, escala, tipo, columna, signed, bits = d
        if isinstance(v, str):
            val = v
        elif tipo == "bool":
            val = bool(v)
        elif tipo == "hex":
            val = f"0x{int(v):X}" if v else None
        else:
            val = int(v)
            if signed and bits:
                val = _con_signo(val, int(bits))
            if escala != 1:
                val = val * escala
        if not columna or val is None or isinstance(val, str):
            if val is not None:
                extra[nombre] = val
            continue
        if columna.endswith("_cv"):
            puesto = _clamp(round(val * 100), -32768, 32767)
        elif columna in ("odometer_m", "fuel_used"):
            puesto = _clamp(val, -2147483648, 2147483647)
        elif columna in ("ignition", "movement"):
            puesto = bool(val)
        else:
            puesto = _clamp(val, -32768, 32767)
        if puesto is None and val is not None:
            extra[nombre] = val
        else:
            cols[columna] = puesto
    return cols, (extra or None)


def gps_valido(gps: Optional[Dict[str, Any]]) -> bool:
    """Sin fix el equipo manda 0,0 con 0 satelites. Eso no es posicion."""
    if not gps:
        return False
    lat, lon = gps.get("lat"), gps.get("lon")
    if lat is None or lon is None:
        return False
    if abs(lat) < 1e-6 and abs(lon) < 1e-6:
        return False
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return False
    return (gps.get("sat") or 0) > 0


def ts_valido(ts: float, ahora: datetime) -> Optional[datetime]:
    try:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None
    if not ((ahora - timedelta(days=TS_MAX_PAST_DAYS)) <= dt
            <= (ahora + timedelta(hours=TS_MAX_FUTURE_HOURS))):
        return None
    return dt


async def resolver_device(conn, imei: str):
    """Devuelve (device_id, modelo) o None."""
    if imei in _device_cache:
        return _device_cache[imei]
    cur = await conn.execute("SELECT id, modelo FROM devices WHERE imei = %s", (imei,))
    row = await cur.fetchone()
    if row:
        _device_cache[imei] = (row[0], row[1] or "*")
        return _device_cache[imei]
    if not AUTO_CREATE_DEVICES:
        return None
    cur = await conn.execute(
        "INSERT INTO devices (tenant_id, imei, nombre) VALUES (%s,%s,%s) "
        "ON CONFLICT (imei) DO UPDATE SET imei = EXCLUDED.imei RETURNING id, modelo",
        (DEFAULT_TENANT_ID, imei, f"Teltonika {imei}"))
    row = await cur.fetchone()
    _device_cache[imei] = (row[0], row[1] or "*")
    log.info("Alta automatica: %s", imei)
    return _device_cache[imei]


@app.post("/ingest/teltonika/ingest")
async def ingest(payload: IngestPayload):
    imei = payload.imei.strip()
    if not imei:
        raise HTTPException(400, "IMEI vacio")

    ahora = datetime.now(timezone.utc)
    async with pool.connection() as conn:
        info = await resolver_device(conn, imei)
    if info is None:
        raise HTTPException(403, f"IMEI {imei} no registrado")
    device_id, modelo = info

    filas, descartes = [], 0

    for rec in payload.records:
        dt = ts_valido(rec.ts, ahora)
        if dt is None:
            descartes += 1
            continue
        gps = rec.gps or {}
        ok = gps_valido(rec.gps)
        if rec.io_raw:
            cols, extra = resolver_io(rec.io_raw, modelo)
        else:
            cols, extra = separar_io(rec.io)
        filas.append({
            "ts": dt,
            "lat_e7": int(round(gps["lat"] * 1e7)) if ok else None,
            "lon_e7": int(round(gps["lon"] * 1e7)) if ok else None,
            "speed": _clamp(gps.get("speed") or 0, 0, 32767),
            "angle": _clamp(gps.get("angle"), 0, 32767),
            "altitude": _alt_i16(gps.get("alt")),
            "sats": _clamp(gps.get("sat"), 0, 255),
            "gps_valid": ok,
            "ignition": cols.get("ignition"),
            "movement": cols.get("movement"),
            "ext_voltage_cv": cols.get("ext_voltage_cv"),
            "bat_voltage_cv": cols.get("bat_voltage_cv"),
            "odometer_m": cols.get("odometer_m"),
            "gsm_signal": cols.get("gsm_signal"),
            "fuel_level": cols.get("fuel_level"),
            "fuel_used": cols.get("fuel_used"),
            "event_id": _clamp(rec.event_id, -2147483648, 2147483647),
            "priority": _clamp(rec.priority, -32768, 32767),
            "lag_s": _clamp((ahora - dt).total_seconds(), -2147483648, 2147483647),
            "io_extra": Jsonb(extra) if extra else None,
        })

    async with pool.connection() as conn:
        if payload.payload_hex:
            await conn.execute(
                "INSERT INTO raw_frames (imei, codec, payload) VALUES (%s,%s,%s)",
                (imei, payload.codec, bytes.fromhex(payload.payload_hex)))

        if descartes:
            await conn.execute(
                "INSERT INTO ingest_rejects (imei, motivo, detalle) VALUES (%s,%s,%s)",
                (imei, "ts_fuera_de_rango", Jsonb({"n": descartes})))

        if not filas:
            return {"ok": True, "insertados": 0, "descartados": descartes}

        async with conn.cursor() as cur:
            await cur.executemany(
                SQL_INSERT,
                [tuple([device_id] + [f[c] for c in COLS[1:]]) for f in filas])

        # Estado en vivo: solo el mas nuevo, y solo si supera al guardado.
        # Esto evita que un lote con buffer mande el vehiculo al pasado.
        n = max(filas, key=lambda f: f["ts"])
        cur = await conn.execute(
            "INSERT INTO device_state (device_id, ts, lat_e7, lon_e7, speed, angle, "
            " gps_valid, ignition, movement, ext_voltage_cv, odometer_m, "
            " sats, gsm_signal, fuel_level, io_extra, updated_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now()) "
            "ON CONFLICT (device_id) DO UPDATE SET "
            "  ts=EXCLUDED.ts, lat_e7=EXCLUDED.lat_e7, lon_e7=EXCLUDED.lon_e7, "
            "  speed=EXCLUDED.speed, angle=EXCLUDED.angle, gps_valid=EXCLUDED.gps_valid, "
            "  ignition=EXCLUDED.ignition, movement=EXCLUDED.movement, "
            "  ext_voltage_cv=EXCLUDED.ext_voltage_cv, odometer_m=EXCLUDED.odometer_m, "
            "  sats=EXCLUDED.sats, gsm_signal=EXCLUDED.gsm_signal, "
            "  fuel_level=EXCLUDED.fuel_level, io_extra=EXCLUDED.io_extra, updated_at=now() "
            "WHERE device_state.ts < EXCLUDED.ts RETURNING device_id",
            (device_id, n["ts"], n["lat_e7"], n["lon_e7"], n["speed"], n["angle"],
             n["gps_valid"], n["ignition"], n["movement"], n["ext_voltage_cv"],
             n["odometer_m"], n["sats"], n["gsm_signal"], n["fuel_level"],
             n["io_extra"]))
        actualizado = await cur.fetchone() is not None

    if actualizado:
        await rds.publish(CANAL_LIVE, json.dumps({
            "imei": imei, "device_id": device_id, "ts": n["ts"].isoformat(),
            "lat": n["lat_e7"] / 1e7 if n["lat_e7"] is not None else None,
            "lon": n["lon_e7"] / 1e7 if n["lon_e7"] is not None else None,
            "speed": n["speed"], "angle": n["angle"],
            "gps_valid": n["gps_valid"], "ignition": n["ignition"],
            "movement": n["movement"], "sats": n["sats"],
            "gsm_signal": n["gsm_signal"], "fuel_level": n["fuel_level"],
            "ext_voltage": (n["ext_voltage_cv"] / 100.0
                            if n["ext_voltage_cv"] is not None else None),
            "odometer_m": n["odometer_m"],
            "io_extra": (n["io_extra"].obj if n["io_extra"] else {}),
            "online": True,
        }))

    return {"ok": True, "insertados": len(filas), "descartados": descartes}


# ---------------------------------------------------------------------
# Consulta
# ---------------------------------------------------------------------

@app.get("/api/health")
async def health():
    async with pool.connection() as conn:
        await conn.execute("SELECT 1")
    await rds.ping()
    return {"ok": True}


@app.get("/api/live")
async def live(tenant_id: int = DEFAULT_TENANT_ID):
    """Sale de Redis. Nunca toca Postgres."""
    data = await rds.get(KEY_LIVE.format(tid=tenant_id))
    if data is None:
        raise HTTPException(503, "Cache en construccion, reintentar en unos segundos")
    return Response(content=data, media_type="application/json")


@app.get("/api/devices")
async def devices(tenant_id: Optional[int] = None):
    sql = "SELECT id, imei, nombre, patente, modelo, activo, tenant_id FROM devices"
    args = ()
    if tenant_id:
        sql += " WHERE tenant_id = %s"
        args = (tenant_id,)
    sql += " ORDER BY nombre NULLS LAST"
    async with pool.connection() as conn:
        cur = await conn.execute(sql, args)
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, r)) for r in await cur.fetchall()]


@app.get("/api/history")
async def history(imei: str,
                  desde: datetime = Query(...),
                  hasta: datetime = Query(...),
                  solo_validos: bool = True,
                  limite: int = Query(50000, le=200000)):
    """Consulta la vista, que ya devuelve lat/lon en grados. El WHERE
    sobre ts permite a Postgres podar particiones."""
    sql = ("SELECT t.ts, t.lat, t.lon, t.speed, t.angle, t.altitude, t.sats, "
           "       t.ignition, t.movement, t.ext_voltage, t.odometer_m, "
           "       t.fuel_level, t.event_id, t.io_extra "
           "FROM telemetry_v t JOIN devices d ON d.id = t.device_id "
           "WHERE d.imei = %s AND t.ts >= %s AND t.ts < %s")
    args = [imei, desde, hasta]
    if solo_validos:
        sql += " AND t.gps_valid"
    sql += " ORDER BY t.ts LIMIT %s"
    args.append(limite)
    async with pool.connection() as conn:
        cur = await conn.execute(sql, args)
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, r)) for r in await cur.fetchall()]


class Comando(BaseModel):
    imei: str
    command: str
    timeout: float = 10.0


@app.post("/api/command")
async def command(cmd: Comando):
    async with httpx.AsyncClient(timeout=cmd.timeout + 5) as client:
        try:
            r = await client.post(f"{CMD_URL}/command", json=cmd.model_dump())
        except httpx.HTTPError as exc:
            raise HTTPException(502, f"Ingestor no responde: {exc}")
    return r.json()


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

        "odometro": f"{odo_km:,.1f} km".replace(",", "§").replace(".", ",").replace("§", "."),
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


@app.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    await ws.accept()
    pubsub = rds.pubsub()
    await pubsub.subscribe(CANAL_LIVE)
    try:
        async for msg in pubsub.listen():
            if msg.get("type") == "message":
                await ws.send_text(msg["data"])
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        await pubsub.unsubscribe(CANAL_LIVE)
        await pubsub.aclose()
