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
_device_cache: Dict[str, int] = {}

# Nombres que emite server.py -> columna. Lo que no esta aca va a io_extra.
IO_A_COLUMNA = {
    "Ignition":         ("ignition",       "bool"),
    "Movement":         ("movement",       "bool"),
    "External Voltage": ("ext_voltage_cv", "cv"),
    "Battery Voltage":  ("bat_voltage_cv", "cv"),
    "Total Odometer":   ("odometer_m",     "int"),
    "GSM Signal":       ("gsm_signal",     "int"),
    "Fuel Level":       ("fuel_level",     "int"),
    "Fuel Counter":     ("fuel_used",      "int"),
}

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
                    "       s.ts, s.lat_e7/1e7, s.lon_e7/1e7, s.speed, s.angle, "
                    "       s.gps_valid, s.ignition, s.movement, "
                    "       s.ext_voltage_cv/100.0, s.odometer_m, "
                    "       (now() - s.ts) < interval '10 minutes' "
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
                    })
            pipe = rds.pipeline()
            for tid, lista in por_tenant.items():
                pipe.set(KEY_LIVE.format(tid=tid), json.dumps(lista), ex=60)
            await pipe.execute()
        except Exception as exc:
            log.error("live cache: %s", exc)
        await asyncio.sleep(LIVE_REFRESH_S)


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
              (_loop_particiones, _loop_live_cache, _loop_whitelist)]
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


def separar_io(io: Dict[str, Any]):
    """Reparte los IO entre columnas tipadas y el jsonb de sobras."""
    cols: Dict[str, Any] = {}
    extra: Dict[str, Any] = {}
    for k, v in io.items():
        destino = IO_A_COLUMNA.get(k)
        if destino is None:
            extra[k] = v
            continue
        col, tipo = destino
        if tipo == "bool":
            cols[col] = _bool(v)
        elif tipo == "cv":
            n = _num(v)
            cols[col] = int(round(n * 100)) if n is not None else None
        else:
            n = _num(v)
            cols[col] = int(n) if n is not None else None
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


async def resolver_device(conn, imei: str) -> Optional[int]:
    if imei in _device_cache:
        return _device_cache[imei]
    cur = await conn.execute("SELECT id FROM devices WHERE imei = %s", (imei,))
    row = await cur.fetchone()
    if row:
        _device_cache[imei] = row[0]
        return row[0]
    if not AUTO_CREATE_DEVICES:
        return None
    cur = await conn.execute(
        "INSERT INTO devices (tenant_id, imei, nombre) VALUES (%s,%s,%s) "
        "ON CONFLICT (imei) DO UPDATE SET imei = EXCLUDED.imei RETURNING id",
        (DEFAULT_TENANT_ID, imei, f"Teltonika {imei}"))
    row = await cur.fetchone()
    _device_cache[imei] = row[0]
    log.info("Alta automatica: %s", imei)
    return row[0]


@app.post("/ingest/teltonika/ingest")
async def ingest(payload: IngestPayload):
    imei = payload.imei.strip()
    if not imei:
        raise HTTPException(400, "IMEI vacio")

    ahora = datetime.now(timezone.utc)
    filas, descartes = [], 0

    for rec in payload.records:
        dt = ts_valido(rec.ts, ahora)
        if dt is None:
            descartes += 1
            continue
        gps = rec.gps or {}
        ok = gps_valido(rec.gps)
        cols, extra = separar_io(rec.io)
        filas.append({
            "ts": dt,
            "lat_e7": int(round(gps["lat"] * 1e7)) if ok else None,
            "lon_e7": int(round(gps["lon"] * 1e7)) if ok else None,
            "speed": int(gps.get("speed") or 0),
            "angle": gps.get("angle"),
            "altitude": gps.get("alt"),
            "sats": gps.get("sat"),
            "gps_valid": ok,
            "ignition": cols.get("ignition"),
            "movement": cols.get("movement"),
            "ext_voltage_cv": cols.get("ext_voltage_cv"),
            "bat_voltage_cv": cols.get("bat_voltage_cv"),
            "odometer_m": cols.get("odometer_m"),
            "gsm_signal": cols.get("gsm_signal"),
            "fuel_level": cols.get("fuel_level"),
            "fuel_used": cols.get("fuel_used"),
            "event_id": rec.event_id,
            "priority": rec.priority,
            "lag_s": int((ahora - dt).total_seconds()),
            "io_extra": Jsonb(extra) if extra else None,
        })

    async with pool.connection() as conn:
        device_id = await resolver_device(conn, imei)
        if device_id is None:
            raise HTTPException(403, f"IMEI {imei} no registrado")

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
            " gps_valid, ignition, movement, ext_voltage_cv, odometer_m, io_extra, updated_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now()) "
            "ON CONFLICT (device_id) DO UPDATE SET "
            "  ts=EXCLUDED.ts, lat_e7=EXCLUDED.lat_e7, lon_e7=EXCLUDED.lon_e7, "
            "  speed=EXCLUDED.speed, angle=EXCLUDED.angle, gps_valid=EXCLUDED.gps_valid, "
            "  ignition=EXCLUDED.ignition, movement=EXCLUDED.movement, "
            "  ext_voltage_cv=EXCLUDED.ext_voltage_cv, odometer_m=EXCLUDED.odometer_m, "
            "  io_extra=EXCLUDED.io_extra, updated_at=now() "
            "WHERE device_state.ts < EXCLUDED.ts RETURNING device_id",
            (device_id, n["ts"], n["lat_e7"], n["lon_e7"], n["speed"], n["angle"],
             n["gps_valid"], n["ignition"], n["movement"], n["ext_voltage_cv"],
             n["odometer_m"], n["io_extra"]))
        actualizado = await cur.fetchone() is not None

    if actualizado:
        await rds.publish(CANAL_LIVE, json.dumps({
            "imei": imei, "device_id": device_id, "ts": n["ts"].isoformat(),
            "lat": n["lat_e7"] / 1e7 if n["lat_e7"] is not None else None,
            "lon": n["lon_e7"] / 1e7 if n["lon_e7"] is not None else None,
            "speed": n["speed"], "angle": n["angle"],
            "gps_valid": n["gps_valid"], "ignition": n["ignition"],
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
