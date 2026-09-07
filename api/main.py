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
import hashlib
import json
import os
import logging
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import bcrypt
import httpx
import jwt
import redis.asyncio as aioredis
from fastapi import (Cookie, Depends, FastAPI, HTTPException, Query, Request,
                     Response, WebSocket, WebSocketDisconnect)
from fastapi.middleware.cors import CORSMiddleware
from psycopg_pool import AsyncConnectionPool
from psycopg.types.json import Jsonb
from pydantic import BaseModel

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(),
                    format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("api")

DATABASE_URL = os.environ["DATABASE_URL"]
if not os.environ.get("JWT_SECRET"):
    raise SystemExit("ERROR: falta JWT_SECRET en el entorno. "
                     "Generar con: openssl rand -hex 32")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
CMD_URL = os.getenv("CMD_URL", "http://ingestor:5028")

TS_MAX_PAST_DAYS = int(os.getenv("TS_MAX_PAST_DAYS", "90"))
TS_MAX_FUTURE_HOURS = int(os.getenv("TS_MAX_FUTURE_HOURS", "24"))
LIVE_REFRESH_S = float(os.getenv("LIVE_REFRESH_S", "3"))
AUTO_CREATE_DEVICES = os.getenv("AUTO_CREATE_DEVICES", "1").lower() in ("1", "true", "yes")
DEFAULT_TENANT_ID = int(os.getenv("DEFAULT_TENANT_ID", "1"))

JWT_SECRET = os.environ.get("JWT_SECRET", "")
JWT_ALG = "HS256"
ACCESS_TTL_MIN = int(os.getenv("ACCESS_TTL_MIN", "15"))
REFRESH_TTL_DIAS = int(os.getenv("REFRESH_TTL_DIAS", "30"))
COOKIE_SEGURA = os.getenv("COOKIE_SEGURA", "1").lower() in ("1", "true", "yes")
MAX_INTENTOS = int(os.getenv("MAX_INTENTOS", "5"))
BLOQUEO_MIN = int(os.getenv("BLOQUEO_MIN", "15"))

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


# ---------------------------------------------------------------------
# AUTENTICACION
# ---------------------------------------------------------------------

def hash_password(clave: str) -> str:
    b = clave.encode("utf-8")
    if len(b) > 72:
        # bcrypt ignora en silencio lo que pase de 72 bytes: dos claves
        # distintas con el mismo prefijo entrarian igual. Se rechaza.
        raise HTTPException(400, "La contrasena no puede superar 72 bytes")
    return bcrypt.hashpw(b, bcrypt.gensalt(rounds=12)).decode()


def verificar_password(clave: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(clave.encode("utf-8")[:72], hashed.encode())
    except (ValueError, TypeError):
        return False


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def crear_access_token(user_id: int, email: str) -> str:
    ahora = datetime.now(timezone.utc)
    return jwt.encode({
        "sub": str(user_id), "email": email, "iat": ahora,
        "exp": ahora + timedelta(minutes=ACCESS_TTL_MIN),
    }, JWT_SECRET, algorithm=JWT_ALG)


def _set_cookies(resp: Response, access: str, refresh: Optional[str] = None):
    resp.set_cookie("gps_at", access, httponly=True, secure=COOKIE_SEGURA,
                    samesite="lax", max_age=ACCESS_TTL_MIN * 60, path="/")
    if refresh is not None:
        resp.set_cookie("gps_rt", refresh, httponly=True, secure=COOKIE_SEGURA,
                        samesite="lax", max_age=REFRESH_TTL_DIAS * 86400,
                        path="/api/auth")


async def auditar(conn, user_id, tenant_id, modulo, accion,
                  entidad=None, entidad_id=None, detalle=None, req=None):
    ip = None
    ua = None
    if req is not None:
        # El primer valor de X-Forwarded-For es el cliente real; los
        # siguientes son los proxies. Tomar req.client.host daria siempre
        # la IP de nginx y la auditoria no serviria de nada.
        xff = req.headers.get("x-forwarded-for", "")
        ip = xff.split(",")[0].strip() or (req.client.host if req.client else None)
        ua = req.headers.get("user-agent")
    await conn.execute(
        "INSERT INTO audit_log (user_id, tenant_id, modulo, accion, entidad, "
        " entidad_id, detalle, ip, user_agent) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (user_id, tenant_id, modulo, accion, entidad, entidad_id,
         Jsonb(detalle) if detalle else None, ip, ua))


class LoginIn(BaseModel):
    email: str
    password: str


class CambioPassword(BaseModel):
    actual: str
    nueva: str


async def _datos_usuario(conn, user_id: int) -> Dict[str, Any]:
    """Usuario con sus empresas, rol y permisos efectivos."""
    cur = await conn.execute(
        "SELECT id, email, nombre, apellido, telefono, es_superadmin, "
        "       debe_cambiar_pw FROM users WHERE id = %s AND activo", (user_id,))
    u = await cur.fetchone()
    if u is None:
        raise HTTPException(401, "Usuario inactivo")

    cur = await conn.execute(
        "SELECT a.id, a.tenant_id, t.nombre, a.role_id, r.nombre, a.alcance "
        "FROM accesses a "
        "JOIN tenants t ON t.id = a.tenant_id "
        "JOIN roles r ON r.id = a.role_id "
        "WHERE a.user_id = %s AND a.activo AND t.activo "
        "ORDER BY t.nombre", (user_id,))
    accesos = []
    for r in await cur.fetchall():
        cur2 = await conn.execute(
            "SELECT module_function_id, accion FROM permisos_efectivos(%s)", (r[0],))
        permisos: Dict[str, Dict[str, bool]] = {}
        for fid, accion in await cur2.fetchall():
            permisos.setdefault(fid, {})[accion] = True
        accesos.append({
            "accessId": r[0], "companyId": f"company-{r[1]:03d}", "tenantId": r[1],
            "companyName": r[2], "roleId": r[3], "roleName": r[4],
            "alcance": r[5], "permissions": permisos,
        })

    if u[5]:  # superadmin: acceso a todas las empresas
        cur = await conn.execute(
            "SELECT id, nombre FROM tenants WHERE activo ORDER BY nombre")
        conocidos = {a["tenantId"] for a in accesos}
        cur2 = await conn.execute("SELECT id, acciones FROM module_functions")
        todas = {r[0]: {a: True for a in r[1]} for r in await cur2.fetchall()}
        for r in await cur.fetchall():
            if r[0] not in conocidos:
                accesos.append({
                    "accessId": None, "companyId": f"company-{r[0]:03d}",
                    "tenantId": r[0], "companyName": r[1], "roleId": None,
                    "roleName": "Administrador de plataforma",
                    "alcance": "todos", "permissions": todas,
                })

    return {
        "id": f"user-{u[0]:03d}", "dbId": u[0], "email": u[1],
        "nombre": u[2], "apellido": u[3], "telefono": u[4],
        "isPlatformAdmin": u[5], "debeCambiarPassword": u[6],
        "accesses": accesos,
    }


@app.post("/api/auth/login")
async def login(datos: LoginIn, respuesta: Response, req: Request):
    email = (datos.email or "").strip().lower()
    ahora = datetime.now(timezone.utc)
    generico = HTTPException(401, "Correo o contrasena incorrectos")

    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT id, password_hash, activo, intentos_fallidos, bloqueado_hasta "
            "FROM users WHERE lower(email) = %s", (email,))
        u = await cur.fetchone()

        if u is None:
            # Se gasta el mismo tiempo que con un usuario real para no
            # revelar por temporizacion que el correo no existe.
            bcrypt.checkpw(b"x", bcrypt.hashpw(b"x", bcrypt.gensalt(rounds=12)))
            raise generico

        user_id, pw_hash, activo, intentos, bloqueado = u

        if bloqueado and bloqueado > ahora:
            raise HTTPException(429, "Cuenta bloqueada temporalmente por "
                                     "intentos fallidos. Reintente mas tarde.")
        if not activo:
            raise generico

        if not verificar_password(datos.password, pw_hash):
            intentos = (intentos or 0) + 1
            hasta = (ahora + timedelta(minutes=BLOQUEO_MIN)
                     if intentos >= MAX_INTENTOS else None)
            await conn.execute(
                "UPDATE users SET intentos_fallidos = %s, bloqueado_hasta = %s "
                "WHERE id = %s", (intentos, hasta, user_id))
            await auditar(conn, user_id, None, "auth", "login_fallido", req=req)
            raise generico

        await conn.execute(
            "UPDATE users SET intentos_fallidos = 0, bloqueado_hasta = NULL, "
            "ultimo_acceso = now() WHERE id = %s", (user_id,))

        refresh = secrets.token_urlsafe(48)
        await conn.execute(
            "INSERT INTO refresh_tokens (user_id, token_hash, expira, user_agent, ip) "
            "VALUES (%s,%s,%s,%s,%s)",
            (user_id, _hash_token(refresh),
             ahora + timedelta(days=REFRESH_TTL_DIAS),
             req.headers.get("user-agent"),
             req.headers.get("x-forwarded-for", "").split(",")[0].strip()
             or req.client.host))

        usuario = await _datos_usuario(conn, user_id)
        await auditar(conn, user_id, None, "auth", "login", req=req)

    _set_cookies(respuesta, crear_access_token(user_id, email), refresh)
    return usuario


@app.post("/api/auth/refresh")
async def refrescar(respuesta: Response, req: Request,
                    gps_rt: Optional[str] = Cookie(None)):
    if not gps_rt:
        raise HTTPException(401, "Sin token de refresco")
    ahora = datetime.now(timezone.utc)
    th = _hash_token(gps_rt)

    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT id, user_id, expira, revocado FROM refresh_tokens "
            "WHERE token_hash = %s", (th,))
        t = await cur.fetchone()
        if t is None:
            raise HTTPException(401, "Token invalido")
        tid, user_id, expira, revocado = t

        if revocado:
            # Reuso de un token ya rotado: senal de robo. Se revoca todo.
            await conn.execute(
                "UPDATE refresh_tokens SET revocado = true WHERE user_id = %s",
                (user_id,))
            await auditar(conn, user_id, None, "auth", "reuso_token_revocado",
                          req=req)
            log.warning("Reuso de refresh token revocado, user_id=%s", user_id)
            raise HTTPException(401, "Sesion invalidada por seguridad")

        if expira <= ahora:
            raise HTTPException(401, "Sesion expirada")

        nuevo = secrets.token_urlsafe(48)
        await conn.execute(
            "UPDATE refresh_tokens SET revocado = true WHERE id = %s", (tid,))
        await conn.execute(
            "INSERT INTO refresh_tokens (user_id, token_hash, expira, user_agent, ip) "
            "VALUES (%s,%s,%s,%s,%s)",
            (user_id, _hash_token(nuevo), ahora + timedelta(days=REFRESH_TTL_DIAS),
             req.headers.get("user-agent"),
             req.headers.get("x-forwarded-for", "").split(",")[0].strip()
             or req.client.host))
        usuario = await _datos_usuario(conn, user_id)

    _set_cookies(respuesta, crear_access_token(user_id, usuario["email"]), nuevo)
    return usuario


@app.post("/api/auth/logout")
async def logout(respuesta: Response, gps_rt: Optional[str] = Cookie(None)):
    if gps_rt:
        async with pool.connection() as conn:
            await conn.execute(
                "UPDATE refresh_tokens SET revocado = true WHERE token_hash = %s",
                (_hash_token(gps_rt),))
    respuesta.delete_cookie("gps_at", path="/")
    respuesta.delete_cookie("gps_rt", path="/api/auth")
    return {"ok": True}


async def usuario_actual(gps_at: Optional[str] = Cookie(None)) -> Dict[str, Any]:
    """Dependencia que protege endpoints. Devuelve el usuario o 401."""
    if not gps_at:
        raise HTTPException(401, "No autenticado")
    try:
        payload = jwt.decode(gps_at, JWT_SECRET, algorithms=[JWT_ALG])
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "Sesion expirada")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "Token invalido")
    async with pool.connection() as conn:
        return await _datos_usuario(conn, int(payload["sub"]))


@app.get("/api/auth/me")
async def yo(usuario: Dict[str, Any] = Depends(usuario_actual)):
    return usuario


@app.post("/api/auth/change-password")
async def cambiar_password(datos: CambioPassword, req: Request,
                           usuario: Dict[str, Any] = Depends(usuario_actual)):
    if len(datos.nueva) < 10:
        raise HTTPException(400, "La contrasena debe tener al menos 10 caracteres")
    nuevo_hash = hash_password(datos.nueva)
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT password_hash FROM users WHERE id = %s", (usuario["dbId"],))
        actual = (await cur.fetchone())[0]
        if not verificar_password(datos.actual, actual):
            raise HTTPException(400, "La contrasena actual no es correcta")
        await conn.execute(
            "UPDATE users SET password_hash = %s, debe_cambiar_pw = false, "
            "updated_at = now() WHERE id = %s", (nuevo_hash, usuario["dbId"]))
        # Cambiar la clave cierra las demas sesiones.
        await conn.execute(
            "UPDATE refresh_tokens SET revocado = true WHERE user_id = %s",
            (usuario["dbId"],))
        await auditar(conn, usuario["dbId"], None, "auth", "cambio_password",
                      req=req)
    return {"ok": True}


def tenant_de(usuario: Dict[str, Any], company_id: Optional[str]) -> int:
    """Resuelve y valida la empresa pedida contra los accesos del usuario."""
    accesos = usuario.get("accesses") or []
    if not accesos:
        raise HTTPException(403, "El usuario no tiene empresas asignadas")
    if not company_id:
        return accesos[0]["tenantId"]
    for a in accesos:
        if a["companyId"] == company_id or str(a["tenantId"]) == str(company_id):
            return a["tenantId"]
    raise HTTPException(403, "Sin acceso a esa empresa")


def exigir_permiso(usuario: Dict[str, Any], tenant_id: int, funcion: str,
                   accion: str = "view"):
    """Los permisos llegan como {funcion: {accion: True}}. Un superadmin
    pasa siempre; el resto necesita la accion concreta."""
    if usuario.get("isPlatformAdmin"):
        return
    for a in usuario.get("accesses") or []:
        if a["tenantId"] != tenant_id:
            continue
        permisos = a.get("permissions") or {}
        if isinstance(permisos, dict) and permisos.get(funcion, {}).get(accion):
            return
        if isinstance(permisos, list) and funcion in permisos:
            return
    raise HTTPException(403, f"Falta el permiso {funcion}:{accion}")


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
async def live(tenant_id: Optional[int] = None,
               usuario: Dict[str, Any] = Depends(usuario_actual)):
    tenant_id = tenant_de(usuario, f"company-{tenant_id:03d}" if tenant_id else None)
    exigir_permiso(usuario, tenant_id, "assets-view", "view")
    # Sale de Redis. Nunca toca Postgres.
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
async def listar_activos(companyId: Optional[str] = None,
                         usuario: Dict[str, Any] = Depends(usuario_actual)):
    tenant = tenant_de(usuario, companyId)
    exigir_permiso(usuario, tenant, "assets-view", "view")
    async with pool.connection() as conn:
        # La autorizacion se resuelve en SQL: el cliente no puede saltarla.
        cur = await conn.execute(
            "SELECT device_id FROM activos_autorizados(%s, %s)",
            (usuario["dbId"], tenant))
        permitidos = {r[0] for r in await cur.fetchall()}
        filas = await _traer_activos(conn)
    return [fila_a_activo(d) for d in filas
            if d["id"] in permitidos and d["tenant_id"] == tenant]


@app.post("/api/assets", status_code=201)
async def crear_activo(a: ActivoIn, req: Request,
                       usuario: Dict[str, Any] = Depends(usuario_actual)):
    tenant = tenant_de(usuario, f"company-{a.tenantId:03d}")
    exigir_permiso(usuario, tenant, "assets-manage", "create")
    a.tenantId = tenant
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
async def editar_activo(asset_id: str, cambios: Dict[str, Any], req: Request,
                        usuario: Dict[str, Any] = Depends(usuario_actual)):
    db_id = int(str(asset_id).replace("asset-", ""))
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT tenant_id FROM devices WHERE id = %s",
                                 (db_id,))
        fila = await cur.fetchone()
    if fila is None:
        raise HTTPException(404, "Activo no encontrado")
    exigir_permiso(usuario, tenant_de(usuario, f"company-{fila[0]:03d}"),
                   "assets-manage", "edit")

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
async def eliminar_activo(asset_id: str, req: Request,
                          usuario: Dict[str, Any] = Depends(usuario_actual)):
    """Baja logica: el historico de telemetria no se toca."""
    db_id = int(str(asset_id).replace("asset-", ""))
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT tenant_id FROM devices WHERE id = %s",
                                 (db_id,))
        fila = await cur.fetchone()
    if fila is None:
        raise HTTPException(404, "Activo no encontrado")
    exigir_permiso(usuario, tenant_de(usuario, f"company-{fila[0]:03d}"),
                   "assets-manage", "delete")
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


# ---------------------------------------------------------------------
# USUARIOS, EMPRESAS Y PERMISOS
#
# El formato de salida sigue el modelo del frontend, no el de la base:
# el frontend ya existe y funciona, adaptarlo seria trabajo sin retorno.
# ---------------------------------------------------------------------

def _company_id(tenant_id: int) -> str:
    return f"company-{tenant_id:03d}"


def _app_id(tenant_id: int) -> str:
    return f"app-{tenant_id:03d}"


async def _catalogo_modulos(conn):
    cur = await conn.execute(
        "SELECT id, nombre, orden FROM modules WHERE activo ORDER BY orden")
    modulos = [{"id": r[0], "name": r[1], "order": r[2]} for r in await cur.fetchall()]
    cur = await conn.execute(
        "SELECT id, module_id, nombre, acciones FROM module_functions ORDER BY module_id, id")
    funciones = [{"id": r[0], "moduleId": r[1], "name": r[2],
                  "actions": list(r[3])} for r in await cur.fetchall()]
    return modulos, funciones


async def _accesos_de(conn, user_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
    """Accesos con permisos efectivos y alcance, en el formato del frontend."""
    if not user_ids:
        return {}

    cur = await conn.execute(
        "SELECT a.id, a.user_id, a.tenant_id, a.role_id, a.alcance, a.activo, "
        "       r.nombre "
        "FROM accesses a JOIN roles r ON r.id = a.role_id "
        "WHERE a.user_id = ANY(%s)", (user_ids,))
    filas = await cur.fetchall()
    if not filas:
        return {}

    ids = [f[0] for f in filas]

    # Permisos efectivos de todos los accesos en una sola consulta
    cur = await conn.execute(
        "SELECT a.id, p.module_id, p.module_function_id, p.accion "
        "FROM accesses a, permisos_efectivos(a.id) p "
        "WHERE a.id = ANY(%s)", (ids,))
    permisos: Dict[int, Dict[str, Dict[str, bool]]] = {}
    modulos_con: Dict[int, set] = {}
    for aid, mod, fid, accion in await cur.fetchall():
        permisos.setdefault(aid, {}).setdefault(fid, {})[accion] = True
        modulos_con.setdefault(aid, set()).add(mod)

    cur = await conn.execute(
        "SELECT access_id, asset_tag_id FROM access_asset_tags "
        "WHERE access_id = ANY(%s)", (ids,))
    etiquetas: Dict[int, List[str]] = {}
    for aid, tid in await cur.fetchall():
        etiquetas.setdefault(aid, []).append(str(tid))

    cur = await conn.execute(
        "SELECT access_id, device_id FROM access_devices "
        "WHERE access_id = ANY(%s)", (ids,))
    equipos: Dict[int, List[str]] = {}
    for aid, did in await cur.fetchall():
        equipos.setdefault(aid, []).append(f"asset-{did}")

    salida: Dict[int, List[Dict[str, Any]]] = {}
    for aid, uid, tid, rid, alcance, activo, rol in filas:
        pf = permisos.get(aid, {})
        salida.setdefault(uid, []).append({
            "id": str(aid),
            "userId": f"user-{uid:03d}",
            "companyId": _company_id(tid),
            "applicationId": _app_id(tid),
            "tenantId": tid,
            "role": str(rid),
            "roleName": rol,
            "status": "active" if activo else "inactive",
            "modules": [{"moduleId": m, "enabled": True}
                        for m in sorted(modulos_con.get(aid, set()))],
            "functions": [{"functionId": fid, "enabled": True, "permissions": acc}
                          for fid, acc in sorted(pf.items())],
            "scope": {
                "type": alcance,
                "assetIds": equipos.get(aid, []),
                "assetTagIds": etiquetas.get(aid, []),
            },
        })
    return salida


def _usuario_publico(r, accesos) -> Dict[str, Any]:
    """Nunca incluye password_hash ni datos de bloqueo."""
    return {
        "id": f"user-{r[0]:03d}", "dbId": r[0], "email": r[1],
        "username": r[1], "name": f"{r[2]} {r[3] or ''}".strip(),
        "nombre": r[2], "apellido": r[3], "telefono": r[4],
        "status": "active" if r[5] else "inactive",
        "isPlatformAdmin": r[6],
        "ultimoAcceso": r[7].isoformat() if r[7] else None,
        "accesses": accesos,
    }


SQL_USERS = ("SELECT id, email, nombre, apellido, telefono, activo, "
             "       es_superadmin, ultimo_acceso FROM users")


@app.get("/api/catalog")
async def catalogo(usuario: Dict[str, Any] = Depends(usuario_actual)):
    """Modulos, funciones, empresas y roles que la UI necesita para
    dibujar la matriz de permisos."""
    async with pool.connection() as conn:
        modulos, funciones = await _catalogo_modulos(conn)
        cur = await conn.execute(
            "SELECT id, nombre, activo FROM tenants ORDER BY nombre")
        empresas, aplicaciones = [], []
        for tid, nombre, activo in await cur.fetchall():
            empresas.append({"id": _company_id(tid), "tenantId": tid,
                             "name": nombre,
                             "status": "active" if activo else "inactive",
                             "applicationId": _app_id(tid)})
            aplicaciones.append({"id": _app_id(tid), "companyId": _company_id(tid),
                                 "tenantId": tid, "shortName": "APP",
                                 "type": "Empresa cliente"})
        cur = await conn.execute(
            "SELECT id, nombre, descripcion, sistema FROM roles ORDER BY id")
        roles = [{"id": str(r[0]), "name": r[1], "description": r[2],
                  "system": r[3]} for r in await cur.fetchall()]
    return {"modules": modulos, "moduleFunctions": funciones,
            "companies": empresas, "applications": aplicaciones, "roles": roles}


@app.get("/api/users")
async def listar_usuarios(usuario: Dict[str, Any] = Depends(usuario_actual)):
    """Un usuario solo ve a quienes comparten alguna de sus empresas."""
    async with pool.connection() as conn:
        if usuario["isPlatformAdmin"]:
            cur = await conn.execute(SQL_USERS + " ORDER BY nombre")
        else:
            exigir_permiso(usuario, usuario["accesses"][0]["tenantId"], "users-view")
            tenants = [a["tenantId"] for a in usuario["accesses"]]
            cur = await conn.execute(
                SQL_USERS + " WHERE id IN (SELECT user_id FROM accesses "
                            " WHERE tenant_id = ANY(%s)) ORDER BY nombre",
                (tenants,))
        filas = await cur.fetchall()
        accesos = await _accesos_de(conn, [f[0] for f in filas])
    return [_usuario_publico(f, accesos.get(f[0], [])) for f in filas]


class UsuarioIn(BaseModel):
    email: str
    nombre: str
    apellido: Optional[str] = None
    telefono: Optional[str] = None
    password: Optional[str] = None
    tenantId: Optional[int] = None
    roleId: Optional[int] = None
    alcance: str = "todos"
    assetTagIds: List[str] = []
    assetIds: List[str] = []


@app.post("/api/users", status_code=201)
async def crear_usuario(u: UsuarioIn, req: Request,
                        usuario: Dict[str, Any] = Depends(usuario_actual)):
    tenant = tenant_de(usuario, _company_id(u.tenantId) if u.tenantId else None)
    exigir_permiso(usuario, tenant, "users-create")

    email = (u.email or "").strip().lower()
    if "@" not in email or len(email) < 5:
        raise HTTPException(400, "Correo invalido")
    if not u.password or len(u.password) < 10:
        raise HTTPException(400, "La contrasena debe tener al menos 10 caracteres")

    pw = hash_password(u.password)
    async with pool.connection() as conn:
        cur = await conn.execute(
            "INSERT INTO users (email, nombre, apellido, telefono, password_hash, "
            "  debe_cambiar_pw) VALUES (%s,%s,%s,%s,%s,true) "
            "ON CONFLICT DO NOTHING RETURNING id",
            (email, u.nombre, u.apellido, u.telefono, pw))
        fila = await cur.fetchone()
        if fila is None:
            raise HTTPException(409, "Ya existe un usuario con ese correo")
        uid = fila[0]

        cur = await conn.execute(
            "INSERT INTO accesses (user_id, tenant_id, role_id, alcance) "
            "VALUES (%s,%s,%s,%s) RETURNING id",
            (uid, tenant, u.roleId or 4, u.alcance))
        aid = (await cur.fetchone())[0]
        await _guardar_alcance(conn, aid, u.alcance, u.assetTagIds, u.assetIds)

        await auditar(conn, usuario["dbId"], tenant, "users", "crear",
                      "user", str(uid), {"email": email}, req)

        cur = await conn.execute(SQL_USERS + " WHERE id = %s", (uid,))
        f = await cur.fetchone()
        accesos = await _accesos_de(conn, [uid])
    return _usuario_publico(f, accesos.get(uid, []))


async def _guardar_alcance(conn, access_id, alcance, tag_ids, asset_ids):
    await conn.execute("DELETE FROM access_asset_tags WHERE access_id = %s", (access_id,))
    await conn.execute("DELETE FROM access_devices WHERE access_id = %s", (access_id,))
    if alcance == "etiquetas" and tag_ids:
        for t in tag_ids:
            try:
                await conn.execute(
                    "INSERT INTO access_asset_tags (access_id, asset_tag_id) "
                    "VALUES (%s,%s) ON CONFLICT DO NOTHING", (access_id, int(t)))
            except (ValueError, TypeError):
                continue
    if alcance == "activos" and asset_ids:
        for a in asset_ids:
            try:
                did = int(str(a).replace("asset-", ""))
            except (ValueError, TypeError):
                continue
            await conn.execute(
                "INSERT INTO access_devices (access_id, device_id) "
                "VALUES (%s,%s) ON CONFLICT DO NOTHING", (access_id, did))


@app.patch("/api/users/{user_id}")
async def editar_usuario(user_id: str, cambios: Dict[str, Any], req: Request,
                         usuario: Dict[str, Any] = Depends(usuario_actual)):
    uid = int(str(user_id).replace("user-", ""))

    columnas = {"nombre": "nombre", "apellido": "apellido",
                "telefono": "telefono", "email": "email"}
    sets, args = [], []
    for k, v in cambios.items():
        if k in columnas:
            sets.append(f"{columnas[k]} = %s")
            args.append(str(v).lower() if k == "email" else v)
    if "status" in cambios:
        sets.append("activo = %s")
        args.append(cambios["status"] == "active")

    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT tenant_id FROM accesses WHERE user_id = %s LIMIT 1", (uid,))
        fila = await cur.fetchone()
        tenant = fila[0] if fila else usuario["accesses"][0]["tenantId"]
        exigir_permiso(usuario, tenant_de(usuario, _company_id(tenant)), "users-edit")

        if sets:
            sets.append("updated_at = now()")
            args.append(uid)
            await conn.execute(
                f"UPDATE users SET {', '.join(sets)} WHERE id = %s", args)

        # Reset de contrasena: cierra todas las sesiones del usuario
        if cambios.get("password"):
            if len(cambios["password"]) < 10:
                raise HTTPException(400, "La contrasena debe tener al menos 10 caracteres")
            await conn.execute(
                "UPDATE users SET password_hash = %s, debe_cambiar_pw = true, "
                "  intentos_fallidos = 0, bloqueado_hasta = NULL WHERE id = %s",
                (hash_password(cambios["password"]), uid))
            await conn.execute(
                "UPDATE refresh_tokens SET revocado = true WHERE user_id = %s", (uid,))

        if "roleId" in cambios or "alcance" in cambios:
            exigir_permiso(usuario, tenant_de(usuario, _company_id(tenant)),
                           "users-permissions")
            cur = await conn.execute(
                "UPDATE accesses SET role_id = coalesce(%s, role_id), "
                "  alcance = coalesce(%s, alcance) "
                "WHERE user_id = %s AND tenant_id = %s RETURNING id",
                (cambios.get("roleId"), cambios.get("alcance"), uid, tenant))
            f = await cur.fetchone()
            if f:
                await _guardar_alcance(conn, f[0],
                                       cambios.get("alcance", "todos"),
                                       cambios.get("assetTagIds", []),
                                       cambios.get("assetIds", []))

        await auditar(conn, usuario["dbId"], tenant, "users", "editar",
                      "user", str(uid), {k: v for k, v in cambios.items()
                                         if k != "password"}, req)

        cur = await conn.execute(SQL_USERS + " WHERE id = %s", (uid,))
        f = await cur.fetchone()
        if f is None:
            raise HTTPException(404, "Usuario no encontrado")
        accesos = await _accesos_de(conn, [uid])
    return _usuario_publico(f, accesos.get(uid, []))


@app.get("/api/companies")
async def listar_empresas(usuario: Dict[str, Any] = Depends(usuario_actual)):
    async with pool.connection() as conn:
        if usuario["isPlatformAdmin"]:
            cur = await conn.execute(
                "SELECT id, nombre, rut, giro, direccion, telefono, email, "
                "       logo_url, activo FROM tenants ORDER BY nombre")
        else:
            tenants = [a["tenantId"] for a in usuario["accesses"]]
            cur = await conn.execute(
                "SELECT id, nombre, rut, giro, direccion, telefono, email, "
                "       logo_url, activo FROM tenants WHERE id = ANY(%s) "
                "ORDER BY nombre", (tenants,))
        return [{
            "id": _company_id(r[0]), "tenantId": r[0], "name": r[1],
            "rut": r[2], "giro": r[3], "direccion": r[4], "telefono": r[5],
            "email": r[6], "logoUrl": r[7],
            "status": "active" if r[8] else "inactive",
            "applicationId": _app_id(r[0]),
        } for r in await cur.fetchall()]


class EmpresaIn(BaseModel):
    name: str
    rut: Optional[str] = None
    giro: Optional[str] = None
    direccion: Optional[str] = None
    telefono: Optional[str] = None
    email: Optional[str] = None


@app.post("/api/companies", status_code=201)
async def crear_empresa(e: EmpresaIn, req: Request,
                        usuario: Dict[str, Any] = Depends(usuario_actual)):
    if not usuario["isPlatformAdmin"]:
        raise HTTPException(403, "Solo un administrador de plataforma puede "
                                 "crear empresas")
    async with pool.connection() as conn:
        cur = await conn.execute(
            "INSERT INTO tenants (nombre, rut, giro, direccion, telefono, email) "
            "VALUES (%s,%s,%s,%s,%s,%s) RETURNING id",
            (e.name, e.rut, e.giro, e.direccion, e.telefono, e.email))
        tid = (await cur.fetchone())[0]
        await conn.execute(
            "INSERT INTO applications (tenant_id, nombre) VALUES (%s,%s) "
            "ON CONFLICT DO NOTHING", (tid, e.name))
        await auditar(conn, usuario["dbId"], tid, "companies", "crear",
                      "company", str(tid), {"name": e.name}, req)
    return {"id": _company_id(tid), "tenantId": tid, "name": e.name,
            "status": "active", "applicationId": _app_id(tid)}


@app.get("/api/audit")
async def listar_auditoria(companyId: Optional[str] = None,
                           limite: int = Query(200, le=1000),
                           usuario: Dict[str, Any] = Depends(usuario_actual)):
    tenant = tenant_de(usuario, companyId)
    exigir_permiso(usuario, tenant, "audit-view")
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT l.ts, l.user_id, u.nombre, u.email, l.modulo, l.accion, "
            "       l.entidad, l.entidad_id, l.detalle, l.ip "
            "FROM audit_log l LEFT JOIN users u ON u.id = l.user_id "
            "WHERE (l.tenant_id = %s OR l.tenant_id IS NULL) "
            "  AND l.ts > now() - interval '90 days' "
            "ORDER BY l.ts DESC LIMIT %s", (tenant, limite))
        return [{
            "timestamp": r[0].isoformat(),
            "actorId": f"user-{r[1]:03d}" if r[1] else "anonymous",
            "actorName": r[2] or r[3] or "Anonimo",
            "module": r[4], "action": r[5],
            "entityType": r[6], "entityId": r[7],
            "detail": r[8], "ip": str(r[9]) if r[9] else None,
        } for r in await cur.fetchall()]


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
