#!/usr/bin/env python3
"""Agrega autenticacion y autorizacion a la API.

Decisiones de seguridad, para que queden explicitas:

  - Contrasenas con bcrypt (coste 12). Nunca en claro, nunca reversibles.
  - Access token JWT de vida corta (15 min) en cookie httpOnly. En cookie
    y no en localStorage porque un XSS puede leer localStorage; una
    cookie httpOnly no.
  - Refresh token opaco y aleatorio, guardado en base solo como hash
    SHA-256. Rota en cada uso: si alguien roba uno usado, ya no sirve, y
    el uso de un token revocado invalida toda la familia (deteccion de
    robo).
  - Bloqueo tras 5 intentos fallidos por 15 minutos.
  - El login responde el mismo mensaje para usuario inexistente y
    contrasena incorrecta: no se filtra que correos existen.
  - La autorizacion de activos se resuelve en SQL con
    activos_autorizados(): el frontend no puede saltarla.

Correr desde /opt/gps:  python3 parche_auth.py
"""
import sys, pathlib, py_compile

API = pathlib.Path("api/main.py")
REQ = pathlib.Path("api/requirements.txt")
if not API.exists():
    sys.exit("ERROR: correr desde /opt/gps")

# ── Dependencias ────────────────────────────────────────────────────────
req = REQ.read_text()
for dep in ("PyJWT==2.10.1", "bcrypt==4.2.1"):
    if dep.split("==")[0] not in req:
        req = req.rstrip() + "\n" + dep + "\n"
REQ.write_text(req)

s = API.read_text()
if "/api/auth/login" in s:
    print("Ya estaba parcheado.")
    sys.exit(0)

CAMBIOS = []

# 1) Imports
CAMBIOS.append((
"""import asyncio
import json
import os
import logging""",
"""import asyncio
import hashlib
import json
import os
import logging
import secrets"""))

CAMBIOS.append((
"""import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query, Response, WebSocket, WebSocketDisconnect""",
"""import bcrypt
import httpx
import jwt
import redis.asyncio as aioredis
from fastapi import (Cookie, Depends, FastAPI, HTTPException, Query, Request,
                     Response, WebSocket, WebSocketDisconnect)"""))

# 2) Configuracion
CAMBIOS.append((
"""CANAL_LIVE = "gps:live\"""",
"""JWT_SECRET = os.environ.get("JWT_SECRET", "")
JWT_ALG = "HS256"
ACCESS_TTL_MIN = int(os.getenv("ACCESS_TTL_MIN", "15"))
REFRESH_TTL_DIAS = int(os.getenv("REFRESH_TTL_DIAS", "30"))
COOKIE_SEGURA = os.getenv("COOKIE_SEGURA", "1").lower() in ("1", "true", "yes")
MAX_INTENTOS = int(os.getenv("MAX_INTENTOS", "5"))
BLOQUEO_MIN = int(os.getenv("BLOQUEO_MIN", "15"))

CANAL_LIVE = "gps:live\""""))

# 3) Bloque de autenticacion, antes del WebSocket
BLOQUE = '''
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
        ip = req.headers.get("x-forwarded-for", "").split(",")[0].strip() or req.client.host
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
        "SELECT a.id, a.tenant_id, t.nombre, a.role_id, r.nombre, a.alcance, "
        "       coalesce(array_agg(rp.module_function_id) "
        "                FILTER (WHERE rp.module_function_id IS NOT NULL), '{}') "
        "FROM accesses a "
        "JOIN tenants t ON t.id = a.tenant_id "
        "JOIN roles r ON r.id = a.role_id "
        "LEFT JOIN role_permissions rp ON rp.role_id = a.role_id "
        "WHERE a.user_id = %s AND a.activo AND t.activo "
        "GROUP BY a.id, a.tenant_id, t.nombre, a.role_id, r.nombre, a.alcance "
        "ORDER BY t.nombre", (user_id,))
    accesos = [{
        "accessId": r[0], "companyId": f"company-{r[1]:03d}", "tenantId": r[1],
        "companyName": r[2], "roleId": r[3], "roleName": r[4],
        "alcance": r[5], "permissions": list(r[6]),
    } for r in await cur.fetchall()]

    if u[5]:  # superadmin: acceso a todas las empresas
        cur = await conn.execute(
            "SELECT id, nombre FROM tenants WHERE activo ORDER BY nombre")
        conocidos = {a["tenantId"] for a in accesos}
        cur2 = await conn.execute("SELECT id FROM module_functions")
        todas = [r[0] for r in await cur2.fetchall()]
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


def exigir_permiso(usuario: Dict[str, Any], tenant_id: int, funcion: str):
    for a in usuario.get("accesses") or []:
        if a["tenantId"] == tenant_id and funcion in a["permissions"]:
            return
    raise HTTPException(403, f"Falta el permiso {funcion}")

'''

CAMBIOS.append(('@app.websocket("/ws/live")', BLOQUE.strip() + "\n\n\n" + '@app.websocket("/ws/live")'))

for i, (v, n) in enumerate(CAMBIOS, 1):
    if s.count(v) != 1:
        sys.exit(f"ERROR: cambio {i} no coincide ({s.count(v)})")
    s = s.replace(v, n)

# 4) Proteger los endpoints de activos y de estado en vivo
PROTEGER = [
('''@app.get("/api/assets")
async def listar_activos():
    async with pool.connection() as conn:
        return [fila_a_activo(d) for d in await _traer_activos(conn)]''',
'''@app.get("/api/assets")
async def listar_activos(companyId: Optional[str] = None,
                         usuario: Dict[str, Any] = Depends(usuario_actual)):
    tenant = tenant_de(usuario, companyId)
    exigir_permiso(usuario, tenant, "assets.view")
    async with pool.connection() as conn:
        # La autorizacion se resuelve en SQL: el cliente no puede saltarla.
        cur = await conn.execute(
            "SELECT device_id FROM activos_autorizados(%s, %s)",
            (usuario["dbId"], tenant))
        permitidos = {r[0] for r in await cur.fetchall()}
        filas = await _traer_activos(conn)
    return [fila_a_activo(d) for d in filas
            if d["id"] in permitidos and d["tenant_id"] == tenant]'''),

('''@app.post("/api/assets", status_code=201)
async def crear_activo(a: ActivoIn):''',
'''@app.post("/api/assets", status_code=201)
async def crear_activo(a: ActivoIn, req: Request,
                       usuario: Dict[str, Any] = Depends(usuario_actual)):
    tenant = tenant_de(usuario, f"company-{a.tenantId:03d}")
    exigir_permiso(usuario, tenant, "assets.create")
    a.tenantId = tenant'''),

('''async def editar_activo(asset_id: str, cambios: Dict[str, Any]):
    db_id = int(str(asset_id).replace("asset-", ""))''',
'''async def editar_activo(asset_id: str, cambios: Dict[str, Any], req: Request,
                        usuario: Dict[str, Any] = Depends(usuario_actual)):
    db_id = int(str(asset_id).replace("asset-", ""))
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT tenant_id FROM devices WHERE id = %s",
                                 (db_id,))
        fila = await cur.fetchone()
    if fila is None:
        raise HTTPException(404, "Activo no encontrado")
    exigir_permiso(usuario, tenant_de(usuario, f"company-{fila[0]:03d}"),
                   "assets.edit")'''),

('''async def eliminar_activo(asset_id: str):
    """Baja logica: el historico de telemetria no se toca."""
    db_id = int(str(asset_id).replace("asset-", ""))''',
'''async def eliminar_activo(asset_id: str, req: Request,
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
                   "assets.delete")'''),

('''@app.get("/api/live")
async def live(tenant_id: int = DEFAULT_TENANT_ID):''',
'''@app.get("/api/live")
async def live(tenant_id: Optional[int] = None,
               usuario: Dict[str, Any] = Depends(usuario_actual)):
    tenant_id = tenant_de(usuario, f"company-{tenant_id:03d}" if tenant_id else None)
    exigir_permiso(usuario, tenant_id, "assets.view")'''),
]

for i, (v, n) in enumerate(PROTEGER, 1):
    if s.count(v) != 1:
        sys.exit(f"ERROR: proteccion {i} no coincide ({s.count(v)})")
    s = s.replace(v, n)

# Quitar el docstring duplicado que quedaba tras el reemplazo de /api/live
s = s.replace('''    exigir_permiso(usuario, tenant_id, "assets.view")
    """Sale de Redis. Nunca toca Postgres."""''',
'''    exigir_permiso(usuario, tenant_id, "assets.view")
    # Sale de Redis. Nunca toca Postgres.''')

# 5) Exigir que el secreto exista
s = s.replace('''DATABASE_URL = os.environ["DATABASE_URL"]''',
'''DATABASE_URL = os.environ["DATABASE_URL"]
if not os.environ.get("JWT_SECRET"):
    raise SystemExit("ERROR: falta JWT_SECRET en el entorno. "
                     "Generar con: openssl rand -hex 32")''')

API.write_text(s)
py_compile.compile(str(API), doraise=True)
print(f"Autenticacion agregada ({len(s)} bytes).")
print("Recordar: agregar JWT_SECRET al .env y al docker-compose.yml")
