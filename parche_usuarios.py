#!/usr/bin/env python3
"""Agrega los endpoints de usuarios, empresas, aplicaciones, roles y
funciones de modulo.

Devuelven exactamente la forma que consume useAccessControl:

    access = {
      id, userId, applicationId, status,
      modules:   [{ moduleId, enabled }],
      functions: [{ functionId, enabled, permissions: {view, create, ...} }],
      scope:     { assetIds, assetTagIds }
    }

Correr desde /opt/gps:  python3 parche_usuarios.py
"""
import sys, pathlib, py_compile

API = pathlib.Path("api/main.py")
if not API.exists():
    sys.exit("ERROR: correr desde /opt/gps")

s = API.read_text()
if "/api/users" in s:
    print("Ya estaba parcheado.")
    sys.exit(0)

BLOQUE = '''
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

'''

marca = '@app.websocket("/ws/live")'
if s.count(marca) != 1:
    sys.exit(f"ERROR: punto de insercion no unico ({s.count(marca)})")

s = s.replace(marca, BLOQUE.strip() + "\n\n\n" + marca)

# exigir_permiso pasa a entender el modelo funcion-accion
VIEJO = '''def exigir_permiso(usuario: Dict[str, Any], tenant_id: int, funcion: str):
    for a in usuario.get("accesses") or []:
        if a["tenantId"] == tenant_id and funcion in a["permissions"]:
            return
    raise HTTPException(403, f"Falta el permiso {funcion}")'''
NUEVO = '''def exigir_permiso(usuario: Dict[str, Any], tenant_id: int, funcion: str,
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
    raise HTTPException(403, f"Falta el permiso {funcion}:{accion}")'''
if s.count(VIEJO) == 1:
    s = s.replace(VIEJO, NUEVO)

# _datos_usuario devuelve permisos como {funcion: {accion: bool}}
V2 = '''        "LEFT JOIN role_permissions rp ON rp.role_id = a.role_id "
        "WHERE a.user_id = %s AND a.activo AND t.activo "
        "GROUP BY a.id, a.tenant_id, t.nombre, a.role_id, r.nombre, a.alcance "
        "ORDER BY t.nombre", (user_id,))
    accesos = [{
        "accessId": r[0], "companyId": f"company-{r[1]:03d}", "tenantId": r[1],
        "companyName": r[2], "roleId": r[3], "roleName": r[4],
        "alcance": r[5], "permissions": list(r[6]),
    } for r in await cur.fetchall()]'''
N2 = '''        "WHERE a.user_id = %s AND a.activo AND t.activo "
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
        })'''
if s.count(V2) == 1:
    s = s.replace(V2, N2)
    s = s.replace(
        '        "JOIN roles r ON r.id = a.role_id "\n'
        '        "LEFT JOIN role_permissions rp ON rp.role_id = a.role_id "',
        '        "JOIN roles r ON r.id = a.role_id "')
    s = s.replace('''        "SELECT a.id, a.tenant_id, t.nombre, a.role_id, r.nombre, a.alcance, "
        "       coalesce(array_agg(rp.module_function_id) "
        "                FILTER (WHERE rp.module_function_id IS NOT NULL), '{}') "''',
'''        "SELECT a.id, a.tenant_id, t.nombre, a.role_id, r.nombre, a.alcance "''')

# El superadmin recibe todas las funciones con todas sus acciones
V3 = '''        cur2 = await conn.execute("SELECT id FROM module_functions")
        todas = [r[0] for r in await cur2.fetchall()]'''
N3 = '''        cur2 = await conn.execute("SELECT id, acciones FROM module_functions")
        todas = {r[0]: {a: True for a in r[1]} for r in await cur2.fetchall()}'''
if s.count(V3) == 1:
    s = s.replace(V3, N3)

# Los permisos de activos ahora se piden con funcion y accion
for viejo, nuevo in [
    ('exigir_permiso(usuario, tenant, "assets.view")',
     'exigir_permiso(usuario, tenant, "assets-view", "view")'),
    ('exigir_permiso(usuario, tenant_id, "assets.view")',
     'exigir_permiso(usuario, tenant_id, "assets-view", "view")'),
    ('exigir_permiso(usuario, tenant, "assets.create")',
     'exigir_permiso(usuario, tenant, "assets-manage", "create")'),
    ('"assets.edit")', '"assets-manage", "edit")'),
    ('"assets.delete")', '"assets-manage", "delete")'),
]:
    s = s.replace(viejo, nuevo)

API.write_text(s)
py_compile.compile(str(API), doraise=True)
print(f"Endpoints de usuarios y empresas agregados ({len(s)} bytes).")
