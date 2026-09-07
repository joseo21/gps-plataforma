#!/usr/bin/env python3
"""Hace que /api/auth/me devuelva los accesos en el mismo formato que
/api/users, que es el que consume useAccessControl:

    { id, userId, companyId, applicationId, status,
      modules: [{moduleId, enabled}],
      functions: [{functionId, enabled, permissions:{...}}],
      scope: {type, assetIds, assetTagIds},
      permissions: {funcion: {accion: true}} }   <- para exigir_permiso

Tener dos formatos para lo mismo garantiza que uno de los dos quede
desactualizado. Se unifica en el del frontend.

Correr desde /opt/gps:  python3 parche_sesion.py
"""
import sys, pathlib, py_compile

API = pathlib.Path("api/main.py")
if not API.exists():
    sys.exit("ERROR: correr desde /opt/gps")

s = API.read_text()
if "_accesos_de(conn, [user_id])" in s:
    print("Ya estaba parcheado.")
    sys.exit(0)

VIEJO = '''    cur = await conn.execute(
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
        })'''

NUEVO = '''    # Mismo formato que /api/users: un solo contrato para los accesos.
    por_usuario = await _accesos_de(conn, [user_id])
    accesos = por_usuario.get(user_id, [])

    cur = await conn.execute(
        "SELECT a.tenant_id, t.nombre FROM accesses a "
        "JOIN tenants t ON t.id = a.tenant_id "
        "WHERE a.user_id = %s AND a.activo AND t.activo", (user_id,))
    nombres = {r[0]: r[1] for r in await cur.fetchall()}

    for a in accesos:
        a["accessId"] = int(a["id"])
        a["companyName"] = nombres.get(a["tenantId"], "")
        a["roleId"] = a["role"]
        a["alcance"] = a["scope"]["type"]
        # Vista plana de los permisos, que es la que usa exigir_permiso.
        a["permissions"] = {f["functionId"]: f["permissions"]
                            for f in a["functions"]}'''

if s.count(VIEJO) != 1:
    sys.exit(f"ERROR: el bloque de accesos no coincide ({s.count(VIEJO)})")
s = s.replace(VIEJO, NUEVO)

# El superadmin recibe todas las empresas en el mismo formato
V2 = '''        for r in await cur.fetchall():
            if r[0] not in conocidos:
                accesos.append({
                    "accessId": None, "companyId": f"company-{r[0]:03d}",
                    "tenantId": r[0], "companyName": r[1], "roleId": None,
                    "roleName": "Administrador de plataforma",
                    "alcance": "todos", "permissions": todas,
                })'''
N2 = '''        modulos_todos = set()
        cur3 = await conn.execute("SELECT DISTINCT module_id FROM module_functions")
        modulos_todos = {r[0] for r in await cur3.fetchall()}

        for r in await cur.fetchall():
            if r[0] in conocidos:
                continue
            accesos.append({
                "id": f"platform-{r[0]}",
                "userId": f"user-{u[0]:03d}",
                "accessId": None,
                "companyId": f"company-{r[0]:03d}",
                "applicationId": f"app-{r[0]:03d}",
                "tenantId": r[0], "companyName": r[1],
                "role": "platform-admin", "roleId": None,
                "roleName": "Administrador de plataforma",
                "status": "active", "alcance": "todos",
                "modules": [{"moduleId": m, "enabled": True}
                            for m in sorted(modulos_todos)],
                "functions": [{"functionId": fid, "enabled": True,
                               "permissions": acc}
                              for fid, acc in sorted(todas.items())],
                "scope": {"type": "todos", "assetIds": [], "assetTagIds": []},
                "permissions": todas,
            })'''
if s.count(V2) != 1:
    sys.exit(f"ERROR: el bloque de superadmin no coincide ({s.count(V2)})")
s = s.replace(V2, N2)

API.write_text(s)
py_compile.compile(str(API), doraise=True)
print(f"Formato de accesos unificado ({len(s)} bytes).")
