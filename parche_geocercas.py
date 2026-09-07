#!/usr/bin/env python3
"""Agrega los endpoints de geocercas y grupos.

Devuelven la forma exacta que consume useGeofences:

    circle  -> { id, type:"circle",  name, color, groupName, center, radius }
    polygon -> { id, type:"polygon", name, color, groupName, coordinates }
    route   -> { id, type:"route",   name, color, groupName, coordinates,
                 toleranceMeters }

Correr desde /opt/gps:  python3 parche_geocercas.py
"""
import sys, pathlib, py_compile

API = pathlib.Path("api/main.py")
if not API.exists():
    sys.exit("ERROR: correr desde /opt/gps")

s = API.read_text()
if "/api/geofences" in s:
    print("Ya estaba parcheado.")
    sys.exit(0)

BLOQUE = '''
# ---------------------------------------------------------------------
# GEOCERCAS
# ---------------------------------------------------------------------

COLOR_GEOCERCA = "#102372"


def fila_a_geocerca(d: Dict[str, Any]) -> Dict[str, Any]:
    g = d.get("geometria") or {}
    base = {
        "id": f"geofence-{d['id']}",
        "dbId": d["id"],
        "type": d["tipo"],
        "name": d["nombre"],
        "color": d.get("color") or COLOR_GEOCERCA,
        "groupName": d.get("group_name") or "",
        "companyId": f"company-{d['tenant_id']:03d}",
        "assetIds": [f"asset-{i}" for i in (d.get("devices") or [])],
    }
    if d["tipo"] == "circle":
        base["center"] = g.get("center")
        base["radius"] = g.get("radius")
    else:
        base["coordinates"] = g.get("coordinates") or []
        if d["tipo"] == "route":
            base["toleranceMeters"] = d.get("tolerance_meters") or 100
    return base


SQL_GEOFENCES = """
SELECT g.id, g.tenant_id, g.nombre, g.tipo, g.geometria, g.color,
       g.group_name, g.tolerance_meters, g.activo,
       coalesce(array_agg(gd.device_id) FILTER (WHERE gd.device_id IS NOT NULL), '{}') AS devices
FROM geofences g
LEFT JOIN geofence_devices gd ON gd.geofence_id = g.id
WHERE g.activo AND g.tenant_id = %s
GROUP BY g.id
ORDER BY g.group_name NULLS FIRST, g.nombre
"""


async def _traer_geocercas(conn, tenant, db_id=None):
    sql, args = SQL_GEOFENCES, (tenant,)
    if db_id is not None:
        sql = sql.replace("WHERE g.activo AND g.tenant_id = %s",
                          "WHERE g.activo AND g.tenant_id = %s AND g.id = %s")
        args = (tenant, db_id)
    cur = await conn.execute(sql, args)
    cols = [c.name for c in cur.description]
    return [dict(zip(cols, r)) for r in await cur.fetchall()]


class GeocercaIn(BaseModel):
    name: str
    type: str
    color: Optional[str] = None
    groupName: Optional[str] = None
    center: Optional[Dict[str, float]] = None
    radius: Optional[float] = None
    coordinates: Optional[List[Dict[str, float]]] = None
    toleranceMeters: Optional[int] = None
    assetIds: List[str] = []
    companyId: Optional[str] = None


def _geometria_de(g: GeocercaIn) -> Dict[str, Any]:
    """Valida y arma la geometria. La base tambien valida con un CHECK:
    dos barreras, porque una geocerca mal formada rompe el calculo de
    eventos en silencio."""
    if g.type == "circle":
        if not g.center or "lat" not in g.center or "lng" not in g.center:
            raise HTTPException(400, "Geocerca circular sin centro")
        if not g.radius or g.radius <= 0:
            raise HTTPException(400, "El radio debe ser mayor que cero")
        return {"center": {"lat": float(g.center["lat"]),
                           "lng": float(g.center["lng"])},
                "radius": float(g.radius)}

    if g.type in ("polygon", "route"):
        puntos = [p for p in (g.coordinates or [])
                  if isinstance(p, dict) and "lat" in p and "lng" in p]
        minimo = 3 if g.type == "polygon" else 2
        if len(puntos) < minimo:
            raise HTTPException(
                400, f"Una {'geocerca poligonal' if g.type == 'polygon' else 'ruta'} "
                     f"necesita al menos {minimo} puntos")
        return {"coordinates": [{"lat": float(p["lat"]), "lng": float(p["lng"])}
                                for p in puntos]}

    raise HTTPException(400, "Tipo invalido: circle, polygon o route")


async def _guardar_activos_geocerca(conn, gid: int, asset_ids: List[str], tenant: int):
    await conn.execute("DELETE FROM geofence_devices WHERE geofence_id = %s", (gid,))
    for a in asset_ids or []:
        try:
            did = int(str(a).replace("asset-", ""))
        except (ValueError, TypeError):
            continue
        # Solo activos de la misma empresa: una geocerca no puede
        # alcanzar equipos de otro cliente.
        await conn.execute(
            "INSERT INTO geofence_devices (geofence_id, device_id) "
            "SELECT %s, id FROM devices WHERE id = %s AND tenant_id = %s "
            "ON CONFLICT DO NOTHING", (gid, did, tenant))


@app.get("/api/geofences")
async def listar_geocercas(companyId: Optional[str] = None,
                           usuario: Dict[str, Any] = Depends(usuario_actual)):
    tenant = tenant_de(usuario, companyId)
    exigir_permiso(usuario, tenant, "geofences-view", "view")
    async with pool.connection() as conn:
        return [fila_a_geocerca(d) for d in await _traer_geocercas(conn, tenant)]


@app.post("/api/geofences", status_code=201)
async def crear_geocerca(g: GeocercaIn, req: Request,
                         usuario: Dict[str, Any] = Depends(usuario_actual)):
    tenant = tenant_de(usuario, g.companyId)
    exigir_permiso(usuario, tenant, "geofences-manage", "create")
    geometria = _geometria_de(g)

    async with pool.connection() as conn:
        cur = await conn.execute(
            "INSERT INTO geofences (tenant_id, nombre, tipo, geometria, color, "
            "  group_name, tolerance_meters, created_by) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
            (tenant, (g.name or "Geocerca sin nombre").strip(), g.type,
             Jsonb(geometria), g.color or COLOR_GEOCERCA, g.groupName,
             g.toleranceMeters if g.type == "route" else None,
             usuario["dbId"]))
        gid = (await cur.fetchone())[0]
        await _guardar_activos_geocerca(conn, gid, g.assetIds, tenant)
        await auditar(conn, usuario["dbId"], tenant, "geofences", "crear",
                      "geofence", str(gid), {"name": g.name, "type": g.type}, req)
        filas = await _traer_geocercas(conn, tenant, gid)
    return fila_a_geocerca(filas[0])


@app.patch("/api/geofences/{geofence_id}")
async def editar_geocerca(geofence_id: str, cambios: Dict[str, Any], req: Request,
                          usuario: Dict[str, Any] = Depends(usuario_actual)):
    gid = int(str(geofence_id).replace("geofence-", ""))

    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT tenant_id, tipo, geometria, tolerance_meters "
            "FROM geofences WHERE id = %s AND activo", (gid,))
        fila = await cur.fetchone()
        if fila is None:
            raise HTTPException(404, "Geocerca no encontrada")
        tenant = tenant_de(usuario, f"company-{fila[0]:03d}")
        exigir_permiso(usuario, tenant, "geofences-manage", "edit")

        sets, args = [], []
        for clave, col in (("name", "nombre"), ("color", "color"),
                           ("groupName", "group_name")):
            if clave in cambios:
                sets.append(f"{col} = %s")
                args.append(cambios[clave])

        # La geometria se rearma entera: un cambio parcial dejaria una
        # forma inconsistente y el trigger de bbox la rechazaria.
        if any(k in cambios for k in ("center", "radius", "coordinates", "type")):
            tipo = cambios.get("type", fila[1])
            g = GeocercaIn(
                name=cambios.get("name", ""), type=tipo,
                center=cambios.get("center") or (fila[2] or {}).get("center"),
                radius=cambios.get("radius") or (fila[2] or {}).get("radius"),
                coordinates=cambios.get("coordinates")
                            or (fila[2] or {}).get("coordinates"),
                toleranceMeters=cambios.get("toleranceMeters", fila[3]))
            sets += ["tipo = %s", "geometria = %s"]
            args += [tipo, Jsonb(_geometria_de(g))]
            if tipo == "route":
                sets.append("tolerance_meters = %s")
                args.append(cambios.get("toleranceMeters", fila[3]) or 100)

        if sets:
            args.append(gid)
            await conn.execute(
                f"UPDATE geofences SET {', '.join(sets)} WHERE id = %s", args)

        if "assetIds" in cambios:
            await _guardar_activos_geocerca(conn, gid, cambios["assetIds"], tenant)

        await auditar(conn, usuario["dbId"], tenant, "geofences", "editar",
                      "geofence", str(gid), cambios, req)
        filas = await _traer_geocercas(conn, tenant, gid)
    if not filas:
        raise HTTPException(404, "Geocerca no encontrada")
    return fila_a_geocerca(filas[0])


@app.delete("/api/geofences/{geofence_id}", status_code=204)
async def eliminar_geocerca(geofence_id: str, req: Request,
                            usuario: Dict[str, Any] = Depends(usuario_actual)):
    gid = int(str(geofence_id).replace("geofence-", ""))
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT tenant_id, nombre FROM geofences WHERE id = %s AND activo", (gid,))
        fila = await cur.fetchone()
        if fila is None:
            raise HTTPException(404, "Geocerca no encontrada")
        tenant = tenant_de(usuario, f"company-{fila[0]:03d}")
        exigir_permiso(usuario, tenant, "geofences-manage", "delete")
        # Baja logica: los eventos historicos siguen apuntando a ella.
        await conn.execute(
            "UPDATE geofences SET activo = false, updated_at = now() WHERE id = %s",
            (gid,))
        await auditar(conn, usuario["dbId"], tenant, "geofences", "eliminar",
                      "geofence", str(gid), {"name": fila[1]}, req)
    return Response(status_code=204)


class ImportacionGeocercas(BaseModel):
    geofences: List[GeocercaIn]
    companyId: Optional[str] = None


@app.post("/api/geofences/import")
async def importar_geocercas(datos: ImportacionGeocercas, req: Request,
                             usuario: Dict[str, Any] = Depends(usuario_actual)):
    """Importacion masiva. Las invalidas se informan sin abortar el resto:
    un archivo de 200 geocercas no debe fallar entero por una mal formada."""
    tenant = tenant_de(usuario, datos.companyId)
    exigir_permiso(usuario, tenant, "geofences-manage", "create")

    creadas, errores = [], []
    async with pool.connection() as conn:
        for i, g in enumerate(datos.geofences):
            try:
                geometria = _geometria_de(g)
            except HTTPException as exc:
                errores.append({"indice": i, "name": g.name, "error": exc.detail})
                continue
            cur = await conn.execute(
                "INSERT INTO geofences (tenant_id, nombre, tipo, geometria, "
                "  color, group_name, tolerance_meters, created_by) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                (tenant, (g.name or "Geocerca sin nombre").strip(), g.type,
                 Jsonb(geometria), g.color or COLOR_GEOCERCA, g.groupName,
                 g.toleranceMeters if g.type == "route" else None,
                 usuario["dbId"]))
            creadas.append((await cur.fetchone())[0])
        await auditar(conn, usuario["dbId"], tenant, "geofences", "importar",
                      None, None, {"creadas": len(creadas),
                                   "errores": len(errores)}, req)
        filas = await _traer_geocercas(conn, tenant)

    return {"creadas": len(creadas), "errores": errores,
            "geofences": [fila_a_geocerca(d) for d in filas
                          if d["id"] in set(creadas)]}


@app.get("/api/geofence-groups")
async def listar_grupos_geocercas(companyId: Optional[str] = None,
                                  usuario: Dict[str, Any] = Depends(usuario_actual)):
    tenant = tenant_de(usuario, companyId)
    exigir_permiso(usuario, tenant, "geofences-view", "view")
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT id, nombre, color, orden FROM geofence_groups "
            "WHERE tenant_id = %s ORDER BY orden, nombre", (tenant,))
        explicitos = [{"id": f"group-{r[0]}", "name": r[1],
                       "color": r[2] or COLOR_GEOCERCA, "order": r[3]}
                      for r in await cur.fetchall()]
        # Grupos que existen solo porque alguna geocerca los nombra.
        cur = await conn.execute(
            "SELECT DISTINCT group_name FROM geofences "
            "WHERE tenant_id = %s AND activo AND group_name IS NOT NULL "
            "  AND group_name <> ''", (tenant,))
        conocidos = {g["name"].lower() for g in explicitos}
        for (nombre,) in await cur.fetchall():
            if nombre.lower() not in conocidos:
                explicitos.append({"id": f"derived-{nombre}", "name": nombre,
                                   "color": COLOR_GEOCERCA, "order": 99,
                                   "derived": True})
    return explicitos


class GrupoIn(BaseModel):
    name: str
    color: Optional[str] = None
    companyId: Optional[str] = None


@app.post("/api/geofence-groups", status_code=201)
async def crear_grupo_geocercas(g: GrupoIn,
                                usuario: Dict[str, Any] = Depends(usuario_actual)):
    tenant = tenant_de(usuario, g.companyId)
    exigir_permiso(usuario, tenant, "geofences-manage", "create")
    async with pool.connection() as conn:
        cur = await conn.execute(
            "INSERT INTO geofence_groups (tenant_id, nombre, color) "
            "VALUES (%s,%s,%s) ON CONFLICT DO NOTHING RETURNING id",
            (tenant, g.name.strip(), g.color or COLOR_GEOCERCA))
        fila = await cur.fetchone()
        if fila is None:
            raise HTTPException(409, "Ya existe un grupo con ese nombre")
    return {"id": f"group-{fila[0]}", "name": g.name.strip(),
            "color": g.color or COLOR_GEOCERCA, "order": 0}


@app.patch("/api/geofence-groups/{group_id}")
async def renombrar_grupo_geocercas(group_id: str, cambios: Dict[str, Any],
                                    usuario: Dict[str, Any] = Depends(usuario_actual)):
    """Renombrar arrastra a todas las geocercas del grupo: si no, quedarian
    apuntando a un nombre que ya no existe."""
    nuevo = str(cambios.get("name") or "").strip()
    if not nuevo:
        raise HTTPException(400, "El nombre no puede quedar vacio")

    async with pool.connection() as conn:
        if group_id.startswith("derived-"):
            anterior = group_id[len("derived-"):]
            tenant = tenant_de(usuario, cambios.get("companyId"))
            exigir_permiso(usuario, tenant, "geofences-manage", "edit")
        else:
            gid = int(group_id.replace("group-", ""))
            cur = await conn.execute(
                "SELECT tenant_id, nombre FROM geofence_groups WHERE id = %s", (gid,))
            fila = await cur.fetchone()
            if fila is None:
                raise HTTPException(404, "Grupo no encontrado")
            tenant, anterior = fila[0], fila[1]
            exigir_permiso(usuario, tenant, "geofences-manage", "edit")
            await conn.execute(
                "UPDATE geofence_groups SET nombre = %s WHERE id = %s", (nuevo, gid))

        await conn.execute(
            "UPDATE geofences SET group_name = %s "
            "WHERE tenant_id = %s AND lower(group_name) = lower(%s)",
            (nuevo, tenant, anterior))
    return {"ok": True, "name": nuevo}

'''

marca = '@app.websocket("/ws/live")'
if s.count(marca) != 1:
    sys.exit(f"ERROR: punto de insercion no unico ({s.count(marca)})")

s = s.replace(marca, BLOQUE.strip() + "\n\n\n" + marca)
API.write_text(s)
py_compile.compile(str(API), doraise=True)
print(f"Endpoints de geocercas agregados ({len(s)} bytes).")
