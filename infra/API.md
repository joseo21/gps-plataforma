# API GpsSinergy — contrato

Base: `https://gps.sinergychile.cl`

Todos los endpoints bajo `/api/` requieren sesión, salvo `/api/auth/login`
y `/api/health`. Sin cookie válida responden **401**.

---

## Autenticación

La sesión vive en dos cookies que pone el servidor. El JavaScript no
puede leerlas (`HttpOnly`), así que un XSS no puede robar el token.

| Cookie | Contenido | Vida | Path |
|---|---|---|---|
| `gps_at` | JWT firmado HS256 | 15 min | `/` |
| `gps_rt` | token opaco aleatorio | 30 días | `/api/auth` |

Toda petición desde el navegador necesita `credentials: "include"`.

### POST /api/auth/login

```json
{ "email": "usuario@empresa.cl", "password": "..." }
```

Responde 200 con el objeto de usuario (ver abajo) y las dos cookies.

- **401** credenciales incorrectas. El mensaje es idéntico para usuario
  inexistente y contraseña errónea: no se filtra qué correos existen.
- **429** cuenta bloqueada. Se bloquea 15 minutos tras 5 intentos fallidos.

### POST /api/auth/refresh

Sin cuerpo. Usa `gps_rt`, lo rota y devuelve cookies nuevas más el usuario.

Llamarlo cuando una petición devuelva 401. **Un solo refresco en vuelo**:
si varias peticiones fallan a la vez, hay que compartir la misma promesa.

Si se reusa un refresh token ya rotado, el servidor lo interpreta como
robo y **revoca todas las sesiones de ese usuario**.

### POST /api/auth/logout

Revoca el refresh token y borra las cookies.

### GET /api/auth/me

Devuelve el usuario de la sesión actual. Es lo que hay que llamar al
cargar la app para restaurar sesión. Si da 401, intentar `/refresh` una
vez antes de mandar al login.

### POST /api/auth/change-password

```json
{ "actual": "...", "nueva": "..." }
```

Mínimo 10 caracteres, máximo 72 bytes. Cierra las demás sesiones del
usuario.

### Objeto de usuario de sesión

```json
{
  "id": "user-001",
  "dbId": 1,
  "email": "informatica@sinergygroup.cl",
  "nombre": "jose",
  "apellido": "olate",
  "isPlatformAdmin": true,
  "debeCambiarPassword": false,
  "accesses": [
    {
      "accessId": 3,
      "companyId": "company-001",
      "tenantId": 1,
      "companyName": "Sinergy Interno",
      "roleId": 4,
      "roleName": "Visualizador",
      "alcance": "todos",
      "permissions": {
        "assets-view": { "view": true },
        "reports-execute": { "view": true, "execute": true, "export": true }
      }
    }
  ]
}
```

Un **superadmin** (`isPlatformAdmin: true`) recibe todas las empresas con
todos los permisos, aunque no tenga accesos explícitos en la tabla.

---

## Permisos

Los identificadores usan guión, como el frontend: `assets-view`,
`users-permissions`. Cada función tiene acciones: `view`, `create`,
`edit`, `delete`, `export`, `execute`.

| Módulo | Funciones |
|---|---|
| assets | `assets-view`, `assets-manage`, `assets-command`, `assets-itinerary` |
| geofences | `geofences-view`, `geofences-manage` |
| reports | `reports-view`, `reports-templates`, `reports-rules`, `reports-execute` |
| maintenance | `maintenance-view`, `maintenance-manage` |
| users | `users-view`, `users-create`, `users-edit`, `users-permissions` |
| companies | `companies-view`, `companies-manage` |
| audit | `audit-view` |

Roles base: Administrador (45 permisos), Supervisor (33), Operador (16),
Visualizador (12).

**Los permisos del cliente son solo para dibujar la interfaz.** La
decisión real la toma la API en cada endpoint. Esconder un botón es
cortesía, no seguridad.

---

## Activos

### GET /api/assets

Query opcional: `companyId=company-001`.
Permiso: `assets-view:view`.

Devuelve solo los activos autorizados para ese usuario, resuelto en SQL
con `activos_autorizados()`. Los tres alcances posibles de un acceso:

- `todos` — toda la flota de la empresa
- `etiquetas` — solo activos con alguna etiqueta asignada al acceso
- `activos` — solo los activos listados explícitamente

Cada activo trae los alias que espera el frontend (`vehiculo`/`name`/
`nombrePantalla`, `patente`/`patent`, `imei`/`deviceId`), el estado
calculado desde telemetría real (`moving`, `idle`, `stopped`, `offline`),
y los campos CAN cuando el equipo los reporta.

### POST /api/assets

Permiso: `assets-manage:create`.

```json
{
  "imei": "863719067621293",
  "vehiculo": "Camioneta 01",
  "patente": "ABCD-12",
  "modelo": "FMC130",
  "assetType": "pickup",
  "conductor": "Juan Perez",
  "tenantId": 1
}
```

El IMEI debe tener 15 a 17 dígitos. Al crearlo **entra automáticamente a
la whitelist del ingestor**: el equipo puede conectar de inmediato, sin
tocar SQL.

- **409** si el IMEI ya está registrado.

### PATCH /api/assets/{id}

Permiso: `assets-manage:edit`. Acepta cualquier subconjunto de los campos
de creación.

### DELETE /api/assets/{id}

Permiso: `assets-manage:delete`. Baja lógica: el histórico de telemetría
no se toca, y el IMEI sale de la whitelist.

---

## Telemetría

### GET /api/live

Query: `tenant_id`. Permiso: `assets-view:view`.

Sale de un caché en Redis que un worker refresca cada 3 segundos. **Nunca
toca Postgres**: con 500 usuarios mirando el mapa, una consulta por
petición serían 100.000 filas por segundo para mostrar lo mismo.

### WS /ws/live

Empuja cada posición nueva a medida que llega. Formato por mensaje:

```json
{
  "imei": "863719067621293",
  "ts": "2026-09-07T14:02:16+00:00",
  "lat": -38.7401, "lon": -72.6185,
  "speed": 0, "angle": 14,
  "gps_valid": true, "ignition": true, "movement": false,
  "sats": 20, "gsm_signal": 5, "fuel_level": 16,
  "ext_voltage": 14.4, "odometer_m": 95081092,
  "io_extra": { "can_engine_rpm": 0, "can_total_mileage": 88270000 },
  "online": true
}
```

Dos cosas a respetar del lado cliente:

**Descartar datos viejos.** Los equipos descargan su buffer interno al
reconectar: pueden llegar posiciones de hace días. Sin una guarda de
timestamp, el vehículo salta al pasado en el mapa.

**Agrupar en lotes.** Con 2.000 equipos a 15 segundos son 133 mensajes
por segundo. Acumular y entregar por lote evita saturar al navegador.

### GET /api/history

Query: `imei`, `desde`, `hasta`, `solo_validos`, `limite` (máx 200.000).

`gps_valid=false` marca las tramas sin fix GPS: el equipo manda 0,0 con 0
satélites. Se guardan porque traen IO útiles, pero no son posiciones.

---

## Usuarios y empresas

### GET /api/catalog

Módulos, funciones con sus acciones, empresas, aplicaciones y roles. Es
lo que la UI necesita para dibujar la matriz de permisos.

### GET /api/users

Permiso: `users-view:view`. Un usuario solo ve a quienes comparten alguna
de sus empresas; un superadmin ve a todos.

Nunca devuelve `password_hash` ni los datos de bloqueo.

Cada acceso viene con la forma que consume `useAccessControl`:

```json
{
  "id": "3",
  "userId": "user-002",
  "companyId": "company-001",
  "applicationId": "app-001",
  "role": "4",
  "status": "active",
  "modules":   [{ "moduleId": "assets", "enabled": true }],
  "functions": [{ "functionId": "assets-view", "enabled": true,
                  "permissions": { "view": true } }],
  "scope": { "type": "todos", "assetIds": [], "assetTagIds": [] }
}
```

### POST /api/users

Permiso: `users-create:create`. Contraseña mínima de 10 caracteres; el
usuario queda con `debeCambiarPassword: true`.

### PATCH /api/users/{id}

Permiso: `users-edit:edit`, y `users-permissions:edit` para cambiar rol o
alcance. Enviar `password` resetea la clave y **cierra todas las sesiones
de ese usuario**.

### GET /api/companies

Empresas visibles para el usuario.

### POST /api/companies

Solo superadmin.

---

## Auditoría

### GET /api/audit

Query: `companyId`, `limite` (máx 1.000). Permiso: `audit-view:view`.
Últimos 90 días.

Registra en servidor: logins exitosos y fallidos, reuso de tokens
revocados, cambios de contraseña, y altas y ediciones de usuarios,
activos y empresas. Con IP real del cliente y user-agent.

Vive en la base, no en `localStorage`: un registro que el propio usuario
puede borrar o editar no es auditoría.

---

## Errores

| Código | Significado |
|---|---|
| 400 | datos inválidos |
| 401 | sin sesión o token expirado — intentar refresh |
| 403 | falta el permiso; el cuerpo dice cuál |
| 404 | no existe o no es visible para ese usuario |
| 409 | conflicto (IMEI o correo duplicado) |
| 429 | cuenta bloqueada por intentos fallidos |

Formato: `{ "detail": "mensaje" }`.

---

## Pendiente

Sin endpoints todavía: **geocercas**, **reportes** y **mantenciones**. El
esquema de geocercas ya está creado en `schema_plataforma.sql`
(`geofences`, `geofence_groups`, `geofence_devices`, `geofence_events`
particionada por mes).

Reportes y mantenciones necesitan primero definir el modelo de datos a
partir de `mockReportTemplates.js`, `reportEventRuleConfig.js` y
`useMaintenanceModule.js`.
