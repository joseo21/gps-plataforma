# GpsSinergy — plataforma de telemetria GPS

Stack para ingesta Teltonika (Codec 8/8E), historico en Aurora PostgreSQL
y frontend Vue.

Dimensionado para 2049 equipos reportando cada 15 segundos:
11,8 millones de registros diarios, 136 inserciones por segundo.

## Estructura

    api/          FastAPI: ingesta, estado vivo, historico, WebSocket
    db/           Esquema SQL particionado por semana
    ingestor/     Servidor TCP Teltonika (server.py)
    frontend/     Build del Vue desde github.com/Sebastian1841/GpsSinergy
    nginx/        Proxy inverso y TLS
    infra/        Guias de despliegue en AWS
    .github/      Pipeline de despliegue automatico

## Antes de desplegar

1. Copiar tu `server.py` a `ingestor/server.py`
2. Clonar el frontend:
   `git clone https://github.com/Sebastian1841/GpsSinergy frontend/src-repo`
3. Crear `.env` a partir de `.env.example`
4. Cargar el esquema: `psql "$DATABASE_URL" -f db/schema.sql`
5. `docker compose up -d --build`

Ver `infra/01-CONSOLA-AWS.md` para crear la infraestructura y
`infra/00-CREAR-TODO.md` para el runbook completo.

## Estado del ingestor

`ingestor/server.py` ya incluye las correcciones de robustez:

- XADD a Redis Stream ANTES del ACK (escritura durable)
- Contrapresion: si el backlog supera MAX_BACKLOG se responde ACK 0 y
  el equipo guarda en su memoria interna
- Whitelist de IMEI contra el set `gps:allowed_imei` de Redis
- Techo de 64 KB para `data_len`
- Cierre de conexion ante desincronizacion de trama
- Registro de conexiones en Redis y ruteo de comandos Codec 12 por
  pub/sub: permite correr mas de un ingestor detras de un balanceador

El parser Codec 8/8E, Codec 12 y BLE es el original, sin cambios.
