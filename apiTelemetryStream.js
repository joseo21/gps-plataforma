/**
 * Stream de telemetria real.
 *
 * Reemplaza a createMockTelemetryStream manteniendo exactamente la misma
 * firma y los mismos metodos, para que useFleetTelemetry no cambie mas
 * que el import.
 *
 * Diferencias de fondo con el mock:
 *   - El mock inventa posiciones cada intervalMs. Este recibe posiciones
 *     reales por WebSocket cuando el equipo reporta, las acumula en un
 *     buffer y las entrega en lotes en cada tick. Asi el frontend sigue
 *     recibiendo la misma cadencia y no se satura con 2000 equipos.
 *   - El equipo se identifica por IMEI del lado servidor y por id de
 *     activo del lado frontend. El indice de abajo hace la traduccion.
 *
 * Configuracion (.env del frontend):
 *   VITE_API_BASE=https://gps.sinergychile.cl
 */

const DEFAULT_INTERVAL_MS = 1000
const DEFAULT_BATCH_SIZE = 25
const RECONNECT_BASE_MS = 1000
const RECONNECT_MAX_MS = 30000

const API_BASE = (import.meta.env?.VITE_API_BASE || "").replace(/\/$/, "")

const normalizeId = (value) => String(value ?? "")

const wsUrl = () => {
  const base = API_BASE || window.location.origin
  return `${base.replace(/^http/, "ws")}/ws/live`
}

/** Campos por los que un activo del frontend puede corresponderse con un
 *  equipo del backend. Ampliar aca si el modelo de activo usa otro nombre. */
const clavesDeEquipo = (activo) => {
  return [
    activo?.imei,
    activo?.deviceId,
    activo?.dispositivo,
    activo?.equipo,
    activo?.device?.imei,
    activo?.patente,
    activo?.patent,
  ]
    .map(normalizeId)
    .filter(Boolean)
}

/**
 * Traduce una posicion del backend al formato de update que consume
 * applyTelemetryBatch.
 *
 * IMPORTANTE: verificar los nombres de campo contra lo que emite
 * generateTelemetryBatch en mockTelemetryStream.js. Si no coinciden, este
 * es el unico lugar a corregir.
 */
const mapearUpdate = (pos, activoId) => {
  const enMovimiento = pos.movement === true || Number(pos.speed) > 2
  const encendido = pos.ignition === true

  let estado = "offline"
  if (pos.online) {
    if (enMovimiento) estado = "moving"
    else if (encendido) estado = "idle"
    else estado = "stopped"
  }

  return {
    id: activoId,
    assetId: activoId,
    deviceId: normalizeId(pos.imei),
    patente: pos.patente || undefined,

    lat: pos.lat,
    lng: pos.lon,
    latitude: pos.lat,
    longitude: pos.lon,

    speed: Number(pos.speed ?? 0),
    heading: Number(pos.angle ?? 0),
    course: Number(pos.angle ?? 0),

    status: estado,
    ignition: encendido,
    movement: enMovimiento,
    online: Boolean(pos.online),

    voltage: pos.ext_voltage ?? null,
    odometer: pos.odometer_m ?? null,

    timestamp: pos.ts,
    updatedAt: pos.ts,
  }
}

export const createApiTelemetryStream = ({
  activos = [],
  intervalMs = DEFAULT_INTERVAL_MS,
  batchSize = DEFAULT_BATCH_SIZE,
  getPriorityIds = null,
  onBatch = () => {},
  tenantId = 1,
} = {}) => {
  // Mismo contrato que el mock: estado por id de activo.
  const telemetryState = new Map()
  // clave de equipo (imei o patente) -> id de activo del frontend
  const indiceEquipos = new Map()
  // Posiciones llegadas por WebSocket todavia no entregadas
  const pendientes = new Map()

  let timerId = null
  let socket = null
  let reconnectMs = RECONNECT_BASE_MS
  let reconnectTimer = null
  let corriendo = false

  const reconstruirIndice = (lista = []) => {
    indiceEquipos.clear()
    lista.forEach((activo) => {
      const id = normalizeId(activo?.id)
      if (!id) return
      clavesDeEquipo(activo).forEach((clave) => {
        if (!indiceEquipos.has(clave)) indiceEquipos.set(clave, id)
      })
    })
  }

  const replaceSnapshot = (nextActivos = []) => {
    telemetryState.clear()
    nextActivos.forEach((activo) => {
      const id = normalizeId(activo?.id)
      if (id) telemetryState.set(id, { id })
    })
    reconstruirIndice(nextActivos)
  }

  const updateSnapshot = (nextActivos = []) => {
    const vigentes = new Set()
    nextActivos.forEach((activo) => {
      const id = normalizeId(activo?.id)
      if (!id) return
      vigentes.add(id)
      if (!telemetryState.has(id)) telemetryState.set(id, { id })
    })
    Array.from(telemetryState.keys()).forEach((id) => {
      if (!vigentes.has(id)) telemetryState.delete(id)
    })
    reconstruirIndice(nextActivos)
  }

  const registrar = (pos) => {
    const activoId =
      indiceEquipos.get(normalizeId(pos?.imei)) ||
      indiceEquipos.get(normalizeId(pos?.patente))

    // Equipo que reporta pero no esta en el universo autorizado del
    // usuario: se ignora en silencio, no es un error.
    if (!activoId) return

    const previo = telemetryState.get(activoId)
    // Los equipos descargan buffers viejos al reconectar. Sin esta guarda,
    // un lote con datos de ayer manda el vehiculo al pasado en el mapa.
    if (previo?.ts && pos.ts && new Date(pos.ts) <= new Date(previo.ts)) return

    telemetryState.set(activoId, { id: activoId, ts: pos.ts })
    pendientes.set(activoId, mapearUpdate(pos, activoId))
  }

  const generateBatch = () => {
    if (!pendientes.size) return []

    const prioritarios =
      typeof getPriorityIds === "function" ? new Set((getPriorityIds() || []).map(normalizeId)) : new Set()

    const ordenados = Array.from(pendientes.values()).sort((a, b) => {
      const pa = prioritarios.has(a.id) ? 0 : 1
      const pb = prioritarios.has(b.id) ? 0 : 1
      return pa - pb
    })

    const lote = ordenados.slice(0, batchSize)
    lote.forEach((update) => pendientes.delete(update.id))
    return lote
  }

  const tick = () => {
    const lote = generateBatch()
    if (lote.length) onBatch(lote)
    return lote
  }

  const cargaInicial = async () => {
    try {
      const r = await fetch(`${API_BASE}/api/live?tenant_id=${tenantId}`, {
        credentials: "include",
      })
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
      const lista = await r.json()
      lista.forEach(registrar)
      // La primera carga se entrega completa, sin recortar por batchSize:
      // el mapa tiene que dibujarse entero de una.
      const inicial = Array.from(pendientes.values())
      pendientes.clear()
      if (inicial.length) onBatch(inicial)
    } catch (error) {
      console.warn("[telemetria] carga inicial fallo:", error)
    }
  }

  const conectar = () => {
    if (!corriendo || socket) return

    try {
      socket = new WebSocket(wsUrl())
    } catch (error) {
      console.warn("[telemetria] no se pudo abrir el socket:", error)
      programarReconexion()
      return
    }

    socket.onopen = () => {
      reconnectMs = RECONNECT_BASE_MS
    }

    socket.onmessage = (evento) => {
      try {
        registrar(JSON.parse(evento.data))
      } catch (error) {
        console.warn("[telemetria] mensaje ilegible:", error)
      }
    }

    socket.onclose = () => {
      socket = null
      programarReconexion()
    }

    socket.onerror = () => {
      if (socket) socket.close()
    }
  }

  const programarReconexion = () => {
    if (!corriendo || reconnectTimer) return
    // Reintento con espera creciente: si el backend esta caido, no se lo
    // castiga con un socket nuevo cada segundo.
    reconnectTimer = window.setTimeout(() => {
      reconnectTimer = null
      conectar()
    }, reconnectMs)
    reconnectMs = Math.min(reconnectMs * 2, RECONNECT_MAX_MS)
  }

  const start = () => {
    if (corriendo) return
    corriendo = true
    cargaInicial().finally(conectar)
    timerId = window.setInterval(tick, intervalMs)
  }

  const stop = () => {
    corriendo = false
    if (timerId) {
      window.clearInterval(timerId)
      timerId = null
    }
    if (reconnectTimer) {
      window.clearTimeout(reconnectTimer)
      reconnectTimer = null
    }
    if (socket) {
      socket.onclose = null
      socket.close()
      socket = null
    }
    pendientes.clear()
  }

  const isRunning = () => corriendo

  replaceSnapshot(activos)

  return {
    telemetryState,
    replaceSnapshot,
    updateSnapshot,
    generateBatch,
    tick,
    start,
    stop,
    isRunning,
  }
}
