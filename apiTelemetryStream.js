/**
 * Stream de telemetria real.
 *
 * Reemplaza a createMockTelemetryStream manteniendo la misma firma, los
 * mismos metodos y EL MISMO FORMATO DE SALIDA, para que useFleetTelemetry
 * y los componentes no cambien mas que el import.
 *
 * El mock emite cada dato con varios alias (speed y velocidad, ignition e
 * ignicion y contacto, odometer y odometro) porque distintos componentes
 * lo leen distinto. Se replican todos: quitar uno rompe una vista y no se
 * nota hasta que alguien la abre.
 *
 * Diferencias de fondo con el mock:
 *   - El mock inventa posiciones cada intervalMs. Este recibe posiciones
 *     reales por WebSocket cuando el equipo reporta, las acumula y las
 *     entrega en lotes en cada tick. Con 2000 equipos eso evita saturar
 *     al navegador.
 *   - El backend identifica por IMEI, el frontend por id de activo. El
 *     indice de abajo hace la traduccion.
 *
 * Configuracion, en frontend/.env.local:
 *   VITE_API_BASE=https://gps.sinergychile.cl
 */

const DEFAULT_INTERVAL_MS = 1000
const DEFAULT_BATCH_SIZE = 25
const RECONNECT_BASE_MS = 1000
const RECONNECT_MAX_MS = 30000

const API_BASE = (import.meta.env?.VITE_API_BASE || "").replace(/\/$/, "")

const TELEMETRY_STATUS = {
  MOVING: "moving",
  IDLE: "idle",
  STOPPED: "stopped",
  OFFLINE: "offline",
}

const normalizeId = (value) => String(value ?? "")

const numero = (value, fallback = null) => {
  const n = Number(value)
  return Number.isFinite(n) ? n : fallback
}

const redondear = (value, decimales = 1) => {
  const n = numero(value)
  return n === null ? null : Number(n.toFixed(decimales))
}

const formatKilometers = (km) => (km === null ? "-" : `${km.toLocaleString("es-CL")} km`)
const formatHours = (h) => (h === null ? "-" : `${h.toLocaleString("es-CL")} h`)
const formatPercent = (p) => (p === null ? "-" : `${Math.round(p)}%`)

const wsUrl = () => {
  const base = API_BASE || window.location.origin
  return `${base.replace(/^http/, "ws")}/ws/live`
}

/** Campos por los que un activo del frontend puede corresponderse con un
 *  equipo del backend. Ampliar si el modelo de activo usa otro nombre. */
const clavesDeEquipo = (activo) =>
  [
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

/**
 * Traduce una posicion del backend al formato exacto de buildTelemetryReport.
 * Es el unico punto de traduccion: si cambia el contrato de la API, se
 * corrige aca y en ningun otro lado.
 */
const mapearUpdate = (pos, activoId) => {
  const io = pos.io_extra || {}
  const online = Boolean(pos.online)
  const encendido = pos.ignition === true
  const enMovimiento = pos.movement === true || numero(pos.speed, 0) > 2

  let estado = TELEMETRY_STATUS.OFFLINE
  if (online) {
    if (enMovimiento) estado = TELEMETRY_STATUS.MOVING
    else if (encendido) estado = TELEMETRY_STATUS.IDLE
    else estado = TELEMETRY_STATUS.STOPPED
  }

  const velocidad = Math.round(numero(pos.speed, 0))
  const odometroKm = pos.odometer_m == null ? null : redondear(pos.odometer_m / 1000, 1)
  const fuelPercent = numero(pos.fuel_level)

  // gsm_signal viene 0-5 del Teltonika; el frontend lo trata como porcentaje.
  const gpsSignal = pos.gsm_signal == null ? 0 : Math.round(numero(pos.gsm_signal, 0) * 20)

  // El adaptador CAN informa el trabajo del motor en minutos.
  const horasTotal =
    io.can_engine_worktime == null ? null : redondear(io.can_engine_worktime / 60, 2)

  const canRpm = numero(io.can_engine_rpm, 0)
  const canEngineTemp = numero(io.can_engine_temp, 0)
  const canEngineLoad = numero(io.can_engine_load, 0)
  const offline = estado === TELEMETRY_STATUS.OFFLINE

  return {
    id: activoId,
    // useActivosTelemetrySync tambien resuelve el activo por estos:
    assetId: activoId,
    deviceId: normalizeId(pos.imei),
    patente: pos.patente || undefined,

    lat: pos.lat,
    lng: pos.lon,
    estado,
    heading: Math.round(numero(pos.angle, 0)),

    ignition: encendido,
    ignicion: encendido,
    contacto: encendido,
    digitalInput1: encendido,
    digitalInput2: offline ? false : Boolean(io.digital_input_2),
    input1: encendido,
    input2: offline ? false : Boolean(io.digital_input_2),

    speed: velocidad,
    velocidad: `${velocidad} km/h`,
    velocidad_kmh: velocidad,

    odometro: formatKilometers(odometroKm),
    odometer: odometroKm,
    horometroTotal: formatHours(horasTotal),
    horometroDiario: "-",              // sin equivalente directo en el equipo
    engineHours: horasTotal,
    engineHoursDaily: null,

    combustible: formatPercent(fuelPercent),
    fuelPercent,
    combustibleNivel: fuelPercent,

    gpsSignal,
    gpsSignalLabel: formatPercent(gpsSignal),
    gpsSatellites: numero(pos.sats, 0),
    gpsFix: pos.gps_valid ? "Fix 3D" : "Sin fix",

    canStatus: offline ? "Sin datos" : "OK",
    canRpm,
    canEngineTemp,
    canBatteryVoltage: numero(pos.ext_voltage, 0),
    canEngineLoad,
    canThrottle: numero(io.can_pedal_position, 0),
    canFuelRate: numero(io.can_fuel_rate, 0),
    canFuelUsed: numero(io.can_fuel_consumed, 0),
    canOilPressure: numero(io.can_oil_pressure, 0),
    canAdBlueLevel: numero(io.can_adblue_level, 0),
    canDtcCount: numero(io.dtc_errors, 0),
    canSummary: offline
      ? "Sin datos"
      : `RPM ${canRpm.toLocaleString("es-CL")} / ${canEngineTemp} C / ${canEngineLoad}%`,

    timestamp: pos.ts,
    lastReport: pos.ts,
    lastReportAt: pos.ts,
    reportedAt: pos.ts,
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
  const telemetryState = new Map()   // id de activo -> ultimo update
  const indiceEquipos = new Map()    // imei o patente -> id de activo
  const pendientes = new Map()       // updates aun no entregados

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
      indiceEquipos.get(normalizeId(pos?.imei)) || indiceEquipos.get(normalizeId(pos?.patente))

    // Equipo que reporta pero no esta en el universo autorizado del
    // usuario: se ignora en silencio, no es un error.
    if (!activoId) return

    const previo = telemetryState.get(activoId)
    // Los equipos descargan buffers viejos al reconectar. Sin esta guarda
    // un lote con datos de ayer manda el vehiculo al pasado en el mapa.
    if (previo?.timestamp && pos.ts && new Date(pos.ts) <= new Date(previo.timestamp)) return

    const update = mapearUpdate(pos, activoId)
    telemetryState.set(activoId, update)
    pendientes.set(activoId, update)
  }

  const generateBatch = () => {
    if (!pendientes.size) return []

    const prioritarios =
      typeof getPriorityIds === "function"
        ? new Set((getPriorityIds() || []).map(normalizeId))
        : new Set()

    const ordenados = Array.from(pendientes.values()).sort((a, b) => {
      return (prioritarios.has(a.id) ? 0 : 1) - (prioritarios.has(b.id) ? 0 : 1)
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
      // el mapa tiene que dibujarse entero de una sola vez.
      const inicial = Array.from(pendientes.values())
      pendientes.clear()
      if (inicial.length) onBatch(inicial)
    } catch (error) {
      console.warn("[telemetria] carga inicial fallo:", error)
    }
  }

  const programarReconexion = () => {
    if (!corriendo || reconnectTimer) return
    // Espera creciente: si el backend esta caido, no se lo castiga con un
    // socket nuevo cada segundo.
    reconnectTimer = window.setTimeout(() => {
      reconnectTimer = null
      conectar()
    }, reconnectMs)
    reconnectMs = Math.min(reconnectMs * 2, RECONNECT_MAX_MS)
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
