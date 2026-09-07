import { ref } from "vue"

/**
 * Adaptador de activos contra la API.
 *
 * Reemplaza a createMockActivosAdapter: misma firma y mismo contrato
 * (`activos` es un ref reactivo), pero los datos vienen de Aurora en vez
 * de la semilla y localStorage.
 *
 * Consecuencia practica: un activo dado de alta desde la interfaz queda
 * en la base, lo ven todos los usuarios desde cualquier navegador, y su
 * IMEI entra automaticamente a la whitelist del ingestor, asi que el
 * equipo puede conectar de inmediato.
 *
 * El ref es compartido a nivel de modulo a proposito: todos los
 * componentes que llamen a este adaptador ven la misma lista, igual que
 * pasaba con useMockDatabase.
 */

const API_BASE = (import.meta.env?.VITE_API_BASE || "").replace(/\/$/, "")

const activos = ref([])
const cargando = ref(false)
const error = ref(null)
let cargaInicial = null

const pedir = async (ruta, opciones = {}) => {
  const r = await fetch(`${API_BASE}${ruta}`, {
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    ...opciones,
  })
  if (!r.ok) {
    let detalle = `HTTP ${r.status}`
    try {
      const cuerpo = await r.json()
      if (cuerpo?.detail) detalle = cuerpo.detail
    } catch {
      /* respuesta sin JSON */
    }
    throw new Error(detalle)
  }
  return r.status === 204 ? null : r.json()
}

const recargar = async () => {
  cargando.value = true
  error.value = null
  try {
    activos.value = await pedir("/api/assets")
  } catch (e) {
    error.value = e.message
    console.error("[activos] no se pudieron cargar:", e)
  } finally {
    cargando.value = false
  }
  return activos.value
}

/** Reemplaza el activo en la lista sin recargarla entera. */
const reemplazar = (activo) => {
  const i = activos.value.findIndex((a) => a.id === activo.id)
  if (i === -1) activos.value = [...activos.value, activo]
  else activos.value = activos.value.map((a) => (a.id === activo.id ? activo : a))
  return activo
}

export const createApiActivosAdapter = () => {
  // Una sola carga inicial aunque varios componentes pidan el adaptador.
  if (!cargaInicial) cargaInicial = recargar()

  const createActivo = async ({ activo, companyId }) => {
    const creado = await pedir("/api/assets", {
      method: "POST",
      body: JSON.stringify({
        imei: String(activo.imei || activo.deviceId || "").trim(),
        vehiculo: activo.vehiculo || activo.nombre || activo.name,
        patente: activo.patente || activo.patent,
        modelo: activo.trackerModel || activo.modelo,
        assetType: activo.assetType || activo.tipoActivo,
        mapIcon: activo.mapIcon || activo.markerIcon,
        conductor: activo.conductor,
        ciudad: activo.ciudad,
        sucursalId: activo.sucursalId,
        marca: activo.marca,
        modeloVehiculo: activo.modeloVehiculo,
        anio: activo.anio ? Number(activo.anio) : null,
        vin: activo.vin,
        color: activo.color,
        sim: activo.sim,
        notas: activo.descripcion || activo.notas,
        tenantId: Number(String(companyId || "company-001").replace(/\D/g, "")) || 1,
      }),
    })
    return reemplazar(creado)
  }

  const updateActivo = async (id, cambios = {}) => {
    const actualizado = await pedir(`/api/assets/${id}`, {
      method: "PATCH",
      body: JSON.stringify(cambios),
    })
    return reemplazar(actualizado)
  }

  const deleteActivo = async (id) => {
    await pedir(`/api/assets/${id}`, { method: "DELETE" })
    activos.value = activos.value.filter((a) => a.id !== id)
    return true
  }

  return {
    activos,
    cargando,
    error,
    recargar,
    createActivo,
    updateActivo,
    deleteActivo,
  }
}
