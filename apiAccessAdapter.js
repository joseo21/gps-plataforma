import { ref } from "vue"

/**
 * Adaptador de acceso contra la API.
 *
 * Reemplaza a createMockAccessAdapter manteniendo el contrato: cuatro
 * refs reactivos que useAccessControl consume.
 *
 *   companies       -> /api/companies
 *   applications    -> /api/catalog
 *   assets          -> /api/assets
 *   moduleFunctions -> /api/catalog
 *
 * Un detalle importante: /api/assets ya viene filtrado por los permisos
 * del usuario, resuelto en SQL con activos_autorizados(). El filtro de
 * visibleAssets en el frontend pasa a ser cosmetico sobre un conjunto ya
 * autorizado. La seguridad esta en el servidor; el frontend solo decide
 * que muestra.
 *
 * Los refs son compartidos a nivel de modulo, igual que en el mock: si
 * cada llamada creara los suyos, cada componente pediria la lista de
 * nuevo y verian datos distintos entre si.
 */

const API_BASE = (import.meta.env?.VITE_API_BASE || "").replace(/\/$/, "")

const companies = ref([])
const applications = ref([])
const assets = ref([])
const moduleFunctions = ref([])
const modules = ref([])
const roles = ref([])

const cargando = ref(false)
const error = ref(null)
let cargaInicial = null

const pedir = async (ruta) => {
  const r = await fetch(`${API_BASE}${ruta}`, {
    credentials: "include",
    headers: { "Content-Type": "application/json" },
  })
  if (!r.ok) throw new Error(`${ruta}: HTTP ${r.status}`)
  return r.json()
}

export const recargarAcceso = async () => {
  cargando.value = true
  error.value = null
  try {
    // En paralelo: son independientes entre si y esperar en cadena
    // triplicaria el tiempo de carga inicial.
    const [catalogo, listaEmpresas, listaActivos] = await Promise.all([
      pedir("/api/catalog"),
      pedir("/api/companies"),
      pedir("/api/assets"),
    ])

    modules.value = catalogo.modules || []
    moduleFunctions.value = catalogo.moduleFunctions || []
    applications.value = catalogo.applications || []
    roles.value = catalogo.roles || []
    companies.value = listaEmpresas || []
    assets.value = listaActivos || []
  } catch (e) {
    error.value = e.message
    console.error("[acceso] no se pudo cargar:", e)
  } finally {
    cargando.value = false
  }
  return { companies, applications, assets, moduleFunctions }
}

export const createApiAccessAdapter = () => {
  if (!cargaInicial) cargaInicial = recargarAcceso()

  return {
    companies,
    applications,
    assets,
    moduleFunctions,
    modules,
    roles,
    cargando,
    error,
    recargar: recargarAcceso,
  }
}
