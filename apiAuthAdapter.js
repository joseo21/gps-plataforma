import { ref } from "vue"

/**
 * Autenticacion contra la API.
 *
 * Reemplaza a createMockAuthAdapter. Diferencias de fondo:
 *
 *   - La sesion vive en cookies httpOnly que pone el servidor. El
 *     JavaScript no puede leerlas: un XSS no puede robar el token.
 *   - El access token dura 15 minutos. Cuando una peticion devuelve 401,
 *     se intenta refrescar una sola vez y se reintenta. Si el refresco
 *     falla, se cierra la sesion.
 *   - Los permisos vienen del servidor y son informativos para la UI. La
 *     decision real la toma la API en cada endpoint: ocultar un boton no
 *     es seguridad, es cortesia.
 */

const API_BASE = (import.meta.env?.VITE_API_BASE || "").replace(/\/$/, "")

const usuario = ref(null)
const cargando = ref(false)
const sesionExpirada = ref(false)

let refrescando = null

const bruto = async (ruta, opciones = {}) => {
  return fetch(`${API_BASE}${ruta}`, {
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    ...opciones,
  })
}

const detalleDeError = async (r) => {
  try {
    const cuerpo = await r.json()
    if (typeof cuerpo?.detail === "string") return cuerpo.detail
    if (Array.isArray(cuerpo?.detail)) return cuerpo.detail[0]?.msg || `HTTP ${r.status}`
  } catch {
    /* respuesta sin JSON */
  }
  return `HTTP ${r.status}`
}

/** Un solo refresco en vuelo aunque varias peticiones fallen a la vez. */
const refrescar = async () => {
  if (!refrescando) {
    refrescando = (async () => {
      const r = await bruto("/api/auth/refresh", { method: "POST" })
      if (!r.ok) throw new Error("refresco fallido")
      usuario.value = await r.json()
      return usuario.value
    })().finally(() => {
      refrescando = null
    })
  }
  return refrescando
}

/** Peticion autenticada con reintento unico tras refrescar. */
export const pedirAutenticado = async (ruta, opciones = {}) => {
  let r = await bruto(ruta, opciones)

  if (r.status === 401 && !ruta.startsWith("/api/auth/")) {
    try {
      await refrescar()
      r = await bruto(ruta, opciones)
    } catch {
      usuario.value = null
      sesionExpirada.value = true
      throw new Error("Sesion expirada")
    }
  }

  if (!r.ok) throw new Error(await detalleDeError(r))
  return r.status === 204 ? null : r.json()
}

export const createApiAuthAdapter = () => {
  const login = async (email, password) => {
    cargando.value = true
    sesionExpirada.value = false
    try {
      const r = await bruto("/api/auth/login", {
        method: "POST",
        body: JSON.stringify({ email: String(email || "").trim(), password }),
      })
      if (!r.ok) throw new Error(await detalleDeError(r))
      usuario.value = await r.json()
      return usuario.value
    } finally {
      cargando.value = false
    }
  }

  const logout = async () => {
    try {
      await bruto("/api/auth/logout", { method: "POST" })
    } finally {
      usuario.value = null
    }
  }

  /** Restaura la sesion al cargar la app. Null si no hay sesion valida. */
  const restaurar = async () => {
    cargando.value = true
    try {
      const r = await bruto("/api/auth/me")
      if (r.ok) {
        usuario.value = await r.json()
        return usuario.value
      }
      if (r.status === 401) {
        try {
          return await refrescar()
        } catch {
          usuario.value = null
        }
      }
      return null
    } catch {
      usuario.value = null
      return null
    } finally {
      cargando.value = false
    }
  }

  const cambiarPassword = async (actual, nueva) =>
    pedirAutenticado("/api/auth/change-password", {
      method: "POST",
      body: JSON.stringify({ actual, nueva }),
    })

  /** Solo para habilitar o esconder controles en la UI. */
  const tienePermiso = (funcion, companyId = null) => {
    const accesos = usuario.value?.accesses || []
    return accesos.some(
      (a) =>
        (!companyId || a.companyId === companyId) &&
        (a.permissions || []).includes(funcion),
    )
  }

  return {
    usuario,
    cargando,
    sesionExpirada,
    login,
    logout,
    restaurar,
    cambiarPassword,
    tienePermiso,
    pedirAutenticado,
  }
}
