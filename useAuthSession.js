import { computed, ref } from "vue"

import { createApiAuthAdapter } from "../../services/auth/apiAuthAdapter.js"

/**
 * Sesion contra la API.
 *
 * Mantiene los 12 exports del composable original para que los once
 * archivos que lo consumen no cambien. Diferencias de fondo:
 *
 *   - El usuario y sus permisos vienen del servidor, no de la lista mock.
 *   - login() ahora es asincrono. Sigue devolviendo { ok, message }.
 *   - La sesion no se guarda en sessionStorage: vive en cookies httpOnly
 *     que el navegador no puede leer.
 *   - La auditoria de login la registra el servidor. Un registro que el
 *     propio usuario puede editar no es auditoria.
 *
 * IMPERSONACION: deshabilitada. La version mock cambiaba de usuario
 * localmente, algo que con permisos reales seria una escalada de
 * privilegios trivial. Necesita un endpoint del servidor que emita un
 * token de suplantacion con su propio registro de auditoria; hasta
 * entonces las funciones existen pero no hacen nada.
 */

const {
  usuario,
  login: loginApi,
  logout: logoutApi,
  restaurar,
} = createApiAuthAdapter()

const restaurando = ref(true)

/**
 * El guard del router corre antes de que sepamos si hay sesion. Sin
 * esperar esta promesa, una recarga de pagina mandaria al login a
 * cualquiera que tenga cookie valida.
 */
export const sesionLista = restaurar().finally(() => {
  restaurando.value = false
})

const authenticatedUser = computed(() => usuario.value)
const currentUser = computed(() => usuario.value)
const impersonatedUser = computed(() => null)

const currentAccesses = computed(() => usuario.value?.accesses || [])

const isPlatformAdmin = computed(() => Boolean(usuario.value?.isPlatformAdmin))
const authenticatedUserIsPlatformAdmin = isPlatformAdmin
const isImpersonating = computed(() => false)

const isAuthenticated = computed(() => Boolean(usuario.value))

const currentRole = computed(() => {
  if (isPlatformAdmin.value) {
    return { id: "platform-admin", name: "Administrador de plataforma" }
  }
  const acceso = currentAccesses.value[0]
  if (!acceso) return null
  return { id: String(acceso.role ?? acceso.roleId ?? ""), name: acceso.roleName || "" }
})

const defaultAuthenticatedRoute = computed(() => {
  const accesos = currentAccesses.value
  if (!accesos.length) return "/sin-acceso"

  const conActivos =
    accesos.find((a) =>
      (a.modules || []).some((m) => m.moduleId === "assets" && m.enabled),
    ) || accesos[0]

  return conActivos?.companyId ? `/app/${conActivos.companyId}/activos` : "/sin-acceso"
})

const login = async ({ identifier, password } = {}) => {
  try {
    await loginApi(identifier, password)
    return { ok: true }
  } catch (error) {
    return { ok: false, message: error.message || "Usuario o contraseña incorrectos." }
  }
}

const logout = async () => {
  await logoutApi()
}

// Impersonacion: pendiente de endpoint en el servidor.
const canImpersonateUser = () => false
const startImpersonation = () => ({ ok: false, message: "No disponible" })
const stopImpersonation = () => ({ returnPath: null })

export function useAuthSession() {
  return {
    authenticatedUser,
    currentUser,
    currentAccesses,
    currentRole,
    isAuthenticated,
    isPlatformAdmin,
    isImpersonating,
    defaultAuthenticatedRoute,
    login,
    logout,
    canImpersonateUser,
    startImpersonation,
    stopImpersonation,
    // Extras que el original no tenia
    restaurando,
    sesionLista,
    impersonatedUser,
    authenticatedUserIsPlatformAdmin,
  }
}
