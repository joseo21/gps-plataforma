#!/usr/bin/env python3
"""Elimina la carga automatica de apiAccessAdapter.js que se disparaba al
importar el modulo, antes de que existiera sesion.

Sintoma: createApiAccessAdapter() hacia `if (!cargaInicial) cargaInicial =
recargarAcceso()` la primera vez que CUALQUIER componente llamaba al
adaptador. Como useAccessControl se instancia en el router (que corre al
cargar la pagina, antes del login), esa primera llamada disparaba las tres
peticiones sin cookie: dieron 401 y la promesa fallida quedo cacheada en
`cargaInicial` para siempre. El login() de useAuthSession llama a
recargarAcceso() aparte y si funciona en esa sesion de navegador, pero un
F5 vuelve a instanciar el modulo, dispara la carga huerfana de nuevo, y la
pantalla se queda en "Invitado" otra vez.

La API misma (/api/auth/me) siempre respondio bien: el bug era 100%
frontend, de orden de ejecucion.

Correr desde /opt/gps:  python3 parche_carrera_acceso.py
"""
import pathlib
import sys

ARCHIVO = pathlib.Path(
    "frontend/src-repo/frontend/src/services/access/apiAccessAdapter.js"
)
if not ARCHIVO.exists():
    sys.exit(f"ERROR: no existe {ARCHIVO}. Correr desde /opt/gps")

s = ARCHIVO.read_text()

if "cargaInicial" not in s:
    print("Ya estaba parcheado.")
    sys.exit(0)

VIEJO = '''const cargando = ref(false)
const error = ref(null)
let cargaInicial = null'''
NUEVO = '''const cargando = ref(false)
const error = ref(null)'''

VIEJO2 = '''export const createApiAccessAdapter = () => {
  if (!cargaInicial) cargaInicial = recargarAcceso()

  return {'''
NUEVO2 = '''export const createApiAccessAdapter = () => {
  // La carga NUNCA se dispara sola al instanciar el adaptador: el router
  // (via useAccessControl) lo hace en cuanto arranca la app, antes de
  // que exista sesion. La carga real la ordena useAuthSession: una vez
  // via sesionLista (recarga de pagina) y una vez tras login() exitoso.
  return {'''

if s.count(VIEJO) != 1:
    sys.exit(f"ERROR: bloque 1 no coincide ({s.count(VIEJO)})")
if s.count(VIEJO2) != 1:
    sys.exit(f"ERROR: bloque 2 no coincide ({s.count(VIEJO2)})")

s = s.replace(VIEJO, NUEVO).replace(VIEJO2, NUEVO2)
ARCHIVO.write_text(s)
print("apiAccessAdapter.js: carga huerfana eliminada")
