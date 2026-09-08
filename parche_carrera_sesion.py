#!/usr/bin/env python3
"""Hace que sesionLista dispare recargarAcceso() cuando restaurar() (via
F5 / carga de pagina) encuentra una sesion valida.

Sin esto, useAuthSession solo llama a recargarAcceso() dentro de login().
Al recargar la pagina con sesion aun valida, restaurar() repone el
usuario correctamente (/api/auth/me responde 200) pero companies/
applications/assets/moduleFunctions quedan vacios para siempre: de ahi
"Invitado" y "Sin modulos disponibles" pese a que el backend esta bien.

Correr desde /opt/gps:  python3 parche_carrera_sesion.py
"""
import pathlib
import sys

ARCHIVO = pathlib.Path(
    "frontend/src-repo/frontend/src/composables/auth/useAuthSession.js"
)
if not ARCHIVO.exists():
    sys.exit(f"ERROR: no existe {ARCHIVO}. Correr desde /opt/gps")

s = ARCHIVO.read_text()

if "sesionLista = restaurar()\n  .then(" in s:
    print("Ya estaba parcheado.")
    sys.exit(0)

VIEJO = '''export const sesionLista = restaurar().finally(() => {
  restaurando.value = false
})'''

NUEVO = '''export const sesionLista = restaurar()
  .then(async (u) => {
    // Al recargar la pagina, restaurar() repone la sesion pero NO trae
    // empresas/activos/catalogo: eso solo ocurria dentro de login(). Sin
    // este llamado, un F5 con sesion valida deja al usuario en
    // "Invitado" pese a que /api/auth/me responde bien.
    if (u) await recargarAcceso()
  })
  .finally(() => {
    restaurando.value = false
  })'''

if s.count(VIEJO) != 1:
    sys.exit(f"ERROR: bloque de sesionLista no coincide ({s.count(VIEJO)})")

s = s.replace(VIEJO, NUEVO)
ARCHIVO.write_text(s)
print("useAuthSession.js: sesionLista ahora recarga empresas/activos tras restaurar")
