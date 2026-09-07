#!/usr/bin/env python3
"""Crea o actualiza un usuario de la plataforma.

Uso desde /opt/gps:
    docker compose exec -T api python - < crear_usuario.py
o mas simple:
    docker compose run --rm api python /app/crear_usuario.py

Pide los datos por consola. La contrasena no se muestra al escribirla ni
queda en el historial de bash.
"""
import asyncio
import getpass
import os
import sys

import bcrypt
import psycopg


def pedir(texto, obligatorio=True, defecto=None):
    val = input(f"{texto}{f' [{defecto}]' if defecto else ''}: ").strip()
    if not val and defecto:
        return defecto
    if not val and obligatorio:
        print("  Requerido.")
        return pedir(texto, obligatorio, defecto)
    return val or None


async def main():
    url = os.environ.get("DATABASE_URL")
    if not url:
        sys.exit("Falta DATABASE_URL en el entorno")

    print("=== Alta de usuario ===")
    email = pedir("Correo").lower()
    nombre = pedir("Nombre")
    apellido = pedir("Apellido", obligatorio=False)

    clave = getpass.getpass("Contrasena (minimo 10 caracteres): ")
    if len(clave) < 10:
        sys.exit("La contrasena debe tener al menos 10 caracteres")
    if len(clave.encode()) > 72:
        sys.exit("La contrasena no puede superar 72 bytes")
    if clave != getpass.getpass("Repetir contrasena: "):
        sys.exit("Las contrasenas no coinciden")

    superadmin = pedir("Administrador de plataforma? (s/n)", defecto="n").lower() == "s"

    pw_hash = bcrypt.hashpw(clave.encode(), bcrypt.gensalt(rounds=12)).decode()

    async with await psycopg.AsyncConnection.connect(url) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO users (email, nombre, apellido, password_hash, "
                "  es_superadmin) VALUES (%s,%s,%s,%s,%s) "
                "ON CONFLICT (id) DO NOTHING RETURNING id",
                (email, nombre, apellido, pw_hash, superadmin))
            fila = await cur.fetchone()

            if fila is None:
                await cur.execute(
                    "UPDATE users SET password_hash = %s, nombre = %s, "
                    "  apellido = %s, es_superadmin = %s, activo = true, "
                    "  intentos_fallidos = 0, bloqueado_hasta = NULL, "
                    "  updated_at = now() "
                    "WHERE lower(email) = %s RETURNING id",
                    (pw_hash, nombre, apellido, superadmin, email))
                fila = await cur.fetchone()
                if fila is None:
                    sys.exit("No se pudo crear ni actualizar el usuario")
                print(f"Usuario actualizado: id={fila[0]}")
            else:
                print(f"Usuario creado: id={fila[0]}")

            user_id = fila[0]

            if not superadmin:
                await cur.execute(
                    "SELECT id, nombre FROM tenants WHERE activo ORDER BY id")
                empresas = await cur.fetchall()
                print("\nEmpresas disponibles:")
                for e in empresas:
                    print(f"  {e[0]}: {e[1]}")
                tid = int(pedir("Id de empresa", defecto=str(empresas[0][0])))

                await cur.execute("SELECT id, nombre FROM roles ORDER BY id")
                roles = await cur.fetchall()
                print("\nRoles disponibles:")
                for r in roles:
                    print(f"  {r[0]}: {r[1]}")
                rid = int(pedir("Id de rol", defecto="1"))

                await cur.execute(
                    "INSERT INTO accesses (user_id, tenant_id, role_id, alcance) "
                    "VALUES (%s,%s,%s,'todos') "
                    "ON CONFLICT (user_id, tenant_id) DO UPDATE "
                    "  SET role_id = EXCLUDED.role_id, activo = true",
                    (user_id, tid, rid))
                print("Acceso asignado.")
            else:
                print("Superadmin: accede a todas las empresas sin acceso explicito.")

        await conn.commit()

    print("\nListo. Probar con:")
    print(f"  curl -i -X POST https://gps.sinergychile.cl/api/auth/login \\")
    print(f"    -H 'Content-Type: application/json' \\")
    print(f"    -d '{{\"email\":\"{email}\",\"password\":\"...\"}}'")


if __name__ == "__main__":
    asyncio.run(main())
