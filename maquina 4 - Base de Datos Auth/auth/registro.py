import getpass
import hashlib
import os

import psycopg
from psycopg.errors import UniqueViolation

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "espia")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def registrar_usuario(username: str, password: str) -> None:
    password_hash = hashlib.sha256(password.encode("utf-8")).hexdigest()

    try:
        with psycopg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT id FROM mqtt_user WHERE LOWER(username) = LOWER(%s)",
                    (username,),
                )
                if cursor.fetchone() is not None:
                    print("❌ Ese usuario ya existe.")
                    return

                cursor.execute(
                    """
                    INSERT INTO mqtt_user
                        (username, password_hash, salt, is_superuser)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (username, password_hash, "", False),
                )
            conn.commit()

    except UniqueViolation:
        print("❌ Ese usuario ya existe.")
        return
    except psycopg.OperationalError:
        print("❌ La base de datos no está disponible temporalmente.")
        return

    print(f"✅ Usuario '{username}' registrado correctamente.")


def main():
    print("\n=== REGISTRO - JUEGO DEL ESPÍA ===\n")
    username = input("Usuario: ").strip()
    if not username:
        print("❌ El usuario no puede estar vacío.")
        return

    password = getpass.getpass("Contraseña: ")
    if not password:
        print("❌ La contraseña no puede estar vacía.")
        return

    registrar_usuario(username, password)


if __name__ == "__main__":
    main()
