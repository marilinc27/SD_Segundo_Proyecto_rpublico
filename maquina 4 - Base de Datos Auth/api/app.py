import hashlib
import os

import psycopg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from psycopg.errors import UniqueViolation

app = FastAPI(
    title="API de Autenticación - Juego del Espía",
    version="1.1.0",
)

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "espia")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


class RegistroRequest(BaseModel):
    username: str
    password: str


@app.get("/health")
def health():
    return {"ok": True, "service": "auth-api"}


@app.post("/registro")
def registrar(datos: RegistroRequest):
    username = datos.username.strip()
    password = datos.password

    if not username:
        raise HTTPException(status_code=400, detail="El usuario no puede estar vacío")
    if not password:
        raise HTTPException(status_code=400, detail="La contraseña no puede estar vacía")

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
                # La comprobación es insensible a mayúsculas/minúsculas.
                cursor.execute(
                    "SELECT id FROM mqtt_user WHERE LOWER(username) = LOWER(%s)",
                    (username,),
                )
                if cursor.fetchone() is not None:
                    raise HTTPException(status_code=409, detail="Ese usuario ya existe")

                cursor.execute(
                    """
                    INSERT INTO mqtt_user
                        (username, password_hash, salt, is_superuser)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (username, password_hash, "", False),
                )
            conn.commit()

    except HTTPException:
        raise
    except UniqueViolation:
        # Protege también frente a dos registros simultáneos del mismo nombre.
        raise HTTPException(status_code=409, detail="Ese usuario ya existe")
    except psycopg.OperationalError:
        raise HTTPException(
            status_code=503,
            detail="La base de datos no está disponible temporalmente",
        )

    return {
        "ok": True,
        "message": "Usuario registrado correctamente",
        "username": username,
    }
