-- ============================================================
-- Base de datos - Juego del Espía
-- Estructura de autenticación MQTT
-- Se ejecuta solamente al inicializar un volumen vacío.
-- ============================================================

CREATE TABLE IF NOT EXISTS mqtt_user (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    salt TEXT NOT NULL DEFAULT '',
    is_superuser BOOLEAN NOT NULL DEFAULT FALSE,
    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Evita usuarios duplicados sin distinguir mayúsculas/minúsculas.
-- Ejemplo: si existe "Milla", se rechazan "milla" y "MILLA".
CREATE UNIQUE INDEX IF NOT EXISTS uq_mqtt_user_username_lower
    ON mqtt_user (LOWER(username));
