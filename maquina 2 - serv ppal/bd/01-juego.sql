-- ============================================================
-- Base de datos del Juego del Espia: salas y palabras
-- Se ejecuta SOLO la primera vez (cuando el volume de la BD esta vacio).
-- ============================================================

-- Salas. El nombre va dentro de los topics MQTT (espia/sala/{nombre}/...),
-- asi que solo letras, numeros y _ : un "/", "+" o "#" romperia el arbol de topics.
CREATE TABLE salas (
    nombre  text PRIMARY KEY CHECK (nombre ~ '^[A-Za-z0-9_]{3,30}$'),
    activa  boolean NOT NULL DEFAULT true
);

-- Palabras secretas con su categoria (la categoria es la ayuda del espia).
CREATE TABLE palabras (
    id        serial PRIMARY KEY,
    palabra   text NOT NULL UNIQUE,
    categoria text NOT NULL,
    activa    boolean NOT NULL DEFAULT true
);

INSERT INTO salas (nombre) VALUES
    ('Comodoro_Rivadavia'), ('Esquel'), ('Puerto_Madryn');

INSERT INTO palabras (palabra, categoria) VALUES
    ('PLAYA', 'Lugar'), ('HOSPITAL', 'Lugar'), ('ESCUELA', 'Lugar'), ('AEROPUERTO', 'Lugar'),
    ('BIBLIOTECA', 'Lugar'), ('CINE', 'Lugar'), ('ESTADIO', 'Lugar'), ('GLACIAR', 'Lugar'),
    ('ASADO', 'Comida'), ('EMPANADA', 'Comida'), ('PIZZA', 'Comida'), ('ALFAJOR', 'Comida'),
    ('MILANESA', 'Comida'), ('MATE', 'Bebida'), ('CAFE', 'Bebida'),
    ('PINGÜINO', 'Animal'), ('GUANACO', 'Animal'), ('BALLENA', 'Animal'), ('CÓNDOR', 'Animal'),
    ('LOBO MARINO', 'Animal'), ('FUTBOL', 'Deporte'), ('TENIS', 'Deporte'), ('NATACIÓN', 'Deporte'),
    ('CICLISMO', 'Deporte'), ('GUITARRA', 'Objeto'), ('PARAGUAS', 'Objeto'), ('MOCHILA', 'Objeto'),
    ('BICICLETA', 'Transporte'), ('COLECTIVO', 'Transporte'), ('AVIÓN', 'Transporte');