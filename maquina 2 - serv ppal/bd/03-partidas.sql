
-- Historial de partidas terminadas

CREATE TABLE IF NOT EXISTS partidas (
    id         serial PRIMARY KEY,
    sala       text NOT NULL,
    numero     integer NOT NULL,            -- numero de partida dentro de esa sala
    palabra    text NOT NULL,
    categoria  text NOT NULL,
    espia      text NOT NULL,
    ganador    text NOT NULL CHECK (ganador IN ('JUGADORES', 'ESPIA')),
    motivo     text NOT NULL,
    jugadores  text[] NOT NULL,             -- los que empezaron la partida
    expulsados text[] NOT NULL,             -- votados o que abandonaron
    rondas     integer NOT NULL,
    terminada  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (sala, numero)
);

GRANT SELECT, INSERT ON partidas TO servicio_juego;
GRANT USAGE ON SEQUENCE partidas_id_seq TO servicio_juego;   -- para el id automatico