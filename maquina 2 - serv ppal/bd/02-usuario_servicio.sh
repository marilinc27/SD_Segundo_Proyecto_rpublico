#!/bin/bash
# Crea el usuario de la BD que usa el servicio central: SOLO LECTURA (menor privilegio).
# La clave viene de la variable de entorno SERVICIO_DB_CLAVE (archivo .env, no va al repositorio).
set -e

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=clave="$SERVICIO_DB_CLAVE" <<'EOSQL'

SELECT format('CREATE ROLE servicio_juego LOGIN PASSWORD %L', :'clave')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'servicio_juego')
\gexec

SELECT format('GRANT CONNECT ON DATABASE %I TO servicio_juego', current_database())
\gexec

GRANT USAGE ON SCHEMA public TO servicio_juego;
GRANT SELECT ON salas, palabras TO servicio_juego;   -- puede leer, NO puede modificar ni borrar

EOSQL