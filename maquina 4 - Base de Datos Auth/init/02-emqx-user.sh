#!/bin/bash
set -Eeuo pipefail

: "${POSTGRES_USER:?Falta POSTGRES_USER}"
: "${POSTGRES_DB:?Falta POSTGRES_DB}"
: "${EMQX_DB_USER:?Falta EMQX_DB_USER}"
: "${EMQX_DB_PASSWORD:?Falta EMQX_DB_PASSWORD}"

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=emqx_user="$EMQX_DB_USER" \
  --set=emqx_password="$EMQX_DB_PASSWORD" <<'EOSQL'

-- Crea el rol de EMQX si todavía no existe.
SELECT format(
    'CREATE ROLE %I LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE',
    :'emqx_user',
    :'emqx_password'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = :'emqx_user'
)
\gexec

-- Actualiza la contraseña y asegura que el rol siga sin privilegios administrativos.
SELECT format(
    'ALTER ROLE %I WITH LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE',
    :'emqx_user',
    :'emqx_password'
)
\gexec

SELECT format(
    'GRANT CONNECT ON DATABASE %I TO %I',
    current_database(),
    :'emqx_user'
)
\gexec

SELECT format('GRANT USAGE ON SCHEMA public TO %I', :'emqx_user')
\gexec

-- EMQX solo necesita leer la tabla para autenticar clientes.
SELECT format('REVOKE ALL PRIVILEGES ON TABLE mqtt_user FROM %I', :'emqx_user')
\gexec
SELECT format('GRANT SELECT ON TABLE mqtt_user TO %I', :'emqx_user')
\gexec

EOSQL
