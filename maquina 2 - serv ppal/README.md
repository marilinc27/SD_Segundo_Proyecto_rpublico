# Servicio central — Juego del Espía

`servicio_jugador.py` es la **única autoridad del juego**. Hace tres cosas:
- recibe los pedidos de los jugadores por MQTT;
- decide si valen, según las reglas y la fase de cada sala;
- publica el estado oficial de cada sala.

Los jugadores nunca hablan entre sí ni con la base de datos: solo publican pedidos y leen lo que publica el servicio.    # No se puede repetir una pista ya dicha en esta partida (esta ronda o las anteriores),

Corre en nodo **alumno12**, en un container, junto a su base de datos (salas y palabras). Se conecta al broker del grupo (EMQX 5.8.6 en alumno13) por **MQTT con TLS**

```
 jugadores (notebooks)          alumno13                    alumno12
 ┌─────────────────┐       ┌──────────────┐       ┌──────────────────────────┐
 │ cliente_jugador │──TLS──│  broker EMQX │──TLS──│ servicio  ──SQL──►  bd   │
 │      (x N)      │  MQTT │  (pub/sub)   │  MQTT │ (container)  (PostgreSQL)│
 └─────────────────┘       └──────────────┘       └──────────────────────────┘
```

Hay dos modelos de comunicación:
- **Servicio ↔ broker:** publish/subscribe, comunicación **indirecta**. El servicio no sabe quiénes son los jugadores ni dónde están.
- **Servicio ↔ base de datos:** cliente-servidor, pedido-respuesta **directo**. El servicio hace un `SELECT` y espera el resultado.

---

## Archivos de esta carpeta

| Archivo | Qué es |
|---|---|
| `servicio_jugador.py` | El servicio |
| `Dockerfile` | Imagen: `python:3.12-slim` + `paho-mqtt` 2.1.0 + `psycopg` 3.2.3 |  
| `compose.yaml` | Los dos containers: `servicio` y `bd` |
| `bd/01-juego.sql` | Crea las tablas `salas` y `palabras` y carga 3 salas y 30 palabras |
| `bd/02-usuario-servicio.sh` | Crea el usuario `servicio_juego`, de **solo lectura** |
| `.env.example` | Modelo del `.env`, sin claves reales |
| `.env` | Las claves reales |
| `cacert.pem` | Certificado **público** de la CA del grupo |

## Requisitos

- Docker y Docker Compose en el nodo. El usuario tiene que estar en el grupo `docker`: `sudo usermod -aG docker $USER` y después `newgrp docker`.
- El usuario `servicio` creado en el broker, con permisos en la ACL (ver "Seguridad").
- `cacert.pem` del grupo en esta carpeta.

## Puesta en marcha

1. Crear el `.env` a partir del modelo y poner claves reales (largas y aleatorias):
   ```bash
   cp .env.example .env
   nano .env
   ```
   | Variable | Qué es |
   |---|---|
   | `MQTT_CLAVE` | Clave del usuario `servicio` en el broker |
   | `BD_ADMIN_CLAVE` | Clave del administrador de la base (`admin_juego`) |
   | `SERVICIO_DB_CLAVE` | Clave del usuario de solo lectura (`servicio_juego`) |

2. Levantar:
   ```bash
   docker compose up -d --build
   ```

3. Mirar el log:
   ```bash
   docker compose logs -f servicio
   ```
   Tiene que aparecer:
   ```
   BD del juego: 3 salas y 30 palabras.
   No hay estado guardado: las salas arrancan vacias.
   Servicio central corriendo contra sd-2026.tailb9d8eb.ts.net:18813 (usuario servicio, TLS). Ctrl+C para salir.
   Servicio conectado: Success
   ```

## Reglas (máquina de estados por sala)

```
LOBBY ──(4 a 6 jugadores, todos READY)──► PISTAS ──(todos dieron pista o perdieron el turno)──► VOTACION
  ▲                                         ▲                                                    │
  │                                         └──────(expulsan a un inocente, quedan 3 o más)──────┤
  └──── FIN ◄──(votan al espía / quedan 2 vivos / el espía abandona)──────────────────────────────┘
```

- **JOIN:** solo en `LOBBY` o `FIN`, y con menos de 6 jugadores. No se puede entrar con la partida empezada.
- **Inicio:** se sortean la palabra (distinta de la anterior y de las que se están jugando en otras salas) y el espía. Cada uno recibe su secreto por su topic privado; el espía recibe la **categoría** como ayuda.
- **PISTAS:** por turnos, en un orden al azar que cambia en cada ronda. Fuera de turno se rechaza. Si se vence el tiempo, el jugador pierde el turno. Máximo 30 caracteres por pista.
  - No se puede **repetir una pista** ya dicha en la partida (por nadie, en ninguna ronda) ni **usar la palabra secreta**. Se compara sin tildes, mayúsculas ni signos. La pista se rechaza **sin pasar el turno**: el jugador recibe un aviso y prueba con otra, y el rechazo queda en el log (`docker compose logs servicio`).
- **VOTACION:** un voto por jugador vivo, no a uno mismo. Se cierra cuando votaron todos los conectados o cuando se vence el tiempo. Si hay empate, no sale nadie.
- **FIN:**
  - ganan los **jugadores** si votan al espía o si el espía abandona;
  - gana el **espía** si quedan 2 vivos.

  Para jugar otra partida, todos tienen que volver a estar listos.

Las palabras se leen **al empezar cada partida**, así que una palabra nueva se usa desde la próxima. Las salas, en cambio, se leen al arrancar el servicio.
