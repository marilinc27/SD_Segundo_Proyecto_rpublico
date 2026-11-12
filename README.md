# Cliente de escritorio — Juego del Espía

`cliente_jugador.py` es el programa con el que se juega: una ventana hecha con **Python + tkinter** que se conecta al broker del grupo por **MQTT con TLS** (biblioteca **paho-mqtt**).

Tiene cuatro pantallas:

1. **Ingreso:** usuario y clave.
2. **Registro:** alta de un usuario nuevo. Es una pantalla aparte, con otro color, para no confundirla con el ingreso.
3. **Salas:** las salas disponibles, cuántos hay en cada una y el botón Unirme.
4. **Juego:** el secreto, los turnos, las pistas, la votación, el resultado y los avisos.

---

## Requisitos

| Qué | Para qué |
|---|---|
| Python 3.10 o más nuevo | Correr el programa |
| tkinter | Las ventanas (en Windows y macOS viene con Python) |
| `paho-mqtt` 2.1.0 | Hablar MQTT con el broker |
| `cacert.pem` | Certificado de la CA del grupo: con él, el cliente comprueba que el broker sea el verdadero |
| Tailscale conectado | Llegar a `sd-2026.tailb9d8eb.ts.net` |
| Un usuario del juego | Se puede crear desde el mismo programa, en la pantalla de **registro** |

## Instalación

**Linux (Ubuntu / Debian):**

```bash
sudo apt install python3-tk
pip install paho-mqtt==2.1.0          # si pip se queja: pip install --break-system-packages paho-mqtt==2.1.0
```

**Windows** (Python instalado desde python.org, que ya trae tkinter):

```
py -m pip install paho-mqtt==2.1.0
```

**macOS** (Python instalado desde python.org, que ya trae tkinter):

```bash
python3 -m pip install paho-mqtt==2.1.0
```

Para comprobar que tkinter está instalado: `python3 -m tkinter` tiene que abrir una ventanita de prueba.

## Cómo ejecutarlo

1. Poné **`cliente_jugador.py` y `cacert.pem` en la misma carpeta**.
2. Conectá Tailscale.
3. Desde esa carpeta, ejecutá:

```bash
python3 cliente_jugador.py            # en Windows: py cliente_jugador.py
```

## Cómo se juega

1. **Ingreso.**
   - Si ya tenés usuario: escribí usuario y clave y apretá **Entrar** (o Enter).
   - Si no tenés usuario: apretá **¿No tenés usuario? Registrate acá**. En la pantalla de registro escribí un usuario (mínimo 3 letras) y una clave (mínimo 6 caracteres) dos veces, y apretá **Crear usuario**. El programa te registra y entra solo. **Volver** regresa al ingreso.
2. **Salas.** Elegí una sala y apretá **Unirme**. El botón está deshabilitado si:
   - la sala está en partida;
   - la sala está llena (6 jugadores);
   - el servicio está caído.

   Arriba a la derecha se ve si el servicio está activo.
3. **Lobby.** Apretá **Estoy listo**. La partida arranca sola cuando hay **entre 4 y 6** jugadores y **todos** están listos.
4. **Partida.** La tarjeta de arriba muestra tu secreto durante toda la partida:
   - **CONFIDENCIAL:** sos jugador y ves la palabra. Da pistas sin decirla.
   - **ESPÍA:** no conocés la palabra, solo su categoría. Disimulá.
5. **Pistas.** Se juega por turnos, en un orden al azar que cambia cada ronda. El campo para escribir aparece **solo en tu turno**. Si se vence el tiempo, perdés el turno.
   - No se puede **repetir una pista** ya dicha en la partida (por nadie, en ninguna ronda). Se compara sin tildes, mayúsculas ni signos: "Frío" y "frio!" cuentan como la misma pista.
   - No se puede **usar la palabra secreta** en la pista (por ejemplo, "el pingüino" si la palabra es Pingüino).
   - Si la pista no sirve, no se manda: el aviso aparece en rojo al lado del campo, el texto queda para corregirlo y **no perdés el turno**.
6. **Votación.** Al terminar las pistas, cada uno vota a quien cree que es el espía (no se puede votar a uno mismo).
   - Si el más votado es el espía, **ganan los jugadores**.
   - Si es un inocente, queda expulsado y se juega otra ronda con la misma palabra.
   - Si hay empate, no sale nadie.
   - Si quedan 2 vivos, **gana el espía**.
7. **Fin.** A los que jugaron, la tarjeta de la izquierda les muestra quién ganó, quién era el espía y cuál era la palabra. El resultado también queda en los avisos.
   - La tarjeta vuelve a "Sin partida" apenas cambia la sala (entra o sale alguien). El resultado sigue en los avisos de los que se quedaron.
   - Un jugador que entra después no ve el resultado de la partida anterior. Lo mismo pasa con quien se fue de la sala (o se desconectó) y vuelve a entrar.
   - Para jugar otra partida, todos aprietan **Estoy listo**. **Salir de la sala** te devuelve a la lista de salas (solo en el lobby o con la partida terminada).

La columna de la derecha muestra los **avisos** de la sala, con el más nuevo arriba y resaltado. Los avisos en rojo son solo para vos (por ejemplo, "Tu voto fue registrado").

**Topics:**

| Topic | El cliente… | QoS | Retained |
|---|---|---|---|
| `espia/sala/+/state` | se suscribe (todas las salas) | 1 | sí |
| `espia/sala/+/private/{usuario}` | se suscribe (su secreto y sus avisos) | 1 | sí (secreto) |
| `espia/sala/{su sala}/events` | se suscribe (solo la sala en la que está) | 0 | no |
| `espia/service/status` | se suscribe (si el servicio está vivo) | 1 | sí |
| `espia/sala/{SALA}/action/{usuario}` | publica sus pedidos (`JOIN`, `READY`, `CLUE`, `VOTE`) | 1 | no |
| `espia/presence/{usuario}` | publica `online` y deja su Last Will `offline` | 1 | sí |

Las suscripciones se repiten **en cada reconexión**: con sesión limpia, el broker las olvida cuando se corta la conexión.

**Validación de pistas en el cliente:** antes de mandar un `CLUE`, el cliente compara la pista con las pistas de esta ronda y del `historial` del último `state`, y con la palabra de su secreto. Para comparar normaliza el texto: lo pasa a minúsculas, le saca las tildes (`unicodedata`, forma NFD) y cambia los signos por espacios. Es **solo una ayuda** para el jugador. **El que decide es el servicio**, que tiene que hacer la misma revisión, porque un cliente modificado podría saltearse la del cliente.

**Resultado a la izquierda:** el cliente anota la partida que vio **en curso** estando adentro de la sala. Al ver esa partida en `FIN`, guarda la lista de jugadores de ese momento y muestra el resultado mientras la lista no cambie. Si sale de la sala, se olvida de la partida. Un jugador nuevo nunca vio esa partida en curso, así que no le aparece.
