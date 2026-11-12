# servicio_jugador.py -- servicio central del Juego del Espia
#
# Es la UNICA autoridad del juego: recibe los pedidos de los jugadores,
# decide si valen y publica el estado oficial de cada sala.
#

import json
import os
import random
import threading
import time
import unicodedata

import paho.mqtt.client as mqtt
import psycopg

# ----------------------------------------------------------------------
# 1. CONFIGURACION
# ----------------------------------------------------------------------
BROKER = os.environ.get("BROKER", "sd-2026.tailb9d8eb.ts.net")
PUERTO = int(os.environ.get("PUERTO", "18813"))   # broker del grupo: nodo alumno13
MQTT_USUARIO = os.environ.get("MQTT_USUARIO", "")
MQTT_CLAVE = os.environ.get("MQTT_CLAVE", "")
CA_CERT = os.environ.get("CA_CERT", "")

# Base de datos del juego: el servicio es CLIENTE de la BD (cliente-servidor, SQL directo).
DB_HOST = os.environ.get("DB_HOST", "bd")
DB_PUERTO = int(os.environ.get("DB_PUERTO", "5432"))
DB_NOMBRE = os.environ.get("DB_NOMBRE", "juego")
DB_USUARIO = os.environ.get("DB_USUARIO", "servicio_juego")
DB_CLAVE = os.environ.get("DB_CLAVE", "")

# Persistencia y caidas
ARCHIVO_ESTADO = os.environ.get("ARCHIVO_ESTADO", "/datos/estado.json")
SEGUNDOS_GRACIA = int(os.environ.get("SEGUNDOS_GRACIA", "60"))
MAX_PROCESADOS = 5000        # cuantos request_id se recuerdan como maximo

MIN_JUGADORES = 4
MAX_JUGADORES = 6
SEGUNDOS_TURNO = int(os.environ.get("SEGUNDOS_TURNO", "30"))         # 0 = sin limite
SEGUNDOS_VOTACION = int(os.environ.get("SEGUNDOS_VOTACION", "45"))   # 0 = sin limite
LARGO_MAXIMO_PISTA = 30

# ----------------------------------------------------------------------
# 1b. BASE DE DATOS: salas y palabras (ya no estan escritas en el codigo)
# ----------------------------------------------------------------------
def consultar(sql):
    # Pedido y respuesta: el servicio pregunta y ESPERA el resultado (sincronico).
    with psycopg.connect(host=DB_HOST, port=DB_PUERTO, dbname=DB_NOMBRE, user=DB_USUARIO,
                         password=DB_CLAVE, connect_timeout=3) as conexion:
        with conexion.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()


def leer_salas():
    filas = consultar("SELECT nombre FROM salas WHERE activa ORDER BY nombre")
    return [fila[0] for fila in filas]


def guardar_partida_en_bd(datos):
    # Corre en un HILO APARTE (lo lanza terminar_partida): si la base tarda o esta caida,
    # el juego no se frena (el candado no queda tomado mientras se espera a la base).
    # ON CONFLICT DO NOTHING: si esta partida ya estaba guardada, no se repite (idempotencia).
    try:
        with psycopg.connect(host=DB_HOST, port=DB_PUERTO, dbname=DB_NOMBRE, user=DB_USUARIO,
                             password=DB_CLAVE, connect_timeout=3) as conexion:
            with conexion.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO partidas (sala, numero, palabra, categoria, espia, ganador,
                                             motivo, jugadores, expulsados, rondas)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (sala, numero) DO NOTHING""",
                    (datos["sala"], datos["numero"], datos["palabra"], datos["categoria"],
                     datos["espia"], datos["ganador"], datos["motivo"], datos["jugadores"],
                     datos["expulsados"], datos["rondas"]))
                if cursor.rowcount == 1:
                    print(f"[{datos['sala']}] Partida {datos['numero']} guardada en el historial.")
                else:
                    print(f"[{datos['sala']}] Partida {datos['numero']} ya estaba en el historial: no se repite.")
    except psycopg.Error as error:
        # La base fallo: se pierde ESTA fila del historial, pero el juego sigue (degradacion).
        print(f"[{datos['sala']}] No se pudo guardar la partida {datos['numero']} en el historial: {error}")


def leer_palabras():
    # (palabra, categoria). La categoria es la ayuda que recibe el espia.
    filas = consultar("SELECT palabra, categoria FROM palabras WHERE activa")
    return [(fila[0], fila[1]) for fila in filas]


def esperar_base_de_datos():
    # Al arrancar, el servicio NO puede jugar sin salas ni palabras: espera a la BD
    # (puede tardar en levantar, igual que el broker).
    while True:
        try:
            salas_bd = leer_salas()
            palabras_bd = leer_palabras()
            if salas_bd and len(palabras_bd) > len(salas_bd):
                print(f"BD del juego: {len(salas_bd)} salas y {len(palabras_bd)} palabras.")
                return salas_bd, palabras_bd
            print("La BD respondio, pero no tiene salas o palabras suficientes. Reintento...")
        except psycopg.Error as error:
            print("Esperando a la base de datos del juego...", str(error).strip()[:80])
        time.sleep(3)


SALAS_VALIDAS, PALABRAS = esperar_base_de_datos()


def refrescar_palabras():
    # Se llama al empezar cada partida: asi se pueden agregar palabras en la BD sin
    # reiniciar el servicio. Si la BD no responde, se sigue con la ultima lista conocida.
    global PALABRAS
    try:
        nuevas = leer_palabras()
        if len(nuevas) > len(SALAS_VALIDAS):
            PALABRAS = nuevas
    except psycopg.Error as error:
        print("No se pudo leer la BD; se usan las palabras que ya estaban:", str(error).strip()[:80])


# ----------------------------------------------------------------------
# 2. EL ESTADO DEL JUEGO (solo existe aca, en el servicio)
# ----------------------------------------------------------------------
candado = threading.Lock()   # exclusion mutua: un solo hilo por vez toca "salas"
procesados = set()           # (usuario, request_id) ya procesados (filtrado de duplicados)
orden_procesados = []        # los mismos, en orden de llegada (para olvidar los mas viejos)
desconectados = {}           # usuario -> momento en que se desconecto (presencia "offline")
salas = {}
# Momento en que el SERVICIO perdio la conexion con el broker (None = conectado).
# Arranca "desconectado": hasta la primera conexion, el juego tampoco avanza.
sin_broker_desde = time.monotonic()


def sala_nueva():
    return {
        "fase": "LOBBY",
        "partida": 0,
        "ronda": 0,
        "jugadores": [],
        "listos": [],
        "expulsados": [],
        "palabra": None,          # SECRETO: no se publica hasta el final
        "categoria": None,        # SECRETO: solo la recibe el espia (y se revela al final)
        "espia": None,            # SECRETO: no se publica hasta el final
        "orden": [],              # orden de los turnos de la ronda (aleatorio)
        "turno": 0,               # posicion en "orden" del que tiene que dar la pista
        "pistas": {},             # pistas de la ronda actual, en el orden en que se dieron
        "historial": [],          # pistas de las rondas anteriores de esta partida
        "votos": {},              # votos de la ronda actual: {votante: votado}. NO se publican
        "ultima_votacion": None,  # resultado de la ultima votacion cerrada (conteo y expulsado)
        "cierre": None,           # en que momento (reloj del servicio) se cierra la fase o el turno
                                  # (None = sin limite de tiempo)
        "segundos": 0,            # cuanto dura la fase o el turno actual (0 = sin limite; se publica)
        "resultado": None,        # al terminar la partida: quien gano y por que
    }


for nombre in SALAS_VALIDAS:
    salas[nombre] = sala_nueva()


def vivos(sala):
    # Los que siguen jugando: estan en la sala y no fueron expulsados.
    lista = []
    for usuario in sala["jugadores"]:
        if usuario not in sala["expulsados"]:
            lista.append(usuario)
    return lista


def turno_de(sala):
    # A quien le toca dar la pista (None si no es fase de pistas).
    if sala["fase"] == "PISTAS" and sala["turno"] < len(sala["orden"]):
        return sala["orden"][sala["turno"]]
    return None


def salas_de(usuario):
    # En que salas esta este usuario (normalmente una sola).
    return [nombre for nombre in SALAS_VALIDAS if usuario in salas[nombre]["jugadores"]]


def votaron_todos_los_conectados(sala):
    # La votacion no espera a los desconectados: alcanza con que voten los que estan.
    presentes = [u for u in vivos(sala) if u not in desconectados]
    return len(presentes) > 0 and all(u in sala["votos"] for u in presentes)


def registrar_procesado(usuario, request_id):
    # Recuerda el pedido para descartar duplicados. Se olvidan los mas viejos para que
    # la lista (y el archivo de estado) no crezcan sin limite.
    clave = (usuario, request_id)
    procesados.add(clave)
    orden_procesados.append(clave)
    if len(orden_procesados) > MAX_PROCESADOS:
        procesados.discard(orden_procesados.pop(0))


# ----------------------------------------------------------------------
# 2b. PERSISTENCIA: el estado de las salas se guarda en un archivo (R4)
# ----------------------------------------------------------------------
def guardar_estado():
    # Se guarda lo que haga falta para seguir la partida despues de un reinicio.
    # El reloj (time.monotonic) no sirve entre reinicios: se guardan los SEGUNDOS QUE FALTAN.
    ahora = time.monotonic()
    copia_salas = {}
    for nombre, sala in salas.items():
        copia = dict(sala)
        if sala["cierre"] is not None:
            copia["segundos_restantes"] = max(0, round(sala["cierre"] - ahora))
        else:
            copia["segundos_restantes"] = None      # sin limite de tiempo
        del copia["cierre"]
        copia_salas[nombre] = copia
    datos = {"salas": copia_salas, "procesados": [list(par) for par in orden_procesados]}
    # Escritura atomica: primero a un archivo temporal y despues se renombra.
    # Si el container se apaga a mitad de la escritura, el archivo anterior queda entero.
    temporal = ARCHIVO_ESTADO + ".tmp"
    try:
        with open(temporal, "w", encoding="utf-8") as archivo:
            json.dump(datos, archivo, ensure_ascii=False)
        os.replace(temporal, ARCHIVO_ESTADO)
    except OSError as error:
        print("No se pudo guardar el estado:", error)


def cargar_estado():
    if not os.path.exists(ARCHIVO_ESTADO):
        print("No hay estado guardado: las salas arrancan vacias.")
        return
    try:
        with open(ARCHIVO_ESTADO, encoding="utf-8") as archivo:
            datos = json.load(archivo)
        ahora = time.monotonic()
        for nombre, guardada in datos.get("salas", {}).items():
            if nombre not in salas:
                continue                            # una sala que ya no esta en la BD
            restantes = guardada.pop("segundos_restantes", None)
            sala = sala_nueva()
            sala.update(guardada)
            if restantes is not None and sala["fase"] in ["PISTAS", "VOTACION"]:
                sala["cierre"] = ahora + restantes  # el reloj sigue donde estaba
                sala["segundos"] = restantes
            else:
                sala["cierre"] = None
            salas[nombre] = sala
        for usuario, request_id in datos.get("procesados", []):
            registrar_procesado(usuario, request_id)
        en_juego = [n for n in SALAS_VALIDAS if salas[n]["fase"] in ["PISTAS", "VOTACION"]]
        print(f"Estado recuperado de {ARCHIVO_ESTADO}. Partidas en curso: {en_juego or 'ninguna'}.")
    except (OSError, ValueError, KeyError, TypeError) as error:
        # Archivo roto: se aparta (no se borra, por si hace falta revisarlo) y se arranca de cero.
        respaldo = ARCHIVO_ESTADO + ".corrupto"
        os.replace(ARCHIVO_ESTADO, respaldo)
        print(f"El estado guardado estaba roto ({error}). Se movio a {respaldo} y se arranca de cero.")


# ----------------------------------------------------------------------
# 3. PUBLICAR: estado publico, avisos y mensajes privados
# ----------------------------------------------------------------------
def estado_publico(sala):
    # Copia SIN la palabra, la categoria, el espia ni a quien voto cada uno.
    datos = {
        "fase": sala["fase"],
        "partida": sala["partida"],
        "ronda": sala["ronda"],
        "jugadores": sala["jugadores"],
        "vivos": vivos(sala),
        "listos": sala["listos"],
        "expulsados": sala["expulsados"],
        "orden": sala["orden"],
        "turno_de": turno_de(sala),
        "pistas": sala["pistas"],
        "historial": sala["historial"],
        "votaron": list(sala["votos"].keys()),   # QUIENES votaron, no A QUIEN
        "desconectados": [u for u in sala["jugadores"] if u in desconectados],
        "ultima_votacion": sala["ultima_votacion"],
        "segundos": sala["segundos"],
    }
    if sala["fase"] == "FIN":
        datos["resultado"] = sala["resultado"]   # recien aca se revelan palabra, categoria y espia
    return datos


def publicar_estado(cliente, nombre_sala):
    datos = estado_publico(salas[nombre_sala])
    cliente.publish(f"espia/sala/{nombre_sala}/state", json.dumps(datos), qos=1, retain=True)


def avisar_sala(cliente, nombre_sala, texto):
    # Aviso de color para todos: QoS 0 y sin retained (la verdad esta en el state).
    cliente.publish(f"espia/sala/{nombre_sala}/events", texto, qos=0)


def avisar_jugador(cliente, nombre_sala, usuario, texto):
    # Aviso solo para ese jugador. SIN retained: asi no pisa su secreto guardado.
    datos = {"tipo": "AVISO", "texto": texto}
    cliente.publish(f"espia/sala/{nombre_sala}/private/{usuario}", json.dumps(datos), qos=1)


def borrar_secreto(cliente, nombre_sala, usuario):
    # Un mensaje VACIO con retain=True borra el retained guardado (MQTT 3.1.1, 3.3.1.3).
    # Asi nadie recibe un secreto viejo de una partida anterior.
    cliente.publish(f"espia/sala/{nombre_sala}/private/{usuario}", "", qos=1, retain=True)


def enviar_secreto(cliente, nombre_sala, usuario, sala):
    if usuario == sala["espia"]:
        # El espia no conoce la palabra, pero recibe la categoria como ayuda.
        datos = {"tipo": "SECRETO", "partida": sala["partida"], "rol": "ESPIA",
                 "categoria": sala["categoria"]}
    else:
        datos = {"tipo": "SECRETO", "partida": sala["partida"], "rol": "JUGADOR",
                 "palabra": sala["palabra"]}
    # CON retained: si el jugador se cae y vuelve, recupera su secreto.
    cliente.publish(f"espia/sala/{nombre_sala}/private/{usuario}", json.dumps(datos),
                    qos=1, retain=True)


# ----------------------------------------------------------------------
# 4. LAS REGLAS DEL JUEGO (maquina de estados)
# ----------------------------------------------------------------------
def abrir_fase(sala, fase, segundos):
    # Cambia de fase (o de turno) y arranca el reloj. time.monotonic() es el reloj
    # del servicio: no lo afectan los cambios de hora del sistema.
    sala["fase"] = fase
    sala["segundos"] = segundos
    if segundos > 0:
        sala["cierre"] = time.monotonic() + segundos
    else:
        sala["cierre"] = None              # 0 segundos = sin limite: nunca vence


def texto_tiempo(segundos, para_que):
    if segundos > 0:
        return f"{segundos} s {para_que}"
    return "sin limite de tiempo"


def abrir_ronda(cliente, nombre_sala):
    # Cada ronda arranca con un orden de turnos NUEVO y aleatorio entre los vivos.
    sala = salas[nombre_sala]
    en_juego = vivos(sala)
    sala["orden"] = random.sample(en_juego, len(en_juego))
    sala["turno"] = 0
    sala["pistas"] = {}
    sala["votos"] = {}
    abrir_fase(sala, "PISTAS", SEGUNDOS_TURNO)
    publicar_estado(cliente, nombre_sala)
    avisar_sala(cliente, nombre_sala,
                f"Ronda {sala['ronda']}. Orden de las pistas: {', '.join(sala['orden'])}. "
                f"Empieza {turno_de(sala)} ({texto_tiempo(SEGUNDOS_TURNO, 'por turno')}).")


def pasar_turno(cliente, nombre_sala):
    # Avanza al siguiente de la lista. Si ya pasaron todos, se vota.
    sala = salas[nombre_sala]
    sala["turno"] = sala["turno"] + 1
    if sala["turno"] >= len(sala["orden"]):
        abrir_votacion(cliente, nombre_sala)
    else:
        abrir_fase(sala, "PISTAS", SEGUNDOS_TURNO)     # el reloj vuelve a empezar para el siguiente
        publicar_estado(cliente, nombre_sala)
        avisar_sala(cliente, nombre_sala, f"Le toca a {turno_de(sala)}.")


def elegir_palabra(nombre_sala):
    # No se puede usar la palabra de la partida anterior de esta sala, ni la que
    # otra sala este jugando en este momento (alguien que mira las dos salas
    # podria pasarsela al espia de la otra).
    prohibidas = [salas[nombre_sala]["palabra"]]
    for otra in SALAS_VALIDAS:
        if otra != nombre_sala and salas[otra]["fase"] in ["PISTAS", "VOTACION"]:
            prohibidas.append(salas[otra]["palabra"])
    posibles = [par for par in PALABRAS if par[0] not in prohibidas]
    if not posibles:                       # por si la lista es muy corta
        posibles = PALABRAS
    return random.choice(posibles)


def iniciar_partida(cliente, nombre_sala):
    sala = salas[nombre_sala]
    sala["partida"] = sala["partida"] + 1
    sala["ronda"] = 1
    refrescar_palabras()
    sala["palabra"], sala["categoria"] = elegir_palabra(nombre_sala)
    sala["espia"] = random.choice(sala["jugadores"])
    sala["listos"] = []
    sala["expulsados"] = []
    sala["historial"] = []
    sala["ultima_votacion"] = None
    sala["resultado"] = None
    print(f"[{nombre_sala}] Partida {sala['partida']}: palabra={sala['palabra']} "
          f"({sala['categoria']}) espia={sala['espia']}")

    for usuario in sala["jugadores"]:
        enviar_secreto(cliente, nombre_sala, usuario, sala)
    avisar_sala(cliente, nombre_sala, f"Empieza la partida {sala['partida']}.")
    abrir_ronda(cliente, nombre_sala)


def abrir_votacion(cliente, nombre_sala):
    sala = salas[nombre_sala]
    sala["votos"] = {}
    abrir_fase(sala, "VOTACION", SEGUNDOS_VOTACION)
    publicar_estado(cliente, nombre_sala)
    avisar_sala(cliente, nombre_sala, f"A votar: ¿quien es el espia? ({texto_tiempo(SEGUNDOS_VOTACION, 'para votar')})")


def terminar_partida(cliente, nombre_sala, ganador, motivo):
    sala = salas[nombre_sala]
    sala["fase"] = "FIN"
    sala["segundos"] = 0
    sala["listos"] = []
    sala["resultado"] = {"ganador": ganador, "motivo": motivo, "espia": sala["espia"],
                         "palabra": sala["palabra"], "categoria": sala["categoria"]}
    # Historial: se copian los datos AHORA (con el candado tomado) y se guardan en otro hilo.
    # En este momento "jugadores" todavia tiene a todos los que empezaron la partida.
    datos = {"sala": nombre_sala, "numero": sala["partida"], "palabra": sala["palabra"],
             "categoria": sala["categoria"], "espia": sala["espia"], "ganador": ganador,
             "motivo": motivo, "jugadores": list(sala["jugadores"]),
             "expulsados": list(sala["expulsados"]), "rondas": sala["ronda"]}
    threading.Thread(target=guardar_partida_en_bd, args=(datos,), daemon=True).start()
    # Los que estan desconectados salen de la sala: si no, nunca van a estar "listos"
    # y la sala no podria empezar otra partida (lo mismo que pasa en el lobby).
    ausentes = [u for u in sala["jugadores"] if u in desconectados]
    for u in ausentes:
        sala["jugadores"].remove(u)
    print(f"[{nombre_sala}] Fin de la partida {sala['partida']}: ganan {ganador} ({motivo})")
    publicar_estado(cliente, nombre_sala)
    if ganador == "ESPIA":
        titulo = "¡Gana el ESPIA!"
    else:
        titulo = "¡Ganan los JUGADORES!"
    avisar_sala(cliente, nombre_sala,
                f"{titulo} {motivo}. El espia era {sala['espia']} y la palabra {sala['palabra']}. "
                f"Para jugar otra partida, todos tienen que estar listos.")
    if ausentes:
        avisar_sala(cliente, nombre_sala, f"Salen de la sala por estar desconectados: {', '.join(ausentes)}.")


def cerrar_votacion(cliente, nombre_sala):
    sala = salas[nombre_sala]

    # 1. Contar los votos: {votado: cantidad}
    conteo = {}
    for votado in sala["votos"].values():
        conteo[votado] = conteo.get(votado, 0) + 1

    # 2. Buscar al mas votado. Si hay empate en el maximo (o nadie voto), no sale nadie.
    expulsado = None
    if conteo:
        maximo = max(conteo.values())
        mas_votados = [usuario for usuario in conteo if conteo[usuario] == maximo]
        if len(mas_votados) == 1:
            expulsado = mas_votados[0]

    # 3. Recien ahora se hace publico el resultado: el conteo y el expulsado (no quien voto a quien).
    sala["ultima_votacion"] = {"ronda": sala["ronda"], "conteo": conteo, "expulsado": expulsado}
    sala["historial"].append({"ronda": sala["ronda"], "pistas": sala["pistas"]})

    # 4. Aplicar las reglas
    if expulsado is None:
        avisar_sala(cliente, nombre_sala, f"Empate o sin votos: nadie sale. Votos: {conteo}")
    elif expulsado == sala["espia"]:
        terminar_partida(cliente, nombre_sala, "JUGADORES", f"Descubrieron al espia ({expulsado})")
        return
    else:
        sala["expulsados"].append(expulsado)
        avisar_sala(cliente, nombre_sala, f"{expulsado} fue expulsado y NO era el espia. Votos: {conteo}")

    if len(vivos(sala)) <= 2:
        terminar_partida(cliente, nombre_sala, "ESPIA", "Quedaron solo 2 jugadores")
        return

    # 5. Nueva ronda, con la misma palabra y un orden de turnos nuevo
    sala["ronda"] = sala["ronda"] + 1
    abrir_ronda(cliente, nombre_sala)


def pedido_join(cliente, nombre_sala, usuario):
    sala = salas[nombre_sala]
    if usuario in sala["jugadores"]:
        avisar_jugador(cliente, nombre_sala, usuario, "Ya estas en la sala.")
        return
    if sala["fase"] not in ["LOBBY", "FIN"]:
        avisar_jugador(cliente, nombre_sala, usuario, "La partida ya empezo. Espera a la proxima.")
        return
    if len(sala["jugadores"]) >= MAX_JUGADORES:
        avisar_jugador(cliente, nombre_sala, usuario, "La sala esta llena.")
        return
    sala["jugadores"].append(usuario)
    borrar_secreto(cliente, nombre_sala, usuario)     # que no le quede un secreto viejo
    publicar_estado(cliente, nombre_sala)
    avisar_sala(cliente, nombre_sala, f"{usuario} entro a la sala.")


def pedido_leave(cliente, nombre_sala, usuario):
    # Salir de la sala: solo antes de empezar (LOBBY) o con la partida terminada (FIN).
    # En plena partida no: eso es abandonar, y lo cubren las reglas de desconexion.
    sala = salas[nombre_sala]
    if usuario not in sala["jugadores"]:
        avisar_jugador(cliente, nombre_sala, usuario, "No estas en esta sala.")
        return
    if sala["fase"] not in ["LOBBY", "FIN"]:
        avisar_jugador(cliente, nombre_sala, usuario, "No se puede salir en plena partida.")
        return
    sala["jugadores"].remove(usuario)
    if usuario in sala["listos"]:
        sala["listos"].remove(usuario)
    borrar_secreto(cliente, nombre_sala, usuario)
    avisar_sala(cliente, nombre_sala, f"{usuario} salio de la sala.")
    # Si los que quedan son 4 o mas y estaban todos listos, la partida arranca.
    if not arrancar_si_corresponde(cliente, nombre_sala):
        publicar_estado(cliente, nombre_sala)


def arrancar_si_corresponde(cliente, nombre_sala):
    # La partida arranca sola cuando hay 4 o mas y TODOS estan listos.
    sala = salas[nombre_sala]
    todos_listos = len(sala["listos"]) == len(sala["jugadores"])
    if len(sala["jugadores"]) >= MIN_JUGADORES and todos_listos:
        iniciar_partida(cliente, nombre_sala)
        return True
    return False


def pedido_ready(cliente, nombre_sala, usuario):
    sala = salas[nombre_sala]
    if usuario not in sala["jugadores"]:
        avisar_jugador(cliente, nombre_sala, usuario, "Primero tenes que entrar a la sala (join).")
        return
    if sala["fase"] not in ["LOBBY", "FIN"]:
        avisar_jugador(cliente, nombre_sala, usuario, "Ahora no se puede: la partida esta en juego.")
        return
    if usuario not in sala["listos"]:
        sala["listos"].append(usuario)

    if not arrancar_si_corresponde(cliente, nombre_sala):
        publicar_estado(cliente, nombre_sala)
        faltan = MIN_JUGADORES - len(sala["jugadores"])
        if faltan > 0:
            avisar_sala(cliente, nombre_sala, f"{usuario} esta listo. Faltan {faltan} jugadores para empezar.")
        else:
            avisar_sala(cliente, nombre_sala, f"{usuario} esta listo. Esperando a que todos esten listos.")


def es_de_esta_ronda(cliente, nombre_sala, usuario, pedido):
    # Descarta pedidos atrasados: los que vienen de otra partida u otra ronda.
    sala = salas[nombre_sala]
    if pedido.get("partida") != sala["partida"] or pedido.get("ronda") != sala["ronda"]:
        avisar_jugador(cliente, nombre_sala, usuario, "Pedido de una ronda anterior, descartado.")
        return False
    return True


def normalizar(texto):
    # Para comparar pistas: minusculas, sin tildes y sin signos.  "¡Árbol!" -> "arbol"
    # NFD separa cada letra de su tilde ("á" -> "a" + tilde suelta); despues se tiran las tildes.
    separado = unicodedata.normalize("NFD", texto.lower())
    letras = ""
    for c in separado:
        if unicodedata.category(c) == "Mn":    # Mn = la tilde suelta
            continue
        if c.isalnum():
            letras += c
        else:
            letras += " "                      # un signo cuenta como espacio
    return " ".join(letras.split())            # deja un solo espacio entre palabras


def pedido_clue(cliente, nombre_sala, usuario, pedido):
    sala = salas[nombre_sala]
    if sala["fase"] != "PISTAS":
        avisar_jugador(cliente, nombre_sala, usuario, "No es momento de dar pistas.")
        return
    if usuario not in vivos(sala):
        avisar_jugador(cliente, nombre_sala, usuario, "No estas jugando esta partida.")
        return
    if not es_de_esta_ronda(cliente, nombre_sala, usuario, pedido):
        return
    if usuario != turno_de(sala):
        avisar_jugador(cliente, nombre_sala, usuario, f"No es tu turno: le toca a {turno_de(sala)}.")
        return
    texto = str(pedido.get("texto", "")).strip()
    if texto == "" or len(texto) > LARGO_MAXIMO_PISTA:
        avisar_jugador(cliente, nombre_sala, usuario, f"La pista tiene que tener entre 1 y {LARGO_MAXIMO_PISTA} letras.")
        return
    # No se puede repetir una pista ya dicha en esta partida (esta ronda o las anteriores),
    # ni usar la palabra secreta. Se rechaza SIN pasar el turno: el jugador prueba con otra.
    pista = normalizar(texto)
    dichas = list(sala["pistas"].values())
    for vieja in sala["historial"]:
        dichas += list(vieja["pistas"].values())
    if pista in [normalizar(d) for d in dichas]:
        print(f"[{nombre_sala}] Pista repetida rechazada: {usuario} -> {texto}")
        avisar_jugador(cliente, nombre_sala, usuario, "Esa pista ya se dijo en esta partida. Proba con otra.")
        return
    palabra = normalizar(sala["palabra"])
    # Con espacios alrededor, "mar" encuentra "el mar azul" pero no "marron"
    if f" {palabra} " in f" {pista} " or pista.replace(" ", "") == palabra.replace(" ", ""):
        print(f"[{nombre_sala}] Pista con la palabra secreta rechazada: {usuario} -> {texto}")
        avisar_jugador(cliente, nombre_sala, usuario, "No podes usar la palabra secreta en la pista.")
        return

    sala["pistas"][usuario] = texto
    avisar_sala(cliente, nombre_sala, f"{usuario} dio su pista: {texto}")
    pasar_turno(cliente, nombre_sala)


def pedido_vote(cliente, nombre_sala, usuario, pedido):
    sala = salas[nombre_sala]
    if sala["fase"] != "VOTACION":
        avisar_jugador(cliente, nombre_sala, usuario, "No es momento de votar.")
        return
    if usuario not in vivos(sala):
        avisar_jugador(cliente, nombre_sala, usuario, "No estas jugando esta partida.")
        return
    if not es_de_esta_ronda(cliente, nombre_sala, usuario, pedido):
        return
    if usuario in sala["votos"]:
        avisar_jugador(cliente, nombre_sala, usuario, "Ya votaste en esta ronda.")
        return
    votado = pedido.get("target")
    if votado not in vivos(sala) or votado == usuario:
        avisar_jugador(cliente, nombre_sala, usuario, "Voto invalido: elegi a otro jugador que siga en juego.")
        return

    sala["votos"][usuario] = votado
    avisar_jugador(cliente, nombre_sala, usuario, f"Tu voto a {votado} fue registrado.")
    if votaron_todos_los_conectados(sala):
        cerrar_votacion(cliente, nombre_sala)         # votaron todos los que estan: no hace falta esperar
    else:
        publicar_estado(cliente, nombre_sala)


def procesar_pedido(cliente, nombre_sala, usuario, pedido):
    tipo = pedido.get("type")
    if tipo == "JOIN":
        pedido_join(cliente, nombre_sala, usuario)
    elif tipo == "LEAVE":
        pedido_leave(cliente, nombre_sala, usuario)
    elif tipo == "READY":
        pedido_ready(cliente, nombre_sala, usuario)
    elif tipo == "CLUE":
        pedido_clue(cliente, nombre_sala, usuario, pedido)
    elif tipo == "VOTE":
        pedido_vote(cliente, nombre_sala, usuario, pedido)
    else:
        avisar_jugador(cliente, nombre_sala, usuario, f"Pedido desconocido: {tipo}")


def procesar_presencia(cliente, usuario, presencia):
    # Llega por espia/presence/{usuario}: "online" al conectarse, y "offline" cuando sale
    # o cuando el broker publica su Last Will (se cayo sin despedirse).
    print(f"Presencia: {usuario} {presencia}")      # para poder seguirlo en el log
    if presencia == "online":
        if usuario in desconectados:
            del desconectados[usuario]
            for nombre_sala in salas_de(usuario):
                avisar_sala(cliente, nombre_sala, f"{usuario} volvio a conectarse.")
                publicar_estado(cliente, nombre_sala)
        return
    if presencia != "offline" or usuario in desconectados:
        return
    desconectados[usuario] = time.monotonic()
    for nombre_sala in salas_de(usuario):
        sala = salas[nombre_sala]
        if sala["fase"] in ["LOBBY", "FIN"]:
            # Antes de empezar, el que se va deja su lugar (si no, nadie podria arrancar)
            sala["jugadores"].remove(usuario)
            if usuario in sala["listos"]:
                sala["listos"].remove(usuario)
            avisar_sala(cliente, nombre_sala, f"{usuario} se desconecto y salio de la sala.")
            if not arrancar_si_corresponde(cliente, nombre_sala):
                publicar_estado(cliente, nombre_sala)
        else:
            # En juego sigue en la partida: si vuelve, continua. Mientras, pierde sus turnos.
            avisar_sala(cliente, nombre_sala, f"{usuario} se desconecto. Si no vuelve, pierde sus turnos.")
            publicar_estado(cliente, nombre_sala)


def revisar_tiempos(cliente):
    # La llama el hilo principal una vez por segundo. Devuelve True si cambio algo.
    ahora = time.monotonic()
    cambio = False
    for nombre_sala in SALAS_VALIDAS:
        sala = salas[nombre_sala]
        if sala["fase"] not in ["PISTAS", "VOTACION"]:
            continue
        # 1. El espia se fue y no volvio a tiempo: ganan los jugadores
        espia = sala["espia"]
        if espia in desconectados and ahora - desconectados[espia] >= SEGUNDOS_GRACIA:
            terminar_partida(cliente, nombre_sala, "JUGADORES", f"El espia ({espia}) abandono la partida")
            cambio = True
            continue
        # 1b. Un inocente se fue y no volvio a tiempo: queda fuera (como un expulsado).
        #     Si no, seguiria contando como vivo y la partida podria no terminar nunca.
        abandonaron = [u for u in vivos(sala) if u != espia and u in desconectados
                       and ahora - desconectados[u] >= SEGUNDOS_GRACIA]
        if abandonaron:
            for u in abandonaron:
                sala["expulsados"].append(u)
            # Se descartan los votos de ellos y a ellos (ya no estan en juego)
            sala["votos"] = {quien: a_quien for quien, a_quien in sala["votos"].items()
                             if quien not in abandonaron and a_quien not in abandonaron}
            avisar_sala(cliente, nombre_sala, f"Abandonaron la partida y quedan fuera: {', '.join(abandonaron)}.")
            if len(vivos(sala)) <= 2:
                terminar_partida(cliente, nombre_sala, "ESPIA", "Quedaron solo 2 jugadores")
            else:
                publicar_estado(cliente, nombre_sala)
            cambio = True
            continue
        # 2. Le toca a alguien desconectado: pierde el turno enseguida (no se espera el reloj)
        if sala["fase"] == "PISTAS" and turno_de(sala) in desconectados:
            avisar_sala(cliente, nombre_sala, f"{turno_de(sala)} esta desconectado: pierde el turno.")
            pasar_turno(cliente, nombre_sala)
            cambio = True
        # 3. Ya votaron todos los conectados (los desconectados no se esperan)
        elif sala["fase"] == "VOTACION" and votaron_todos_los_conectados(sala):
            cerrar_votacion(cliente, nombre_sala)
            cambio = True
        # 4. Se vencio el tiempo del turno o de la votacion
        elif sala["cierre"] is not None and ahora >= sala["cierre"]:
            if sala["fase"] == "PISTAS":
                avisar_sala(cliente, nombre_sala, f"{turno_de(sala)} no dio su pista a tiempo: pierde el turno.")
                pasar_turno(cliente, nombre_sala)
            else:
                avisar_sala(cliente, nombre_sala, "Se termino el tiempo de votar.")
                cerrar_votacion(cliente, nombre_sala)      # se cuenta con los votos que haya
            cambio = True
    return cambio


# ----------------------------------------------------------------------
# 5. MQTT: conexion y recepcion de mensajes
# ----------------------------------------------------------------------
def al_conectar(cliente, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        # El broker contesto el CONNECT con un rechazo (por ejemplo, usuario o clave incorrectos)
        print("EL BROKER RECHAZO LA CONEXION:", reason_code)
        return
    print("Servicio conectado:", reason_code)
    reanudar_juego()
    # Aviso de que el servicio esta vivo (su Last Will dice "offline" si se cae)
    cliente.publish("espia/service/status", "online", qos=1, retain=True)
    cliente.subscribe("espia/sala/+/action/+", qos=1)
    cliente.subscribe("espia/presence/+", qos=1)      # presencia de los jugadores (Last Will)
    # Al (re)conectarse republica el estado de todas las salas y los secretos de las
    # partidas en curso: si el broker se reinicio y perdio los retained, se recuperan.
    with candado:
        for nombre_sala in SALAS_VALIDAS:
            publicar_estado(cliente, nombre_sala)
            sala = salas[nombre_sala]
            if sala["fase"] in ["PISTAS", "VOTACION"]:
                for usuario in sala["jugadores"]:
                    enviar_secreto(cliente, nombre_sala, usuario, sala)


def al_desconectar(cliente, userdata, flags, reason_code, properties):
    # Si aparece seguido, hay OTRO cliente con el mismo client ID en el broker,
    # o la ACL le esta negando algo al servicio.
    global sin_broker_desde
    print("SERVICIO DESCONECTADO:", reason_code, "(reintenta solo; el juego queda en pausa)")
    if sin_broker_desde is None:
        sin_broker_desde = time.monotonic()      # desde aca, el reloj del juego no corre


def reanudar_juego():
    # Sin broker nadie puede jugar: el tiempo que duro el corte NO cuenta.
    # Se corren para adelante los vencimientos de turnos y votaciones, y la gracia de
    # los desconectados. Despues al_conectar publica el estado con los segundos que quedan.
    global sin_broker_desde
    with candado:
        if sin_broker_desde is None:
            return
        ahora = time.monotonic()
        pausa = ahora - sin_broker_desde
        sin_broker_desde = None
        for nombre_sala in SALAS_VALIDAS:
            sala = salas[nombre_sala]
            if sala["fase"] in ["PISTAS", "VOTACION"] and sala["cierre"] is not None:
                sala["cierre"] = sala["cierre"] + pausa
                sala["segundos"] = max(0, round(sala["cierre"] - ahora))   # lo que le quedaba
        for usuario in desconectados:
            desconectados[usuario] = desconectados[usuario] + pausa
        if pausa >= 1:
            print(f"Juego reanudado: estuvo en pausa {round(pausa)} s sin broker.")
        guardar_estado()


def al_recibir(cliente, userdata, msg):
    try:
        partes = msg.topic.split("/")

        # PRESENCIA: espia/presence/gustavo -> "online" u "offline"
        if partes[1] == "presence":
            with candado:
                procesar_presencia(cliente, partes[2], msg.payload.decode().strip())
                guardar_estado()
            return

        # PEDIDOS: espia/sala/Esquel/action/gustavo -> ["espia","sala","Esquel","action","gustavo"]
        nombre_sala = partes[2]
        usuario = partes[4]          # QUIEN pide sale del topic, nunca del mensaje
        pedido = json.loads(msg.payload.decode())

        if nombre_sala not in SALAS_VALIDAS:
            print("Sala inexistente, se descarta:", nombre_sala)
            return

        request_id = pedido.get("request_id")
        if not request_id:
            avisar_jugador(cliente, nombre_sala, usuario, "Pedido sin request_id, descartado.")
            return

        with candado:
            if (usuario, request_id) in procesados:
                print(f"Duplicado descartado: {request_id} de {usuario}")
                return
            registrar_procesado(usuario, request_id)
            print(f"[{nombre_sala}] {usuario}: {pedido}")
            procesar_pedido(cliente, nombre_sala, usuario, pedido)
            guardar_estado()                     # cada cambio queda en disco

    except Exception as error:
        # Un mensaje roto NO tira abajo el servicio: se descarta y se sigue.
        print("Mensaje invalido descartado:", msg.topic, msg.payload[:50], error)


# Estado guardado (si el servicio se reinicio en medio de una partida, sigue donde estaba)
os.makedirs(os.path.dirname(ARCHIVO_ESTADO) or ".", exist_ok=True)
cargar_estado()

# El client ID es el usuario (si hay); asi coincide con "Use Username as Client ID".
cliente = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                      client_id=MQTT_USUARIO or "servicio", clean_session=False)
if MQTT_USUARIO:
    cliente.username_pw_set(MQTT_USUARIO, MQTT_CLAVE)    # autenticacion (R5)
if CA_CERT:
    cliente.tls_set(ca_certs=CA_CERT)                    # TLS: verifica el certificado del broker con la CA
# Last Will del servicio: si se cae sin despedirse, el broker publica "offline" por el.
cliente.will_set("espia/service/status", "offline", qos=1, retain=True)
cliente.on_connect = al_conectar
cliente.on_message = al_recibir
cliente.on_disconnect = al_desconectar
cliente.reconnect_delay_set(1, 5)   # si se corta, reintenta cada 1 a 5 segundos
# connect_async: NO falla si el broker todavia no esta (por ejemplo, si arrancan juntos
# con docker compose). El hilo de paho lo intenta solo hasta que el broker aparece.
cliente.connect_async(BROKER, PUERTO)
cliente.loop_start()                # la red se atiende en OTRO hilo

seguridad = []
if MQTT_USUARIO:
    seguridad.append(f"usuario {MQTT_USUARIO}")
if CA_CERT:
    seguridad.append("TLS")
print(f"Servicio central corriendo contra {BROKER}:{PUERTO} "
      f"({', '.join(seguridad) if seguridad else 'sin autenticacion ni TLS'}). Ctrl+C para salir.")
try:
    while True:
        time.sleep(1)
        with candado:               # el mismo candado que usa al_recibir
            # Sin broker, el juego queda en PAUSA: no se vencen turnos ni votaciones.
            if sin_broker_desde is None and revisar_tiempos(cliente):
                guardar_estado()
except KeyboardInterrupt:
    # Salida prolija: el Last Will solo se publica si se corta SIN despedirse,
    # asi que avisamos a mano que el servicio se apaga.
    cliente.publish("espia/service/status", "offline", qos=1, retain=True).wait_for_publish(2)
    cliente.loop_stop()
    cliente.disconnect()
    print("Servicio detenido.")