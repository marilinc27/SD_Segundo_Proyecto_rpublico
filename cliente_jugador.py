# Juego del Espia - cliente de escritorio (Python + tkinter + paho-mqtt)
#
# PARTE 1: pantalla de ingreso (usuario y clave), TLS y Last Will.
#          El registro de un usuario nuevo es OTRA pantalla, para no confundirlo con el ingreso.
# PARTE 2: pantalla de salas (estado de las 3 salas, Unirme, presencia del servicio).
# PARTE 3: pantalla de juego (secreto, turnos, pista, votacion, resultado y avisos).
#          Antes de mandar una pista se revisa que no este repetida ni sea la palabra secreta.
#          El resultado se ve a la izquierda solo para los que jugaron, mientras la sala no cambie.
# Salir de la sala (pedido LEAVE) en el lobby o con la partida terminada.
#
# Ejecutar:  python3 cliente_jugador.py
# Necesita:  pip install paho-mqtt==2.1.0   (tkinter viene con Python; en Linux: sudo apt install python3-tk)
#            y el certificado de la CA del grupo (cacert.pem) en la misma carpeta.
#
# Variables de entorno (opcionales, para probar con otro broker):
#   BROKER, PUERTO      broker MQTT con TLS (por defecto el del grupo por sd-2026)
#   CA_CERT             certificado de la CA; vacio = sin TLS (solo para un broker local)
#   API_REGISTRO        API de registro de usuarios
#
# HILOS (ver el cuaderno, "Productor-consumidor"):
#   - el hilo principal corre tkinter: es el UNICO que toca las ventanas;
#   - el hilo de paho y el hilo del registro solo dejan avisos en la "cola";
#   - revisar_cola() (hilo principal) los saca cada 100 ms y actualiza la pantalla.

import json
import os
import queue
import threading
import time
import tkinter as tk
import unicodedata
import urllib.error
import urllib.request
import uuid

import paho.mqtt.client as mqtt

CARPETA = os.path.dirname(os.path.abspath(__file__))
BROKER = os.environ.get("BROKER", "sd-2026.tailb9d8eb.ts.net")   # el nombre del certificado
PUERTO = int(os.environ.get("PUERTO", "18813"))                    # MQTT con TLS
CA_CERT = os.environ.get("CA_CERT", os.path.join(CARPETA, "cacert.pem"))
API_REGISTRO = os.environ.get("API_REGISTRO", "http://sd-2026.tailb9d8eb.ts.net:18010").rstrip("/")

MIN_USUARIO = 3              # los mismos minimos que pide la API
MIN_CLAVE = 6
MAX_JUGADORES = 6            # el mismo maximo que el servicio (solo para mostrar "llena")
SEGUNDOS_ESPERA = 10         # si el broker no contesta en este tiempo, se cancela

# Colores y letras (tono "expediente secreto", como la web)
FONDO = "#1b1d21"
PAPEL = "#e9e2cf"
TINTA = "#1b1d21"
ROJO = "#a8322d"
GRIS = "#8a8f98"
VERDE = "#3c7a4b"
TARJETA = "#f7f3e8"
RESALTADO = "#f2d98c"
LETRA = ("Helvetica", 11)
LETRA_TITULO = ("Courier", 22, "bold")

# ----------------------------------------------------------------------
# Estado del cliente. Lo toca SOLO el hilo principal.
# ----------------------------------------------------------------------
cola = queue.Queue()         # el unico objeto compartido entre hilos
cliente = None               # el cliente MQTT (se crea al apretar Entrar)
usuario = ""                 # con quien se entro
conectado = False
intento = 0                  # numero de intento de ingreso (para el tiempo de espera)
salas_vistas = {}            # sala -> ultimo estado recibido (diccionario del JSON)
secretos = {}                # sala -> mi secreto en esa sala (PARTE 3)
servicio_estado = None       # "online", "offline" o None (todavia no se sabe)
mi_sala = None               # la sala en la que estoy (segun el servicio), o None
pidiendo = None              # sala a la que pedi entrar y todavia no contesto
sala_eventos = None          # sala de la que estoy escuchando los events
esperando = False            # mande una pista o un voto y todavia no llego el state siguiente
saliendo = None              # sala de la que pedi salir y todavia no contesto
partida_jugada = None        # (sala, partida) que vi EN CURSO estando adentro: la jugue yo
jugadores_al_terminar = None # quienes estaban en la sala cuando termino esa partida (una "foto")
clave_reloj = None           # (partida, ronda, fase, turno): si cambia, arranca la cuenta de nuevo
cierre_local = None          # cuando vence (en time.monotonic de ESTA maquina); None = sin limite


# ----------------------------------------------------------------------
# 1. PRODUCTORES: corren en OTROS hilos. Solo hacen cola.put(...)
# ----------------------------------------------------------------------
def al_conectar(cli, userdata, flags, reason_code, properties):
    # Hilo de paho. Llega la respuesta del broker (CONNACK).
    cola.put(("conexion", not reason_code.is_failure, str(reason_code)))


def al_fallar_conexion(cli, userdata):
    # Hilo de paho. No se llego a hablar con el broker (nombre, puerto, red o certificado).
    cola.put(("fallo", None, None))


def al_desconectar(cli, userdata, flags, reason_code, properties):
    cola.put(("desconexion", None, str(reason_code)))


def al_recibir(cli, userdata, msg):
    cola.put(("mensaje", msg.topic, msg.payload.decode("utf-8", errors="replace")))


def registrar_en_api(nombre, clave):
    # Corre en un hilo aparte: el pedido HTTP espera la respuesta y congelaria la ventana.
    datos = json.dumps({"username": nombre, "password": clave}).encode("utf-8")
    pedido = urllib.request.Request(f"{API_REGISTRO}/registro", data=datos, method="POST",
                                    headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(pedido, timeout=SEGUNDOS_ESPERA):
            cola.put(("registro", True, "Usuario registrado."))
    except urllib.error.HTTPError as error:          # la API contesto, pero con un error
        # Al jugador, mensajes generales: el detalle tecnico no le sirve.
        if error.code == 409:
            cola.put(("registro", False, "Ese usuario ya existe."))
        else:
            cola.put(("registro", False, "No se pudo crear el usuario. Probá de nuevo más tarde."))
    except (urllib.error.URLError, OSError):         # ni siquiera se llego a la API
        cola.put(("registro", False, "No se pudo crear el usuario. Probá de nuevo más tarde."))


# ----------------------------------------------------------------------
# 2. CONSUMIDOR: corre en el hilo principal (lo llama tkinter cada 100 ms)
# ----------------------------------------------------------------------
def revisar_cola():
    hay_cambios = False
    while True:
        try:
            tipo, dato1, dato2 = cola.get_nowait()
        except queue.Empty:
            break
        if tipo == "conexion":
            resultado_conexion(dato1, dato2)
        elif tipo == "fallo":
            # Si ya estaba adentro, es un reintento de paho mientras el broker no esta:
            # NO hay que frenarlo, sigue intentando solo hasta que el broker vuelva.
            if not conectado:
                cancelar_ingreso("No se pudo conectar. Probá de nuevo más tarde.")
        elif tipo == "desconexion":
            resultado_desconexion(dato2)
        elif tipo == "registro":
            resultado_registro(dato1, dato2)
        elif tipo == "mensaje":
            if procesar_mensaje(dato1, dato2):         # solo GUARDA lo que llego
                hay_cambios = True
    if hay_cambios:
        actualizar_pantalla()                          # redibuja UNA vez por vuelta
    ventana.after(100, revisar_cola)


def procesar_mensaje(topic, texto):
    # Hilo principal. Guarda el mensaje donde corresponde. Devuelve True si hay que redibujar.
    global servicio_estado, pidiendo, esperando, saliendo, clave_reloj
    partes = topic.split("/")
    if topic == "espia/service/status":
        anterior = servicio_estado
        servicio_estado = texto
        if texto == "offline":
            agregar_aviso("El servicio se cayo: el juego queda en espera hasta que vuelva.", privado=True)
        elif texto == "online" and anterior == "offline":
            agregar_aviso("El servicio volvio: la partida sigue donde estaba.", privado=True)
            clave_reloj = None      # la cuenta regresiva arranca de nuevo con lo que mande el servicio
            esperando = False
        return True
    if len(partes) != 5 and len(partes) != 4:
        return False
    sala = partes[2]                       # espia/sala/{SALA}/... -> la sala sale del TOPIC
    if texto == "":
        return False                       # retained borrado: no hay nada que mostrar
    if partes[3] == "events":
        if sala == mi_sala:
            agregar_aviso(texto)           # los events son texto, no JSON
        return False
    try:
        datos = json.loads(texto)
    except ValueError:
        return False                       # un mensaje raro no tiene que tirar el programa
    if not isinstance(datos, dict):
        return False
    if partes[3] == "state":
        salas_vistas[sala] = datos         # el estado nuevo de esa sala reemplaza al viejo
        if sala == mi_sala:
            esperando = False              # llego la respuesta a mi pista o mi voto
        return True
    if partes[3] == "private":
        if datos.get("tipo") == "SECRETO":
            secretos[sala] = datos         # se usa en la PARTE 3
        elif datos.get("tipo") == "AVISO":
            if pidiendo == sala:
                pidiendo = None            # el servicio contesto (por ejemplo, "La sala esta llena")
            if sala == mi_sala:
                esperando = False          # el servicio contesto (por ejemplo, "No es tu turno")
                saliendo = None            # si pedi salir y no me dejaron, el boton vuelve
                agregar_aviso("» " + datos.get("texto", ""), privado=True)
            else:
                mostrar_aviso_salas(f"{sala}: {datos.get('texto', '')}")
        return True
    return False


# ----------------------------------------------------------------------
# 3. INGRESO: lo que pasa al apretar los botones (hilo principal)
# ----------------------------------------------------------------------
def validar(nombre, clave):
    # Devuelve el problema (texto) o None si esta todo bien. La usan el ingreso y el registro.
    if len(nombre) < MIN_USUARIO:
        return f"El usuario tiene que tener al menos {MIN_USUARIO} letras."
    if len(clave) < MIN_CLAVE:
        return f"La clave tiene que tener al menos {MIN_CLAVE} caracteres."
    return None


def boton_entrar(evento=None):
    nombre = campo_usuario.get().strip()
    clave = campo_clave.get()
    problema = validar(nombre, clave)
    if problema:
        mostrar_aviso(problema, error=True)
        return
    conectar(nombre, clave)


def ir_a_registro():
    # Del ingreso a la pantalla de registro
    mostrar_aviso("")
    mostrar_aviso_registro("")
    mostrar_pantalla(marco_registro)
    reg_usuario.focus()


def ir_a_ingreso():
    # Del registro de vuelta al ingreso
    mostrar_aviso_registro("")
    mostrar_pantalla(marco_ingreso)
    campo_usuario.focus()


def boton_crear_usuario(evento=None):
    nombre = reg_usuario.get().strip()
    clave = reg_clave.get()
    problema = validar(nombre, clave)
    if problema is None and clave != reg_clave2.get():
        problema = "Las dos claves no son iguales."
    if problema:
        mostrar_aviso_registro(problema, error=True)
        return
    habilitar_botones_registro(False)
    mostrar_aviso_registro("Registrando...")
    threading.Thread(target=registrar_en_api, args=(nombre, clave), daemon=True).start()


def resultado_registro(salio_bien, texto):
    habilitar_botones_registro(True)
    if not salio_bien:
        mostrar_aviso_registro(texto, error=True)
        return
    # Salio bien: paso usuario y clave a la pantalla de ingreso y entro solo
    nombre = reg_usuario.get().strip()
    clave = reg_clave.get()
    for campo in (reg_usuario, reg_clave, reg_clave2):
        campo.delete(0, "end")
    campo_usuario.delete(0, "end")
    campo_usuario.insert(0, nombre)
    campo_clave.delete(0, "end")
    campo_clave.insert(0, clave)
    ir_a_ingreso()
    conectar(nombre, clave, texto + " Entrando...")


def conectar(nombre, clave, mensaje="Conectando..."):
    global cliente, usuario, intento
    if CA_CERT and not os.path.exists(CA_CERT):
        mostrar_aviso("No se pudo conectar. Probá de nuevo más tarde.", error=True)
        return
    usuario = nombre
    intento += 1
    habilitar_botones(False)
    mostrar_aviso(mensaje)

    # El client ID es el usuario (el broker usa "Use Username as Client ID").
    cliente = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=nombre)
    cliente.username_pw_set(nombre, clave)         # LOGIN: el broker los verifica contra mqtt_user
    if CA_CERT:
        cliente.tls_set(ca_certs=CA_CERT)          # TLS: verifica que el broker sea el del grupo
    # LAST WILL: si este programa se corta sin despedirse, el broker publica "offline" por nosotros.
    cliente.will_set(f"espia/presence/{nombre}", "offline", qos=1, retain=True)
    cliente.on_connect = al_conectar
    cliente.on_connect_fail = al_fallar_conexion
    cliente.on_disconnect = al_desconectar
    cliente.on_message = al_recibir
    cliente.reconnect_delay_set(1, 5)  # si se corta, reintenta cada 1 a 5 s (por defecto llega a 120 s)
    # connect_async NO espera: la conexion se hace en el hilo de paho y la respuesta llega a la cola.
    cliente.connect_async(BROKER, PUERTO, keepalive=10)
    cliente.loop_start()
    # Si en SEGUNDOS_ESPERA no hubo respuesta, se cancela (por ejemplo, un puerto equivocado).
    ventana.after(SEGUNDOS_ESPERA * 1000, revisar_espera, intento)


def revisar_espera(numero_de_intento):
    if numero_de_intento == intento and cliente is not None and not conectado:
        cancelar_ingreso("No se pudo conectar. Probá de nuevo más tarde.")


def resultado_conexion(salio_bien, motivo):
    global conectado, sala_eventos, clave_reloj
    if not salio_bien:
        if conectado:
            # Ya habia entrado: es una RECONEXION. Recien levantado, el broker puede rechazar
            # un momento (por ejemplo, si todavia no llego a la base de usuarios).
            # No se frena a paho: sigue reintentando solo.
            etiqueta_conexion.config(text="Reconectando...")
            etiqueta_conexion_juego.config(text="⚠ Reconectando...")
            return
        # Usuario o clave incorrectos: hay que FRENAR a paho, si no reintenta para siempre.
        if "user name or password" in motivo.lower() or "not authorized" in motivo.lower():
            cancelar_ingreso("Usuario o clave incorrectos.")
        else:
            cancelar_ingreso("No se pudo conectar. Probá de nuevo más tarde.")
        return
    primera_vez = not conectado
    conectado = True
    # PRESENCIA: aviso que estoy (retained: el que se suscriba despues tambien lo ve).
    cliente.publish(f"espia/presence/{usuario}", "online", qos=1, retain=True)
    # SUSCRIPCIONES: en CADA conexion. Con sesion limpia, si se corta, el broker las olvida.
    cliente.subscribe("espia/sala/+/state", qos=1)               # las 3 salas con un wildcard
    cliente.subscribe(f"espia/sala/+/private/{usuario}", qos=1)  # solo lo mio (la ACL no deja otra cosa)
    cliente.subscribe("espia/service/status", qos=1)             # si el servicio esta vivo
    sala_eventos = None                  # que se vuelva a suscribir a los events de mi sala
    etiqueta_conexion.config(text="")
    etiqueta_conexion_juego.config(text="")
    if primera_vez:
        mostrar_salas()
    else:
        agregar_aviso("Conexion recuperada.", privado=True)
        clave_reloj = None       # la cuenta regresiva arranca de nuevo con lo que mande el servicio
        actualizar_pantalla()


def resultado_desconexion(motivo):
    # paho reconecta solo; solo se avisa (en la PARTE 2 se muestra en la pantalla de juego)
    if conectado:
        etiqueta_conexion.config(text="Se corto la conexion. Reconectando...")
        etiqueta_conexion_juego.config(text="⚠ Reconectando...")
        agregar_aviso("Se corto la conexion. Reconectando...", privado=True)


def cancelar_ingreso(texto):
    global cliente, conectado
    if cliente is not None:
        cliente.loop_stop()          # frena el hilo de paho (y sus reintentos)
        cliente.disconnect()
        cliente = None
    conectado = False
    mostrar_aviso(texto, error=True)
    habilitar_botones(True)


def cerrar_ventana():
    # Salida prolija: el Last Will solo se publica si el programa se corta SIN despedirse,
    # asi que avisamos "offline" a mano antes de desconectar.
    if cliente is not None and conectado:
        aviso = cliente.publish(f"espia/presence/{usuario}", "offline", qos=1, retain=True)
        try:
            aviso.wait_for_publish(2)
        except (RuntimeError, ValueError):
            pass
        cliente.disconnect()
        cliente.loop_stop()
    ventana.destroy()


# ----------------------------------------------------------------------
# 3b. SALAS: unirse (hilo principal)
# ----------------------------------------------------------------------
def nuevo_pedido(tipo, sala, **extra):
    # Mismo formato de pedido que la consola y la web.
    estado = salas_vistas.get(sala, {})
    pedido = {"request_id": str(uuid.uuid4()), "type": tipo,
              "partida": estado.get("partida"), "ronda": estado.get("ronda")}
    pedido.update(extra)
    # publish se puede llamar desde el hilo de tkinter: paho protege su cola de salida.
    cliente.publish(f"espia/sala/{sala}/action/{usuario}", json.dumps(pedido), qos=1)


def boton_unirme(sala):
    global pidiendo
    pidiendo = sala
    mostrar_aviso_salas(f"Pidiendo entrar a {sala}...")
    nuevo_pedido("JOIN", sala)
    actualizar_pantalla()          # deshabilita los botones mientras espera
    # NO se cambia de pantalla aca: se espera a que el servicio me incluya en el state.


def se_puede_unir(estado):
    return (estado.get("fase") in ["LOBBY", "FIN"]
            and len(estado.get("jugadores", [])) < MAX_JUGADORES)


def buscar_mi_sala():
    # El servicio es el que decide: estoy en la sala cuyo state me incluye.
    for sala, estado in salas_vistas.items():
        if usuario in estado.get("jugadores", []):
            return sala
    return None


def actualizar_pantalla():
    global mi_sala, pidiendo, saliendo
    if not conectado:
        return
    global partida_jugada, jugadores_al_terminar
    anterior = mi_sala
    mi_sala = buscar_mi_sala()
    if mi_sala != anterior:
        # Sali de la sala (o me sacaron por desconexion): lo que jugue antes ya no se muestra
        # a la izquierda. Si vuelvo a entrar, soy como un jugador nuevo.
        partida_jugada = None
        jugadores_al_terminar = None
    suscribir_eventos()
    if mi_sala is not None:
        if pidiendo == mi_sala:
            pidiendo = None
            mostrar_aviso_salas("")
        mostrar_sala()             # PARTE 3: pantalla de juego
    else:
        if anterior is not None and saliendo == anterior:
            saliendo = None
            mostrar_aviso_salas(f"Saliste de {anterior}.")
        elif anterior is not None:
            # Estaba en una sala y el servicio me saco (por ejemplo, se corto la conexion en el lobby)
            mostrar_aviso_salas(f"Ya no estas en {anterior}: si se corto la conexion antes de empezar, "
                                "hay que volver a unirse.")
        mostrar_salas()


def dibujar_salas():
    # Indicador del servicio (su Last Will)
    if servicio_estado == "online":
        etiqueta_servicio.config(text="● Servicio activo", fg=VERDE)
    elif servicio_estado == "offline":
        etiqueta_servicio.config(text="● Servicio caido", fg=ROJO)
    else:
        etiqueta_servicio.config(text="● Servicio: sin datos", fg=GRIS)
    # Las filas se rearman cada vez (son pocas): nombre, como esta y el boton
    for fila in marco_filas.winfo_children():
        fila.destroy()
    if not salas_vistas:
        tk.Label(marco_filas, text="Esperando el estado de las salas...", font=LETRA,
                 bg=PAPEL, fg=GRIS).pack(pady=10)
    for sala in sorted(salas_vistas):
        estado = salas_vistas[sala]
        cantidad = len(estado.get("jugadores", []))
        fase = estado.get("fase", "?")
        if fase == "LOBBY":
            como = "Esperando jugadores"
        elif fase == "FIN":
            como = "Partida terminada"
        else:
            como = "En partida"
        if cantidad >= MAX_JUGADORES:
            como = "Llena"
        fila = tk.Frame(marco_filas, bg=PAPEL)
        fila.pack(fill="x", pady=4)
        tk.Label(fila, text=sala.replace("_", " "), font=("Courier", 12, "bold"), bg=PAPEL,
                 fg=TINTA, width=20, anchor="w").pack(side="left")
        tk.Label(fila, text=f"{como} · {cantidad}/{MAX_JUGADORES}", font=LETRA, bg=PAPEL,
                 fg=TINTA, width=22, anchor="w").pack(side="left")
        habilitado = (se_puede_unir(estado) and servicio_estado != "offline"
                      and pidiendo is None)
        tk.Button(fila, text="Unirme", font=LETRA, width=8,
                  state="normal" if habilitado else "disabled",
                  command=lambda s=sala: boton_unirme(s)).pack(side="right")


def suscribir_eventos():
    # Escucho los events SOLO de mi sala. Si cambie de sala (o me reconecte), se actualiza.
    global sala_eventos
    if sala_eventos == mi_sala:
        return
    if sala_eventos is not None:
        cliente.unsubscribe(f"espia/sala/{sala_eventos}/events")
    if mi_sala is not None:
        cliente.subscribe(f"espia/sala/{mi_sala}/events", qos=0)
    sala_eventos = mi_sala


# ----------------------------------------------------------------------
# 3c. JUEGO: botones de la sala (hilo principal)
# ----------------------------------------------------------------------
def boton_salir():
    # Pedir salir de la sala (solo en el lobby o con la partida terminada).
    # Igual que con Unirme: no se cambia de pantalla aca, se espera el state que ya no me incluye.
    global saliendo
    saliendo = mi_sala
    nuevo_pedido("LEAVE", mi_sala)
    boton_sal.config(state="disabled")
    boton_lst.config(state="disabled")


def boton_listo():
    nuevo_pedido("READY", mi_sala)
    boton_lst.config(state="disabled")         # hasta que el state me muestre como listo


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


def problema_con_pista(texto):
    # Devuelve el problema (texto) o None si la pista se puede mandar.
    # OJO: es solo una AYUDA para el jugador. El que decide es el servicio, que tiene que
    # hacer la misma revision (un cliente modificado podria saltearse esta).
    estado = salas_vistas.get(mi_sala, {})
    pista = normalizar(texto)
    if pista == "":
        return "La pista tiene que tener alguna letra o numero."
    # 1) Que no se haya dicho antes en esta partida (esta ronda y las anteriores)
    dichas = list(estado.get("pistas", {}).values())
    for vieja in estado.get("historial", []):
        dichas += list(vieja.get("pistas", {}).values())
    for dicha in dichas:
        if dicha and normalizar(str(dicha)) == pista:
            return f"La pista \"{dicha}\" ya se dijo en esta partida. Proba con otra."
    # 2) Que no sea la palabra secreta (solo la conozco si no soy el espia)
    secreto = secretos.get(mi_sala)
    if secreto and secreto.get("partida") == estado.get("partida") and secreto.get("palabra"):
        palabra = normalizar(secreto["palabra"])
        # Con espacios alrededor, "mar" encuentra "el mar azul" pero no "marron"
        if f" {palabra} " in f" {pista} " or pista.replace(" ", "") == palabra.replace(" ", ""):
            return "No podes usar la palabra secreta en la pista."
    return None


def boton_pista(evento=None):
    global esperando
    texto = campo_pista.get().strip()
    if not texto:
        return
    problema = problema_con_pista(texto)
    if problema:
        etiqueta_error_pista.config(text=problema)
        agregar_aviso("» " + problema, privado=True)
        campo_pista.focus()                    # el texto queda para corregirlo
        return
    etiqueta_error_pista.config(text="")
    esperando = True
    nuevo_pedido("CLUE", mi_sala, texto=texto)
    campo_pista.delete(0, "end")
    dibujar_juego()


def boton_votar(a_quien):
    global esperando
    esperando = True
    nuevo_pedido("VOTE", mi_sala, target=a_quien)
    dibujar_juego()


def agregar_aviso(texto, privado=False):
    # Columna de avisos: el mas nuevo arriba y resaltado.
    hora = time.strftime("%H:%M:%S")
    caja_avisos.config(state="normal")
    caja_avisos.tag_remove("nuevo", "1.0", "end")
    caja_avisos.insert("1.0", f"{hora}  {texto}\n", ("privado",) if privado else ())
    caja_avisos.tag_add("nuevo", "1.0", "2.0")
    caja_avisos.config(state="disabled")       # solo lectura


# ----------------------------------------------------------------------
# 3d. LA PANTALLA DE JUEGO ES UNA FUNCION DEL ESTADO
# ----------------------------------------------------------------------
def dibujar_juego():
    # Mira el ULTIMO state de mi sala y mi secreto, y decide TODO lo que se ve.
    # No hay variables sueltas tipo "ya vote": sale del state. Redibujar dos veces da lo mismo.
    global clave_reloj, cierre_local, partida_jugada, jugadores_al_terminar
    if mi_sala is None or mi_sala not in salas_vistas:
        return
    estado = salas_vistas[mi_sala]
    fase = estado.get("fase", "?")
    jugadores = estado.get("jugadores", [])
    vivos = estado.get("vivos", [])
    pistas = estado.get("pistas", {})
    desconectados = estado.get("desconectados", [])
    turno = estado.get("turno_de")

    etiqueta_titulo_sala.config(text=f"SALA {mi_sala.replace('_', ' ').upper()}")
    # Aviso arriba a la derecha: primero la conexion propia, despues el servicio.
    texto_aviso = etiqueta_conexion_juego.cget("text")
    if texto_aviso in ["", "⚠ Servicio caido"]:
        etiqueta_conexion_juego.config(text="⚠ Servicio caido" if servicio_estado == "offline" else "")
    nombres_fase = {"LOBBY": "Esperando jugadores", "PISTAS": "Ronda de pistas",
                    "VOTACION": "Votacion", "FIN": "Esperando jugadores para la proxima partida"}
    if fase in ["PISTAS", "VOTACION"]:
        etiqueta_partida.config(text=f"Partida {estado.get('partida')} · Ronda {estado.get('ronda')} · {nombres_fase[fase]}")
    else:
        etiqueta_partida.config(text=nombres_fase.get(fase, fase))

    # --- Tarjeta del secreto (solo si es de ESTA partida: defensa en profundidad)
    secreto = secretos.get(mi_sala)
    if secreto and secreto.get("partida") != estado.get("partida"):
        secreto = None
    # --- Resultado: a la IZQUIERDA solo para los que jugaron y mientras la sala no cambie.
    # 1) Si veo la partida EN CURSO estando adentro, la anoto como "jugada por mi".
    if fase in ["PISTAS", "VOTACION"] and usuario in jugadores:
        partida_jugada = (mi_sala, estado.get("partida"))
        jugadores_al_terminar = None
    resultado = estado.get("resultado")
    ver_resultado = False
    if fase == "FIN" and resultado and partida_jugada == (mi_sala, estado.get("partida")):
        # 2) La primera vez que la veo terminada: saco la "foto" de la sala y lo dejo en avisos.
        if jugadores_al_terminar is None:
            jugadores_al_terminar = sorted(jugadores)
            agregar_aviso("Resultado: " + texto_resultado(resultado))
        # 3) Si entro o salio alguien, el resultado deja la izquierda (queda en los avisos).
        if sorted(jugadores) == jugadores_al_terminar:
            ver_resultado = True
        else:
            partida_jugada = None
    # Un jugador nuevo nunca tiene partida_jugada de esa partida: no ve el resultado viejo.

    if ver_resultado:
        gano_espia = "esp" in str(resultado.get("ganador", "")).lower()
        sello.config(text="CASO CERRADO", fg=ROJO)
        if gano_espia:
            texto_secreto.config(text="GANA EL ESPIA", fg=ROJO)
        else:
            texto_secreto.config(text="GANAN LOS JUGADORES", fg=VERDE)
        detalle_secreto.config(text=texto_resultado(resultado, con_ganador=False))
    elif fase in ["PISTAS", "VOTACION"] and secreto:
        if secreto.get("rol") == "ESPIA":
            sello.config(text="ESPIA", fg=ROJO)
            texto_secreto.config(text="Sos el espia", fg=ROJO)
            detalle_secreto.config(text=f"No conoces la palabra. Ayuda: es de la categoria {secreto.get('categoria')}.\n"
                                        "Disimula con tus pistas y que no te descubran.")
        else:
            sello.config(text="CONFIDENCIAL", fg=ROJO)
            texto_secreto.config(text=secreto.get("palabra", "?"), fg=TINTA)
            detalle_secreto.config(text="Esta es la palabra. Da pistas sin decirla: el espia no la conoce.")
    elif fase in ["PISTAS", "VOTACION"]:
        sello.config(text="", fg=ROJO)
        texto_secreto.config(text="...", fg=GRIS)
        detalle_secreto.config(text="Esperando tu secreto.")
    else:
        sello.config(text="EN ESPERA", fg=GRIS)
        texto_secreto.config(text="Sin partida", fg=GRIS)
        detalle_secreto.config(text="Cuando haya 4 o mas y todos esten listos, se sortean la palabra y el espia.")

    # --- Turnos (PISTAS) o quienes votaron (VOTACION)
    if fase == "PISTAS":
        partes = []
        for u in estado.get("orden", []):
            if u == turno:
                partes.append(f"▶ {u}")
            elif u in pistas:
                partes.append(f"{u} ✓")
            else:
                partes.append(u)
        etiqueta_turnos.config(text="Turnos:  " + "   ".join(partes))
    elif fase == "VOTACION":
        votaron = estado.get("votaron", [])
        etiqueta_turnos.config(text=f"Votaron {len(votaron)} de {len(vivos)}: " + (", ".join(votaron) or "nadie todavia"))
    else:
        etiqueta_turnos.config(text="")

    # --- Reloj: arranca de nuevo SOLO si cambio el turno o la fase (ver cuaderno)
    clave = (estado.get("partida"), estado.get("ronda"), fase, turno)
    if clave != clave_reloj:
        clave_reloj = clave
        etiqueta_error_pista.config(text="")      # el error de una pista vieja no queda colgado
        segundos = estado.get("segundos") or 0
        if fase in ["PISTAS", "VOTACION"] and segundos > 0:
            cierre_local = time.monotonic() + segundos
        else:
            cierre_local = None
    mostrar_reloj()

    # --- Pistas: las de esta ronda y las de rondas anteriores
    lineas = []
    if fase in ["PISTAS", "VOTACION"]:
        lineas.append(f"Ronda {estado.get('ronda')}:  " + ("  ·  ".join(f"{u}: {p}" for u, p in pistas.items()) or "(todavia ninguna)"))
        for vieja in reversed(estado.get("historial", [])):
            lineas.append(f"Ronda {vieja.get('ronda')}:  " + "  ·  ".join(f"{u}: {p}" for u, p in vieja.get("pistas", {}).items()))
    ultima = estado.get("ultima_votacion")
    if ultima and fase in ["PISTAS", "VOTACION"]:
        conteo = ", ".join(f"{u} {n}" for u, n in ultima.get("conteo", {}).items()) or "sin votos"
        expulsado = ultima.get("expulsado")
        lineas.append(f"Votacion de la ronda {ultima.get('ronda')}: {conteo} → "
                      + (f"expulsado {expulsado}" if expulsado else "empate, nadie sale"))
    etiqueta_pistas.config(text="\n".join(lineas) if lineas else "Todavia no hay pistas.")

    # --- Lo que puedo hacer AHORA (sale todo del state)
    es_mi_turno = fase == "PISTAS" and turno == usuario
    sigo_vivo = usuario in vivos
    ya_vote = usuario in estado.get("votaron", [])
    fila_pista.pack_forget()
    marco_votos.pack_forget()
    fila_lobby.pack_forget()
    boton_lst.pack_forget()
    boton_sal.pack_forget()
    for b in marco_votos.winfo_children():
        b.destroy()
    if es_mi_turno:
        etiqueta_accion.config(text="¡Te toca! Escribi tu pista (una palabra o frase corta):")
        fila_pista.pack(fill="x", pady=4)
        estado_campo = "disabled" if esperando else "normal"
        campo_pista.config(state=estado_campo)
        boton_pst.config(state=estado_campo)
        if not esperando:
            campo_pista.focus()
    elif fase == "PISTAS":
        if not sigo_vivo:
            etiqueta_accion.config(text="Fuiste expulsado: miras la partida.")
        else:
            etiqueta_accion.config(text=f"Le toca a {turno}. Espera tu turno.")
    elif fase == "VOTACION":
        if not sigo_vivo:
            etiqueta_accion.config(text="Fuiste expulsado: miras la votacion.")
        elif ya_vote:
            etiqueta_accion.config(text="Ya votaste. Esperando a los demas.")
        else:
            etiqueta_accion.config(text="¿Quien es el espia? Vota (no a vos mismo):")
            marco_votos.pack(fill="x", pady=4)
            for u in vivos:
                if u != usuario:
                    tk.Button(marco_votos, text=u, font=LETRA, state="disabled" if esperando else "normal",
                              command=lambda x=u: boton_votar(x)).pack(side="left", padx=3)
    else:   # LOBBY o FIN
        listos = estado.get("listos", [])
        fila_lobby.pack(anchor="w", pady=4)
        if usuario in listos:
            etiqueta_accion.config(text=f"Estas listo. Listos: {len(listos)} de {len(jugadores)} (minimo 4).")
        else:
            etiqueta_accion.config(text=f"Apreta \"Estoy listo\" para empezar. Listos: {len(listos)} de {len(jugadores)} (minimo 4).")
            boton_lst.config(state="disabled" if saliendo else "normal")
            boton_lst.pack(side="left")
        boton_sal.config(state="disabled" if saliendo else "normal")
        boton_sal.pack(side="left", padx=(8, 0))

    # --- Jugadores (con su situacion)
    marcas = []
    for u in jugadores:
        texto = u + (" (vos)" if u == usuario else "")
        if u in desconectados:
            texto += " · desconectado"
        if fase in ["PISTAS", "VOTACION"] and u not in vivos:
            texto += " · expulsado"
        if fase in ["LOBBY", "FIN"] and u in estado.get("listos", []):
            texto += " · listo"
        marcas.append(texto)
    etiqueta_jugadores.config(text="Jugadores:  " + "   |   ".join(marcas))


def texto_resultado(r, con_ganador=True):
    # Arma el texto del resultado que manda el servicio en el state.
    partes = []
    if con_ganador:
        if "esp" in str(r.get("ganador", "")).lower():
            partes.append("gana el espia.")
        else:
            partes.append("ganan los jugadores.")
    if r.get("motivo"):
        partes.append(str(r.get("motivo")).rstrip(".") + ".")
    partes.append(f"El espia era {r.get('espia')} y la palabra era {r.get('palabra')}"
                  + (f" ({r.get('categoria')})." if r.get("categoria") else "."))
    return " ".join(partes)


def mostrar_reloj():
    if mi_sala is None:
        return
    fase = salas_vistas.get(mi_sala, {}).get("fase")
    if fase not in ["PISTAS", "VOTACION"]:
        etiqueta_reloj.config(text="")
    elif servicio_estado == "offline":
        etiqueta_reloj.config(text="⏱ en pausa (sin servicio)", fg=GRIS)
    elif cierre_local is None:
        etiqueta_reloj.config(text="⏱ sin limite de tiempo", fg=GRIS)
    else:
        quedan = max(0, round(cierre_local - time.monotonic()))
        etiqueta_reloj.config(text=f"⏱ {quedan} s", fg=ROJO if quedan <= 5 else TINTA)


def latido_reloj():
    # Cada medio segundo actualiza la cuenta regresiva (es solo para mostrar: decide el servicio).
    mostrar_reloj()
    ventana.after(500, latido_reloj)


# ----------------------------------------------------------------------
# 4. PANTALLAS
# ----------------------------------------------------------------------
def mostrar_aviso_salas(texto):
    etiqueta_aviso_salas.config(text=texto)


def mostrar_aviso(texto, error=False):
    etiqueta_aviso.config(text=texto, fg=ROJO if error else TINTA)


def mostrar_aviso_registro(texto, error=False):
    etiqueta_aviso_reg.config(text=texto, fg=ROJO if error else TINTA)


def habilitar_botones(si):
    estado = "normal" if si else "disabled"
    boton_ent.config(state=estado)
    boton_ir_registro.config(state=estado)


def habilitar_botones_registro(si):
    estado = "normal" if si else "disabled"
    boton_crear.config(state=estado)
    boton_volver.config(state=estado)


def mostrar_pantalla(marco):
    if marco.winfo_manager():
        return                     # ya se esta mostrando
    for m in (marco_ingreso, marco_registro, marco_salas, marco_sala):
        m.pack_forget()
    # Las dos fichas (ingreso y registro) van centradas y chicas; las otras ocupan todo
    es_ficha = marco is marco_ingreso or marco is marco_registro
    marco.pack(expand=True, fill="none" if es_ficha else "both", padx=30, pady=30)


def mostrar_salas():
    mostrar_aviso("")
    etiqueta_bienvenida.config(text=f"Conectado como {usuario}")
    dibujar_salas()
    mostrar_pantalla(marco_salas)


def mostrar_sala():
    dibujar_juego()
    mostrar_pantalla(marco_sala)


ventana = tk.Tk()
ventana.title("Juego del Espia")
ventana.geometry("980x640")
ventana.minsize(900, 600)
ventana.configure(bg=FONDO)
ventana.protocol("WM_DELETE_WINDOW", cerrar_ventana)     # la X de la ventana

# --- Pantalla de ingreso: una "ficha" de papel sobre fondo oscuro
marco_ingreso = tk.Frame(ventana, bg=PAPEL, padx=30, pady=24)
tk.Label(marco_ingreso, text="JUEGO DEL ESPIA", font=LETRA_TITULO, bg=PAPEL, fg=TINTA).pack()
tk.Label(marco_ingreso, text="ACCESO", font=("Courier", 10, "bold"),
         bg=PAPEL, fg=ROJO).pack(pady=(0, 16))

tk.Label(marco_ingreso, text="Usuario", font=LETRA, bg=PAPEL, fg=TINTA, anchor="w").pack(fill="x")
campo_usuario = tk.Entry(marco_ingreso, font=LETRA, width=28)
campo_usuario.pack(fill="x", pady=(0, 10), ipady=3)
tk.Label(marco_ingreso, text="Clave", font=LETRA, bg=PAPEL, fg=TINTA, anchor="w").pack(fill="x")
campo_clave = tk.Entry(marco_ingreso, font=LETRA, width=28, show="•")     # no se ve la clave
campo_clave.pack(fill="x", pady=(0, 16), ipady=3)

botones = tk.Frame(marco_ingreso, bg=PAPEL)
botones.pack()
boton_ent = tk.Button(botones, text="Entrar", font=LETRA, width=11, bg=TINTA, fg=PAPEL,
                      activebackground=ROJO, activeforeground=PAPEL, command=boton_entrar)
boton_ent.pack(side="left", padx=5)

etiqueta_aviso = tk.Label(marco_ingreso, text="", font=("Helvetica", 10), bg=PAPEL, fg=TINTA,
                          wraplength=300, justify="center")
etiqueta_aviso.pack(pady=(14, 0))
# El registro NO esta en esta pantalla: este "enlace" lleva a la otra
boton_ir_registro = tk.Button(marco_ingreso, text="¿No tenés usuario? Registrate acá",
                              font=("Helvetica", 10, "underline"), bg=PAPEL, fg=ROJO,
                              activebackground=PAPEL, activeforeground=TINTA, relief="flat",
                              borderwidth=0, highlightthickness=0, cursor="hand2",
                              command=ir_a_registro)
boton_ir_registro.pack(pady=(10, 0))
tk.Label(marco_ingreso, text=f"SISTEMAS DISTRIBUIDOS - 2026",
         font=("Helvetica", 8), bg=PAPEL, fg=GRIS).pack(pady=(10, 0))

campo_clave.bind("<Return>", boton_entrar)        # Enter en la clave = Entrar
campo_usuario.focus()

# --- Pantalla de registro: otra ficha, con otro color y otro sello, para que se note la diferencia
marco_registro = tk.Frame(ventana, bg=TARJETA, padx=30, pady=24,
                          highlightbackground=VERDE, highlightcolor=VERDE, highlightthickness=3)
tk.Label(marco_registro, text="JUEGO DEL ESPIA", font=LETRA_TITULO, bg=TARJETA, fg=TINTA).pack()
tk.Label(marco_registro, text="ALTA DE AGENTE NUEVO", font=("Courier", 10, "bold"),
         bg=TARJETA, fg=VERDE).pack(pady=(0, 6))
tk.Label(marco_registro, text=f"Elegí un usuario (mínimo {MIN_USUARIO} letras) y una clave "
                              f"(mínimo {MIN_CLAVE} caracteres).",
         font=("Helvetica", 9), bg=TARJETA, fg=GRIS, wraplength=300).pack(pady=(0, 12))

tk.Label(marco_registro, text="Usuario nuevo", font=LETRA, bg=TARJETA, fg=TINTA, anchor="w").pack(fill="x")
reg_usuario = tk.Entry(marco_registro, font=LETRA, width=28)
reg_usuario.pack(fill="x", pady=(0, 10), ipady=3)
tk.Label(marco_registro, text="Clave", font=LETRA, bg=TARJETA, fg=TINTA, anchor="w").pack(fill="x")
reg_clave = tk.Entry(marco_registro, font=LETRA, width=28, show="•")
reg_clave.pack(fill="x", pady=(0, 10), ipady=3)
tk.Label(marco_registro, text="Repetí la clave", font=LETRA, bg=TARJETA, fg=TINTA, anchor="w").pack(fill="x")
reg_clave2 = tk.Entry(marco_registro, font=LETRA, width=28, show="•")
reg_clave2.pack(fill="x", pady=(0, 16), ipady=3)

botones_reg = tk.Frame(marco_registro, bg=TARJETA)
botones_reg.pack()
boton_crear = tk.Button(botones_reg, text="Crear usuario", font=LETRA, width=12, bg=VERDE, fg=PAPEL,
                        activebackground=TINTA, activeforeground=PAPEL, command=boton_crear_usuario)
boton_crear.pack(side="left", padx=5)
boton_volver = tk.Button(botones_reg, text="Volver", font=LETRA, width=8, command=ir_a_ingreso)
boton_volver.pack(side="left", padx=5)

etiqueta_aviso_reg = tk.Label(marco_registro, text="", font=("Helvetica", 10), bg=TARJETA, fg=TINTA,
                              wraplength=300, justify="center")
etiqueta_aviso_reg.pack(pady=(14, 0))

reg_clave2.bind("<Return>", boton_crear_usuario)   # Enter en la segunda clave = Crear usuario

# --- Pantalla de salas
marco_salas = tk.Frame(ventana, bg=PAPEL, padx=24, pady=18)
cabecera = tk.Frame(marco_salas, bg=PAPEL)
cabecera.pack(fill="x")
etiqueta_bienvenida = tk.Label(cabecera, text="", font=("Courier", 14, "bold"), bg=PAPEL, fg=TINTA)
etiqueta_bienvenida.pack(side="left")
etiqueta_servicio = tk.Label(cabecera, text="", font=("Helvetica", 10, "bold"), bg=PAPEL, fg=GRIS)
etiqueta_servicio.pack(side="right")
etiqueta_conexion = tk.Label(marco_salas, text="", font=("Helvetica", 10), bg=PAPEL, fg=ROJO)
etiqueta_conexion.pack(fill="x")
tk.Label(marco_salas, text="SALAS DISPONIBLES", font=("Courier", 10, "bold"), bg=PAPEL,
         fg=ROJO, anchor="w").pack(fill="x", pady=(10, 4))
marco_filas = tk.Frame(marco_salas, bg=PAPEL)
marco_filas.pack(fill="x")
etiqueta_aviso_salas = tk.Label(marco_salas, text="", font=("Helvetica", 10), bg=PAPEL, fg=TINTA,
                                wraplength=460, justify="left", anchor="w")
etiqueta_aviso_salas.pack(fill="x", pady=(14, 0))

# --- Pantalla de juego: dos columnas (juego a la izquierda, avisos a la derecha)
marco_sala = tk.Frame(ventana, bg=FONDO)
izquierda = tk.Frame(marco_sala, bg=PAPEL, padx=20, pady=14)
izquierda.pack(side="left", fill="both", expand=True)
derecha = tk.Frame(marco_sala, bg=PAPEL, padx=12, pady=14, width=300)
derecha.pack(side="right", fill="y", padx=(12, 0))
derecha.pack_propagate(False)                    # que respete el ancho de 300

cabecera_sala = tk.Frame(izquierda, bg=PAPEL)
cabecera_sala.pack(fill="x")
etiqueta_titulo_sala = tk.Label(cabecera_sala, text="", font=("Courier", 16, "bold"), bg=PAPEL, fg=TINTA)
etiqueta_titulo_sala.pack(side="left")
etiqueta_conexion_juego = tk.Label(cabecera_sala, text="", font=("Helvetica", 10, "bold"), bg=PAPEL, fg=ROJO)
etiqueta_conexion_juego.pack(side="right")
etiqueta_partida = tk.Label(izquierda, text="", font=("Helvetica", 10), bg=PAPEL, fg=GRIS, anchor="w")
etiqueta_partida.pack(fill="x")

# Tarjeta del secreto: siempre visible
tarjeta = tk.Frame(izquierda, bg=TARJETA, highlightbackground=TINTA, highlightthickness=2, padx=16, pady=10)
tarjeta.pack(fill="x", pady=10)
sello = tk.Label(tarjeta, text="", font=("Courier", 11, "bold"), bg=TARJETA, fg=ROJO, anchor="e")
sello.pack(fill="x")
texto_secreto = tk.Label(tarjeta, text="", font=("Courier", 26, "bold"), bg=TARJETA, fg=TINTA)
texto_secreto.pack()
detalle_secreto = tk.Label(tarjeta, text="", font=LETRA, bg=TARJETA, fg=TINTA, wraplength=560, justify="center")
detalle_secreto.pack()

etiqueta_turnos = tk.Label(izquierda, text="", font=("Helvetica", 11, "bold"), bg=PAPEL, fg=TINTA, anchor="w")
etiqueta_turnos.pack(fill="x")
etiqueta_reloj = tk.Label(izquierda, text="", font=("Courier", 14, "bold"), bg=PAPEL, fg=TINTA, anchor="w")
etiqueta_reloj.pack(fill="x")
tk.Label(izquierda, text="PISTAS", font=("Courier", 10, "bold"), bg=PAPEL, fg=ROJO, anchor="w").pack(fill="x", pady=(8, 0))
etiqueta_pistas = tk.Label(izquierda, text="", font=LETRA, bg=PAPEL, fg=TINTA, anchor="w", justify="left", wraplength=580)
etiqueta_pistas.pack(fill="x")

tk.Label(izquierda, text="TU JUGADA", font=("Courier", 10, "bold"), bg=PAPEL, fg=ROJO, anchor="w").pack(fill="x", pady=(10, 0))
etiqueta_accion = tk.Label(izquierda, text="", font=LETRA, bg=PAPEL, fg=TINTA, anchor="w", justify="left")
etiqueta_accion.pack(fill="x")
marco_accion = tk.Frame(izquierda, bg=PAPEL)
marco_accion.pack(fill="x")
fila_pista = tk.Frame(marco_accion, bg=PAPEL)                 # se muestra solo en mi turno
campo_pista = tk.Entry(fila_pista, font=LETRA, width=32)
campo_pista.pack(side="left", ipady=3)
campo_pista.bind("<Return>", boton_pista)
boton_pst = tk.Button(fila_pista, text="Dar pista", font=LETRA, bg=TINTA, fg=PAPEL, command=boton_pista)
boton_pst.pack(side="left", padx=6)
etiqueta_error_pista = tk.Label(fila_pista, text="", font=("Helvetica", 10), bg=PAPEL, fg=ROJO,
                                anchor="w", justify="left", wraplength=260)
etiqueta_error_pista.pack(side="left", padx=6)
marco_votos = tk.Frame(marco_accion, bg=PAPEL)               # un boton por candidato
fila_lobby = tk.Frame(marco_accion, bg=PAPEL)                 # botones del lobby
boton_lst = tk.Button(fila_lobby, text="Estoy listo", font=LETRA, bg=TINTA, fg=PAPEL, command=boton_listo)
boton_sal = tk.Button(fila_lobby, text="Salir de la sala", font=LETRA, command=boton_salir)

etiqueta_jugadores = tk.Label(izquierda, text="", font=("Helvetica", 10), bg=PAPEL, fg=TINTA,
                              anchor="w", justify="left", wraplength=580)
etiqueta_jugadores.pack(fill="x", side="bottom")

tk.Label(derecha, text="AVISOS", font=("Courier", 11, "bold"), bg=PAPEL, fg=ROJO, anchor="w").pack(fill="x")
caja_avisos = tk.Text(derecha, font=("Helvetica", 10), bg=TARJETA, fg=TINTA, wrap="word",
                      relief="flat", state="disabled", spacing1=2, spacing3=4)
caja_avisos.pack(fill="both", expand=True, pady=(6, 0))
caja_avisos.tag_config("nuevo", background=RESALTADO, font=("Helvetica", 10, "bold"))
caja_avisos.tag_config("privado", foreground=ROJO)

mostrar_pantalla(marco_ingreso)
ventana.after(100, revisar_cola)
ventana.after(500, latido_reloj)
ventana.mainloop()