# receptor.py -- el receptor HTTP de los webhooks del broker EMQX
#
# Lo llama el broker (nadie mas). Recibe un JSON por POST /webhook, lo muestra
# en pantalla (docker compose logs receptor), lo guarda en un archivo y lo
# muestra en una pagina (GET /eventos).
#
# Variables de entorno:
#   WEBHOOK_TOKEN  secreto compartido con el broker (obligatorio)
#   LOG_FILE       donde guardar los avisos (por defecto /data/eventos.log)
#   PORT           puerto (por defecto 5000)
import hmac
import html
import json
import os
import sys
import time

from flask import Flask, request, send_file

TOKEN = os.environ.get("WEBHOOK_TOKEN", "")
ARCHIVO_LOG = os.environ.get("LOG_FILE", "/data/eventos.log")
PUERTO = int(os.environ.get("PORT", "5000"))

if not TOKEN:
    sys.exit("Falta la variable de entorno WEBHOOK_TOKEN (el secreto compartido con el broker).")

os.makedirs(os.path.dirname(ARCHIVO_LOG) or ".", exist_ok=True)
app = Flask(__name__)


def hora():
    return time.strftime("%Y-%m-%d %H:%M:%S")


@app.post("/webhook")
def webhook():
    esperado = f"Bearer {TOKEN}"
    recibido = request.headers.get("Authorization", "")
    if not hmac.compare_digest(recibido.encode(), esperado.encode()):
        print(f"{hora()}  RECHAZADO (token incorrecto) desde {request.remote_addr}", flush=True)
        return {"ok": False, "error": "no autorizado"}, 401

    datos = request.get_json(force=True, silent=True)
    if datos is None:
        return {"ok": False, "error": "el cuerpo no es JSON"}, 400

    linea = json.dumps({"hora": hora(), "desde": request.remote_addr, "datos": datos},
                       ensure_ascii=False)
    print(linea, flush=True)
    with open(ARCHIVO_LOG, "a", encoding="utf-8") as f:
        f.write(linea + "\n")
    return {"ok": True}, 200


@app.get("/eventos")
def eventos():
    # Leer el parámetro de página desde la URL (por defecto la página 1)
    try:
        pagina = int(request.args.get("p", 1))
    except ValueError:
        pagina = 1

    por_pagina = 25  # Cantidad de eventos por página

    try:
        with open(ARCHIVO_LOG, encoding="utf-8") as f:
            todas_las_lineas = f.readlines()
    except FileNotFoundError:
        todas_las_lineas = []

    total_eventos = len(todas_las_lineas)

    # Invertir para mostrar primero los más nuevos
    lineas_invertidas = list(reversed(todas_las_lineas))

    # Calcular índices para la paginación
    total_paginas = max(1, (total_eventos + por_pagina - 1) // por_pagina)
    pagina = max(1, min(pagina, total_paginas))

    inicio = (pagina - 1) * por_pagina
    fin = inicio + por_pagina
    lineas_pagina = lineas_invertidas[inicio:fin]

    filas = []
    for linea in lineas_pagina:
        try:
            evento = json.loads(linea)
            hora_str = html.escape(evento.get("hora", ""))
            datos = evento.get("datos", {})


            es_fin_partida = False
            accion_cruda = "aviso del sistema"
            color = "#333" # Color por defecto

            # 1. Chequeamos primero si viene de nuestra regla de bloqueo ACL
            if "tipo_evento" in datos and datos.get("tipo_evento") == "bloqueo_acl":
                accion_cruda = f"Denegación ACL ({datos.get('action', 'acción')})"
                color = "#8e44ad" # Morado para seguridad/bloqueos

            elif "aca" in datos and datos.get("aca") == "esta regla disparo":
                accion_cruda = f"Denegación ACL ({datos.get('action', 'acción')})"
                color = "#8e44ad" # Morado para seguridad/bloqueos

            # 2. Si no es un bloqueo ACL, evaluamos el resto de eventos normales
            elif "event" in datos:
                accion_cruda = datos.get("event")
                if "connected" in accion_cruda:
                    color = "#27ae60" # Verde
                elif "disconnected" in accion_cruda:
                    color = "#c0392b" # Rojo

            elif "action" in datos:
                accion_cruda = datos.get("action")
                color = "#2980b9" # Azul

            elif "topic" in datos:
                topic = datos.get("topic")
                payload = datos.get("payload", "")
                fase_val = datos.get("fase", "")

                if "events" in topic and isinstance(payload, str) and "Gana" in payload:
                    accion_cruda = "fin de partida"
                    color = "#d35400" # Naranja
                elif "state" in topic and fase_val == "FIN":
                    accion_cruda = "fin de una partida"
                    es_fin_partida = True
                    color = "#d35400" # Naranja
                elif "state" in topic:
                    accion_cruda = f"estado: {fase_val}" if fase_val else "estado de sala"
                    color = "#8e44ad"
                else:
                    accion_cruda = f"tópico: {topic.split('/')[-1]}"
                    color = "#8e44ad"

            elif "mensaje" in datos:
                texto_msj = str(datos.get("mensaje", ""))
                if "Gana" in texto_msj or "ganan" in texto_msj.lower():
                    accion_cruda = "fin de partida"
                    color = "#d35400" # Naranja
                else:
                    accion_cruda = "aviso de sala"
                    color = "#2980b9"

            elif "evento" in datos:
                accion_cruda = datos.get("evento")
            else:
                claves = list(datos.keys())
                accion_cruda = f"datos ({', '.join(claves[:2])})" if claves else "aviso del sistema"

            accion = html.escape(str(accion_cruda))

            cliente_crudo = datos.get("clientid", datos.get("origen", ""))
            usuario_crudo = datos.get("username", "")

            if not cliente_crudo:
                if "mensaje" in datos or "sala" in datos:
                    cliente_crudo = "servicio"
                elif "topic" in datos:
                    cliente_crudo = "servicio (interno)"
                else:
                    cliente_crudo = "-"

            if not usuario_crudo:
                if cliente_crudo == "servicio" or "servicio" in cliente_crudo:
                    usuario_crudo = "servicio"
                else:
                    usuario_crudo = "-"

            cliente = html.escape(str(cliente_crudo))
            usuario = html.escape(str(usuario_crudo))

            detalles_json = json.dumps(datos, indent=2, ensure_ascii=False)
            detalles = html.escape(detalles_json)

            filas.append(f'''
                <tr>
                    <td class="hora">{hora_str}</td>
                    <td><span class="etiqueta" style="background-color: {color};">{accion}</span></td>
                    <td><strong>{cliente}</strong> <br><small>({usuario})</small></td>
                    <td>
                        <details>
                            <summary>Ver JSON</summary>
                            <pre>{detalles}</pre>
                        </details>
                    </td>
                </tr>
            ''')
        except json.JSONDecodeError:
            continue

    cuerpo_tabla = "\n".join(filas) if filas else "<tr><td colspan='4'>Todavía no llegó ningún aviso.</td></tr>"

    # Construir controles de paginación
    paginacion_html = ""
    if total_paginas > 1:
        prev_disabled = "disabled" if pagina <= 1 else ""
        next_disabled = "disabled" if pagina >= total_paginas else ""

        primera_url = "/eventos?p=1"
        prev_url = f"/eventos?p={pagina - 1}" if pagina > 1 else "#"
        next_url = f"/eventos?p={pagina + 1}" if pagina < total_paginas else "#"

        paginacion_html = f'''
        <div class="paginacion">
            <div>
                <a href="{primera_url}" class="btn-pagina {prev_disabled}" style="margin-right: 5px;">⏮ Primera</a>
                <a href="{prev_url}" class="btn-pagina {prev_disabled}">« Anterior</a>
            </div>
            <span class="info-pagina">Página <strong>{pagina}</strong> de <strong>{total_paginas}</strong> (Total: {total_eventos} eventos)</span>
            <a href="{next_url}" class="btn-pagina {next_disabled}">Siguiente »</a>
        </div>
        '''

    html_out = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="utf-8">
        <title>Monitor de Eventos - EMQX</title>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f6f8; color: #333; margin: 0; padding: 20px; }}
            .contenedor {{ max-width: 1000px; margin: 0 auto; background: white; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); padding: 20px; }}
            .header-flex {{ display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid #ecf0f1; padding-bottom: 10px; margin-top: 0; }}
            h2 {{ color: #2c3e50; margin: 0; border: none; padding: 0; }}
            .controles-header {{ display: flex; gap: 10px; }}
            .btn-control {{ background-color: #2980b9; color: white; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer; font-weight: bold; font-size: 0.9em; transition: background 0.2s; text-decoration: none; display: inline-block; }}
            .btn-control:hover {{ background-color: #1f618d; }}
            .btn-control.btn-descargar {{ background-color: #27ae60; }}
            .btn-control.btn-descargar:hover {{ background-color: #1e8449; }}
            .btn-control.pausado {{ background-color: #c0392b; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
            th, td {{ padding: 12px 15px; text-align: left; border-bottom: 1px solid #ecf0f1; }}
            th {{ background-color: #f8f9fa; color: #7f8c8d; text-transform: uppercase; font-size: 0.85em; }}
            tr:hover {{ background-color: #f9fbfd; }}
            .hora {{ color: #7f8c8d; font-size: 0.9em; }}
            .etiqueta {{ color: white; padding: 4px 8px; border-radius: 4px; font-size: 0.85em; font-weight: bold; text-transform: uppercase; }}
            details summary {{ cursor: pointer; color: #2980b9; font-weight: 500; font-size: 0.9em; outline: none; }}
            pre {{ background: #2c3e50; color: #ecf0f1; padding: 10px; border-radius: 4px; font-size: 0.85em; overflow-x: auto; margin-top: 8px; }}
            .paginacion {{ display: flex; justify-content: space-between; align-items: center; margin-top: 20px; padding-top: 15px; border-top: 1px solid #ecf0f1; }}
            .btn-pagina {{ background-color: #34495e; color: white; padding: 8px 14px; border-radius: 4px; text-decoration: none; font-size: 0.9em; font-weight: 500; transition: background 0.2s; display: inline-block; }}
            .btn-pagina:hover {{ background-color: #2c3e50; }}
            .btn-pagina.disabled {{ background-color: #bdc3c7; pointer-events: none; }}
            .info-pagina {{ color: #7f8c8d; font-size: 0.9em; }}
        </style>
        <script>
            let pausado = false;
            let timer = null;

            function togglePausa() {{
                pausado = !pausado;
                const btn = document.getElementById('btnPausa');
                if (pausado) {{
                    btn.textContent = "▶ Reanudar Actualización";
                    btn.className = "btn-control pausado";
                    if (timer) clearTimeout(timer);
                }} else {{
                    btn.textContent = "⏸ Pausar Actualización";
                    btn.className = "btn-control";
                    iniciarTimer();
                }}
            }}

            function iniciarTimer() {{
                if (timer) clearTimeout(timer);
                timer = setTimeout(() => {{
                    if (!pausado) {{
                        sessionStorage.setItem('scrollpos', window.scrollY);
                        window.location.reload();
                    }}
                }}, 3000);
            }}

            window.onload = function() {{
                const scrollpos = sessionStorage.getItem('scrollpos');
                if (scrollpos) {{
                    window.scrollTo(0, parseInt(scrollpos));
                    sessionStorage.removeItem('scrollpos');
                }}
                iniciarTimer();
            }};
        </script>
    </head>
    <body>
        <div class="contenedor">
            <div class="header-flex">
                <h2>📡 Monitor de Eventos del Broker</h2>
                <div class="controles-header">
                    <a href="/descargar" class="btn-control btn-descargar">Descargar Log</a>
                    <button id="btnPausa" class="btn-control" onclick="togglePausa()">⏸ Pausar Actualización</button>
                </div>
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Hora (Local)</th>
                        <th>Acción / Evento</th>
                        <th>Cliente / Usuario</th>
                        <th>Carga útil (Payload)</th>
                    </tr>
                </thead>
                <tbody>
                    {cuerpo_tabla}
                </tbody>
            </table>
            {paginacion_html}
        </div>
    </body>
    </html>
    """
    return html_out

@app.get("/descargar")
def descargar():
    try:
        return send_file(ARCHIVO_LOG, as_attachment=True, download_name="eventos_historico.log")
    except FileNotFoundError:
        return "El archivo de log aún no existe.", 404

@app.get("/salud")
def salud():
    return {"ok": True}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PUERTO)
