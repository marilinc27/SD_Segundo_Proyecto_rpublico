# Imagen del servicio central del Juego del Espia
FROM python:3.12-slim

WORKDIR /app

# Las librerias que necesita (versiones fijas, igual que con EMQX):
#   paho-mqtt: cliente MQTT (habla con el broker)
#   psycopg:   cliente PostgreSQL (habla con la base de datos del juego)
RUN pip install --no-cache-dir paho-mqtt==2.1.0 "psycopg[binary]==3.2.3"

COPY servicio_jugador.py .

# -u: sin buffer, para que los print aparezcan enseguida en "docker compose logs"
CMD ["python", "-u", "servicio_jugador.py"]