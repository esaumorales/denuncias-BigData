import os
import json
import threading
from flask import Flask, render_template, jsonify, request
from kafka import KafkaConsumer
import time

# ── Archivo de persistencia ──────────────────────────────────────
STATS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data_clean', 'sisco_stats.json')

# Tipos de hecho que se consideran alertas críticas
TIPOS_CRITICOS = {
    "homicidio", "feminicidio", "robo agravado",
    "secuestro", "trata de personas", "terrorismo", "violacion sexual"
}

# Forzar ruta absoluta para los templates
template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'templates'))
app = Flask(__name__, template_folder=template_dir)

# ── Cargar stats persistentes o iniciar en 0 ─────────────────────
def cargar_stats():
    """Carga estadísticas guardadas del disco, o retorna un dict limpio."""
    if os.path.exists(STATS_FILE):
        try:
            with open(STATS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f"Stats restauradas desde {STATS_FILE}")
            print(f"  → Total acumulado: {data.get('total_denuncias', 0):,}")
            return data
        except Exception as e:
            print(f"Error leyendo stats: {e} — iniciando en 0")
    return {
        "total_denuncias":   0,
        "alertas_criticas":  0,
        "ultima_alerta":     "Sin alertas criticas recientes",
        "top_tipo_hecho":    {},
        "top_departamento":  {}
    }

def guardar_stats():
    """Guarda las estadísticas actuales a disco."""
    try:
        os.makedirs(os.path.dirname(STATS_FILE), exist_ok=True)
        with open(STATS_FILE, 'w', encoding='utf-8') as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Error guardando stats: {e}")

# Memoria en tiempo real
stats = cargar_stats()
eventos_recientes = []
_counter_since_save = 0   # guardar cada N mensajes


def kafka_listener():
    global _counter_since_save
    while True:
        time.sleep(2)
        try:
            consumer = KafkaConsumer(
                'denuncias_sidpol',
                bootstrap_servers=['localhost:9092'],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id=None,
                value_deserializer=lambda x: x.decode('utf-8'),
                consumer_timeout_ms=5000,
            )
            print("Dashboard SISCO conectado a Kafka — escuchando denuncias_sidpol...")

            for message in consumer:
                try:
                    data = json.loads(message.value)

                    tipo_hecho   = data.get('tipo_hecho',   'Desconocido')
                    departamento = data.get('departamento', 'Desconocido')
                    cantidad     = int(data.get('cantidad',  1))

                    # Ignorar registros vacíos
                    if not tipo_hecho or not departamento or tipo_hecho == 'Desconocido':
                        continue

                    stats["total_denuncias"] += cantidad

                    stats["top_tipo_hecho"][tipo_hecho] = \
                        stats["top_tipo_hecho"].get(tipo_hecho, 0) + cantidad

                    stats["top_departamento"][departamento] = \
                        stats["top_departamento"].get(departamento, 0) + cantidad

                    is_critical = any(t in tipo_hecho.lower() for t in TIPOS_CRITICOS)
                    if is_critical:
                        stats["alertas_criticas"] += 1
                        stats["ultima_alerta"] = (
                            f"{tipo_hecho} en {data.get('distrito','?')}, {departamento}"
                        )

                    data['is_critical'] = is_critical

                    eventos_recientes.insert(0, data)
                    if len(eventos_recientes) > 50:
                        eventos_recientes.pop()

                    # Guardar a disco cada 50 mensajes
                    _counter_since_save += 1
                    if _counter_since_save >= 50:
                        guardar_stats()
                        _counter_since_save = 0

                except Exception as e:
                    print(f"Error procesando mensaje: {e}")

            consumer.close()
            # Guardar al desconectarse
            guardar_stats()
            print("Consumer cerrado, reconectando en 3 segundos...")
            time.sleep(3)

        except Exception as e:
            print(f"Error en listener Kafka: {e} — reintentando en 5 segundos...")
            time.sleep(5)


@app.after_request
def add_no_cache(response):
    """Evitar que el navegador cachee HTML/JS viejos."""
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def api_data():
    # Top-5 para barras
    top_tipos  = sorted(stats["top_tipo_hecho"].items(),   key=lambda x: x[1], reverse=True)[:5]
    top_deptos = sorted(stats["top_departamento"].items(), key=lambda x: x[1], reverse=True)[:5]

    # TODOS los departamentos para el mapa de calor
    todos_deptos = sorted(stats["top_departamento"].items(), key=lambda x: x[1], reverse=True)

    return jsonify({
        "stats": {
            "total_denuncias":  stats["total_denuncias"],
            "alertas_criticas": stats["alertas_criticas"],
            "ultima_alerta":    stats["ultima_alerta"],
            "top_tipos":        [{"tipo": k, "cantidad": v} for k, v in top_tipos],
            "top_departamentos": [{"departamento": k, "cantidad": v} for k, v in top_deptos],
            "mapa_departamentos": [{"departamento": k, "cantidad": v} for k, v in todos_deptos],
        },
        "events": eventos_recientes
    })

@app.route('/api/reset', methods=['POST'])
def api_reset():
    """Endpoint para reiniciar todo a 0 sin detener el servidor."""
    stats["total_denuncias"]  = 0
    stats["alertas_criticas"] = 0
    stats["ultima_alerta"]    = "Sin alertas criticas recientes"
    stats["top_tipo_hecho"].clear()
    stats["top_departamento"].clear()
    eventos_recientes.clear()
    guardar_stats()
    return jsonify({"status": "ok", "message": "Stats reiniciadas a 0"})

@app.route('/api/dispatch', methods=['POST'])
def api_dispatch():
    data = request.json
    event_id = data.get("id")
    for ev in eventos_recientes:
        if ev.get("id") == event_id:
            ev["estado_respuesta"] = "PATRULLA ENVIADA"
            break
    return jsonify({"status": "success", "message": f"Patrulla enviada para evento {event_id}"})


if __name__ == '__main__':
    thread = threading.Thread(target=kafka_listener, daemon=True)
    thread.start()
    print("Iniciando Dashboard SISCO en http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=False)
