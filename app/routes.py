import time
import random
from datetime import datetime
from flask import Blueprint, render_template, jsonify, request
import sys
import os

# Rutas para el sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import KAFKA_TOPIC_IN
from app.stats_store import stats, eventos_recientes, guardar_stats
from app.kafka_services import get_producer

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def index():
    return render_template('index.html')

@main_bp.route('/api/data')
def api_data():
    # Top-5 para barras
    top_tipos  = sorted(stats["top_tipo_hecho"].items(),   key=lambda x: x[1], reverse=True)[:5]
    top_deptos = sorted(stats["top_departamento"].items(), key=lambda x: x[1], reverse=True)[:5]

    # Top 300 distritos para no saturar Leaflet en el frontend
    todos_loc = sorted(stats["mapa_ubicaciones"].items(), key=lambda x: x[1], reverse=True)[:300]

    return jsonify({
        "stats": {
            "total_denuncias":  stats["total_denuncias"],
            "alertas_criticas": stats["alertas_criticas"],
            "ultima_alerta":    stats["ultima_alerta"],
            "top_tipos":        [{"tipo": k, "cantidad": v} for k, v in top_tipos],
            "top_departamentos": [{"departamento": k, "cantidad": v} for k, v in top_deptos],
            "mapa_distritos":   [{"loc": k, "cantidad": v} for k, v in todos_loc],
            "timeline":         stats.get("timeline", {})
        },
        "events": eventos_recientes
    })

@main_bp.route('/api/reset', methods=['POST'])
def api_reset():
    """Endpoint para reiniciar el dashboard al estado histórico base (2018-2025)."""
    import subprocess
    from app.stats_store import cargar_stats
    
    # 1. Ejecutar el script que precalcula los 6.8 Millones de registros
    script_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts", "init_historico.py")
    print("Recalculando histórico para la presentación...")
    subprocess.run([sys.executable, script_path], check=True)
    
    # 2. Recargar a memoria
    nuevos_stats = cargar_stats()
    
    # 3. Actualizar el diccionario global en memoria sin romper la referencia
    stats.clear()
    stats.update(nuevos_stats)
    eventos_recientes.clear()
    
    return jsonify({"status": "ok", "message": "Dashboard reiniciado al estado histórico (6.8M denuncias)"})

@main_bp.route('/api/dispatch', methods=['POST'])
def api_dispatch():
    data = request.json
    event_id = data.get("id")
    for ev in eventos_recientes:
        if ev.get("id") == event_id:
            ev["estado_respuesta"] = "PATRULLA ENVIADA"
            break
    return jsonify({"status": "success", "message": f"Patrulla enviada para evento {event_id}"})

@main_bp.route('/denunciar')
def denuncia_page():
    """Renderiza la página del portal del ciudadano para denuncias manuales."""
    import json
    ubigeo_path = os.path.join(os.path.dirname(__file__), 'ubigeo.json')
    try:
        with open(ubigeo_path, 'r', encoding='utf-8') as f:
            ubigeo = json.load(f)
    except:
        ubigeo = {}
    return render_template('denuncia.html', ubigeo=ubigeo)

@main_bp.route('/api/denunciar', methods=['POST'])
def api_denunciar():
    """Endpoint para recibir denuncias manuales desde el UI."""
    prod = get_producer()
    if not prod:
        return jsonify({"status": "error", "message": "Productor Kafka no conectado."}), 500
        
    data = request.json
    ahora = datetime.now()
    
    evento = {
        "id": f"MANUAL-{int(time.time()*1000)}-{random.randint(1000,9999)}",
        "anio": str(ahora.year),
        "mes": f"{ahora.month:02d}",
        "departamento": data.get("departamento", "LIMA").upper(),
        "provincia": data.get("provincia", "DESCONOCIDO").upper(),
        "distrito": data.get("distrito", "DESCONOCIDO").upper(),
        "tipo_hecho": data.get("tipo_hecho", "OTRO").upper(),
        "modalidad": data.get("tipo_hecho", "OTRO").upper(),
        "cantidad": 1,
        "timestamp_emision": ahora.isoformat(),
        "estado_respuesta": "PENDIENTE",
    }
    
    try:
        prod.send(KAFKA_TOPIC_IN, value=evento)
        prod.flush()
        return jsonify({"status": "success", "message": "Denuncia enviada exitosamente a Kafka"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
