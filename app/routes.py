import time
import random
from datetime import datetime
from flask import Blueprint, render_template, jsonify, request
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import KAFKA_TOPIC_IN
from app.services.stats_store import stats, recent_events, save_stats
from app.services.kafka_services import get_producer

main_bp = Blueprint('main', __name__)


@main_bp.route('/')
def index():
    return render_template('index.html')


@main_bp.route('/api/data')
def api_data():
    year_filter = request.args.get('year', '').strip()
    dept_filter = request.args.get('department', '').strip().upper()

    # Timeline filtered by year
    timeline = stats.get("timeline", {})
    if year_filter:
        timeline = {k: v for k, v in timeline.items() if k.startswith(year_filter)}

    # Location map filtered by department
    location_map = stats.get("location_map", {})
    if dept_filter:
        location_map = {k: v for k, v in location_map.items() if k.upper().startswith(dept_filter)}
    top_locations = sorted(location_map.items(), key=lambda x: x[1], reverse=True)[:300]

    # Top departments filtered
    top_depts_src = stats["top_departments"]
    if dept_filter:
        top_depts_src = {k: v for k, v in top_depts_src.items() if dept_filter in k.upper()}
    top_departments = sorted(top_depts_src.items(), key=lambda x: x[1], reverse=True)[:5]

    # Top crime types — global (no year/dept breakdown available)
    top_crime_types = sorted(stats["top_crime_types"].items(), key=lambda x: x[1], reverse=True)[:5]

    # Total filtered by department
    if dept_filter:
        total = sum(v for k, v in stats.get("location_map", {}).items() if k.upper().startswith(dept_filter))
    else:
        total = stats["total_reports"]

    # Events filtered
    events = list(recent_events)
    if year_filter:
        events = [e for e in events if str(e.get('year', '')) == year_filter]
    if dept_filter:
        events = [e for e in events if e.get('department', '').upper() == dept_filter]

    return jsonify({
        "stats": {
            "total_reports":    total,
            "critical_alerts":  stats["critical_alerts"],
            "last_alert":       stats["last_alert"],
            "top_crime_types":  [{"crime_type": k, "count": v} for k, v in top_crime_types],
            "top_departments":  [{"department": k, "count": v} for k, v in top_departments],
            "location_map":     [{"loc": k, "count": v} for k, v in top_locations],
            "timeline":         timeline
        },
        "events": events
    })


@main_bp.route('/api/reset', methods=['POST'])
def api_reset():
    import subprocess
    from app.services.stats_store import load_stats

    script_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts", "init_historico.py")
    print("Recalculating historical baseline...")
    subprocess.run([sys.executable, script_path], check=True)

    new_stats = load_stats()
    stats.clear()
    stats.update(new_stats)
    recent_events.clear()

    return jsonify({"status": "ok", "message": "Dashboard reset to historical baseline"})


@main_bp.route('/api/dispatch', methods=['POST'])
def api_dispatch():
    data     = request.json
    event_id = data.get("id")
    for ev in recent_events:
        if ev.get("id") == event_id:
            ev["response_status"] = "UNIT DISPATCHED"
            break
    return jsonify({"status": "success", "message": f"Unit dispatched for event {event_id}"})


@main_bp.route('/denunciar')
def denuncia_page():
    import json
    ubigeo_path = os.path.join(os.path.dirname(__file__), 'data', 'ubigeo.json')
    try:
        with open(ubigeo_path, 'r', encoding='utf-8') as f:
            ubigeo = json.load(f)
    except Exception:
        ubigeo = {}
    return render_template('denuncia.html', ubigeo=ubigeo)


@main_bp.route('/api/denunciar', methods=['POST'])
def api_denunciar():
    prod = get_producer()
    if not prod:
        return jsonify({"status": "error", "message": "Kafka producer not connected."}), 500

    data = request.json
    now  = datetime.now()

    event = {
        "id":              f"MANUAL-{int(time.time()*1000)}-{random.randint(1000,9999)}",
        "year":            str(now.year),
        "month":           f"{now.month:02d}",
        "department":      data.get("department", data.get("departamento", "LIMA")).upper(),
        "province":        data.get("province",   data.get("provincia",   "UNKNOWN")).upper(),
        "district":        data.get("district",   data.get("distrito",    "UNKNOWN")).upper(),
        "crime_type":      data.get("crime_type", data.get("tipo_hecho",  "OTHER")).upper(),
        "count":           1,
        "timestamp":       now.isoformat(),
        "response_status": "PENDING",
    }

    try:
        prod.send(KAFKA_TOPIC_IN, value=event)
        prod.flush()
        return jsonify({"status": "success", "message": "Report submitted to Kafka"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
