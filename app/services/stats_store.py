import os
import json

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STATS_FILE = os.path.join(BASE_DIR, 'data', 'clean', 'sisco_stats.json')

def load_stats():
    default = {
        "total_reports":   0,
        "critical_alerts": 0,
        "last_alert":      "No critical alerts yet",
        "top_crime_types": {},
        "top_departments": {},
        "location_map":    {},
        "timeline":        {}
    }
    if os.path.exists(STATS_FILE):
        try:
            with open(STATS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            for k, v in default.items():
                if k not in data:
                    data[k] = v
            return data
        except Exception:
            pass
    return default

def save_stats():
    try:
        os.makedirs(os.path.dirname(STATS_FILE), exist_ok=True)
        with open(STATS_FILE, 'w', encoding='utf-8') as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Error saving stats: {e}")

stats        = load_stats()
recent_events = []
