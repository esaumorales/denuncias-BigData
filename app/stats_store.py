import os
import json

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATS_FILE = os.path.join(BASE_DIR, 'data', 'clean', 'sisco_stats.json')

def cargar_stats():
    """Carga estadísticas guardadas del disco, o retorna un dict limpio."""
    default_stats = {
        "total_denuncias":   0,
        "alertas_criticas":  0,
        "ultima_alerta":     "Sin alertas criticas recientes",
        "top_tipo_hecho":    {},
        "top_departamento":  {},
        "mapa_ubicaciones":  {},
        "timeline":          {}
    }
    if os.path.exists(STATS_FILE):
        try:
            with open(STATS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            # Mezclar con defaults para asegurar que no falten llaves
            for k, v in default_stats.items():
                if k not in data:
                    data[k] = v
            return data
        except Exception:
            pass
    return default_stats

def guardar_stats():
    """Guarda las estadísticas actuales a disco."""
    try:
        os.makedirs(os.path.dirname(STATS_FILE), exist_ok=True)
        with open(STATS_FILE, 'w', encoding='utf-8') as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Error guardando stats: {e}")

# Memoria en tiempo real compartida
stats = cargar_stats()
eventos_recientes = []
