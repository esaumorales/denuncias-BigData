import os
import json
import pandas as pd

# Rutas
# Subimos un nivel porque el script ahora está en scripts/
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLEAN_DATA = os.path.join(BASE_DIR, "data", "clean", "denuncias_sidpol_clean.parquet")
STATS_FILE = os.path.join(BASE_DIR, "data", "clean", "sisco_stats.json")

def init_historico():
    print("Cargando dataset histórico...")
    if not os.path.exists(CLEAN_DATA):
        print(f"Error: No se encontró {CLEAN_DATA}")
        return

    df = pd.read_parquet(CLEAN_DATA)
    
    # Filtrar solo el histórico (2018 - 2025)
    df['anio_num'] = pd.to_numeric(df['anio'], errors='coerce')
    df_hist = df[df['anio_num'] <= 2025].copy()
    
    print(f"Registros históricos encontrados: {len(df_hist)}")
    
    stats = {
        "total_denuncias":   0,
        "alertas_criticas":  0,
        "ultima_alerta":     "Carga histórica inicial completada",
        "top_tipo_hecho":    {},
        "top_departamento":  {},
        "mapa_ubicaciones":  {},
        "timeline":          {}
    }
    
    # Calcular totales agrupados para hacerlo mucho más rápido
    # Total de denuncias
    stats['total_denuncias'] = int(df_hist['cantidad'].sum())
    
    # Top Tipo de Hecho
    top_tipo = df_hist.groupby('tipo_hecho')['cantidad'].sum().to_dict()
    stats['top_tipo_hecho'] = {k: int(v) for k, v in top_tipo.items() if pd.notna(k) and k != ''}
    
    # Top Departamento
    top_dpto = df_hist.groupby('departamento')['cantidad'].sum().to_dict()
    stats['top_departamento'] = {k: int(v) for k, v in top_dpto.items() if pd.notna(k) and k != ''}
    
    # Mapa Ubicaciones (departamento|provincia|distrito)
    df_hist['loc_key'] = df_hist['departamento'].astype(str) + "|" + df_hist['provincia'].astype(str) + "|" + df_hist['distrito'].astype(str)
    mapa_loc = df_hist.groupby('loc_key')['cantidad'].sum().to_dict()
    stats['mapa_ubicaciones'] = {k: int(v) for k, v in mapa_loc.items()}
    
    # Timeline (anio-mes)
    df_hist['mes_str'] = df_hist['mes'].astype(str).str.zfill(2)
    df_hist['date_key'] = df_hist['anio'].astype(str) + "-" + df_hist['mes_str']
    timeline = df_hist.groupby('date_key')['cantidad'].sum().to_dict()
    stats['timeline'] = {k: int(v) for k, v in timeline.items() if 'nan' not in k}
    
    # Guardar en JSON
    with open(STATS_FILE, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
        
    print(f"Estadísticas históricas guardadas exitosamente en: {STATS_FILE}")
    print(f"Total denuncias pre-cargadas: {stats['total_denuncias']}")

if __name__ == "__main__":
    init_historico()
