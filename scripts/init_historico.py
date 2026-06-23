import os
import json
import pandas as pd

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLEAN_DATA  = os.path.join(BASE_DIR, "data", "clean", "denuncias_sidpol_clean.parquet")
STATS_FILE  = os.path.join(BASE_DIR, "data", "clean", "sisco_stats.json")

def init_historical():
    print("Loading historical dataset...")
    if not os.path.exists(CLEAN_DATA):
        print(f"Error: file not found — {CLEAN_DATA}")
        return

    df = pd.read_parquet(CLEAN_DATA)

    # Support both old Spanish column names and new English ones
    col_map = {
        "anio": "year", "mes": "month",
        "departamento": "department", "provincia": "province",
        "distrito": "district", "tipo_hecho": "crime_type",
        "cantidad": "count",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    df['year_num'] = pd.to_numeric(df.get('year', pd.Series(dtype=str)), errors='coerce')
    df_hist = df[df['year_num'] <= 2025].copy()

    print(f"Historical records found: {len(df_hist):,}")

    stats = {
        "total_reports":   0,
        "critical_alerts": 0,
        "last_alert":      "Historical load complete",
        "top_crime_types": {},
        "top_departments": {},
        "location_map":    {},
        "timeline":        {}
    }

    stats['total_reports'] = int(df_hist['count'].sum())

    if 'crime_type' in df_hist.columns:
        top_ct = df_hist.groupby('crime_type')['count'].sum().to_dict()
        stats['top_crime_types'] = {k: int(v) for k, v in top_ct.items() if pd.notna(k) and k != ''}

    if 'department' in df_hist.columns:
        top_dept = df_hist.groupby('department')['count'].sum().to_dict()
        stats['top_departments'] = {k: int(v) for k, v in top_dept.items() if pd.notna(k) and k != ''}

    if all(c in df_hist.columns for c in ['department', 'province', 'district']):
        df_hist['loc_key'] = (
            df_hist['department'].astype(str) + "|" +
            df_hist['province'].astype(str)   + "|" +
            df_hist['district'].astype(str)
        )
        loc_map = df_hist.groupby('loc_key')['count'].sum().to_dict()
        stats['location_map'] = {k: int(v) for k, v in loc_map.items()}

    if 'year' in df_hist.columns and 'month' in df_hist.columns:
        df_hist['month_str'] = df_hist['month'].astype(str).str.zfill(2)
        df_hist['date_key']  = df_hist['year'].astype(str) + "-" + df_hist['month_str']
        timeline = df_hist.groupby('date_key')['count'].sum().to_dict()
        stats['timeline'] = {k: int(v) for k, v in timeline.items() if 'nan' not in k}

    with open(STATS_FILE, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print(f"Historical stats saved to: {STATS_FILE}")
    print(f"Total reports pre-loaded: {stats['total_reports']:,}")

if __name__ == "__main__":
    init_historical()
