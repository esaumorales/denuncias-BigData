"""
prepare_data.py
───────────────
Script de limpieza ETL local (Bronze → Silver).
Lee el CSV crudo de data_raw/, lo limpia y normaliza,
y guarda el resultado listo para usar en data_clean/.

Este script es independiente de Spark y se puede correr
directamente con: python prepare_data.py
"""

import os
import pandas as pd

# ── Rutas ──────────────────────────────────────────────────────
# Subimos un nivel porque el script ahora está en scripts/
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR   = os.path.join(BASE_DIR, "data", "raw")
OUTPUT_DIR  = os.path.join(BASE_DIR, "data", "clean")

os.makedirs(OUTPUT_DIR, exist_ok=True)

RAW_CSV     = os.path.join(INPUT_DIR, "DATASET_Denuncias_Policiales_Ene 2018 a Abr 2026.csv")
CLEAN_FILE  = os.path.join(OUTPUT_DIR, "denuncias_sidpol_clean.parquet")

def main():
    print("=" * 55)
    print("  ETL LOCAL — Bronze to Silver")
    print("  SIDPOL — Denuncias Policiales (MININTER / PNP)")
    print("=" * 55)

    # 1. LEER ─────────────────────────────────────────────────
    print(f"\n[1/4] Leyendo dataset crudo desde:\n      {RAW_CSV}")
    df = pd.read_csv(
        RAW_CSV,
        encoding="utf-8-sig",   # maneja BOM automaticamente
        dtype=str,              # leer todo como texto primero
        quotechar='"',
        names=["ANIO", "MES", "DPTO_HECHO_NEW", "PROV_HECHO", "DIST_HECHO", "UBIGEO_HECHO", "P_MODALIDADES", "cantidad"],
        header=None
    )
    print(f"      Registros cargados: {len(df):,}")
    print(f"      Columnas originales: {list(df.columns)}")

    # 2. RENOMBRAR COLUMNAS ───────────────────────────────────
    print("\n[2/4] Renombrando columnas al esquema SISCO...")
    rename_map = {
        "ANIO":           "anio",
        "MES":            "mes",
        "DPTO_HECHO_NEW": "departamento",
        "PROV_HECHO":     "provincia",
        "DIST_HECHO":     "distrito",
        "UBIGEO_HECHO":   "ubigeo",
        "P_MODALIDADES":  "tipo_hecho",
        "cantidad":       "cantidad",
    }
    df = df.rename(columns=rename_map)
    print(f"      Columnas nuevas: {list(df.columns)}")

    # 3. LIMPIEZA ─────────────────────────────────────────────
    print("\n[3/4] Aplicando limpieza y normalización...")
    registros_antes = len(df)

    # Eliminar filas con datos esenciales nulos
    df = df.dropna(subset=["departamento", "tipo_hecho", "cantidad"])

    if 'anio' in df.columns and 'mes' in df.columns:
        print("Ordenando dataset cronológicamente por Año y Mes...")
        df['ANIO_NUM'] = pd.to_numeric(df['anio'], errors='coerce')
        df['MES_NUM'] = pd.to_numeric(df['mes'], errors='coerce')
        
        # Se mantiene toda la historia (desde 2018) para que el procesamiento batch esté completo
        # df = df[df['ANIO_NUM'] >= 2023]
        
        df = df.sort_values(by=['ANIO_NUM', 'MES_NUM']).drop(columns=['ANIO_NUM', 'MES_NUM'])

    # Convertir cantidad a entero
    df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce").fillna(0).astype(int)

    # Eliminar filas con cantidad <= 0
    df = df[df["cantidad"] > 0]

    # Normalizar texto: mayúsculas en geografía, título en tipo_hecho
    df["departamento"] = df["departamento"].str.strip().str.upper()
    df["provincia"]    = df["provincia"].str.strip().str.title()
    df["distrito"]     = df["distrito"].str.strip().str.title()
    df["tipo_hecho"]   = df["tipo_hecho"].str.strip().str.title()
    df["anio"]         = df["anio"].str.strip()
    df["mes"]          = df["mes"].str.strip()

    # Eliminar duplicados exactos
    df = df.drop_duplicates()

    registros_despues = len(df)
    eliminados = registros_antes - registros_despues
    print(f"      Registros antes:   {registros_antes:,}")
    print(f"      Registros despues: {registros_despues:,}")
    print(f"      Eliminados:        {eliminados:,}")

    # Estadísticas rápidas
    print(f"\n      Departamentos únicos: {df['departamento'].nunique()}")
    print(f"      Tipos de hecho únicos: {df['tipo_hecho'].nunique()}")
    print(f"      Rango de años: {df['anio'].min()} - {df['anio'].max()}")
    print(f"      Total denuncias (suma): {df['cantidad'].sum():,}")

    # 4. GUARDAR ──────────────────────────────────────────────
    print(f"\n[4/4] Guardando dataset limpio en:\n      {CLEAN_FILE}")
    os.makedirs(os.path.dirname(CLEAN_FILE), exist_ok=True)
    df.to_parquet(CLEAN_FILE, index=False)
    size_mb = os.path.getsize(CLEAN_FILE) / (1024 * 1024)
    print(f"      Archivo generado: {size_mb:.2f} MB")

    print("\n" + "=" * 55)
    print("  ETL completado. data_clean/ listo para usar.")
    print("=" * 55)

if __name__ == "__main__":
    main()
