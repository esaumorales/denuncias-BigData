from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import json
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, DATA_RAW_DIR, DATA_CLEAN_DIR

def main():
    print("Iniciando Job Batch ETL - Denuncias Policiales SIDPOL hacia MinIO Data Lake...")

    spark = SparkSession.builder \
        .appName("SISCO_Batch_ETL_SIDPOL") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # El CSV no tiene fila de cabecera — se define el schema explícito y header=false
    csv_path = os.path.join(DATA_RAW_DIR, "DATASET_Denuncias_Policiales_Ene 2018 a Abr 2026.csv")

    schema = StructType([
        StructField("anio",         StringType(),  True),
        StructField("mes",          StringType(),  True),
        StructField("departamento", StringType(),  True),
        StructField("provincia",    StringType(),  True),
        StructField("distrito",     StringType(),  True),
        StructField("ubigeo",       StringType(),  True),
        StructField("tipo_hecho",   StringType(),  True),
        StructField("cantidad",     IntegerType(), True),
    ])

    print(f"Leyendo dataset SIDPOL desde: {csv_path}")
    denuncias_df = spark.read \
        .option("header", "false") \
        .option("encoding", "UTF-8") \
        .option("quote", '"') \
        .schema(schema) \
        .csv(csv_path)

    # Limpieza básica
    clean_df = denuncias_df \
        .filter(col("departamento").isNotNull()) \
        .filter(col("distrito").isNotNull()) \
        .filter(col("tipo_hecho").isNotNull()) \
        .filter(col("cantidad") > 0) \
        .filter(col("anio").cast("int").isNotNull()) \
        .filter(col("anio").cast("int") <= 2025)

    total = clean_df.count()
    print(f"Total de registros limpios a procesar: {total:,}")

    # 1. Ranking de distritos
    print("Calculando ranking de distritos...")
    ranking_df = clean_df.groupBy("departamento", "provincia", "distrito") \
                         .agg(spark_sum("cantidad").alias("total_denuncias")) \
                         .orderBy(desc("total_denuncias"))
    ranking_df.write.mode("overwrite").parquet("s3a://clean-data/denuncias_parquet/ranking_distritos/")

    # 2. Top tipos de hecho por departamento
    print("Calculando top tipos de hecho por departamento...")
    tipos_df = clean_df.groupBy("departamento", "tipo_hecho") \
                       .agg(spark_sum("cantidad").alias("total_denuncias")) \
                       .orderBy("departamento", desc("total_denuncias"))
    tipos_df.write.mode("overwrite").parquet("s3a://clean-data/denuncias_parquet/tipos_hecho/")

    # 3. Tendencia temporal
    print("Calculando tendencia temporal...")
    tendencia_df = clean_df.groupBy("anio", "mes", "departamento") \
                           .agg(spark_sum("cantidad").alias("total_denuncias")) \
                           .orderBy("anio", "mes")
    tendencia_df.write.mode("overwrite").parquet("s3a://clean-data/denuncias_parquet/tendencia_temporal/")

    # 4. Exportar stats al JSON local para el dashboard (serving layer)
    print("Exportando stats batch al dashboard...")

    total_denuncias = int(clean_df.agg(spark_sum("cantidad")).collect()[0][0] or 0)

    top_tipo_hecho = {
        row["tipo_hecho"]: int(row["total"])
        for row in clean_df.groupBy("tipo_hecho")
                           .agg(spark_sum("cantidad").alias("total"))
                           .collect()
        if row["tipo_hecho"]
    }

    top_departamento = {
        row["departamento"]: int(row["total"])
        for row in clean_df.groupBy("departamento")
                           .agg(spark_sum("cantidad").alias("total"))
                           .collect()
        if row["departamento"]
    }

    mapa_ubicaciones = {
        f"{r['departamento']}|{r['provincia']}|{r['distrito']}": int(r["total"])
        for r in clean_df.groupBy("departamento", "provincia", "distrito")
                         .agg(spark_sum("cantidad").alias("total"))
                         .collect()
        if r["departamento"] and r["distrito"]
    }

    # Timeline: agrupa por anio+mes si mes existe, solo por anio si no
    timeline_rows = clean_df.groupBy("anio", "mes") \
                            .agg(spark_sum("cantidad").alias("total")) \
                            .collect()
    has_mes = any(r["mes"] is not None for r in timeline_rows)
    if has_mes:
        timeline = {
            f"{r['anio']}-{str(r['mes']).zfill(2)}": int(r["total"])
            for r in timeline_rows
            if r["anio"] and r["mes"] and "nan" not in str(r["anio"])
        }
    else:
        timeline = {
            r["anio"]: int(r["total"])
            for r in timeline_rows
            if r["anio"] and "nan" not in str(r["anio"])
        }

    batch_stats = {
        "total_denuncias":  total_denuncias,
        "alertas_criticas": 0,
        "ultima_alerta":    "Carga batch completada (2018-2025)",
        "top_tipo_hecho":   top_tipo_hecho,
        "top_departamento": top_departamento,
        "mapa_ubicaciones": mapa_ubicaciones,
        "timeline":         timeline,
    }

    stats_path = os.path.join(DATA_CLEAN_DIR, "sisco_stats.json")
    os.makedirs(DATA_CLEAN_DIR, exist_ok=True)
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(batch_stats, f, ensure_ascii=False, indent=2)
    print(f"Stats batch guardadas en: {stats_path}")
    print(f"Total denuncias pre-cargadas: {total_denuncias:,}")

    print("ETL Batch SISCO completado exitosamente.")
    spark.stop()

if __name__ == "__main__":
    main()
