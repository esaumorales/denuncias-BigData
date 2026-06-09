from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, desc, year, month
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

def main():
    print("Iniciando Job Batch ETL - Denuncias Policiales SIDPOL hacia MinIO Data Lake...")

    # 1. Crear sesión de Spark con conectores para S3
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

    # Reducir verbosidad de logs
    spark.sparkContext.setLogLevel("WARN")

    # 2. Leer el CSV de Denuncias Policiales SIDPOL
    csv_path = "/home/jovyan/work/data_raw/DATASET_Denuncias_Policiales_Ene 2018 a Abr 2026.csv"

    # Definir esquema real del CSV SIDPOL
    schema = StructType([
        StructField("ANIO",           StringType(),  True),
        StructField("MES",            StringType(),  True),
        StructField("DPTO_HECHO_NEW", StringType(),  True),
        StructField("PROV_HECHO",     StringType(),  True),
        StructField("DIST_HECHO",     StringType(),  True),
        StructField("UBIGEO_HECHO",   StringType(),  True),
        StructField("P_MODALIDADES",  StringType(),  True),
        StructField("cantidad",       IntegerType(), True),
    ])

    print(f"Leyendo dataset SIDPOL desde: {csv_path}")
    denuncias_df = spark.read \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .option("quote", '"') \
        .schema(schema) \
        .csv(csv_path)

    # Renombrar columnas a nombres amigables
    denuncias_df = denuncias_df \
        .withColumnRenamed("ANIO",           "anio") \
        .withColumnRenamed("MES",            "mes") \
        .withColumnRenamed("DPTO_HECHO_NEW", "departamento") \
        .withColumnRenamed("PROV_HECHO",     "provincia") \
        .withColumnRenamed("DIST_HECHO",     "distrito") \
        .withColumnRenamed("UBIGEO_HECHO",   "ubigeo") \
        .withColumnRenamed("P_MODALIDADES",  "modalidad")

    # 3. Limpieza basica
    clean_df = denuncias_df.dropna(subset=["departamento", "distrito", "modalidad"]) \
                           .filter(col("cantidad") > 0) \
                           .filter(col("anio").cast("int") <= 2025)

    total = clean_df.count()
    print(f"Total de registros limpios a procesar: {total:,}")

    # 4. Análisis Batch: Ranking de distritos por total de denuncias
    print("Calculando ranking de distritos...")
    ranking_df = clean_df.groupBy("departamento", "provincia", "distrito") \
                         .agg(spark_sum("cantidad").alias("total_denuncias")) \
                         .orderBy(desc("total_denuncias"))

    ranking_path = "s3a://clean-data/denuncias_parquet/ranking_distritos/"
    print(f"Guardando ranking en: {ranking_path}")
    ranking_df.write.mode("overwrite").parquet(ranking_path)

    # 5. Análisis Batch: Top tipos de delito por departamento
    print("Calculando top tipos de delito por departamento...")
    tipos_df = clean_df.groupBy("departamento", "tipo_hecho", "modalidad") \
                       .agg(spark_sum("cantidad").alias("total_denuncias")) \
                       .orderBy("departamento", desc("total_denuncias"))

    tipos_path = "s3a://clean-data/denuncias_parquet/tipos_hecho/"
    print(f"Guardando tipos de hecho en: {tipos_path}")
    tipos_df.write.mode("overwrite").parquet(tipos_path)

    # 6. Análisis Batch: Tendencia por año y mes
    print("Calculando tendencia temporal...")
    tendencia_df = clean_df.groupBy("anio", "mes", "departamento") \
                           .agg(spark_sum("cantidad").alias("total_denuncias")) \
                           .orderBy("anio", "mes")

    tendencia_path = "s3a://clean-data/denuncias_parquet/tendencia_temporal/"
    print(f"Guardando tendencia temporal en: {tendencia_path}")
    tendencia_df.write.mode("overwrite").parquet(tendencia_path)

    print("ETL Batch SISCO completado exitosamente.")
    spark.stop()

if __name__ == "__main__":
    main()
