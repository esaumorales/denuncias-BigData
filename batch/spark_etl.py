from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import json
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, DATA_RAW_DIR, DATA_CLEAN_DIR

def main():
    print("Starting Batch ETL Job — SIDPOL Police Reports → MinIO Data Lake...")

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

    # CSV has no header row — schema is defined explicitly with English column names
    csv_path = os.path.join(DATA_RAW_DIR, "DATASET_Denuncias_Policiales_Ene 2018 a Abr 2026.csv")

    schema = StructType([
        StructField("year",       StringType(),  True),
        StructField("month",      StringType(),  True),
        StructField("department", StringType(),  True),
        StructField("province",   StringType(),  True),
        StructField("district",   StringType(),  True),
        StructField("ubigeo",     StringType(),  True),
        StructField("crime_type", StringType(),  True),
        StructField("count",      IntegerType(), True),
    ])

    print(f"Reading SIDPOL dataset from: {csv_path}")
    df = spark.read \
        .option("header", "false") \
        .option("encoding", "UTF-8") \
        .option("quote", '"') \
        .schema(schema) \
        .csv(csv_path)

    # Basic cleaning
    clean_df = df \
        .filter(col("department").isNotNull()) \
        .filter(col("district").isNotNull()) \
        .filter(col("crime_type").isNotNull()) \
        .filter(col("count") > 0) \
        .filter(col("year").cast("int").isNotNull()) \
        .filter(col("year").cast("int") <= 2025)

    total = clean_df.count()
    print(f"Clean records to process: {total:,}")

    # 1. District ranking
    print("Computing district ranking...")
    ranking_df = clean_df.groupBy("department", "province", "district") \
                         .agg(spark_sum("count").alias("total_count")) \
                         .orderBy(desc("total_count"))
    ranking_df.write.mode("overwrite").parquet("s3a://clean-data/reports_parquet/district_ranking/")

    # 2. Top crime types by department
    print("Computing top crime types by department...")
    crime_types_df = clean_df.groupBy("department", "crime_type") \
                             .agg(spark_sum("count").alias("total_count")) \
                             .orderBy("department", desc("total_count"))
    crime_types_df.write.mode("overwrite").parquet("s3a://clean-data/reports_parquet/crime_types/")

    # 3. Temporal trend
    print("Computing temporal trend...")
    trend_df = clean_df.groupBy("year", "month", "department") \
                       .agg(spark_sum("count").alias("total_count")) \
                       .orderBy("year", "month")
    trend_df.write.mode("overwrite").parquet("s3a://clean-data/reports_parquet/temporal_trend/")

    # 4. Export aggregated stats to local JSON for the serving layer (dashboard)
    print("Exporting batch stats to dashboard serving layer...")

    total_reports = int(clean_df.agg(spark_sum("count")).collect()[0][0] or 0)

    top_crime_types = {
        row["crime_type"]: int(row["total"])
        for row in clean_df.groupBy("crime_type")
                           .agg(spark_sum("count").alias("total"))
                           .collect()
        if row["crime_type"]
    }

    top_departments = {
        row["department"]: int(row["total"])
        for row in clean_df.groupBy("department")
                           .agg(spark_sum("count").alias("total"))
                           .collect()
        if row["department"]
    }

    location_map = {
        f"{r['department']}|{r['province']}|{r['district']}": int(r["total"])
        for r in clean_df.groupBy("department", "province", "district")
                         .agg(spark_sum("count").alias("total"))
                         .collect()
        if r["department"] and r["district"]
    }

    timeline_rows = clean_df.groupBy("year", "month") \
                            .agg(spark_sum("count").alias("total")) \
                            .collect()
    has_month = any(r["month"] is not None for r in timeline_rows)
    if has_month:
        timeline = {
            f"{r['year']}-{str(r['month']).zfill(2)}": int(r["total"])
            for r in timeline_rows
            if r["year"] and r["month"] and "nan" not in str(r["year"])
        }
    else:
        timeline = {
            r["year"]: int(r["total"])
            for r in timeline_rows
            if r["year"] and "nan" not in str(r["year"])
        }

    batch_stats = {
        "total_reports":   total_reports,
        "critical_alerts": 0,
        "last_alert":      "Batch load complete (2018-2025)",
        "top_crime_types": top_crime_types,
        "top_departments": top_departments,
        "location_map":    location_map,
        "timeline":        timeline,
    }

    stats_path = os.path.join(DATA_CLEAN_DIR, "sisco_stats.json")
    os.makedirs(DATA_CLEAN_DIR, exist_ok=True)
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(batch_stats, f, ensure_ascii=False, indent=2)
    print(f"Batch stats saved to: {stats_path}")
    print(f"Total reports loaded: {total_reports:,}")

    print("Batch ETL completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
