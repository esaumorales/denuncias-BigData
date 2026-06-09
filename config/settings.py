import os

# ── Rutas Base ──────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DATA_RAW_DIR = os.path.join(DATA_DIR, "raw")
DATA_CLEAN_DIR = os.path.join(DATA_DIR, "clean")

# ── Configuración Kafka ─────────────────────────────────────────
# Dentro de Docker usa 'kafka:9092', fuera usa 'localhost:29092'
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
KAFKA_TOPIC_IN = "denuncias_sidpol"
KAFKA_TOPIC_OUT = "eventos_procesados"
KAFKA_GROUP_ID = "sisco_alert_group"

# ── Configuración MinIO (S3) ────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
