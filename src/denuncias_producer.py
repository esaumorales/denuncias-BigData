import os
import time
import json
import csv
import random
import socket
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# ── Configuración ────────────────────────────────────────────────
TOPIC      = "denuncias_sidpol"

# Ruta del CSV: priorizar data_clean (ya procesado), si no existe usar data_raw
_BASE = os.path.join(os.path.dirname(__file__), '..')
_CLEAN = os.path.join(_BASE, "data_clean", "denuncias_sidpol_clean.csv")
_RAW   = os.path.join(_BASE, "data_raw", "DATASET_Denuncias_Policiales_Ene 2018 a Abr 2026.csv")

if os.path.exists(_CLEAN):
    CSV_FILE = _CLEAN
    print(f"Usando dataset limpio (data_clean): {os.path.abspath(_CLEAN)}")
else:
    CSV_FILE = _RAW
    print(f"Advertencia: data_clean no encontrado. Usando data_raw.")
    print(f"Tip: ejecuta 'python prepare_data.py' para generar el dataset limpio.")

DELAY_MIN  = 0.5   # segundos entre registros (ajustable)
DELAY_MAX  = 1.5

# El broker correcto depende del entorno:
# - Dentro de Docker    → kafka:9092
# - Fuera de Docker     → localhost:9092
def detectar_broker():
    candidatos = [
        os.getenv("KAFKA_BROKER", ""),
        "localhost:9092",
        "kafka:9092",
    ]
    for broker in candidatos:
        if not broker:
            continue
        host, port = broker.rsplit(":", 1)
        try:
            socket.setdefaulttimeout(3)
            socket.socket().connect((host, int(port)))
            print(f"Broker alcanzable: {broker}")
            return broker
        except Exception:
            print(f"Broker no alcanzable: {broker}")
    return "localhost:9092"

def crear_topico(broker):
    try:
        admin = KafkaAdminClient(bootstrap_servers=broker, request_timeout_ms=10000)
        topics_existentes = admin.list_topics()
        if TOPIC not in topics_existentes:
            nuevo = NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)
            admin.create_topics([nuevo])
            print(f"Topico '{TOPIC}' creado.")
        else:
            print(f"Topico '{TOPIC}' ya existe.")
        admin.close()
    except TopicAlreadyExistsError:
        pass
    except Exception as e:
        print(f"Advertencia al crear topico: {e} — continuando de todos modos.")

def inicializar_productor(broker):
    max_retries = 10
    for intento in range(1, max_retries + 1):
        try:
            print(f"Conectando a Kafka en {broker} (intento {intento}/{max_retries})...")
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                request_timeout_ms=15000,
                api_version=(2, 6, 0),
            )
            print("Conectado exitosamente a Kafka.")
            return producer
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)
    print("No se pudo conectar a Kafka.")
    return None

def generar_id():
    return f"SIDPOL-{int(time.time()*1000)}-{random.randint(1000,9999)}"

def simular_streaming(producer):
    csv_path = os.path.abspath(CSV_FILE)
    if not os.path.exists(csv_path):
        print(f"Archivo CSV no encontrado: {csv_path}")
        return

    print(f"Leyendo dataset: {csv_path}")
    print("Simulando ingreso de denuncias en tiempo real al sistema SIDPOL...")

    enviados = 0
    errores  = 0

    with open(csv_path, mode='r', encoding='utf-8-sig', errors='replace') as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                # Compatible con data_clean (columnas amigables)
                # y data_raw (columnas originales SIDPOL)
                es_clean = "tipo_hecho" in row

                if es_clean:
                    modalidad    = row.get("tipo_hecho",   "").strip()
                    departamento = row.get("departamento", "").strip().upper()
                    provincia    = row.get("provincia",    "").strip()
                    distrito     = row.get("distrito",     "").strip()
                    anio         = row.get("anio",         "").strip()
                    mes          = row.get("mes",          "").strip()
                else:
                    modalidad    = row.get("P_MODALIDADES",  "").strip()
                    departamento = row.get("DPTO_HECHO_NEW", "").strip().upper()
                    provincia    = row.get("PROV_HECHO",     "").strip()
                    distrito     = row.get("DIST_HECHO",     "").strip()
                    anio         = row.get("ANIO",           "").strip()
                    mes          = row.get("MES",            "").strip()

                cantidad_raw = row.get("cantidad", "1").strip()
                cantidad = int(float(cantidad_raw)) if cantidad_raw else 1

                evento = {
                    "id":                generar_id(),
                    "anio":              anio,
                    "mes":               mes,
                    "departamento":      departamento,
                    "provincia":         provincia,
                    "distrito":          distrito,
                    "tipo_hecho":        modalidad,
                    "modalidad":         modalidad,
                    "cantidad":          cantidad,
                    "timestamp_emision": datetime.now().isoformat(),
                    "estado_respuesta":  "PENDIENTE",
                }

                # Saltar filas vacias
                if not modalidad or not departamento:
                    continue

                future = producer.send(TOPIC, value=evento)
                future.get(timeout=15)  # esperar confirmación
                enviados += 1

                print(
                    f"[{enviados:>5}] {evento['tipo_hecho'][:35]:<35} "
                    f"| {evento['departamento']:<15} | {evento['distrito'][:20]:<20}"
                    f"| {cantidad} casos"
                )

                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

            except Exception as e:
                errores += 1
                print(f"Error en fila {enviados + errores}: {e}")
                if errores > 20:
                    print("Demasiados errores consecutivos. Revisa la conexion con Kafka.")
                    break

    print(f"\nFin del dataset. Enviados: {enviados} | Errores: {errores}")
    producer.flush()


if __name__ == "__main__":
    broker = detectar_broker()
    crear_topico(broker)
    producer = inicializar_productor(broker)
    if producer:
        simular_streaming(producer)
        producer.close()
