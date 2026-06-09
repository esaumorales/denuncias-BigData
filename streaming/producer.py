import os
import time
import json
import random
import socket
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import KAFKA_TOPIC_IN, DATA_CLEAN_DIR, DATA_RAW_DIR

# ── Configuración ────────────────────────────────────────────────
TOPIC      = KAFKA_TOPIC_IN

# Ruta del archivo: priorizar data_clean (ya procesado, en Parquet), si no existe usar data_raw
_CLEAN = os.path.join(DATA_CLEAN_DIR, "denuncias_sidpol_clean.parquet")
_RAW   = os.path.join(DATA_RAW_DIR, "DATASET_Denuncias_Policiales_Ene 2018 a Abr 2026.csv")

if os.path.exists(_CLEAN):
    DATA_FILE = _CLEAN
    print(f"Usando dataset limpio (Parquet): {os.path.abspath(_CLEAN)}")
else:
    DATA_FILE = _RAW
    print(f"Advertencia: data_clean no encontrado. Usando data_raw.")
    print(f"Tip: ejecuta 'python prepare_data.py' para generar el dataset limpio.")

DELAY_MIN  = 0.05   # streaming super rapido
DELAY_MAX  = 0.2

# El broker correcto depende del entorno:
# - Dentro de Docker    → kafka:9092
# - Fuera de Docker     → localhost:9092
def detectar_broker():
    candidatos = [
        os.getenv("KAFKA_BROKER", ""),
        "localhost:29092",
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
    return "localhost:29092"

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
    data_path = os.path.abspath(DATA_FILE)
    if not os.path.exists(data_path):
        print(f"Archivo de datos no encontrado: {data_path}")
        return

    print(f"Leyendo dataset: {data_path}")
    print("Simulando ingreso de denuncias en tiempo real al sistema SIDPOL...")

    enviados = 0
    errores  = 0

    try:
        if data_path.endswith('.parquet'):
            df = pd.read_parquet(data_path)
        else:
            df = pd.read_csv(data_path, encoding='utf-8-sig', dtype=str).fillna("")
            
        filas = df.to_dict(orient='records')
        
        for row in filas:
            try:
                # Estandarización de llaves por si viene del raw
                departamento = str(row.get('departamento', row.get('DEPARTAMENTO', ''))).strip().upper()
                provincia    = str(row.get('provincia', row.get('PROVINCIA', ''))).strip().upper()
                distrito     = str(row.get('distrito', row.get('DISTRITO', ''))).strip().upper()
                tipo_hecho   = str(row.get('tipo_hecho', row.get('TIPO DE HECHO', ''))).strip().title()
                modalidad    = str(row.get('modalidad', row.get('SUB TIPO DE HECHO', ''))).strip().title()
                
                if not modalidad:
                    modalidad = tipo_hecho
                
                # Campos numéricos
                try:
                    cantidad = int(row.get('cantidad', row.get('CANTIDAD', 1)))
                except (ValueError, TypeError):
                    cantidad = 1

                # Fecha
                anio = str(row.get('anio', row.get('AÑO', ''))).strip()
                if anio.endswith('.0'):
                    anio = anio[:-2]
                mes  = str(row.get('mes', row.get('MES', ''))).strip()

                evento = {
                    "id": f"SIDPOL-{int(time.time()*1000)}-{random.randint(1000,9999)}",
                    "anio": anio,
                    "mes": mes,
                    "departamento": departamento,
                    "provincia": provincia,
                    "distrito": distrito,
                    "tipo_hecho": tipo_hecho,
                    "modalidad": modalidad,
                    "cantidad": cantidad,
                    "timestamp_emision": datetime.now().isoformat(),
                    "estado_respuesta": "PENDIENTE"
                }

                if not modalidad or not departamento:
                    continue

                # Simular streaming SOLO para datos del 2026
                if str(anio).strip() != "2026":
                    continue
                else:
                    print(f"DEBUG - Fila de 2026 encontrada: anio={anio}, mod={modalidad}, dep={departamento}")

                future = producer.send(TOPIC, value=evento)
                future.get(timeout=15)
                enviados += 1

                print(f"[{enviados}] Enviado: {tipo_hecho}")
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

            except Exception as e:
                errores += 1
                print(f"Error en fila {enviados + errores}: {e}")
                if errores > 20:
                    print("Demasiados errores consecutivos. Revisa la conexion con Kafka.")
                    break

    except Exception as e:
        print(f"Error leyendo el archivo: {e}")

    print(f"\nFin del dataset. Enviados: {enviados} | Errores: {errores}")
    producer.flush()
    producer.close()


if __name__ == "__main__":
    broker = detectar_broker()
    crear_topico(broker)
    producer = inicializar_productor(broker)
    if producer:
        simular_streaming(producer)
        producer.close()
