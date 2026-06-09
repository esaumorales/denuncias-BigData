import time
import json
import os
import sys
from kafka import KafkaConsumer, KafkaProducer

# Agregar raíz al sys.path para importaciones
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import KAFKA_BROKER, KAFKA_TOPIC_OUT, KAFKA_GROUP_ID
from app.stats_store import stats, eventos_recientes, guardar_stats
import app.stats_store as store

_counter_since_save = 0
producer = None

def get_producer():
    global producer
    if producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                request_timeout_ms=5000
            )
        except Exception as e:
            print(f"Advertencia: No se pudo conectar al Producer Kafka: {e}")
            producer = None
    return producer

def kafka_listener():
    global _counter_since_save
    while True:
        time.sleep(2)
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC_OUT,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id=None,
                value_deserializer=lambda x: x.decode('utf-8'),
                consumer_timeout_ms=5000,
            )
            print("Dashboard SISCO conectado a Kafka — escuchando eventos_procesados (desde Flink)...")

            for message in consumer:
                try:
                    data = json.loads(message.value)

                    tipo_hecho   = data.get('tipo_hecho',   'Desconocido')
                    departamento = data.get('departamento', 'Desconocido')
                    cantidad     = int(data.get('cantidad',  1))

                    # Ignorar registros vacíos
                    if not tipo_hecho or not departamento or tipo_hecho == 'Desconocido':
                        continue

                    stats["total_denuncias"] += cantidad

                    stats["top_tipo_hecho"][tipo_hecho] = \
                        stats["top_tipo_hecho"].get(tipo_hecho, 0) + cantidad

                    stats["top_departamento"][departamento] = \
                        stats["top_departamento"].get(departamento, 0) + cantidad
                        
                    # Mapa detallado por distrito
                    provincia = data.get('provincia', 'Desconocido')
                    distrito = data.get('distrito', 'Desconocido')
                    loc_key = f"{departamento}|{provincia}|{distrito}"
                    stats["mapa_ubicaciones"][loc_key] = stats["mapa_ubicaciones"].get(loc_key, 0) + cantidad

                    # Evolucion temporal
                    anio = data.get('anio')
                    mes = data.get('mes')
                    if anio and mes:
                        date_key = f"{anio}-{str(mes).zfill(2)}"
                        stats["timeline"][date_key] = stats["timeline"].get(date_key, 0) + cantidad

                    is_critical = data.get('is_critical', False)
                    if is_critical:
                        stats["alertas_criticas"] += 1
                        stats["ultima_alerta"] = data.get('alert_message', f"{tipo_hecho} en {data.get('distrito','?')}, {departamento}")

                    eventos_recientes.insert(0, data)
                    if len(eventos_recientes) > 50:
                        eventos_recientes.pop()

                    # Guardar a disco cada 50 mensajes
                    _counter_since_save += 1
                    if _counter_since_save >= 50:
                        guardar_stats()
                        _counter_since_save = 0

                except Exception as e:
                    print(f"Error procesando mensaje: {e}")

            consumer.close()
            # Guardar al desconectarse
            guardar_stats()
            print("Consumer cerrado, reconectando en 3 segundos...")
            time.sleep(3)

        except Exception as e:
            print(f"Error en listener Kafka: {e} — reintentando en 5 segundos...")
            time.sleep(5)
