import time
import json
import os
import sys
from kafka import KafkaConsumer, KafkaProducer

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.settings import KAFKA_BROKER, KAFKA_TOPIC_IN, KAFKA_TOPIC_OUT, KAFKA_GROUP_ID
from app.services.stats_store import stats, eventos_recientes, guardar_stats

TIPOS_CRITICOS = {
    "homicidio", "feminicidio", "robo agravado",
    "secuestro", "trata de personas", "terrorismo", "violacion sexual",
}

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


def enriquecer(data):
    """Misma lógica de alertas que Flink — se usa solo en modo fallback."""
    tipo     = data.get('tipo_hecho', '').lower()
    cantidad = int(data.get('cantidad', 1))
    distrito = data.get('distrito', '?')
    depto    = data.get('departamento', '?')

    data['is_critical']   = False
    data['alert_message'] = None

    for t in TIPOS_CRITICOS:
        if t in tipo:
            data['is_critical']   = True
            data['alert_message'] = (
                f"[ALERTA CRITICA] {data.get('tipo_hecho','')} en {distrito}, {depto}. "
                f"Casos: {cantidad}."
            )
            break

    if not data['is_critical'] and cantidad >= 50:
        data['is_critical']   = True
        data['alert_message'] = (
            f"[ALERTA VOLUMEN] Pico inusual: {data.get('tipo_hecho','')} "
            f"en {distrito}, {depto} — {cantidad} casos."
        )
    return data


def _make_consumer(topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=None,
        value_deserializer=lambda x: x.decode('utf-8'),
        consumer_timeout_ms=5000,
    )


def _procesar(data, desde_flink):
    """Actualiza stats en memoria con un evento. Retorna False si debe ignorarse."""
    global _counter_since_save

    if not desde_flink:
        data = enriquecer(data)

    tipo_hecho   = data.get('tipo_hecho',   '')
    departamento = data.get('departamento', '')
    cantidad     = int(data.get('cantidad',  1))

    anio_evento = int(data.get('anio', 0) or 0)
    if not tipo_hecho or not departamento:
        return False
    if anio_evento > 0 and anio_evento < 2026:
        return False

    stats["total_denuncias"] += cantidad
    stats["top_tipo_hecho"][tipo_hecho]     = stats["top_tipo_hecho"].get(tipo_hecho, 0) + cantidad
    stats["top_departamento"][departamento] = stats["top_departamento"].get(departamento, 0) + cantidad

    provincia = data.get('provincia', 'Desconocido')
    distrito  = data.get('distrito',  'Desconocido')
    loc_key   = f"{departamento}|{provincia}|{distrito}"
    stats["mapa_ubicaciones"][loc_key] = stats["mapa_ubicaciones"].get(loc_key, 0) + cantidad

    anio = data.get('anio')
    mes  = data.get('mes')
    if anio and mes:
        date_key = f"{anio}-{str(mes).zfill(2)}"
        stats["timeline"][date_key] = stats["timeline"].get(date_key, 0) + cantidad

    if data.get('is_critical'):
        stats["alertas_criticas"] += 1
        stats["ultima_alerta"] = data.get('alert_message', f"{tipo_hecho} en {distrito}, {departamento}")

    eventos_recientes.insert(0, data)
    if len(eventos_recientes) > 50:
        eventos_recientes.pop()

    _counter_since_save += 1
    if _counter_since_save >= 50:
        guardar_stats()
        _counter_since_save = 0

    return True


def kafka_listener():
    """
    Arquitectura Lambda — capa speed.

    Modo PRINCIPAL : escucha eventos_procesados (salida de Flink).
    Modo FALLBACK  : si Flink no produce mensajes en FLINK_TIMEOUT segundos,
                     cambia a denuncias_sidpol y enriquece localmente.
    Cuando Flink vuelve, regresa al modo principal automáticamente.
    """
    FLINK_TIMEOUT = 8

    flink_activo         = True
    ultimo_msg_flink     = time.time()

    while True:
        time.sleep(2)
        try:
            topic = KAFKA_TOPIC_OUT if flink_activo else KAFKA_TOPIC_IN
            modo  = "Flink → eventos_procesados" if flink_activo else "FALLBACK → denuncias_sidpol"
            print(f"[SISCO] Listener modo: {modo}")

            consumer = _make_consumer(topic)

            for message in consumer:
                try:
                    data        = json.loads(message.value)
                    desde_flink = (message.topic == KAFKA_TOPIC_OUT)

                    if desde_flink:
                        ultimo_msg_flink = time.time()

                    _procesar(data, desde_flink)

                except Exception as e:
                    print(f"Error procesando mensaje: {e}")

            consumer.close()
            guardar_stats()

            # — Lógica de detección de estado de Flink —
            seg_sin_flink = time.time() - ultimo_msg_flink
            if flink_activo and seg_sin_flink > FLINK_TIMEOUT:
                print(f"[SISCO] Flink sin output {seg_sin_flink:.0f}s — activando fallback.")
                flink_activo = False

            elif not flink_activo:
                # Sondear brevemente si Flink volvió
                try:
                    test = _make_consumer(KAFKA_TOPIC_OUT)
                    for _ in test:
                        ultimo_msg_flink = time.time()
                        flink_activo = True
                        print("[SISCO] Flink activo de nuevo — regresando al modo principal.")
                        break
                    test.close()
                except Exception:
                    pass

            time.sleep(3)

        except Exception as e:
            print(f"Error en listener Kafka: {e} — reintentando en 5s...")
            time.sleep(5)
