import time
import json
import os
import sys
from kafka import KafkaConsumer, KafkaProducer

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.settings import KAFKA_BROKER, KAFKA_TOPIC_IN, KAFKA_TOPIC_OUT, KAFKA_GROUP_ID
from app.services.stats_store import stats, recent_events, save_stats

CRITICAL_CRIME_TYPES = {
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
            print(f"Warning: could not connect Kafka producer: {e}")
            producer = None
    return producer


def enrich(event):
    """Alert detection logic — mirrors Flink job. Used only in fallback mode."""
    crime_type = event.get('crime_type', '').lower()
    count      = int(event.get('count', 1))
    district   = event.get('district', '?')
    department = event.get('department', '?')

    event['is_critical']   = False
    event['alert_message'] = None

    for ct in CRITICAL_CRIME_TYPES:
        if ct in crime_type:
            event['is_critical']   = True
            event['alert_message'] = (
                f"[CRITICAL ALERT] {event.get('crime_type','')} in {district}, {department}. "
                f"Cases: {count}."
            )
            break

    if not event['is_critical'] and count >= 50:
        event['is_critical']   = True
        event['alert_message'] = (
            f"[VOLUME ALERT] Unusual spike: {event.get('crime_type','')} "
            f"in {district}, {department} — {count} cases."
        )
    return event


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


def _process(event, from_flink):
    """Update in-memory stats with one event. Returns False if event should be skipped."""
    global _counter_since_save

    if not from_flink:
        event = enrich(event)

    crime_type = event.get('crime_type', '')
    department = event.get('department', '')
    count      = int(event.get('count', 1))

    year_val = int(event.get('year', 0) or 0)
    if not crime_type or not department:
        return False
    if year_val > 0 and year_val < 2026:
        return False

    stats["total_reports"]              += count
    stats["top_crime_types"][crime_type] = stats["top_crime_types"].get(crime_type, 0) + count
    stats["top_departments"][department] = stats["top_departments"].get(department, 0) + count

    province = event.get('province', 'Unknown')
    district = event.get('district', 'Unknown')
    loc_key  = f"{department}|{province}|{district}"
    stats["location_map"][loc_key] = stats["location_map"].get(loc_key, 0) + count

    year  = event.get('year')
    month = event.get('month')
    if year and month:
        date_key = f"{year}-{str(month).zfill(2)}"
        stats["timeline"][date_key] = stats["timeline"].get(date_key, 0) + count

    if event.get('is_critical'):
        stats["critical_alerts"] += 1
        stats["last_alert"] = event.get('alert_message', f"{crime_type} in {district}, {department}")

    recent_events.insert(0, event)
    if len(recent_events) > 50:
        recent_events.pop()

    _counter_since_save += 1
    if _counter_since_save >= 50:
        save_stats()
        _counter_since_save = 0

    return True


def kafka_listener():
    """
    Lambda architecture — speed layer.

    PRIMARY mode : reads from eventos_procesados (Flink output).
    FALLBACK mode: if Flink produces no messages for FLINK_TIMEOUT seconds,
                   switches to denuncias_sidpol and enriches locally.
    Automatically returns to primary mode when Flink recovers.
    """
    FLINK_TIMEOUT    = 8
    flink_active     = True
    last_flink_msg   = time.time()

    while True:
        time.sleep(2)
        try:
            topic = KAFKA_TOPIC_OUT if flink_active else KAFKA_TOPIC_IN
            mode  = "Flink → eventos_procesados" if flink_active else "FALLBACK → denuncias_sidpol"
            print(f"[SISCO] Listener mode: {mode}")

            consumer = _make_consumer(topic)

            for message in consumer:
                try:
                    event      = json.loads(message.value)
                    from_flink = (message.topic == KAFKA_TOPIC_OUT)

                    if from_flink:
                        last_flink_msg = time.time()

                    _process(event, from_flink)

                except Exception as e:
                    print(f"Error processing message: {e}")

            consumer.close()
            save_stats()

            sec_without_flink = time.time() - last_flink_msg
            if flink_active and sec_without_flink > FLINK_TIMEOUT:
                print(f"[SISCO] Flink silent for {sec_without_flink:.0f}s — enabling fallback.")
                flink_active = False
            elif not flink_active:
                try:
                    test = _make_consumer(KAFKA_TOPIC_OUT)
                    for _ in test:
                        last_flink_msg = time.time()
                        flink_active   = True
                        print("[SISCO] Flink recovered — returning to primary mode.")
                        break
                    test.close()
                except Exception:
                    pass

            time.sleep(3)

        except Exception as e:
            print(f"Kafka listener error: {e} — retrying in 5s...")
            time.sleep(5)
