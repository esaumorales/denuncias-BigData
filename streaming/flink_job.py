from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
import json
import sys
import os

try:
    sys.path.append("/opt/flink")
    from config.settings import KAFKA_TOPIC_IN, KAFKA_TOPIC_OUT, KAFKA_GROUP_ID, KAFKA_BROKER
except ModuleNotFoundError:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from config.settings import KAFKA_TOPIC_IN, KAFKA_TOPIC_OUT, KAFKA_GROUP_ID, KAFKA_BROKER

CRITICAL_CRIME_TYPES = {
    "Homicidio",
    "Feminicidio",
    "Robo Agravado",
    "Secuestro",
    "Trata de Personas",
    "Terrorismo",
    "Violacion Sexual",
}

def process_stream():
    print("Starting PyFlink Streaming Job — SISCO Speed Layer...")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_broker = "kafka:9092"
    kafka_props  = {
        'bootstrap.servers': kafka_broker,
        'group.id':          KAFKA_GROUP_ID,
        'auto.offset.reset': 'latest'
    }

    kafka_consumer = FlinkKafkaConsumer(
        topics=KAFKA_TOPIC_IN,
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    stream = env.add_source(kafka_consumer)

    def enrich_and_detect_alerts(json_str):
        try:
            event      = json.loads(json_str)
            crime_type = event.get('crime_type', '')
            count      = int(event.get('count', 1))
            district   = event.get('district',   'Unknown')
            department = event.get('department', '')

            event['is_critical']   = False
            event['alert_message'] = None

            for ct in CRITICAL_CRIME_TYPES:
                if ct.lower() in crime_type.lower():
                    event['is_critical']   = True
                    event['alert_message'] = (
                        f"[CRITICAL ALERT] {crime_type} in {district}, {department}. "
                        f"Cases: {count}."
                    )
                    break

            if count >= 50 and not event['is_critical']:
                event['is_critical']   = True
                event['alert_message'] = (
                    f"[VOLUME ALERT] Unusual spike: {crime_type} "
                    f"in {district}, {department} — {count} cases."
                )

            return json.dumps(event)
        except Exception:
            return None

    enriched_stream = stream.map(enrich_and_detect_alerts, output_type=Types.STRING()) \
                            .filter(lambda x: x is not None)

    enriched_stream.print()

    kafka_producer = FlinkKafkaProducer(
        topic=KAFKA_TOPIC_OUT,
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': kafka_broker}
    )
    enriched_stream.add_sink(kafka_producer)

    print(f"Listening on '{KAFKA_TOPIC_IN}', writing enriched events to '{KAFKA_TOPIC_OUT}'...")
    env.execute("SISCO_Streaming_Lambda_Layer")

if __name__ == '__main__':
    process_stream()
