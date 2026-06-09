from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
import json

import sys
import os

# Añadir ruta para importar configuración local o en docker
try:
    # Si estamos en Docker, el config está en /opt/flink/config
    sys.path.append("/opt/flink")
    from config.settings import KAFKA_TOPIC_IN, KAFKA_TOPIC_OUT, KAFKA_GROUP_ID, KAFKA_BROKER
except ModuleNotFoundError:
    # Fallback si se ejecuta desde IDE local
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from config.settings import KAFKA_TOPIC_IN, KAFKA_TOPIC_OUT, KAFKA_GROUP_ID, KAFKA_BROKER

# Tipos de hecho que se consideran ALERTAS CRITICAS en SISCO
TIPOS_CRITICOS = {
    "Homicidio",
    "Feminicidio",
    "Robo Agravado",
    "Secuestro",
    "Trata de Personas",
    "Terrorismo",
    "Violacion Sexual",
}

def process_stream():
    print("Iniciando Streaming Job PyFlink — Sistema SISCO (Capa Speed)...")

    # Crear entorno de ejecución
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Configuración de Kafka
    kafka_broker = "kafka:9092"
    topic_in  = KAFKA_TOPIC_IN
    topic_out = KAFKA_TOPIC_OUT
    kafka_group  = KAFKA_GROUP_ID

    kafka_props = {
        'bootstrap.servers': kafka_broker,
        'group.id':          kafka_group,
        'auto.offset.reset': 'latest'
    }

    deserialization_schema = SimpleStringSchema()
    
    # Consumer (Lee Data Cruda)
    kafka_consumer = FlinkKafkaConsumer(
        topics=topic_in,
        deserialization_schema=deserialization_schema,
        properties=kafka_props
    )

    stream = env.add_source(kafka_consumer)

    def enrich_and_detect_alerts(json_str):
        try:
            data = json.loads(json_str)
            tipo_hecho  = data.get('tipo_hecho', '')
            modalidad   = data.get('modalidad', '')
            distrito    = data.get('distrito',   'Desconocido')
            departamento = data.get('departamento', '')
            cantidad    = int(data.get('cantidad', 1))

            data['is_critical'] = False
            data['alert_message'] = None

            # Alerta por tipo de hecho crítico
            for tipo_critico in TIPOS_CRITICOS:
                if tipo_critico.lower() in tipo_hecho.lower():
                    data['is_critical'] = True
                    data['alert_message'] = (
                        f"[ALERTA CRITICA - SISCO] {tipo_hecho} ({modalidad}) detectado "
                        f"en {distrito}, {departamento}. Casos reportados: {cantidad}."
                    )
                    break

            # Alerta por volumen inusual
            if cantidad >= 50 and not data['is_critical']:
                data['is_critical'] = True
                data['alert_message'] = (
                    f"[ALERTA VOLUMEN - SISCO] Pico inusual de denuncias: "
                    f"{tipo_hecho} en {distrito}, {departamento} - {cantidad} casos en un registro."
                )

            # Devolvemos el JSON enriquecido
            return json.dumps(data)
        except Exception:
            return None

    # Aplicar lógica de enriquecimiento
    enriched_stream = stream.map(enrich_and_detect_alerts, output_type=Types.STRING()) \
                            .filter(lambda x: x is not None)

    # Imprimir en consola para depuración
    enriched_stream.print()

    # Sink / Producer (Escribe Data Procesada al Topic 2)
    kafka_producer = FlinkKafkaProducer(
        topic=topic_out,
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': kafka_broker}
    )
    enriched_stream.add_sink(kafka_producer)

    print(f"Escuchando '{topic_in}', procesando y escribiendo en '{topic_out}'...")
    env.execute("SISCO_Streaming_Lambda_Layer")

if __name__ == '__main__':
    process_stream()
