from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
import json

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
    print("Iniciando Streaming Job PyFlink — Sistema SISCO...")

    # Crear entorno de ejecución
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Configuración de Kafka
    kafka_broker = "kafka:9092"
    kafka_topic  = "denuncias_sidpol"
    kafka_group  = "sisco_alert_group"

    kafka_props = {
        'bootstrap.servers': kafka_broker,
        'group.id':          kafka_group,
        'auto.offset.reset': 'latest'
    }

    deserialization_schema = SimpleStringSchema()
    kafka_consumer = FlinkKafkaConsumer(
        topics=kafka_topic,
        deserialization_schema=deserialization_schema,
        properties=kafka_props
    )

    stream = env.add_source(kafka_consumer)

    def alert_logic(json_str):
        try:
            data = json.loads(json_str)
            tipo_hecho  = data.get('tipo_hecho', '')
            modalidad   = data.get('modalidad', '')
            distrito    = data.get('distrito',   'Desconocido')
            departamento = data.get('departamento', '')
            cantidad    = int(data.get('cantidad', 1))

            # Alerta por tipo de hecho crítico
            for tipo_critico in TIPOS_CRITICOS:
                if tipo_critico.lower() in tipo_hecho.lower():
                    return (
                        f"[ALERTA CRITICA — SISCO] {tipo_hecho} ({modalidad}) detectado "
                        f"en {distrito}, {departamento}. Casos reportados: {cantidad}."
                    )

            # Alerta por volumen inusual (umbral arbitrario para el demo)
            if cantidad >= 50:
                return (
                    f"[ALERTA VOLUMEN — SISCO] Pico inusual de denuncias: "
                    f"{tipo_hecho} en {distrito}, {departamento} — {cantidad} casos en un registro."
                )

            return None
        except Exception:
            return None

    # Aplicar lógica de alerta
    alerts_stream = stream.map(alert_logic, output_type=Types.STRING()) \
                          .filter(lambda x: x is not None)

    alerts_stream.print()

    print(f"Escuchando tópico '{kafka_topic}' en busca de alertas...")
    env.execute("SISCO_Detector_Denuncias_Streaming")

if __name__ == '__main__':
    process_stream()
