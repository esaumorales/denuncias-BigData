import json
from kafka import KafkaProducer

try:
    print("Intentando conectar a Kafka...")
    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        request_timeout_ms=5000
    )
    print("Conectado exitosamente!")
    producer.close()
except Exception as e:
    import traceback
    print("Fallo conexion:")
    traceback.print_exc()
