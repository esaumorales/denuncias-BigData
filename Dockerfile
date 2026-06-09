FROM python:3.9-slim

WORKDIR /app

# Instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# El código se monta por volumen en docker-compose, o se puede copiar aquí:
COPY streaming/producer.py streaming/

# Definir comando por defecto
CMD ["python", "streaming/producer.py"]
