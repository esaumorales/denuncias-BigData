FROM python:3.9-slim

WORKDIR /app

# Instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente
COPY src/denuncias_producer.py src/

# Definir comando por defecto
CMD ["python", "src/denuncias_producer.py"]
