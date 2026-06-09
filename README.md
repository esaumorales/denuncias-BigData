# SISCO — Sistema Inteligente de Seguridad Ciudadana (Big Data)

Este documento explica en detalle la estructura completa del proyecto, para qué sirve cada componente y el **orden exacto de ejecución** de todo el pipeline (desde la limpieza de los datos crudos hasta la visualización en tiempo real en el Dashboard web).

---

## 📁 1. Estructura de Carpetas y Archivos

Cada carpeta tiene un propósito específico dentro de nuestra **Arquitectura Lambda** (que mezcla procesamiento Batch histórico y procesamiento Streaming en tiempo real).

### `data/` (Capa de Datos Locales)
* **`raw/` (Bronce):** Aquí vive el archivo original en formato `.csv` descargado directamente del Ministerio del Interior (datos sucios, columnas mal escritas, archivos pesados de millones de filas).
* **`clean/` (Plata):** Aquí se guardan los datos limpios en formato **Parquet**. Parquet es el estándar de oro en Big Data porque pesa muchísimo menos y es extremadamente rápido de leer. Aquí también se guarda `sisco_stats.json` que funciona como nuestra base de datos local para mantener los contadores del Dashboard.

### `scripts/` (ETL de Preparación y Carga)
* **`prepare_data.py`:** Es el script que convierte el CSV crudo a Parquet limpio. Se ejecuta una sola vez al principio del proyecto.
* **`init_historico.py`:** Simula la carga masiva (Batch) del historial. Cuando el usuario da clic en "REINICIAR A 0", este script inyecta de golpe todos los datos de 2018 a 2025 para que el Dashboard no empiece vacío.

### `streaming/` (La Capa de Velocidad / Speed Layer)
* **`producer.py`:** Simula el sistema en vivo de las comisarías del Perú. Se queda "escuchando" el Parquet y lanza únicamente los casos del año **2026** uno por uno hacia el tópico de Kafka llamado `denuncias_sidpol`, simulando un flujo en tiempo real (Streaming).
* **`flink_job.py`:** El cerebro analítico en tiempo real. Este script de **Apache Flink** captura todo lo que cae en Kafka, detecta si es una "Alerta Crítica" (por ejemplo, picos inusuales de violencia o extorsión), empaqueta el resultado y lo manda a un segundo tópico de Kafka llamado `eventos_procesados`.

### `batch/` (La Capa de Volumen / Batch Layer)
* **`spark_etl.py`:** El cerebro analítico histórico. Usando **Apache Spark**, este script procesa la totalidad del bloque histórico (millones de registros) en memoria distribuida para encontrar patrones a largo plazo, y guarda el reporte final en nuestro Data Lake (MinIO).

### `app/` (El Dashboard y Consumidor / Serving Layer)
* **`main.py`:** El archivo principal de la aplicación web (Flask). Inicia el servidor de la página web en el puerto 5000.
* **`routes.py`:** Controla la lógica de navegación (qué pasa cuando entras a la ruta `/` o a `/api/data`).
* **`kafka_services.py`:** Es el hilo consumidor constante. Está suscrito al tópico de Kafka `eventos_procesados` (los que Flink ya analizó). A medida que recibe datos, actualiza la memoria.
* **`stats_store.py`:** La memoria viva del Dashboard. Suma los contadores, arma el top de departamentos y guarda el mapa de ubicaciones.
* **`templates/`:** Contiene los archivos HTML (`index.html` y `denuncia.html`). El mapa de calor, los gráficos y el diseño visual están programados aquí usando HTML, CSS, JavaScript, Chart.js y Leaflet.js.

### `config/`
* **`settings.py`:** Almacena variables de entorno, como la IP del servidor Kafka, los nombres de los tópicos y rutas de archivos, para no tener que escribirlas manualmente en todos lados.

### Archivos de la Raíz (DevOps & Orquestación)
* **`docker-compose.yml`:** El manifiesto de infraestructura. Descarga y enciende los motores de Big Data: Zookeeper, Kafka, MinIO, Jupyter-PySpark, JobManager y TaskManager de Flink.
* **`Dockerfile` y `Dockerfile.flink`:** Instrucciones de instalación para crear máquinas virtuales (contenedores) a nuestra medida.
* **`orquestador.py`:** El gran controlador automático. Ejecuta todos los comandos en el orden correcto para que el usuario no tenga que levantar las cosas una por una a mano.

---

## ⚙️ 2. Orden de Ejecución (Paso a Paso)

Si fueras a ejecutar el proyecto manualmente desde cero, este es el flujo lógico y secuencial (que nuestro `orquestador.py` hace por ti en la vida real):

### FASE 1: Limpieza Inicial (ETL Bronce a Plata)
1. Ejecutas **`python scripts/prepare_data.py`**
   * *Acción:* Agarra el CSV sucio de 6.8 millones de filas y crea un archivo `denuncias_sidpol_clean.parquet`. (Solo se hace 1 vez).

### FASE 2: Despliegue de Infraestructura
2. Ejecutas **`docker-compose up -d`**
   * *Acción:* Enciende todo el cluster de servidores Big Data (Kafka, Flink, Spark, MinIO). 

### FASE 3: Inicialización del Histórico (La Base)
3. Ejecutas **`python scripts/init_historico.py`**
   * *Acción:* Carga instantáneamente al backend todo el rango histórico (2018 a 2025) para que el mapa de calor y las gráficas comiencen llenas, simulando lo que ya pasó hasta ayer.

### FASE 4: El Motor de Reglas en Tiempo Real (Flink)
4. Ejecutas el Job de Flink: **`docker exec flink-jobmanager bash -c "flink run -py /tmp/flink_job.py"`**
   * *Acción:* Flink se queda corriendo en las sombras, conectándose a Kafka, esperando que comiencen a caer las denuncias nuevas.

### FASE 5: Levantar el Dashboard (UI)
5. Ejecutas **`python app/main.py`**
   * *Acción:* Prende la interfaz web (`http://localhost:5000`) y activa el consumidor de Kafka en el backend para atrapar todo lo que Flink envíe.

### FASE 6: Activar el Caño del Streaming (Live Data)
6. Ejecutas **`python streaming/producer.py`**
   * *Acción:* Este script se comporta como la "vida real". Empieza a enviar las denuncias del año **2026** hacia Kafka gota a gota.
   * Flink las recibe de Kafka, las evalúa, y se las envía al Dashboard (Flask), quien las dibuja mágicamente en la pantalla.

### FASE 7: El Proceso Batch de Fondo
7. Ejecutas **`docker exec jupyter-pyspark spark-submit /home/jovyan/work/batch/spark_etl.py`**
   * *Acción:* Como un proceso nocturno, PySpark barre todo el historial para generar resúmenes estáticos avanzados en el Data Lake de MinIO, sin interrumpir el flujo del streaming.

*(Nota: Nuestro script `orquestador.py` hace los pasos 2 al 7 automáticamente con solo un clic).*

---

## 🎨 3. Prompt para Generar Arquitectura (Midjourney / DALL-E)

Si necesitas generar un diagrama espectacular para tu presentación, puedes usar el siguiente Prompt en cualquier IA de generación de imágenes:

> **Prompt:**
> *A professional and highly detailed isometric Big Data architecture diagram for a system called "SISCO". The diagram follows the Lambda Architecture pattern. On the left side, show a data source labeled "SIDPOL (CSV)". From there, the data flows into a central message broker labeled "Apache Kafka" (streaming bus). Inside Kafka, show two TOPICS labeled "denuncias_sidpol" and "eventos_procesados". From Kafka, the data splits into two paths (Speed Layer and Batch Layer). The top path (Speed Layer) goes to a node labeled "Apache Flink (Real-Time Rules Engine)" which points to a sleek "Web Dashboard (Flask)". The bottom path (Batch Layer) goes to a node labeled "Apache Spark (ETL Processing)", which points to a data lake storage bucket labeled "MinIO Data Lake". Use a clean, modern cyber-tech color palette with glowing blue, neon orange, and dark background. Include small technology logos for Kafka, Flink, Spark, and Python. Vector illustration style, highly professional, suitable for an enterprise presentation, clear data flow lines with glowing arrows.*
