# SISCO — Sistema Inteligente de Seguridad Ciudadana

Este proyecto es una plataforma de **Big Data y Streaming en tiempo real** que simula, procesa y visualiza denuncias policiales en Perú (basado en datos reales del SIDPOL del Ministerio del Interior).

El sistema lee millones de registros históricos, los normaliza, los envía a través de Apache Kafka como un flujo de eventos en vivo, y los muestra en un dashboard interactivo web.

---

## 🏗️ Arquitectura del Sistema

La arquitectura implementa un pipeline clásico de datos: **Bronze (Crudo) → Silver (Limpio) → Streaming (Kafka) → Visualización.**

1. **`data_raw/` (Capa Bronze)**
   - Contiene el dataset crudo original del SIDPOL: `DATASET_Denuncias_Policiales_Ene 2018 a Abr 2026.csv`.
   - Tiene millones de registros, valores nulos, y nombres de columnas técnicas.

2. **`prepare_data.py` (Proceso ETL Bronze → Silver)**
   - Un script de preparación de datos offline.
   - **Qué hace:** Lee el CSV crudo, renombra las columnas a nombres más amigables (ej. `DPTO_HECHO_NEW` → `departamento`), elimina filas vacías, capitaliza correctamente textos (ej. "LIMA" → "Lima"), y guarda un archivo optimizado en `data_clean/`.

3. **`data_clean/` (Capa Silver)**
   - Aquí se guarda `denuncias_sidpol_clean.csv`. Es el archivo que realmente consumimos, mucho más ligero y listo para enviar al dashboard.
   - También almacena `sisco_stats.json`, que guarda la persistencia del dashboard.

4. **`src/denuncias_producer.py` (Productor de Eventos Kafka)**
   - Es un script en Python que corre dentro del contenedor Docker `denuncias-producer`.
   - **Qué hace:** Abre el CSV de `data_clean/` y lee línea por línea. Cada registro (denuncia) lo transforma en un archivo JSON y lo envía al topic `denuncias_sidpol` en el broker local de Apache Kafka.
   - Tiene pausas de 0.5 a 1.5 segundos entre cada envío para **simular tráfico en tiempo real**.

5. **Apache Kafka + Zookeeper (El Bus de Mensajes)**
   - Corren como contenedores de Docker (definidos en `docker-compose.yml`).
   - Actúan como el sistema circulatorio del proyecto. Todo evento que emite el producer queda guardado en Kafka, y cualquier otra aplicación puede "suscribirse" para leer esos eventos.

6. **`app/dashboard.py` (Dashboard Backend / Consumidor Kafka)**
   - Es una aplicación web construida en **Flask (Python)**.
   - Tiene un hilo secundario (`kafka_listener`) que está constantemente conectado a Kafka consumiendo del topic `denuncias_sidpol`.
   - Cada evento que llega lo suma a contadores globales (Total, Alertas Críticas, Mapa, etc.) y mantiene una lista de los últimos 50 eventos.
   - Guarda los contadores periódicamente en `sisco_stats.json` para no perder la data al reiniciar.
   - Ofrece una API REST en `/api/data` para el Frontend.

7. **`app/templates/index.html` (Dashboard Frontend)**
   - La interfaz visual del usuario. Un SPA (Single Page Application) oscuro, moderno y dinámico.
   - Consulta `/api/data` cada 1.5 segundos.
   - Usa **Leaflet.js** para dibujar un **mapa de burbujas proporcionales**. Mapea los nombres de los departamentos a coordenadas de latitud/longitud en Perú. El tamaño y color (azul → naranja → rojo) de la burbuja dependen del porcentaje de denuncias de cada región en tiempo real.
   - Permite enviar "patrullas virtuales" haciendo POST a `/api/dispatch`.

---

## 🔄 El Flujo de Datos Paso a Paso (Data Lineage)

1. **ETL previo:** Corres `python prepare_data.py`. Esto transforma el CSV pesado en un CSV rápido y limpio en la carpeta `data_clean`.
2. **Arranque de Infra:** Corres `docker-compose up -d`. Esto levanta Kafka, Zookeeper, y el `denuncias-producer`.
3. **El Producer transmite:** En su contenedor, el producer empieza a leer el CSV limpio. Fila 1: "Robo en Lima", lo convierte a JSON `{"tipo_hecho":"Robo", "departamento":"LIMA"}` y lo inyecta a Kafka.
4. **Dashboard Backend escucha:** Ejecutas `python app/dashboard.py`. El listener de Kafka atrapa el JSON. Actualiza el contador `stats["LIMA"] += 1`. Si "Robo" es crítico, aumenta `stats["alertas_criticas"]`.
5. **Dashboard Frontend dibuja:** El navegador pide `/api/data`. Recibe el JSON completo, actualiza el HTML, aumenta la burbuja roja de Lima en el mapa de Leaflet, y desliza la nueva denuncia en la tabla con un pequeño efecto visual.
6. **Persistencia:** Si cierras el dashboard y lo vuelves a abrir, el backend lee el estado anterior de `sisco_stats.json` y la cuenta sigue exactamente donde la dejaste.

---

## 🛠️ Cómo Iniciar y Reiniciar el Proyecto

### 1. Iniciar desde Cero Absoluto
Si quieres purgar todo (borrar historial de Kafka) y arrancar como si fuera la primera vez:
```bash
# Apagar todo y borrar volumenes (destruye base de datos de kafka)
docker-compose down -v

# Limpiar y regenerar el archivo CSV limpio (si hubo cambios en el raw)
python prepare_data.py

# Levantar infraestructura (Kafka + Producer)
docker-compose up -d

# Ejecutar el backend del Dashboard
python app/dashboard.py
```

### 2. Reiniciar solo los contadores visuales del Dashboard
Si no quieres tocar Docker ni Kafka, pero quieres que el Dashboard visualmente vuelva a estar en 0:
- Entra al Dashboard Web ([http://localhost:5000](http://localhost:5000))
- Haz clic en el botón **REINICIAR A 0** arriba a la derecha. Esto purgará el `sisco_stats.json` y reseteará los arrays de memoria.

### 3. Matar puertos atascados
Si obtienes errores de que el puerto `5000` ya está en uso:
- Presiona `Ctrl + C` en tu terminal actual.
- Usa el comando `taskkill /F /IM python.exe` (cuidado, mata todos los procesos de Python).
- Vuelve a correr `python app/dashboard.py`.

---

## 📁 Estructura de Archivos Principal

```
Datos-Bigdata/
├── data_raw/                          # CSV crudo de MININTER
├── data_clean/                        # CSV limpio y JSON persistente de estadísticas
├── prepare_data.py                    # Script ETL offline (Bronze -> Silver)
├── docker-compose.yml                 # Define Kafka, Zookeeper y el Producer
├── src/
│   └── denuncias_producer.py          # Script de envío de mensajes hacia Kafka
├── app/
│   ├── dashboard.py                   # Servidor Web Flask (Backend & Kafka Consumer)
│   └── templates/
│       └── index.html                 # Frontend SPA HTML/CSS/JS (Leaflet map)
└── README.md                          # Esta documentación
```
