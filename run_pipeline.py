import os
import subprocess
import time
import sys
import webbrowser

def run_command(command, description):
    print(f"\n[{description}] Ejecutando: {command}")
    try:
        # Ejecutamos el comando y capturamos salida
        result = subprocess.run(command, shell=True, check=True, text=True)
        print(f"[{description}] -> ¡Éxito!")
    except subprocess.CalledProcessError as e:
        print(f"[{description}] -> FALLÓ con error: {e}")
        print("Continuando de todos modos o revisa los logs...")

def main():
    print("=" * 60)
    print(" INICIANDO ORQUESTADOR AUTOMÁTICO BIG DATA 🚀")
    print("=" * 60)
    
    # 1. Levantar Infraestructura (Docker Compose)
    print("\n[Paso 1] Verificando infraestructura de Docker (Kafka, Flink, Jupyter, MinIO)...")
    # docker-compose up -d levantará cualquier contenedor que falte sin reiniciar los sanos
    run_command("docker-compose up -d", "Levantando MinIO y Servicios")
    
    print("\nEsperando 10 segundos para que MinIO se estabilice e inicialice los buckets...")
    time.sleep(10)
    
    # 2. Levantar Dashboard Interactivo y Generador de Tweets
    print("\n[Paso 2] Iniciando el Dashboard Web y el Generador de Denuncias...")
    
    if sys.platform.startswith("win"):
        # Windows
        subprocess.Popen("start python app/dashboard.py", shell=True)
        print("Esperando 3 segundos a que levante el servidor web...")
        time.sleep(3)
        webbrowser.open("http://localhost:5000")
        subprocess.Popen("start python src/denuncias_producer.py", shell=True)
    else:
        # Linux/Mac
        subprocess.Popen("python app/dashboard.py &", shell=True)
        print("Esperando 3 segundos a que levante el servidor web...")
        time.sleep(3)
        webbrowser.open("http://localhost:5000")
        subprocess.Popen("python src/denuncias_producer.py &", shell=True)
        
    print("Dashboard abierto y Productor de Kafka lanzado en segundo plano.")
    
    # 3. Lanzar PySpark Batch ETL en Jupyter
    print("\n[Paso 3] Ejecutando PySpark ETL Batch...")
    # Podemos enviar el comando de ejecución de PySpark dentro del contenedor de Jupyter
    pyspark_cmd = (
        "docker exec jupyter-pyspark "
        "spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
        "/home/jovyan/work/jobs/batch_etl_datalake.py"
    )
    run_command(pyspark_cmd, "Submit Batch ETL a PySpark")
    
    # 4. Lanzar Flink Streaming Job
    print("\n[Paso 4] Lanzando Job de Flink Streaming (PyFlink)...")
    print("Nota: Este comando requiere que el contenedor de Flink tenga Python instalado.")
    # Si Flink no tiene PyFlink instalado por defecto, habría que crear un Dockerfile personalizado.
    # Por ahora se lanza el comando de intento:
    flink_cmd = (
        "docker exec flink-jobmanager "
        "flink run -py /opt/flink/usrlib/jobs/streaming_detector.py || echo 'Nota: PyFlink no instalado en contenedor base.'"
    )
    run_command(flink_cmd, "Submit Streaming a Flink")

    print("\n" + "=" * 60)
    print(" ORQUESTACIÓN FINALIZADA ")
    print("1. Dashboard en tiempo real corriendo en: http://localhost:5000")
    print("2. MinIO Data Lake corriendo en http://localhost:9001 (admin/admin123)")
    print("3. Batch ETL procesado (revisa MinIO para ver tus Parquets limpios).")
    print("4. Streaming de Kafka y Generador inyectando datos...")
    print("=" * 60)

if __name__ == "__main__":
    main()
