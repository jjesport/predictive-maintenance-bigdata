# Mantenimiento Predictivo en Líneas de Soldadura Automotriz mediante Arquitectura Big Data en Tiempo Real
<img width="1024" height="572" alt="image" src="https://github.com/user-attachments/assets/0fbab0b0-8ad4-417a-a876-4e6ff3d31044" />

Proyecto académico de ingeniería de datos para el monitoreo continuo y detección estadística de anomalías sobre telemetría industrial SCADA (**I-BiDaaS / Centro Ricerche Fiat - CRF**).

## 1. Arquitectura de la Solución

`Archivos CSV SCADA (CRF)` -> `Simulador Python Producer` -> `Apache Kafka (KRaft: sensores_scada)` -> `Apache Spark Structured Streaming` -> `Apache Hadoop HDFS (Parquet)` -> `Detección de Anomalías (Z-score) & Visualización`

### Stack Tecnológico
- **Ingestión:** Python 3 (`kafka-python-ng`) con control de tasa (`msg/s`) particionado por `sensor_id`.
- **Mensajería en Streaming:** Apache Kafka 3.8.1 (KRaft).
- **Procesamiento Distribuido:** Apache Spark 3.5.3 (Structured Streaming + Spark SQL).
- **Almacenamiento Distribuido:** Apache Hadoop HDFS 3.2.1 (NameNode + DataNode) en formato columnar **Apache Parquet (Snappy)**.
- **Orquestación:** Docker Compose.

## 2. Estructura del Repositorio

- `docker-compose.yml`: Despliegue del clúster (Kafka, NameNode, DataNode, Spark).
- `producer/kafka_producer.py`: Simulador SCADA hacia Kafka con soporte de encoding y tasa.
- `processing/spark_streaming.py`: Pipeline Spark Structured Streaming hacia HDFS (Parquet).
- `analysis/anomaly_detection.py`: Detección de anomalías (`|Z| > 3.0` por sensor/unidad) y generación de gráficas.
- `data/sample_raw.csv`: Muestra de 50 registros crudos del dataset SCADA.
- `results/`: Resúmenes cuantitativos (`metrics_overview.json`, `summary_by_unit.csv`, `top_anomalous_sensors.csv`), muestras de 50 registros por etapa (`sample_*.csv`) y gráficas ejecutivas (`fig1_*.png`, `fig2_*.png`, `fig3_*.png`).

## 3. Guía de Ejecución

**Prerrequisitos:** Docker y Docker Compose activos, Python 3.9+ con `kafka-python-ng` (`pip install kafka-python-ng`), y el dataset SCADA descomprimido en `data/raw/` (o usar `data/sample_raw.csv` para pruebas ligeras).

```bash
# 1. Levantar contenedores del clúster (Kafka, NameNode, DataNode, Spark)
docker compose up -d

# 2. Crear estructura de directorios en HDFS y tópico en Kafka
docker exec namenode hdfs dfs -mkdir -p /scada/processed /scada/metrics /scada/anomalies /scada/checkpoints
docker exec namenode hdfs dfs -chmod -R 777 /scada
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic sensores_scada --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# 3. Iniciar Spark Structured Streaming en segundo plano y verificar log
docker exec -d spark /bin/bash -c "/opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/spark/work-dir/processing/spark_streaming.py --starting-offsets earliest > /tmp/spark_stream.log 2>&1"
docker exec spark tail -n 20 /tmp/spark_stream.log

# 4. Ejecutar el simulador de telemetría SCADA hacia Kafka
python3 producer/kafka_producer.py --month 2020_01 --rate 2500 --max-events 25000

# 5. (Esperar 15-20 s) Verificar archivos Parquet almacenados en HDFS
docker exec namenode hdfs dfs -ls -R /scada/processed /scada/metrics

# 6. Ejecutar detección estadística de anomalías (|Z| > 3.0) y exportar reportes/gráficas en results/
docker exec spark /opt/spark/bin/spark-submit /opt/spark/work-dir/analysis/anomaly_detection.py

# 7. Apagar el clúster al finalizar (opcional)
docker compose down
