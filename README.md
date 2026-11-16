# Mantenimiento Predictivo en Líneas de Soldadura Automotriz mediante Arquitectura Big Data en Tiempo Real

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

1. **Levantar infraestructura y crear rutas/tópico:**
   ```bash
   docker compose up -d
   docker exec namenode hdfs dfs -mkdir -p /scada/processed /scada/metrics /scada/anomalies /scada/checkpoints
   docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic sensores_scada --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
