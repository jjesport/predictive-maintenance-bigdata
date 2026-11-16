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
- `results/`: Resúmenes cuantitativos (`metrics_overview.json`, `summary_by_unit.csv`, `top_anomalous_sensors.csv`), muestras de 50 registros por etapa (`sample_*.csv`) - gráficas ejecutivas (`fig1_*.png`, `fig2_*.png`, `fig3_*.png`).

## 3. Guía de Ejecución

1. **Levantar infraestructura y crear rutas/tópico:**
   ```bash
   docker compose up -d
   docker exec namenode hdfs dfs -mkdir -p /scada/processed /scada/metrics /scada/anomalies /scada/checkpoints
   docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic sensores_scada --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```
2. **Iniciar Spark Structured Streaming:**
   ```bash
   docker exec -d spark /bin/bash -c "/opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/spark/work-dir/processing/spark_streaming.py --starting-offsets earliest > /tmp/spark_stream.log 2>&1"
   ```
3. **Ejecutar el simulador de sensores:**
   ```bash
   python3 producer/kafka_producer.py --month 2020_01 --rate 2500 --max-events 25000
   ```B�4. **Ejecutar detección de anomalías y exportar resultados:**
   ```bash
   docker exec spark /opt/spark/bin/spark-submit /opt/spark/work-dir/analysis/anomaly_detection.py
   ```

## 4. Resultados Obtenidos (Muestra Validada de 25,000 Eventos)

- **Registros curados en HDFS:** `25,000`
- **Ventanas temporales (1 min) calculadas:** `1,198`
- **Sensores físicos activos:** `106`
- **Anomalías estadísticas detectadas (`|Z| > 3.0`):** `302` (`1.208%` del flujo)
- **Sensores con mayor incidencia:** Sensor `92` (`mg`, Aceleración) con `17` anomalías (`Z_max = 17.54`) y Sensor `110` (`mg`) con `17` anomalías (`3.99%` de sus lecturas).
