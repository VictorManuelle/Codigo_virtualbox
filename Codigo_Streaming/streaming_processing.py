from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

def create_streaming_session():
    """Crear sesión de Spark Streaming"""
    return SparkSession.builder \
        .appName("GasNaturalStreaming") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/streaming_checkpoint") \
        .getOrCreate()

def main():
    spark = create_streaming_session()
    
    # Schema para los datos de streaming
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("empresa", StringType(), True),
        StructField("municipio", StringType(), True),
        StructField("estrato", StringType(), True),
        StructField("cargo_fijo", DoubleType(), True),
        StructField("rango_0", DoubleType(), True),
        StructField("rango_21", DoubleType(), True),
        StructField("consumo_simulado", IntegerType(), True)
    ])
    
    print(" Iniciando procesamiento en tiempo real...")
    print(" Leyendo datos desde Kafka topic: gas-tarifas-stream")
    
    #  Leer stream desde Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "gas-tarifas-stream") \
        .option("startingOffsets", "latest") \
        .load()
    
    #  Procesar datos del stream
    processed_df = kafka_df \
        .select(
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), schema).alias("data")
        ) \
        .select(
            "kafka_timestamp",
            "data.*"
        ) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withColumn("timestamp_parsed", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("costo_total_estimado", 
                   col("cargo_fijo") + (col("consumo_simulado") * col("rango_0")))
    
    
    
    #  Estadísticas por empresa (ventana de 1 minuto)
    stats_empresa = processed_df \
        .withWatermark("timestamp_parsed", "2 minutes") \
        .groupBy(
            window(col("timestamp_parsed"), "1 minute"),
            col("empresa")
        ) \
        .agg(
            avg("cargo_fijo").alias("avg_cargo_fijo"),
            avg("rango_0").alias("avg_rango_0"),
            count("*").alias("total_eventos"),
            avg("costo_total_estimado").alias("avg_costo_total"),
            min("cargo_fijo").alias("min_cargo_fijo"),
            max("cargo_fijo").alias("max_cargo_fijo")
        )
    
    #  Conteo de eventos por estrato (ventana de 30 segundos)
    conteo_estratos = processed_df \
        .withWatermark("timestamp_parsed", "1 minute") \
        .groupBy(
            window(col("timestamp_parsed"), "30 seconds"),
            col("estrato")
        ) \
        .agg(
            count("*").alias("conteo_eventos"),
            avg("cargo_fijo").alias("cargo_fijo_promedio"),
            avg("rango_0").alias("rango_0_promedio")
        )
    
    # Alertas de cargos fijos anómalos
    alertas = processed_df \
        .withWatermark("timestamp_parsed", "1 minute") \
        .filter(col("cargo_fijo") > 40000) \
        .select(
            col("timestamp_parsed"),
            col("empresa"),
            col("municipio"),
            col("estrato"),
            col("cargo_fijo"),
            lit("ALERTA: CARGO FIJO ALTO").alias("tipo_alerta")
        )
    
    #  Visualizar resultados en consola
    print(" Iniciando visualización de resultados en tiempo real...")
    print("  Presiona Ctrl+C para detener el streaming\n")
    
    # Consulta 1: Estadísticas por empresa
    query1 = stats_empresa \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 10) \
        .start()
    
    # Consulta 2: Conteo por estrato
    query2 = conteo_estratos \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 8) \
        .start()
    
    # Consulta 3: Alertas
    query3 = alertas \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    # 5. Esperar terminación
    try:
        query1.awaitTermination()
        query2.awaitTermination()
        query3.awaitTermination()
    except KeyboardInterrupt:
        print("\n Deteniendo streaming...")
        query1.stop()
        query2.stop()
        query3.stop()

if __name__ == "__main__":
    main()
