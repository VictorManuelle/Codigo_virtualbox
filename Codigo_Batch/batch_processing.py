from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
import requests
import os

def download_dataset():
    """Descargar datos desde la fuente original"""
    print(" Descargando datos desde datos.gov.co...")
    url = 'https://www.datos.gov.co/resource/ek3f-5wn4.csv'
    
    os.makedirs('data/raw', exist_ok=True)
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            with open('data/raw/gas_tarifas.csv', 'w', encoding='utf-8') as f:
                f.write(response.text)
            print(" Datos descargados exitosamente")
            return True
        else:
            print(f" Error descargando: {response.status_code}")
            return False
    except Exception as e:
        print(f" Error: {e}")
        return False

def main():
    # Crear sesión de Spark
    spark = SparkSession.builder \
        .appName("GasNaturalBatch") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    # Cargar desde fuente original
    if not download_dataset():
        return
    
    # Leer datos
    df = spark.read.format('csv') \
        .option('header', 'true') \
        .option('inferSchema', 'true') \
        .load('data/raw/gas_tarifas.csv')
    
    print("=== DATOS ORIGINALES ===")
    df.printSchema()
    df.show(10)
    
    
    print("=== LIMPIEZA Y TRANSFORMACIÓN ===")
    
    # Limpieza de datos
    df_clean = df \
        .filter(F.col('CARGO_FIJO').isNotNull()) \
        .filter(F.col('CARGO_FIJO') > 0) \
        .na.fill({"ESTRATO": "NO_ESPECIFICADO"}) \
        .withColumn("FECHA", F.concat(F.col("ANO"), F.lit("-"), 
                                     F.lpad(F.col("MES"), 2, "0"), F.lit("-01"))) \
        .withColumn("FECHA", F.to_date(F.col("FECHA"), "yyyy-MM-dd")) \
        .withColumn("EMPRESA_LIMPIA", F.trim(F.upper(F.col("EMPRESA")))) \
        .withColumn("COSTO_ESTIMADO_50M3", 
                   F.col("CARGO_FIJO") + (20 * F.col("RANGO_0")) + (30 * F.col("RANGO_21")))
    
    
    print("=== ANÁLISIS EXPLORATORIO (EDA) ===")
    
    # Estadísticas básicas
    print(" Estadísticas descriptivas:")
    df_clean.select("CARGO_FIJO", "RANGO_0", "RANGO_21").summary().show()
    
    # Conteo por empresa
    print(" Conteo por empresa:")
    df_clean.groupBy("EMPRESA").count().orderBy(F.desc("count")).show(truncate=False)
    
    # Promedios por estrato
    print(" Promedios por estrato:")
    df_clean.groupBy("ESTRATO").agg(
        F.avg("CARGO_FIJO").alias("CARGO_FIJO_PROMEDIO"),
        F.avg("RANGO_0").alias("RANGO_0_PROMEDIO"),
        F.avg("RANGO_21").alias("RANGO_21_PROMEDIO"),
        F.count("*").alias("TOTAL_REGISTROS")
    ).orderBy("ESTRATO").show()
    
    # Evolución temporal
    print(" Evolución anual:")
    df_clean.groupBy("ANO").agg(
        F.avg("CARGO_FIJO").alias("CARGO_FIJO_PROMEDIO"),
        F.min("CARGO_FIJO").alias("CARGO_FIJO_MIN"),
        F.max("CARGO_FIJO").alias("CARGO_FIJO_MAX")
    ).orderBy("ANO").show()
    
    # Top municipios con mayor cargo fijo
    print(" Top 10 municipios con mayor cargo fijo:")
    df_clean.groupBy("MUNICIPIOS").agg(
        F.avg("CARGO_FIJO").alias("CARGO_FIJO_PROMEDIO")
    ).orderBy(F.desc("CARGO_FIJO_PROMEDIO")).limit(10).show(truncate=False)
    
    # Almacenar resultados procesados
    print(" Almacenando resultados procesados...")
    df_clean.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet("data/processed/gas_tarifas_cleaned")
    
    print(" Procesamiento BATCH completado exitosamente!")
    spark.stop()

    # Es para evitar que se ejecute todo el programa cuando solo se quiere usar una parte
if __name__ == "__main__":
    main()
