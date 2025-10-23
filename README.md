Readme
# Todo se realiza desde un entorno virtual en Virtualbox

# Archivos usados
Virtualbox 7.2.2
Ubuntu-22.04.5
Kafka 2.12-3.7.2
Spark 3.5.6 
Putty

# Codigo_virtualbox

# Codigos que se utilizo en cada puty abierto

# Para batch en una consola
Se introduce este codigo: nano batch_processing.py, para crear un archivo y ahi colocar el codigo python desde la consola.

Este codigo para el procesamiento de datos batch: spark-submit batch_processing.py

# Para Kafka en una nueva consola
Se introduce este codigo: nano kafka_generator.py, para crear un archivo y ahi colocar el codigo python desde la consola.

Este codigo para el generador de datos kafka: python kafka_generator.py

# Para Streaming en una nueva consola

Se introduce este codigo: nano streaming_processing.py, para crear un archivo y ahi colocar el codigo python desde la consola.

Este codigo para iniciar spark-streaming: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 streaming_processing.py
