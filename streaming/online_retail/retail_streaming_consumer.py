from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, sum as _sum, round as _round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

# Consumidor: Spark Structured Streaming calcula las ventas por pais en ventanas de 1 minuto
spark = SparkSession.builder \
    .appName("RetailKafkaStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Esquema de los mensajes JSON que envia retail_producer.py
schema = StructType([
    StructField("invoice", StringType()),
    StructField("description", StringType()),
    StructField("quantity", IntegerType()),
    StructField("price", DoubleType()),
    StructField("total", DoubleType()),
    StructField("country", StringType()),
    StructField("customer_id", StringType()),
    StructField("original_date", StringType()),
    StructField("timestamp", LongType())
])

# Lector de streaming desde el topic ventas_retail
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ventas_retail") \
    .load()

# Parsear el JSON y convertir el timestamp a tipo fecha
ventas = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast("timestamp"))

# Ingresos, unidades y lineas de venta por pais en ventanas de 1 minuto
ventas_por_pais = ventas \
    .groupBy(window(col("event_time"), "1 minute"), col("country")) \
    .agg(count("*").alias("lineas"),
         _sum("quantity").alias("unidades"),
         _round(_sum("total"), 2).alias("ingresos_gbp")) \
    .orderBy(col("window").desc(), col("ingresos_gbp").desc())

# Mostrar los resultados en la consola
query = ventas_por_pais \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 15) \
    .start()

query.awaitTermination()
