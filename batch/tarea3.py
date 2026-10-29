# -*- coding: utf-8 -*-
# Tarea 3 - Procesamiento de Datos con Apache Spark (Big Data - UNAD)
# Analisis en batch del dataset Online Retail II almacenado en HDFS

import os
import itertools
import matplotlib
matplotlib.use("Agg")  # La VM no tiene entorno grafico: las graficas se guardan como PNG
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F

# Inicializa la sesion de Spark
spark = SparkSession.builder.appName("Tarea3_OnlineRetailII").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Ruta del archivo .csv en HDFS y carpeta de salida de las graficas
file_path = "hdfs://localhost:9000/Tarea3/online_retail_II.csv"
out_dir = os.path.expanduser("~/graficas_tarea3")
os.makedirs(out_dir, exist_ok=True)


def titulo(texto):
    print("\n" + "=" * 70)
    print(texto)
    print("=" * 70)


def guardar(nombre):
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, nombre), dpi=120)
    plt.close()
    print(f"  Grafica guardada: {nombre}")


# ---------------------------------------------------------------
# 1. CARGA DEL DATASET
# ---------------------------------------------------------------
titulo("1. CARGA DEL DATASET DESDE HDFS")
df = (spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(file_path)
      .withColumnRenamed("Customer ID", "CustomerID"))

df.printSchema()
df.show(10, truncate=False)
total = df.count()
print(f"Total de registros cargados: {total:,}")

# ---------------------------------------------------------------
# 2. CALIDAD DE LOS DATOS
# ---------------------------------------------------------------
titulo("2. CALIDAD DE LOS DATOS")
print("Valores nulos por columna:")
df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()

duplicados = total - df.dropDuplicates().count()
cancelados = df.filter(F.col("Invoice").cast("string").startswith("C")).count()
print(f"Registros duplicados: {duplicados:,}")
print(f"Registros de facturas canceladas (inician con 'C'): {cancelados:,} ({cancelados / total * 100:.2f}%)")
print(f"Registros con cantidad <= 0: {df.filter(F.col('Quantity') <= 0).count():,}")
print(f"Registros con precio <= 0: {df.filter(F.col('Price') <= 0).count():,}")

# ---------------------------------------------------------------
# 3. LIMPIEZA Y TRANSFORMACION
# ---------------------------------------------------------------
titulo("3. LIMPIEZA Y TRANSFORMACION")
fecha_txt = F.col("InvoiceDate").cast("string")
dias = {1: "Domingo", 2: "Lunes", 3: "Martes", 4: "Miercoles",
        5: "Jueves", 6: "Viernes", 7: "Sabado"}
mapa_dias = F.create_map([F.lit(x) for x in itertools.chain(*dias.items())])

ventas = (df.dropDuplicates()
          .filter(~F.col("Invoice").cast("string").startswith("C"))
          .filter((F.col("Quantity") > 0) & (F.col("Price") > 0))
          .filter(F.col("Description").isNotNull())
          .withColumn("Description", F.trim(F.upper(F.col("Description"))))
          .withColumn("Fecha", F.coalesce(F.to_timestamp(fecha_txt),
                                          F.to_timestamp(fecha_txt, "M/d/yyyy H:mm")))
          .filter(F.col("Fecha").isNotNull())
          .withColumn("Total", F.round(F.col("Quantity") * F.col("Price"), 2))
          .withColumn("Anio", F.year("Fecha"))
          .withColumn("AnioMes", F.date_format("Fecha", "yyyy-MM"))
          .withColumn("Hora", F.hour("Fecha"))
          .withColumn("NumDia", F.dayofweek("Fecha"))
          .withColumn("DiaSemana", mapa_dias[F.col("NumDia")])
          .cache())

limpios = ventas.count()
print(f"Registros despues de la limpieza: {limpios:,}")
print(f"Registros eliminados: {total - limpios:,} ({(total - limpios) / total * 100:.2f}%)")
ventas.select("Invoice", "Description", "Quantity", "Price", "Total",
              "Country", "AnioMes", "DiaSemana", "Hora").show(10, truncate=False)

# ---------------------------------------------------------------
# 4. ESTADISTICAS DESCRIPTIVAS
# ---------------------------------------------------------------
titulo("4. ESTADISTICAS DESCRIPTIVAS")
ventas.select("Quantity", "Price", "Total").summary().show()
ingresos_total = ventas.agg(F.sum("Total")).first()[0]
print(f"Ingresos totales del periodo: GBP {ingresos_total:,.2f}")
print(f"Facturas unicas: {ventas.select('Invoice').distinct().count():,}")
print(f"Clientes unicos: {ventas.filter(F.col('CustomerID').isNotNull()).select('CustomerID').distinct().count():,}")
print(f"Productos unicos: {ventas.select('StockCode').distinct().count():,}")
print(f"Paises atendidos: {ventas.select('Country').distinct().count():,}")

# ---------------------------------------------------------------
# 5. INGRESOS POR PAIS
# ---------------------------------------------------------------
titulo("5. INGRESOS POR PAIS (TOP 10)")
por_pais = (ventas.groupBy("Country")
            .agg(F.round(F.sum("Total"), 2).alias("Ingresos"),
                 F.countDistinct("Invoice").alias("Facturas"))
            .orderBy(F.desc("Ingresos")))
por_pais.show(10, truncate=False)
uk = por_pais.filter(F.col("Country") == "United Kingdom").first()
if uk:
    print(f"Participacion del Reino Unido en los ingresos: {uk['Ingresos'] / ingresos_total * 100:.2f}%")

# ---------------------------------------------------------------
# 6. PRODUCTOS MAS VENDIDOS
# ---------------------------------------------------------------
titulo("6. TOP 10 PRODUCTOS MAS VENDIDOS (UNIDADES)")
top_productos = (ventas.groupBy("Description")
                 .agg(F.sum("Quantity").alias("Unidades"),
                      F.round(F.sum("Total"), 2).alias("Ingresos"))
                 .orderBy(F.desc("Unidades")))
top_productos.show(10, truncate=False)

# ---------------------------------------------------------------
# 7. VENTAS MENSUALES
# ---------------------------------------------------------------
titulo("7. INGRESOS MENSUALES")
mensual = (ventas.groupBy("AnioMes")
           .agg(F.round(F.sum("Total"), 2).alias("Ingresos"),
                F.countDistinct("Invoice").alias("Facturas"))
           .orderBy("AnioMes"))
mensual.show(30, truncate=False)

# ---------------------------------------------------------------
# 8. COMPORTAMIENTO POR DIA Y HORA
# ---------------------------------------------------------------
titulo("8. INGRESOS POR DIA DE LA SEMANA")
por_dia = (ventas.groupBy("NumDia", "DiaSemana")
           .agg(F.round(F.sum("Total"), 2).alias("Ingresos"),
                F.countDistinct("Invoice").alias("Facturas"))
           .orderBy("NumDia"))
por_dia.show(truncate=False)

titulo("9. FACTURAS POR HORA DEL DIA")
por_hora = (ventas.groupBy("Hora")
            .agg(F.countDistinct("Invoice").alias("Facturas"),
                 F.round(F.sum("Total"), 2).alias("Ingresos"))
            .orderBy("Hora"))
por_hora.show(24, truncate=False)

# ---------------------------------------------------------------
# 10. MEJORES CLIENTES
# ---------------------------------------------------------------
titulo("10. TOP 10 CLIENTES POR INGRESOS")
(ventas.filter(F.col("CustomerID").isNotNull())
 .groupBy("CustomerID", "Country")
 .agg(F.round(F.sum("Total"), 2).alias("Ingresos"),
      F.countDistinct("Invoice").alias("Facturas"))
 .orderBy(F.desc("Ingresos"))
 .show(10, truncate=False))

# ---------------------------------------------------------------
# 11. CONSULTAS: FILTRO Y ORDENAMIENTO (como en el Anexo 2)
# ---------------------------------------------------------------
titulo("11. TRANSACCIONES MAYORES A GBP 5.000")
grandes = ventas.filter(F.col("Total") > 5000).select("Invoice", "Description", "Quantity", "Total", "Country", "Fecha")
print(f"Cantidad de transacciones mayores a GBP 5.000: {grandes.count():,}")
grandes.show(10, truncate=False)

titulo("12. TRANSACCIONES ORDENADAS DE MAYOR A MENOR VALOR")
ventas.sort(F.col("Total").desc()).select("Invoice", "Description", "Quantity", "Price", "Total", "Country").show(10, truncate=False)

# ---------------------------------------------------------------
# 13. OPERACION CON RDD: map + reduceByKey
# ---------------------------------------------------------------
titulo("13. RDD: FACTURAS POR PAIS CON map() Y reduceByKey()")
rdd = ventas.select("Country", "Invoice").distinct().rdd
conteo = (rdd.map(lambda fila: (fila["Country"], 1))
          .reduceByKey(lambda a, b: a + b)
          .sortBy(lambda x: -x[1]))
for pais, n in conteo.take(5):
    print(f"  {pais}: {n:,} facturas")

# ---------------------------------------------------------------
# 14. VISUALIZACION DE RESULTADOS
# ---------------------------------------------------------------
titulo("14. GENERACION DE GRAFICAS")

pdf = por_pais.filter(F.col("Country") != "United Kingdom").limit(10).toPandas()
plt.figure(figsize=(10, 6))
plt.barh(pdf["Country"][::-1], pdf["Ingresos"][::-1], color="#1f77b4")
plt.title("Top 10 paises por ingresos (sin Reino Unido)")
plt.xlabel("Ingresos (GBP)")
guardar("01_ingresos_por_pais.png")

pdf = mensual.toPandas()
plt.figure(figsize=(12, 6))
plt.plot(pdf["AnioMes"], pdf["Ingresos"], marker="o", color="#2ca02c")
plt.title("Ingresos mensuales (dic 2009 - dic 2011)")
plt.xlabel("Mes")
plt.ylabel("Ingresos (GBP)")
plt.xticks(rotation=60)
plt.grid(alpha=0.3)
guardar("02_ingresos_mensuales.png")

pdf = top_productos.limit(10).toPandas()
plt.figure(figsize=(11, 6))
plt.barh(pdf["Description"][::-1], pdf["Unidades"][::-1], color="#ff7f0e")
plt.title("Top 10 productos mas vendidos (unidades)")
plt.xlabel("Unidades vendidas")
guardar("03_top_productos.png")

pdf = por_dia.toPandas()
plt.figure(figsize=(9, 5))
plt.bar(pdf["DiaSemana"], pdf["Ingresos"], color="#9467bd")
plt.title("Ingresos por dia de la semana")
plt.ylabel("Ingresos (GBP)")
guardar("04_ingresos_por_dia.png")

pdf = por_hora.toPandas()
plt.figure(figsize=(10, 5))
plt.bar(pdf["Hora"], pdf["Facturas"], color="#d62728")
plt.title("Numero de facturas por hora del dia")
plt.xlabel("Hora")
plt.ylabel("Facturas")
plt.xticks(pdf["Hora"])
guardar("05_facturas_por_hora.png")

titulo("ANALISIS FINALIZADO")
print(f"Graficas guardadas en: {out_dir}")
input("Consola web de Spark activa en http://192.168.101.101:4040 - presione Enter para finalizar...")
spark.stop()
