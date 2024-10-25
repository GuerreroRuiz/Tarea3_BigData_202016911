#Importamos librerias necesarias
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import to_date


# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()

# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3BD/dataset.csv'

# Lee el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)

#imprimimos el esquema
print("Esquema del DataFrame")
df.printSchema()

# Muestra las primeras 10 filas del DataFrame
print("Primeras 10 filas del Dataframe")
df.show(10)

# Estadisticas básicas
print("Estadisticas básicas")
df.summary().show()

# Consulta1: Filtrar transacciones  realizadas desde Colombia
print("Consulta1: Transacciones realizadas desde Colombia")
transacciones_colombia = df.filter(F.col('Country') == 'Colombia').select(
    'Transaction_ID', 'Product', 'Category', 'Value', 'Date'
)

# Mostrar el resultado de Consulta1
transacciones_colombia.show()


# Consulta2: Agrupar por 'Country' y realizar un conteo de las transacciones
print("Consulta2: Transacciones por Pais")
transacciones_por_pais = df.groupBy('Country').count().orderBy(F.desc('count'))

# Mostrar el resultado de Consulta2
transacciones_por_pais.show()



#Consulta3: Sumar la cantidad de productos por categoría
print("Consulta3: Cantidad de productos por categoria")
productos_por_categoria = df.groupBy('Category').agg(F.sum('Quantity').alias('Total_Quantity'))

# Mostrar el resultado de Consulta3
productos_por_categoria.show()


#Consulta4: Top 5 productos mas enviados por cantidad total
print ("Consulta4: Top 5 productos mas enviados")
top_productos = df.groupBy('Product').agg(F.sum('Quantity').alias('Total_Quantity')).orderBy(F.desc('Total_Quantity'))

# Mostrar el resultado de Consulta4
top_productos.show(5)


#Consulta5: Distribuciones  de valor de envio por método de envío
print ("Consulta5: Distribuciones de valor de envio por método de envio")
valor_por_envio = df.groupBy('Shipping_Method').agg(
    F.avg('Value').alias('Average_Value'),
    F.min('Value').alias('Min_Value'),
    F.max('Value').alias('Max_Value')
)

# Mostrar el resultado de Consulta5
valor_por_envio.show()



#Consulta6: Transacciones por método de envio Sea y categoria Electronics
# Filtrar por método de envío "sea", categoría "electronics"
envios_filtrados = df.filter(
    (F.col('Shipping_Method') == 'Sea') &
    (F.col('Category') == 'Electronics')
)

# Mostrar el resultado de Consulta6
print("Consulta6: Transacciones Sea y Electronics")
envios_filtrados.show()

#Consulta7: Peso promedio de productos por método de envio
#Filtrar por metodo de envio y realizar un promedio del peso total de las transacciones
peso_promedio = df.groupBy('Shipping_Method').agg(
    F.avg('Weight').alias('Peso_Promedio')
).orderBy(F.desc('Peso_Promedio'))

#Mostrar el resultado de Consulta7
print("Consulta7: Peso promedio de productos por método de envío:")
peso_promedio.show(truncate=False)



#Consulta8: Total de importaciones y exportaciones por pais
#Filtrar por pais y realizar conteo de transacciones totales
importaciones_exportaciones = df.groupBy('Country', 'Import_Export').count()

#Mostrar el resultado de Consulta8
print("Consulta8: Total de importaciones/exportaciones por país:")
importaciones_exportaciones.show(truncate=False)
