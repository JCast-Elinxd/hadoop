import sys, os, shutil
from pyspark import SparkContext
from pyspark.sql import SparkSession

# sudo apt update
# sudo apt install openjdk-17-jdk
# export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# export PATH=$JAVA_HOME/bin:$PATH
# spark-submit 1_topsectorperyear.py input/* output_1_topsector

spark = SparkSession.builder.appName("Punto1").getOrCreate()
sc = spark.sparkContext

# "ALL" "DEBUG" "INFO" "WARN" "ERROR" "FATAL"
sc.setLogLevel("ERROR")
# =========================
# 1. Separar inputs y output
# =========================
output_path = sys.argv[-1]

if os.path.exists(output_path):
    shutil.rmtree(output_path)

input_path = "input/"

# =========================
# 2. Cargar archivos
# =========================
nasdaq = sc.textFile(input_path + "NASDAQ*.csv")
companies = sc.textFile(input_path + "companylist*.tsv")

# Validación básica
if nasdaq.isEmpty() or companies.isEmpty():
    raise Exception("No se encontraron datos en NASDAQ o companylist")

# =========================
# 4. Limpiar headers (nombres de variables)
# =========================
nasdaq = nasdaq.filter(lambda x: "symbol" not in x.lower())
companies = companies.filter(lambda x: "symbol" not in x.lower())

# =========================
# 5. Parseo (elegir variables y estructura de tupla)
# =========================

print("RAW Nasd:")
print(nasdaq.take(5))
print("RAW Comp")
print(companies.take(5))

def parse_nasdaq(line):
    parts = line.split(",") #Extraigo cada linea
    if len(parts) < 8: # Si está incompleta la descarto
        return None
    try:
        symbol = parts[1]
        year = parts[2].split("-")[0]
        volume = int(parts[7])
        return (symbol, (year, volume)) # Volume es la cantidad de operaciones
    except:
        return None

def parse_company(line):
    parts = line.split("\t")
    if len(parts) < 4:
        return None
    try:
        symbol = parts[0]
        sector = parts[3]
        return (symbol, sector)
    except:
        return None

nasdaq_rdd = nasdaq.map(parse_nasdaq).filter(lambda x: x is not None)
company_rdd = companies.map(parse_company).filter(lambda x: x is not None)

print("PARSED Nasd:")
print(nasdaq_rdd.take(5))
print("PARSED Comp")
print(company_rdd.take(5))

# =========================
# 6. JOIN
# =========================
joined = nasdaq_rdd.join(company_rdd)

print("\n \n \n JOINED:")
print(joined.take(5))

# =========================
# 7. REMAPPED Ajustar la tupla (Para que la key sea (Year, Sector) y el valor sea volume)
# =========================

# x tiene la forma: (symbol, ((year, volume), sector))
# x[1] = ((year, volume), sector) → ignoramos el symbol
# x[1][0] = (year, volume)
# x[1][0][0] = year
# x[1][0][1] = volume
# x[1][1] = sector

year_sector = joined.map(lambda x: ((x[1][0][0], x[1][1]), x[1][0][1]))

print("\n \n \n REMAPPED:")
print(year_sector.take(5))

# =========================
# 8. Reducir
# =========================
reduced = year_sector.reduceByKey(lambda a, b: a + b)

print("\n \n \n REDUCED:")
print(reduced.take(5))

# =========================
# 9. Máximo por año
# =========================

# x tiene la forma: ((year, sector), volume)
# x[0][1] = sector

by_sector = reduced.map(lambda x: (x[0][0], (x[0][1], x[1])))

top = by_sector.reduceByKey(lambda a, b: a if a[1] > b[1] else b)

print("\n \n \n TOPPED:")
print(top.take(5))

# =========================
# 10. Formato final
# =========================
result = top.map(lambda x: f"{x[1][0]},{x[0]},{x[1][1]}")

# =========================
# 11. Guardar
# =========================
result.saveAsTextFile(output_path)


sc.stop()