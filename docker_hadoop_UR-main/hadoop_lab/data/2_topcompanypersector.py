import sys, os, shutil
from pyspark import SparkContext
from pyspark.sql import SparkSession

# sudo apt update
# sudo apt install openjdk-17-jdk
# export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 export PATH=$JAVA_HOME/bin:$PATH
# spark-submit 2_topcompanypersector.py input/* output_2_topcompany

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
        date = parts[2]
        adj_close_price = float(parts[8])
        return (symbol, (date, adj_close_price)) # Volume es la cantidad de operaciones
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
# 7. REMAPPED Ajustar la tupla (Para que la key sea (Year, Symbol) y el valor sea date, adj_close)
# =========================

# x tiene la forma: (Symbol,[{(date),adj_close} , Sector])
# x[1] = [{(date),adj_close} , Sector] → ignoramos el symbol
# x[1][0] = (date, adj_close)
# x[1][0][0] = date
# x[1][1] = sector

year_symbol = joined.map(lambda x: ((x[0],x[1][0][0].split("-")[0]), (x[1][0][0], x[1][0][1], x[1][1]))   )

print("\n \n \n REMAPPED:")
print(year_symbol.take(5))

# =========================
# 8. Reducir (Conseguir ultimo y primer precio)
# =========================
reduced_last = year_symbol.reduceByKey(lambda a, b: (a if a[0] > b[0] else b))
reduced_first = year_symbol.reduceByKey(lambda a, b: (a if a[0] < b[0] else b))

reduced = reduced_last.fullOuterJoin(reduced_first)
print("\n \n \n REDUCED:")
print(reduced.take(5))


# =========================
# 9. Mappear (Calcular growth)
# =========================

# x tiene la forma: ((symbol, year), ((last_date, last_price, sector), (first_date, first_price, sector)))

map_growth = reduced.map(lambda x: (x[0],  # (symbol, year)
    (int(((x[1][0][1] - x[1][1][1])*100 / x[1][1][1]) if x[1][1][1] != 0 else 0),   # growth
    x[1][0][2])
))

print("\n \n \n GROWTH:")
print(map_growth.take(5))


# =========================
# 10. Top
# =========================


# x tiene la forma: ((symbol, year), (growth,sector))
# quiero ((sector,year),(symbol,growth))

by_sector_year = map_growth.map(lambda x: ((x[1][1],x[0][1]), (x[0][0], x[1][0])))

top = by_sector_year.reduceByKey(lambda a, b: a if a[1] > b[1] else b)

print("\n \n \n TOPPED:")
print(top.take(5))

# =========================
# 11. Formato final
# =========================
# x tiene la forma ((sector,year),(symbol,growth))
# quiero {sector,year,symbol,growth}

result = top \
    .map(lambda x: (x[0][0], x[0][1], x[1][0], x[1][1])) \
    .sortBy(lambda x: (x[0], x[1])) \
    .map(lambda x: f"{x[0]},{x[1]},{x[2]},{x[3]}")

# =========================
# 11. Guardar
# =========================
result.saveAsTextFile(output_path)

sc.stop()