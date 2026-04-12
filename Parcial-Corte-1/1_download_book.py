import os
import requests
import random

def generar_lista_gutenberg(n):
    lista = []
    for i in range(n):
        index = random.randint(0,5000)
        item = f"https://www.gutenberg.org/cache/epub/{index}/pg{index}.txt"
        lista.append(item)
    return lista

# Definición de rutas según tu estructura
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, "input")
# El archivo de lista se busca en /data/lista_libros.txt
ARCHIVO_LISTA = os.path.join(BASE_DIR, "lista_libros.txt")

# 2.a Leer los links del archivo
#with open(ARCHIVO_LISTA, 'r') as f:
#    links = [line.strip() for line in f if line.strip()]
    
# 2.b Crear una lsita aleatoria (comentar a)
links = generar_lista_gutenberg(50)

# 3. Descarga masiva
for url in links:
    # Extraer el nombre del archivo de la URL (ej: pg11.txt)
    nombre_archivo = url.split('/')[-1]
    ruta_destino = os.path.join(INPUT_DIR, nombre_archivo)
    
    # Realizar la descarga
    print(f"Descargando {nombre_archivo}...")
    respuesta = requests.get(url)
    
    # Guardar el contenido en /data/input
    with open(ruta_destino, 'wb') as f_libro:
        f_libro.write(respuesta.content)

print("Descarga completada en /data/input")