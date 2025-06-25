from pymongo import MongoClient, UpdateOne
from tqdm import tqdm

client = MongoClient("")

# Base de datos y colecciones
db = client["rtve_modelado"]
coleccion = db["fichas_documentales_sin_valor"]

# Filtro selectivo
filtro = {
    "EMISION.CADENA.valor": "TELEDEPORTE"}

# Preparar cursor
cursor = coleccion.find(filtro, { "_id": 1 })

# Crear operaciones en lote
lote = []
batch_size = 1000  # Ajusta según memoria
count = 0

for doc in tqdm(cursor, desc="Actualizando", unit="doc"):
    operacion = UpdateOne(
        { "_id": doc["_id"] },
        {
            "$set": {
                "EMISION.CADENA.id_tesauro": "107",
                "EMISION.CADENA.id_termino": "442"
            }
        }
    )
    lote.append(operacion)
    count += 1

    # Ejecutar en lotes
    if len(lote) == batch_size:
        coleccion.bulk_write(lote, ordered=False)
        lote = []

# Ejecutar las que queden
if lote:
    coleccion.bulk_write(lote, ordered=False)

print(f"Total documentos procesados: {count}")
