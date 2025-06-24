from pymongo import MongoClient
from pymongo.cursor import CursorType
from pymongo.errors import BulkWriteError

# Conexión al clúster
client = MongoClient("")
db = client["rtve_modelado"]

source = db["fichas_documentales"]
target = db["fichas_documentales_sin_valor"]

# Borramos la colección destino si existe (opcional)
target.drop()

# Parámetros
batch_size = 1000
total_copiados = 0

# Cursor sin timeout (evita que el servidor cierre la conexión por inactividad)
cursor = source.find({}, no_cursor_timeout=True).batch_size(batch_size)

batch = []

try:
    for doc in cursor:
        doc.pop("_id", None)  # Evita duplicados
        batch.append(doc)

        if len(batch) >= batch_size:
            try:
                target.insert_many(batch, ordered=False)
                total_copiados += len(batch)
                print(f"Copiados: {total_copiados}")
            except BulkWriteError as bwe:
                print("⚠️ Error de escritura en bloque:", bwe.details)
            batch = []

    # Inserta últimos documentos si quedan
    if batch:
        try:
            target.insert_many(batch, ordered=False)
            total_copiados += len(batch)
            print(f"Copiados: {total_copiados}")
        except BulkWriteError as bwe:
            print("⚠️ Error de escritura final:", bwe.details)

finally:
    cursor.close()

print("✅ Copia finalizada.")
