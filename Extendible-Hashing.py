from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from hashlib import sha1
import pandas as pd


@dataclass
class Bucket:
    capacity: int
    local_depth: int
    items: Dict[Any, Any] = field(default_factory=dict)

    def is_full(self) -> bool:
        return len(self.items) >= self.capacity


class ExtendibleHashing:
    def __init__(
        self,
        bucket_capacity: int = 4,
        key_selector: Callable[[Any], Any] = lambda r: r["key"],
        hash_fn: Callable[[Any], int] = hash,
        file_path: str = "Dataset-restaurantes.csv"
    ):
        if bucket_capacity <= 0:
            raise ValueError("La capacidad del bucket debe ser mayor a 0")
        self.bucket_capacity = bucket_capacity
        self.key_selector = key_selector
        self.hash_fn = hash_fn
        self.file_path = file_path

        self.global_depth = 1
        root = Bucket(capacity=bucket_capacity, local_depth=1)
        self.directory: List[Bucket] = [root, root]
        
        # Leer el archivo CSV al iniciar
        self.df = pd.read_csv(file_path)
        self.registros = self.df.to_dict(orient="records")

    # Helpers para manejo de los buckets
    def _index(self, key_hash: int, depth: Optional[int] = None) -> int:
        d = self.global_depth if depth is None else depth
        return key_hash & ((1 << d) - 1)

    def _double_directory(self) -> None:
        self.directory += self.directory
        self.global_depth += 1

    def _all_indexes_of_bucket(self, bucket: Bucket) -> List[int]:
        return [i for i, b in enumerate(self.directory) if b is bucket]

    def _split_bucket(self, idx: int) -> None:
        old = self.directory[idx]
        if old.local_depth == self.global_depth:
            self._double_directory()

        new_ld = old.local_depth + 1
        b0 = Bucket(self.bucket_capacity, new_ld)
        b1 = Bucket(self.bucket_capacity, new_ld)

        # Reasignar punteros del directorio
        for i in self._all_indexes_of_bucket(old):
            bit = (i >> (new_ld - 1)) & 1
            self.directory[i] = b1 if bit else b0

        # Redistribuir elementos
        for k, reg in old.items.items():
            h = self.hash_fn(k)
            bit = (self._index(h, new_ld) >> (new_ld - 1)) & 1
            (b1 if bit else b0).items[k] = reg

    # Operaciones para archivo
    def search(self, key: Any) -> Optional[Any]:
        h = self.hash_fn(key)
        idx = self._index(h)
        return self.directory[idx].items.get(key)

    def add(self, registro: Any) -> None:
        key = self.key_selector(registro)
        h = self.hash_fn(key)

        while True:
            idx = self._index(h)
            bucket = self.directory[idx]

            # Si el registro ya existe, actualizar
            if key in bucket.items:
                bucket.items[key] = registro
                return

            # Si el bucket tiene espacio, insertar
            if not bucket.is_full():
                bucket.items[key] = registro
                return

            # Si el bucket está lleno, hacer un split y reintentar
            self._split_bucket(idx)

        # Agregar el registro al CSV si es nuevo
        self.registros.append(registro)
        self.df = pd.DataFrame(self.registros)  # Convertimos la lista de registros a DataFrame
        self._save_to_csv()  # Guardamos el archivo CSV actualizado

    def _save_to_csv(self) -> None:
        # Guardamos el DataFrame al archivo CSV
        self.df.to_csv(self.file_path, index=False)

    def remove(self, key: Any) -> bool:
        h = self.hash_fn(key)
        idx = self._index(h)
        bucket = self.directory[idx]

        if key in bucket.items:
            del bucket.items[key]
            return True

        return False


# Testeo de las funciones

# Cargar tu archivo CSV (ajusta el path si está en otra carpeta)
df = pd.read_csv("Dataset-restaurantes.csv")

# Convertimos cada fila en un diccionario
registros = df.to_dict(orient="records")

print("Total de registros:", len(registros))
print("Ejemplo:", registros[0])


def custom_hash(k):
    return int(sha1(str(k).encode()).hexdigest(), 16)

# Mi test 1 para probar el ID de cada restaurante
print("\n--- Test 1: Usando 'Restaurant ID' ---")
eh_id = ExtendibleHashing(
    bucket_capacity=4,
    key_selector=lambda r: r["Restaurant ID"],
    hash_fn=custom_hash,
    file_path="Dataset-restaurantes.csv"  # Ruta al archivo CSV
)

for reg in registros:
    eh_id.add(reg)

print('\n')
print("Buscar ID=17293273 →", eh_id.search(17293273)) # Este es el id del restaurante Thai Kitchen
print("Buscar ID inexistente=999999 →", eh_id.search(999999))
print('\n')


print("Eliminar ID=17294850 →", eh_id.remove(17294850))  # Este es el id del Thai Kitchen
print("Buscar ID=17294850 (post-eliminación) →", eh_id.search(17294850))

print('\n')


# Mi test 2 para probar la variable de 'Votes' de un restaurante
print("\n--- Test 2: Usando 'Votes' ---")
eh_votes = ExtendibleHashing(
    bucket_capacity=4,
    key_selector=lambda r: r["Votes"],
    hash_fn=custom_hash,
    file_path="Dataset-restaurantes.csv"
)

for reg in registros:
    eh_votes.add(reg)

print("Buscar Votes=500 →", eh_votes.search(500))  # Applebee's
print('\n')
print("Buscar Votes=1020 →", eh_votes.search(1020))  # Moon algo
print('\n')


# Test 3 usando 'Aggregate rating'
print("\n--- Test 3: Usando 'Aggregate rating' ---")
eh_rating = ExtendibleHashing(
    bucket_capacity=4,
    key_selector=lambda r: r["Aggregate rating"],
    hash_fn=custom_hash,
    file_path="Dataset-restaurantes.csv"
)

for reg in registros:
    eh_rating.add(reg)

print('\n')

print("Buscar rating=4.3 →", eh_rating.search(4.3))
print("Buscar rating=5.0 →", eh_rating.search(5.0))

print('\n')

# --- Test 4: add() con registro COMPLETO ---
print("\n--- Test 4: add() con registro completo ---")
n0 = len(pd.read_csv("Dataset-restaurantes.csv"))

nuevo_id = int(pd.to_numeric(df["Restaurant ID"], errors="coerce").max()) + 101
base = registros[0]

nuevo_registro = {
    "Restaurant ID": nuevo_id,
    "Restaurant Name": f"Restaurante {nuevo_id}",
    "Country Code": int(base["Country Code"]),
    "City": base["City"],
    "Address": "Av. Prueba 123",
    "Locality": base["Locality"],
    "Locality Verbose": base["Locality Verbose"],
    "Longitude": float(base["Longitude"]),
    "Latitude": float(base["Latitude"]),
    "Cuisines": base["Cuisines"],
    "Average Cost for two": int(base["Average Cost for two"]),
    "Currency": base["Currency"],
    "Has Table booking": base["Has Table booking"],
    "Has Online delivery": base["Has Online delivery"],
    "Is delivering now": base["Is delivering now"],
    "Switch to order menu": base["Switch to order menu"],
    "Price range": int(base["Price range"]),
    "Aggregate rating": 4.3,
    "Rating color": "Green",
    "Rating text": "Very Good Burgers ",
    "Votes": 5.0
}

print(
    f"Agregando registro... ID={nuevo_registro['Restaurant ID']}, Name={nuevo_registro['Restaurant Name']}, City={nuevo_registro['City']}")
print("Resultado add():", eh_id.add(nuevo_registro))
print("Buscar ID nuevo →", eh_id.search(nuevo_id))


# --- Test 5, haciendo add---
print("\n--- Test 5: segundo add() con registro completo ---")

nuevo_id2 = int(pd.to_numeric(
    df["Restaurant ID"], errors="coerce").max()) + 202
nuevo_registro2 = {
    "Restaurant ID": nuevo_id2,
    "Restaurant Name": f"[TEST] Restaurante {nuevo_id2}",
    "Country Code": int(base["Country Code"]),
    "City": base["City"],
    "Address": "Jr. Demo 456",
    "Locality": base["Locality"],
    "Locality Verbose": base["Locality Verbose"],
    "Longitude": float(base["Longitude"]) + 0.0002,
    "Latitude": float(base["Latitude"]) + 0.0002,
    "Cuisines": base["Cuisines"],
    "Average Cost for two": int(base["Average Cost for two"]) + 10,
    "Currency": base["Currency"],
    "Has Table booking": base["Has Table booking"],
    "Has Online delivery": base["Has Online delivery"],
    "Is delivering now": base["Is delivering now"],
    "Switch to order menu": base["Switch to order menu"],
    "Price range": int(base["Price range"]),
    "Aggregate rating": 4.7,
    "Rating color": "Dark Green",
    "Rating text": "Excellent",
    "Votes": 8
}

print(
    f"Agregando registro... ID={nuevo_registro2['Restaurant ID']}, Name={nuevo_registro2['Restaurant Name']}, City={nuevo_registro2['City']}")
print("Resultado add():", eh_id.add(nuevo_registro2))
print("Buscar ID nuevo →", eh_id.search(nuevo_id2))
