# ==========================================================
# test_extendible_hashing.py
# Pruebas con Dataset.csv (restaurantes)
# ==========================================================

import os
import csv
import shutil
from pathlib import Path
from test_parser.indexes.hashing.extendible_hashing import ExtendibleHashing
# ----------------------------------------------------------
# Configuración inicial
# ----------------------------------------------------------
script_dir = Path(__file__).resolve().parent
csv_path = script_dir / "Dataset.csv"
INDEX_PATH = script_dir / "data"

# ----------------------------------------------------------
# Cargar dataset
# ----------------------------------------------------------
def load_records(limit: int = 15):
    records = []

    # Leer con soporte para UTF-8 con BOM
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = [name.strip() for name in reader.fieldnames]
        print("[INFO] Columnas detectadas:", fieldnames)

        # Mapeo flexible (por si el CSV tiene espacios o nombres levemente distintos)
        def find_col(possible_names):
            for name in fieldnames:
                for p in possible_names:
                    if name.lower().strip() == p.lower().strip():
                        return name
            raise KeyError(f"No se encontró ninguna columna con nombres {possible_names}")

        col_id = find_col(["Restaurant ID", "RestaurantID", "ID"])
        col_name = find_col(["Restaurant Name", "Name"])
        col_city = find_col(["City"])
        col_long = find_col(["Longitude"])
        col_lat = find_col(["Latitude"])
        col_cuis = find_col(["Cuisines"])
        col_rating = find_col(["Aggregate rating", "Rating", "Aggregate Rating"])

        for i, row in enumerate(reader):
            if i >= limit:
                break
            try:
                record = {
                    "Restaurant ID": int(row[col_id]),
                    "Restaurant Name": row[col_name],
                    "City": row[col_city],
                    "Longitude": float(row[col_long]),
                    "Latitude": float(row[col_lat]),
                    "Cuisines": row[col_cuis],
                    "Rating": float(row[col_rating])
                }
                records.append(record)
            except Exception as e:
                print(f"[WARN] Fila {i} ignorada ({e})")
                continue

    print(f"[INFO] Cargadas {len(records)} filas válidas del dataset.")
    return records

# ----------------------------------------------------------
# Pruebas funcionales
# ----------------------------------------------------------
def run_basic_tests():
    if INDEX_PATH.exists():
        shutil.rmtree(INDEX_PATH)

    eh = ExtendibleHashing(
        base_path=str(INDEX_PATH),
        bucket_capacity=2,
        key_selector=lambda r: r["Restaurant ID"],
        hash_fn=lambda k: k
    )

    data = load_records()
    print(f"\n[INFO] Cargados {len(data)} registros de prueba.")

    print("\n=== INSERTANDO REGISTROS ===")
    for reg in data:
        eh.add(reg)
    eh.debug_dump()

    print("\n=== BÚSQUEDAS ===")
    for test_id in [data[0]["Restaurant ID"], data[-1]["Restaurant ID"], 999999]:
        print(f"Buscar ID={test_id}:", eh.search(test_id))

    print("\n=== ELIMINACIÓN Y REINSERCIÓN ===")
    test_id = data[3]["Restaurant ID"]
    print(f"Eliminando ID={test_id} ...")
    eh.remove(test_id)
    print("Buscar tras eliminación:", eh.search(test_id))
    print("Reinsertando ...")
    eh.add(data[3])
    print("Buscar tras reinserción:", eh.search(test_id))
    eh.debug_dump()

    print("\n=== REAPERTURA DEL ÍNDICE ===")
    eh2 = ExtendibleHashing(
        base_path=str(INDEX_PATH),
        bucket_capacity=2,
        key_selector=lambda r: r["Restaurant ID"],
        hash_fn=lambda k: k
    )
    print("Buscar ID existente tras recarga:", eh2.search(data[1]["Restaurant ID"]))
    eh2.debug_dump()

    print(f"\n=== ESTADÍSTICAS ===\nLecturas: {eh2.reads} | Escrituras: {eh2.writes}")

if __name__ == "__main__":
    run_basic_tests()
