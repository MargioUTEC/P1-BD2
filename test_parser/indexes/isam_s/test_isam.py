import os
from pathlib import Path
from test_parser.indexes.isam_s.isam import _index_summary, _print_result, _quick_summary, _read_restaurants_csv, ISAM
# === CONFIGURACIÓN ===
DATA_PATH  = Path("test_parser/indexes/isam_s/data")

DATA_PATH.mkdir(parents=True, exist_ok=True)
script_dir = Path(__file__).resolve().parent

#CSV_PATH   = Path("test_parser/indexes/datasets/Restaurants.csv")
CSV_PATH = script_dir / "Dataset.csv"
DATA_FILE  = DATA_PATH / "restaurants.dat"
INDEX_FILE = DATA_PATH / "restaurants.idx"

# === TEST PRINCIPAL ===
def run_isam_demo():
    print("=== DEMO ISAM ===")

    # 1) Leer dataset
    print("[INFO] Leyendo CSV...")
    recs = _read_restaurants_csv(CSV_PATH)
    print(f"[INFO] {len(recs)} registros totales.")

    # 2) Construir ISAM
    isam = ISAM(str(DATA_FILE), str(INDEX_FILE))
    isam.build(recs[:64])  # construir con un subconjunto para demo
    print("[OK] Índice construido correctamente.")

    # 3) Resumen
    _quick_summary(isam, max_pages=4)
    _index_summary(isam.index, isam.data)

    # 4) Búsquedas exactas
    print("\n=== BÚSQUEDAS ===")
    _print_result("Buscar 'Le Petit Souffle' (Makati)",
                  isam.search("Le Petit Souffle", "Makati City"))
    _print_result("Buscar 'Izakaya Kikufuji' (Makati)",
                  isam.search("Izakaya Kikufuji", "Makati City"))

    # 5) Inserción
    print("\n=== INSERCIÓN ===")
    new_rec = recs[100]
    isam.insert(new_rec)
    _print_result("Después de insertar",
                  isam.search(new_rec.name, new_rec.city, new_rec.restaurant_id))

    # 6) Eliminación
    print("\n=== ELIMINACIÓN ===")
    ok = isam.delete(new_rec.name, new_rec.city, new_rec.restaurant_id)
    print(f"[DEL] {'Eliminado correctamente' if ok else 'No encontrado'}")

    # 7) Reconstrucción
    print("\n=== RECONSTRUCCIÓN DEL ÍNDICE ===")
    isam.index.rebuild_from_data(isam.data)
    _index_summary(isam.index, isam.data)

    print("\n=== SCAN FINAL ===")
    isam.scan_all()

if __name__ == "__main__":
    run_isam_demo()
