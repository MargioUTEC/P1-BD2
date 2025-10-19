# ============================================================
# test_bplustree.py
# Pruebas funcionales del B+Tree persistente
# ============================================================

import os
import shutil
import time
from test_parser.indexes.bmas.bplustree import BPlusTreeIndex, Record, DATA_DIR, DATA_FILE, META_FILE

# ------------------------------------------------------------
# Función principal de pruebas
# ------------------------------------------------------------
def run_tests():
    print("=== TEST B+TREE PERSISTENTE ===\n")

    # Rutas
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base_dir, "..", "..", "core", "Dataset.csv")

    if not os.path.exists(csv_path):
        print(f"[ERROR] No se encontró el archivo {csv_path}")
        return

    # Reiniciar carpeta data
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    os.makedirs(DATA_DIR, exist_ok=True)

    print("[INFO] Cargando registros desde CSV...")
    records = Record.load_from_csv(csv_path)
    print(f"[OK] Se cargaron {len(records)} registros.")

    # --------------------------------------------------------
    # Crear árbol
    # --------------------------------------------------------
    print("\n[INFO] Creando índice B+Tree persistente...")
    bpt = BPlusTreeIndex()

    start_time = time.time()

    # Insertar primeros 10 registros
    print("\n[INSERTANDO REGISTROS]")
    for rec in records[:10]:
        key = int(rec.data["restaurant_id"])
        value = rec.data["restaurant_name"]
        bpt.insert(key, value)
        print(f"  [INSERT] {key} -> {value}")

    print("\n[ESTRUCTURA DEL ÁRBOL]")
    bpt.print_tree()

    # --------------------------------------------------------
    # Búsqueda exacta
    # --------------------------------------------------------
    first_key = int(records[0].data["restaurant_id"])
    print(f"\n[SEARCH] {first_key}:")
    result = bpt.search(first_key)
    print(f"  Resultado: {result}")

    # --------------------------------------------------------
    # Búsqueda por rango
    # --------------------------------------------------------
    print("\n[RANGE SEARCH] entre 6300000 y 6320000:")
    rango = bpt.range_search(6300000, 6320000)
    for k, v in rango:
        print(f"  {k} -> {v}")
    if not rango:
        print("  [VACÍO] No se encontraron registros en ese rango.")

    # --------------------------------------------------------
    # Eliminación y reinserción
    # --------------------------------------------------------
    test_key = int(records[3].data["restaurant_id"])
    print(f"\n[REMOVE] Eliminando clave {test_key}...")
    bpt.remove(test_key)
    print(f"  Buscar tras eliminar: {bpt.search(test_key)}")
    print(f"  Reinsertando {test_key}...")
    bpt.insert(test_key, records[3].data["restaurant_name"])
    print(f"  Buscar tras reinserción: {bpt.search(test_key)}")

    # --------------------------------------------------------
    # Reapertura del índice
    # --------------------------------------------------------
    print("\n[REAPERTURA DEL ÍNDICE]")
    reopened = BPlusTreeIndex(DATA_FILE, META_FILE)
    sample_key = int(records[1].data["restaurant_id"])
    print(f"  Buscar {sample_key} tras reapertura: {reopened.search(sample_key)}")
    reopened.print_tree()

    # --------------------------------------------------------
    # Estadísticas
    # --------------------------------------------------------
    elapsed = time.time() - start_time
    print(f"\n[ESTADÍSTICAS FINALES]")
    reopened.stats()
    print(f"Tiempo total de ejecución: {elapsed:.3f} s")
    print(f"Archivos generados en: {DATA_DIR}")

# ------------------------------------------------------------
# Ejecución directa
# ------------------------------------------------------------
if __name__ == "__main__":
    run_tests()
