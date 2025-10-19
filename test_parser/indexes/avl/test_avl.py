# test_parser/indexes/avl/test_avl.py
import os
import csv
from test_parser.indexes.avl.avl_file import AVLFile

# ===============================================================
# UTILIDADES
# ===============================================================
def setup_data_dir():
    """Prepara carpeta temporal para los archivos del AVL."""
    base_dir = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(base_dir, exist_ok=True)
    return os.path.join(base_dir, "avl_test")

def load_dataset(csv_path: str, n: int = 10) -> list[dict]:
    """Carga hasta n registros del CSV original."""
    registros = []
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= n:
                break
            registros.append(row)
    return registros


# ===============================================================
# PRUEBAS UNITARIAS
# ===============================================================
def test_manual_insertion():
    """Inserta manualmente algunos nodos y prueba las operaciones básicas."""
    print("=== TEST 1: Inserción manual ===")
    base_path = setup_data_dir()

    # limpiar archivos previos
    for ext in [".avl", ".dat"]:
        try:
            os.remove(base_path + ext)
        except FileNotFoundError:
            pass

    avl = AVLFile(base_path)

    # Insertar registros simples
    registros = [
        {"restaurant_id": 5, "restaurant_name": "Café Lima", "city": "Lima", "longitude": -77.03, "latitude": -12.05,
         "average_cost_for_two": 50, "aggregate_rating": 4.2, "votes": 300},
        {"restaurant_id": 2, "restaurant_name": "Bistró Cusco", "city": "Cusco", "longitude": -71.97, "latitude": -13.52,
         "average_cost_for_two": 40, "aggregate_rating": 4.0, "votes": 250},
        {"restaurant_id": 8, "restaurant_name": "Trattoria Arequipa", "city": "Arequipa", "longitude": -71.53, "latitude": -16.4,
         "average_cost_for_two": 45, "aggregate_rating": 4.5, "votes": 400},
        {"restaurant_id": 1, "restaurant_name": "Cevichería Piura", "city": "Piura", "longitude": -80.63, "latitude": -5.19,
         "average_cost_for_two": 35, "aggregate_rating": 4.1, "votes": 200},
    ]

    for r in registros:
        avl.insert(r)

    print("IDs en orden (inorder):", avl.inorder_ids())

    # Buscar un nodo
    print("\nBuscar ID=2 →")
    print(avl.search(2))

    # Buscar rango
    print("\nBuscar rango [2,6]:")
    for r in avl.range_search(2, 6):
        print(f"{r['restaurant_id']} - {r['restaurant_name']}")

    # Eliminar un nodo
    print("\nEliminar ID=2")
    avl.remove(2)
    print("IDs tras eliminar:", avl.inorder_ids())

    # Persistencia
    print("\nReabrir archivo y verificar persistencia...")
    avl2 = AVLFile(base_path)
    print("IDs en disco:", avl2.inorder_ids())

    print("\n TEST 1 finalizado.\n")


def test_csv_insertion():
    """Carga registros desde Dataset.csv y prueba inserciones automáticas."""
    print("=== TEST 2: Inserción desde Dataset.csv ===")
    base_path = setup_data_dir()

    # limpiar archivos previos
    for ext in [".avl", ".dat"]:
        try:
            os.remove(base_path + ext)
        except FileNotFoundError:
            pass

    avl = AVLFile(base_path)

    csv_path = os.path.join(os.path.dirname(__file__), "..", "..", "core", "Dataset.csv")
    if not os.path.exists(csv_path):
        print("[ERROR] No se encontró Dataset.csv")
        return

    registros = load_dataset(csv_path, 15)
    print(f"[INFO] {len(registros)} registros cargados del CSV")

    for i, r in enumerate(registros):
        try:
            avl.insert(r)
        except Exception as e:
            print(f"[WARN] fila {i} inválida: {e}")

    ids = avl.inorder_ids()
    print(f"IDs ordenados ({len(ids)}):", ids[:20])

    # Buscar un ID de muestra
    if ids:
        mid_id = ids[len(ids)//2]
        print(f"\nBuscar ID = {mid_id}")
        print(avl.search(mid_id))

        # Eliminar y volver a mostrar
        print(f"\nEliminar ID = {mid_id}")
        avl.remove(mid_id)
        print("IDs tras eliminar:", avl.inorder_ids())

        # Reabrir y comprobar persistencia
        avl2 = AVLFile(base_path)
        print("IDs en disco tras reabrir:", avl2.inorder_ids())

    print("\n TEST 2 finalizado.\n")


def test_random_order():
    """Inserta IDs en orden aleatorio para verificar rebalanceo AVL."""
    print("=== TEST 3: Inserción en orden aleatorio ===")
    base_path = setup_data_dir()

    # limpiar archivos previos
    for ext in [".avl", ".dat"]:
        try:
            os.remove(base_path + ext)
        except FileNotFoundError:
            pass

    avl = AVLFile(base_path)

    import random
    ids = list(range(1, 16))
    random.shuffle(ids)

    for i in ids:
        avl.insert({
            "restaurant_id": i,
            "restaurant_name": f"Rest_{i}",
            "city": "Lima",
            "longitude": -77.0,
            "latitude": -12.0,
            "average_cost_for_two": 40,
            "aggregate_rating": 4.0,
            "votes": 100
        })

    inorder_ids = avl.inorder_ids()
    print("IDs en orden:", inorder_ids)
    assert inorder_ids == sorted(ids), " Error: el AVL no se balanceó correctamente"

    print("TEST 3 finalizado.\n")


# ===============================================================
# MAIN DE PRUEBA
# ===============================================================
if __name__ == "__main__":
    test_manual_insertion()
    test_csv_insertion()
    test_random_order()
