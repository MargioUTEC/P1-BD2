import time
import importlib
import inspect
import pandas as pd
import sys, os

# === Asegurar path raíz ===
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# === Rutas a tus módulos ===
TECHNIQUES = {
    "ISAM": "test_parser.indexes.isam_s.isam",
    "Extendible Hashing": "test_parser.indexes.hashing.extendible_hashing",
    "B+Tree": "test_parser.indexes.bmas.bplustree",
    "AVL": "test_parser.indexes.avl.avl_file",
    "R-Tree": "test_parser.indexes.rtree_point.rtree_points"
}

# === Dataset dummy ===
registros_dummy = [{"Restaurant ID": i, "Votes": i * 2} for i in range(200)]
search_keys = [10, 50, 120, 150]


# === Función auxiliar: medir tiempo de ejecución ===
def measure_time(func, *args, **kwargs):
    start = time.perf_counter()
    func(*args, **kwargs)
    end = time.perf_counter()
    return (end - start) * 1000  # ms


# === Benchmark por módulo ===
def benchmark_module(name, module):
    print(f"\n=== Probando {name} ===")


    # Buscar clase principal
    clase_principal = None
    for cname, cls in inspect.getmembers(module, inspect.isclass):
        if any(hasattr(cls, m) for m in ["add", "insert", "search", "range_search", "range_search_km", "remove", "delete", "knn"]):
            clase_principal = cls
            break

    # dentro de benchmark_module
    if clase_principal:
        if name == "AVL":
            instancia = clase_principal(base_path="avl_benchmark_test")
        else:
            instancia = clase_principal()


    if not clase_principal:
        print(f"⚠️ No se encontró clase reconocible en {name}.")
        return None

    # Intentar crear instancia
    try:
        instancia = clase_principal()
    except Exception as e:
        print(f"⚠️ No se pudo inicializar {name}: {e}")
        return None

    resultados = {"Técnica": name}




    # ⚙️ Operaciones disponibles
    operaciones = [
        op for op in ["add", "insert", "search", "range_search", "range_search_km", "remove","delete", "knn"]
        if hasattr(instancia, op)
    ]

    for op in operaciones:
        fn = getattr(instancia, op)
        try:
            # === CASO ESPECIAL ISAM ===
            if name == "ISAM":
                from test_parser.indexes.isam_s.isam import Record

                if op == "insert":
                    rec = Record.from_minimal(
                        restaurant_id=1,
                        name="TestResto",
                        city="Lima",
                        longitude=0.0,
                        latitude=0.0,
                        aggregate_rating=4.0
                    )
                    tiempo = measure_time(fn, rec)

                elif op == "search":
                    tiempo = measure_time(fn, name="TestResto", city="Lima", restaurant_id=1)

                elif op in ("remove", "delete"):  # 👈 agrega esta línea
                    delete_fn = getattr(instancia, "delete", None)
                    if delete_fn:
                        tiempo = measure_time(delete_fn, name="TestResto", city="Lima", restaurant_id=1)
                    else:
                        tiempo = measure_time(fn, name="TestResto", city="Lima", restaurant_id=1)

                elif op == "range_search":
                    tiempo = measure_time(fn, "a", "z")

                else:
                    continue


            elif name == "B+Tree":
                if op == "insert":
                    # Inserta una clave entera y un valor cualquiera (string)
                    tiempo = measure_time(fn, 10, "Restaurante 10")
                elif op == "search":
                    # Busca por la misma clave
                    tiempo = measure_time(fn, 10)
                elif op == "range_search":
                    # Búsqueda por rango: dos claves numéricas
                    tiempo = measure_time(fn, 5, 20)
                elif op in ("remove", "delete"):
                    tiempo = measure_time(fn, 10)
                else:
                    continue
       


            # === CASO GENERAL ===
            elif op in ("add", "insert"):
                tiempo = measure_time(fn, registros_dummy[0])
            elif op == "search":
                tiempo = measure_time(fn, search_keys[0])
            elif op == "range_search":
                tiempo = measure_time(fn, "A", "Z")
            elif op == "range_search_km":
                tiempo = measure_time(fn, 0.0, 0.0, 10.0)
            elif op == "knn":
                tiempo = measure_time(fn, 0.0, 0.0, 5)
            elif op == "remove":
                tiempo = measure_time(fn, search_keys[0])
            else:
                continue

            resultados[f"{op}_ms"] = tiempo

        except Exception as e:
            resultados[f"{op}_ms"] = None
            print(f"⚠️ Error en {name}.{op}(): {e}")

    return resultados



# === MAIN ===
if __name__ == "__main__":
    resultados = []

    for nombre, ruta in TECHNIQUES.items():
        try:
            mod = importlib.import_module(ruta)
            res = benchmark_module(nombre, mod)
            if res:
                resultados.append(res)
        except Exception as e:
            print(f"❌ Error importando/ejecutando {nombre}: {e}")

    # === Exportar resultados ===
    df = pd.DataFrame(resultados)
    df.to_csv("resultados_benchmark.csv", index=False)

    print("\n=== RESULTADOS FINALES ===")
    print(df)
