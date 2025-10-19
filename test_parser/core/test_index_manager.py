from pathlib import Path
import os
import time

from test_parser.core.index_manager import IndexManager
from test_parser.indexes.isam_s.isam import _read_restaurants_csv


def pp(title, obj):
    print(f"\n--- {title} ---")
    print(obj)


def assert_true(cond, msg):
    if not cond:
        raise AssertionError(msg)


def run_index_manager_demo():
    script_dir = Path(__file__).resolve().parent
    CSV_PATH = script_dir / "Dataset.csv"

    assert_true(CSV_PATH.exists(), f"No se encontró Dataset.csv en: {CSV_PATH}")

    print("=== REBUILD (limpio) ===")
    mgr = IndexManager(base_dir="test_parser/data")
    t0 = time.time()
    mgr.rebuild_from_csv(str(CSV_PATH), limit=50)
    print(f"[OK] Rebuild en {time.time() - t0:.3f}s")
    mgr.summary()

    recs = _read_restaurants_csv(str(CSV_PATH))
    assert_true(len(recs) >= 12, "Se requieren al menos 12 registros para este test.")
    ref = recs[0]           # para búsquedas reiteradas
    alt = recs[1]           # para re-apertura/persistencia
    to_insert = recs[10]    # para INSERT / DELETE
    to_rangeL = min(int(recs[2].restaurant_id), int(recs[6].restaurant_id))
    to_rangeR = max(int(recs[2].restaurant_id), int(recs[6].restaurant_id))

    # ===== 2) BÚSQUEDAS =====
    print("\n=== BÚSQUEDAS ===")

    # 2.a) Por nombre/ciudad → ISAM
    by_name = mgr.search_by_name(ref.name, ref.city)
    pp("search_by_name (ISAM)", by_name)
    assert_true(by_name is not None and len(by_name) >= 1, "ISAM no encontró el registro por nombre/ciudad.")

    # 2.b) Por ID → cadena de fallback (AVL → B+Tree → Hash)
    by_id = mgr.search_by_id(int(ref.restaurant_id))
    pp("search_by_id (AVL→B+Tree→Hash)", by_id)
    assert_true(len(by_id) >= 1, "search_by_id no devolvió resultados.")
    # Verificación básica del shape del resultado de fallback
    # (puede venir del AVL con campos de su payload, o del B+Tree con name, o del Hash con dict original)
    assert_true(any(
        isinstance(x, dict) or hasattr(x, "restaurant_id") for x in by_id
    ), "El resultado de search_by_id tiene un formato inesperado.")

    # 2.c) Rango de ID (B+Tree)
    rmin, rmax = min(to_rangeL, to_rangeR), max(to_rangeL, to_rangeR)
    rng = mgr.search_range_id(rmin, rmax)
    pp(f"search_range_id [{rmin}, {rmax}] (B+Tree)", rng)
    assert_true(isinstance(rng, list), "range_id debe devolver lista.")
    # No necesariamente habrá resultados en ese rango para cualquier CSV, así que aceptamos lista vacía.

    # 2.d) Búsqueda espacial (R-Tree) — usa coordenadas del ref si están bien formadas
    print("\n— Búsqueda espacial (R-Tree)")
    try:
        near = mgr.search_near(float(ref.longitude), float(ref.latitude), radius_km=5.0)
        pp("search_near (R-Tree)", near)
        # near puede ser vacío si no hay vecinos; no hacemos assert fuerte aquí
    except Exception as e:
        print(f"[WARN] R-Tree: no se pudo ejecutar búsqueda espacial: {e}")

    # ===== 3) INSERCIÓN & ELIMINACIÓN =====
    print("\n=== INSERCIÓN / ELIMINACIÓN ===")
    # Insert
    mgr.insert(to_insert)
    # Debe existir por ID
    inserted_chk = mgr.search_by_id(int(to_insert.restaurant_id))
    pp("Post-insert search_by_id", inserted_chk)
    assert_true(len(inserted_chk) >= 1, "El registro insertado no se encontró por ID.")

    # Delete
    mgr.delete(to_insert.name, to_insert.city, to_insert.restaurant_id)
    deleted_chk = mgr.search_by_id(int(to_insert.restaurant_id))
    pp("Post-delete search_by_id", deleted_chk)
    assert_true(len(deleted_chk) == 0, "El registro eliminado aún aparece por ID.")

    # ===== 4) REAPERTURA / PERSISTENCIA =====
    print("\n=== REAPERTURA DEL MANAGER ===")
    mgr.close()
    mgr2 = IndexManager(base_dir="test_parser/data")
    # Query con otro ID para verificar persistencia en estructuras
    reopened = mgr2.search_by_id(int(alt.restaurant_id))
    pp("search_by_id tras reapertura", reopened)
    assert_true(len(reopened) >= 1, "Tras reapertura, el registro esperado no se encontró por ID.")

    # Segundo manager también debería consultar R-Tree, ISAM, etc.
    reopened_name = mgr2.search_by_name(ref.name, ref.city)
    pp("search_by_name tras reapertura", reopened_name)
    assert_true(reopened_name is not None and len(reopened_name) >= 1,
                "Tras reapertura, ISAM no encontró registro por nombre/ciudad.")

    # ===== 5) RESUMEN/STATS =====
    print("\n=== SUMMARY & STATS ===")
    mgr2.summary()
    stats = mgr2.get_stats()
    pp("get_stats()", stats)

    mgr2.close()
    print("\n[OK] Test IndexManager finalizado correctamente.")


if __name__ == "__main__":
    run_index_manager_demo()
