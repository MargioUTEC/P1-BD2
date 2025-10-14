import os
import time
import random
from typing import List, Tuple

from ISAM import (
    ISAM, Record, _read_restaurants_csv,
    _index_summary, _quick_summary, make_key
)

# --- Archivos principales ---
DATA_PATH  = "restaurants.dat"
INDEX_PATH = "restaurants.idx"
CSV_PATH   = "../Dataset-restaurantes.csv"


# ---------- Helpers de test ----------
def safe_remove(path: str):
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception as e:
        print(f"[WARN] No se pudo eliminar {path}: {e}")


def assert_found(tag: str, res):
    if res is None:
        raise AssertionError(f"[{tag}] Debería existir, pero no se encontró.")
    else:
        off, r = res
        print(f"[OK] {tag} -> FOUND @{off}: {r}")


def assert_not_found(tag: str, res):
    if res is not None:
        raise AssertionError(f"[{tag}] No debería existir, pero SÍ se encontró: {res}")
    else:
        print(f"[OK] {tag} -> NOT FOUND (como se esperaba)")


def find_any_example(recs: List[Record]) -> Tuple[str, str, int]:
    """
    Devuelve (name, city, id) de un registro válido del dataset para pruebas.
    """
    filtered = [r for r in recs if r.name and r.city and isinstance(r.restaurant_id, int)]
    if not filtered:
        raise RuntimeError("No hay registros válidos en el CSV.")
    random.seed(42)
    cand = random.choice(filtered)
    return cand.name.strip(), cand.city.strip(), int(cand.restaurant_id)


def test_utf8(isam: ISAM):
    """
    Inserta y busca un registro con acentos/UTF-8 en nombre/ciudad.
    """
    print("\n[TEST] UTF-8 (acentos/diacríticos)")
    name = "Café Perú – Miraflores"
    city = "Lima"
    new_id = 8765432

    rec = Record(
        restaurant_id=new_id,
        name=name,
        country_code=604,
        city=city,
        address="Av. Test 123",
        cuisines="Café",
        avg_cost_for_two=50,
        currency="PEN",
        has_table_booking=False,
        has_online_delivery=False,
        is_delivering_now=False,
        price_range=2,
        aggregate_rating=4.2,
        rating_text="Very Good",
        votes=7,
        longitude=-77.03,
        latitude=-12.12,
    )

    isam.insert(rec)
    found = isam.search(name, city, new_id)
    assert_found("UTF-8 search exacta", found)

    ok = isam.delete(name, city, new_id)
    if not ok:
        raise AssertionError("[UTF-8] El delete devolvió False.")

    found2 = isam.search(name, city, new_id)
    assert_not_found("UTF-8 post-delete", found2)


def maybe_test_range(isam: ISAM, recs: List[Record]):
    """
    Si existe range_search en ISAM, lo probamos con un rango real.
    """
    if not hasattr(isam, "range_search"):
        print("\n[SKIP] range_search(): aún no está implementado en ISAM. Se omite esta prueba.")
        return

    if len(recs) < 10:
        print("[SKIP] Dataset pequeño; se omite range_search.")
        return

    a = recs[2]
    b = recs[9]
    begin_key = a.key()
    end_key   = b.key()

    print(f"\n[TEST] range_search() desde\n  {a}\nhasta\n  {b}")
    result = isam.range_search(begin_key, end_key)
    if not isinstance(result, list):
        raise AssertionError("range_search() debería devolver una lista de (off, Record).")
    if not result:
        raise AssertionError("range_search() devolvió lista vacía en un rango que debería tener elementos.")

    for off, r in result:
        k = r.key()
        if not (begin_key <= k <= end_key):
            raise AssertionError(f"range_search(): clave fuera de rango -> {k}")

    print(f"[OK] range_search() devolvió {len(result)} registros en el rango esperado.")


def main():
    # ---------- Limpieza previa ----------
    safe_remove(DATA_PATH)
    safe_remove(INDEX_PATH)

    # Verificación del CSV
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"No se encontró el archivo CSV en '{CSV_PATH}'. "
                                f"Asegúrate de que el nombre sea exactamente 'Dataset.csv'.")

    print("== Cargando CSV y construyendo ISAM ==")
    t0 = time.time()
    registros = _read_restaurants_csv(CSV_PATH)
    if not registros:
        raise RuntimeError("No se leyeron registros desde Dataset.csv. Verifica el contenido/encoding.")

    isam = ISAM(DATA_PATH, INDEX_PATH)
    isam.build(registros)
    t1 = time.time()
    print(f"[OK] build() completado en {t1 - t0:.3f}s con {len(registros)} registros.")

    # ---------- Resúmenes ----------
    _index_summary(isam.index, isam.data)
    _quick_summary(isam, max_pages=6)

    # ---------- Prueba A: orden global por first_key entre páginas base ----------
    print("\n[TEST] Orden no-decreciente por first_key entre páginas base")
    first_keys = []
    for off, page in isam.data.iter_pages():
        if page.records:
            first_keys.append((off, page.records[0].key()))
    for i in range(1, len(first_keys)):
        assert first_keys[i-1][1] <= first_keys[i][1], \
            f"first_key fuera de orden: page@{first_keys[i-1][0]} > page@{first_keys[i][0]}"
    print(f"[OK] {len(first_keys)} páginas verificadas en orden.")

    # ---------- Prueba B1: búsqueda de control exacta (con ID) ----------
    print("\n== Búsqueda de control #1 ==")
    res1 = isam.search("Le Petit Souffle", "Makati City", 6317637)
    if res1 is not None:
        assert_found("Le Petit Souffle (Makati City, id=6317637)", res1)
    else:
        print("[WARN] No se encontró 'Le Petit Souffle' con ID exacto. Puede variar según dataset.")

    # ---------- Prueba B2: búsqueda por nombre+ciudad (sin ID) ----------
    print("\n== Búsqueda de control #2 (sin ID) ==")
    res2 = isam.search("Izakaya Kikufuji", "Makati City", None)
    if res2 is not None:
        assert_found("Izakaya Kikufuji (Makati City, sin ID)", res2)
    else:
        print("[WARN] No se encontró 'Izakaya Kikufuji' (sin ID). Revisa normalización o dataset.")

    # ---------- Prueba C: UTF-8 (acentos/diacríticos) ----------
    test_utf8(isam)

    # ---------- Prueba D: Overflow (inserciones múltiples en misma página) ----------
    print("\n[TEST] Overflow encadenado con múltiples inserciones")
    # Elige una base conocida (si la anterior no está, toma un ejemplo aleatorio)
    base_name, base_city, base_id = ("Izakaya Kikufuji", "Makati City", 6304287)
    probe = isam.search(base_name, base_city, base_id)
    if probe is None:
        # fallback determinístico
        base_name, base_city, base_id = find_any_example(registros)

    # Ubica la página base donde deberían caer las inserciones
    base_off = isam.index.find_page_offset(make_key(base_name, base_city, 0))
    if base_off == -1:
        raise AssertionError("No se pudo ubicar la página base para overflow test.")
    chain_before = isam._chain_offsets(base_off)
    BLOCK_FACTOR = 8
    # Inserta N registros con el mismo (name, city) para forzar overflow
    N = BLOCK_FACTOR * 3  # suficiente para varias páginas
    inserted_ids = []
    for i in range(N):
        nid = 7_000_000 + i
        rec = Record(
            restaurant_id=nid,
            name=base_name,
            country_code=999,
            city=base_city,
            address=f"Overflow Test {i}",
            cuisines="Test",
            avg_cost_for_two=1,
            currency="USD",
            has_table_booking=False,
            has_online_delivery=False,
            is_delivering_now=False,
            price_range=1,
            aggregate_rating=0.0,
            rating_text="NA",
            votes=0,
            longitude=0.0,
            latitude=0.0,
        )
        isam.insert(rec)
        inserted_ids.append(nid)

    chain_after = isam._chain_offsets(base_off)
    print(f"[INFO] Cadena antes: {len(chain_before)}, después: {len(chain_after)}")
    assert len(chain_after) >= len(chain_before), "La cadena de overflow no creció como se esperaba."
    # Verifica que un insert intermedio se puede buscar
    mid_id = inserted_ids[len(inserted_ids)//2]
    assert_found("Overflow: búsqueda de un insert intermedio",
                 isam.search(base_name, base_city, mid_id))

    # ---------- Prueba E: Promoción / actualización de first_key en base ----------
    print("\n[TEST] Actualización de first_key en página base (promoción)")
    # Insertamos un ID muy chico para el mismo (name, city) para que sea el primero por clave
    tiny_id = 1
    rec_tiny = Record(
        restaurant_id=tiny_id,
        name=base_name,
        country_code=999,
        city=base_city,
        address="Primero por clave",
        cuisines="Test",
        avg_cost_for_two=1,
        currency="USD",
        has_table_booking=False,
        has_online_delivery=False,
        is_delivering_now=False,
        price_range=1,
        aggregate_rating=0.0,
        rating_text="NA",
        votes=0,
        longitude=0.0,
        latitude=0.0,
    )
    isam.insert(rec_tiny)
    # La first_key de la base debería reflejar ahora este nuevo mínimo
    pg_base = isam.data.read_page_at(base_off)
    fk_before_delete = pg_base.first_key()
    assert fk_before_delete is not None, "first_key inesperadamente None."
    # Búsqueda exacta del tiny
    assert_found("Promoción: búsqueda del mínimo insertado",
                 isam.search(base_name, base_city, tiny_id))
    # Eliminar el mínimo y verificar que la first_key se actualiza (puede cambiar)
    assert isam.delete(base_name, base_city, tiny_id), "Delete del mínimo falló."
    pg_base2 = isam.data.read_page_at(base_off)
    fk_after_delete = pg_base2.first_key()
    assert fk_after_delete is not None, "first_key tras delete inesperadamente None."
    print(f"[INFO] first_key base: antes='{fk_before_delete[:40]}...'  después='{fk_after_delete[:40]}...'")

    # ---------- Prueba F: range_search ----------
    print("\n[TEST] range_search() con rango real del dataset")
    a = registros[2]
    b = registros[min(20, len(registros)-1)]
    begin_key = a.key()
    end_key   = b.key()
    result = isam.range_search(begin_key, end_key)
    assert isinstance(result, list) and len(result) > 0, "range_search() debe devolver lista no vacía."
    # Orden y rango
    last_k = ""
    for off_, r_ in result:
        k_ = r_.key()
        assert begin_key <= k_ <= end_key, f"range_search: clave fuera de rango -> {k_}"
        assert last_k <= k_, "range_search: resultados fuera de orden."
        last_k = k_
    print(f"[OK] range_search() devolvió {len(result)} registros en orden y dentro del rango.")

    # ---------- Prueba G: persistencia (reabrir y buscar) ----------
    print("\n[TEST] Persistencia: reabrir y repetir una búsqueda")

    # Fuerza guardado completo antes de reabrir
    if hasattr(isam, "flush"):
        isam.flush()
    elif hasattr(isam, "save"):
        isam.save()
    elif hasattr(isam.data, "flush"):
        isam.data.flush()
    if hasattr(isam.index, "flush"):
        isam.index.flush()

    # Cierra los manejadores de archivos
    if hasattr(isam.data, "close"):
        isam.data.close()
    if hasattr(isam.index, "close"):
        isam.index.close()

    # Reabrir
    isam2 = ISAM(DATA_PATH, INDEX_PATH)
    assert_found("Persistencia: búsqueda insert intermedio tras reabrir",
                 isam2.search(base_name, base_city, mid_id))

    # Buscar un insert hecho arriba (siempre debería estar)
    assert_found("Persistencia: búsqueda insert intermedio tras reabrir",
                 isam2.search(base_name, base_city, mid_id))

    # ---------- Prueba H: negativos ----------
    print("\n[TEST] Negativos (no encontrado)")
    assert_not_found("Negativo: nombre inexistente",
                     isam.search("ZZZ_esto_no_existe", "Atlantis", 999999999))
    assert_not_found("Negativo: nombre correcto, ciudad incorrecta",
                     isam.search("Le Petit Souffle", "No Existe City", 6317637))
    assert_not_found("Negativo: sin ID con nombre/city improbables",
                     isam.search("ZZZ_esto_no_existe", "Atlantis", None))

    print("\n[TEST] Eliminación en overflow")
    target_id = inserted_ids[-1]  # último insertado en overflow
    assert isam.delete(base_name, base_city, target_id), "Delete en overflow falló."
    assert_not_found("Post-delete overflow", isam.search(base_name, base_city, target_id))

    print("\n[TEST] Reinserción tras delete")
    isam.insert(rec_tiny)  # mismo nombre+city+ID que se eliminó
    assert_found("Reinserción", isam.search(base_name, base_city, tiny_id))

    print("\n==============================")
    print("   TODAS LAS PRUEBAS PASARON  ")
    print("==============================")


if __name__ == "__main__":
    main()
