import os
import shutil
import time
from pathlib import Path
import pandas as pd
from test_parser.indexes.isam_s.isam import ISAM, Record, _read_restaurants_csv, normalize_text
from test_parser.indexes.hashing.extendible_hashing import ExtendibleHashing
from test_parser.indexes.rtree_point.rtree_points import RTreePoints
from test_parser.indexes.avl.avl_file import AVLFile
from test_parser.indexes.bmas.bplustree import BPlusTreeIndex as BPTree, Record as BPRecord


class IndexManager:
    """
    Gestor unificado: construye, consulta, inserta y elimina en:
    - ISAM (nombre/ciudad)
    - Hash Extensible (ID)
    - R-Tree (espacial)
    - B+Tree (ID→nombre, rango)
    - AVL (ID primario completo)
    """
    def __init__(self, base_dir: str = "test_parser/data"):
        self.base_dir = Path(base_dir).resolve()
        self.base_dir.mkdir(parents=True, exist_ok=True)

        # ISAM
        self.isam_data_path = self.base_dir / "restaurants.dat"
        self.isam_index_path = self.base_dir / "restaurants.idx"
        self.isam = ISAM(
            data_path=str(self.isam_data_path),
            index_path=str(self.isam_index_path)
        )

        # HASH
        self.hash_path = self.base_dir / "hash"
        self.hash = ExtendibleHashing(
            base_path=str(self.hash_path),
            bucket_capacity=4,
            key_selector=lambda r: r["Restaurant ID"],
            name="restaurants_hash"
        )

        # R-TREE
        self.rtree_path = self.base_dir / "rtree_index"
        self.rtree = RTreePoints(index_name=str(self.rtree_path), max_children=50)

        # AVL
        self.avl_path = self.base_dir / "restaurants_avl"
        self.avl = AVLFile(str(self.avl_path))

        # B+TREE
        self.bpt = BPTree()

    def build_from_csv(self, csv_path: str, limit: int | None = 50, using_indexes: list[str] | None = None):
        """
        Construye los índices desde un CSV.
        Si el path es relativo, se asume test_parser/core/Dataset.csv.
        Si using_indexes se especifica, solo se construyen esas estructuras.
        """
        from pathlib import Path
        import os

        csv_path = Path(csv_path)
        if not csv_path.is_absolute():
            base_core = Path(__file__).resolve().parent.parent / "core"
            csv_path = base_core / csv_path.name
        csv_path = csv_path.resolve()

        if not csv_path.exists():
            raise FileNotFoundError(f"No existe el archivo CSV en: {csv_path}")

        print(f"[INFO] Cargando dataset de restaurantes desde: {csv_path}")
        recs = _read_restaurants_csv(str(csv_path))
        if limit:
            recs = recs[:limit]
        print(f"[INFO] {len(recs)} registros cargados.")

        # ======================================================
        # Determinar qué índices construir
        # ======================================================
        if using_indexes:
            using_indexes = [u.upper() for u in using_indexes]
            print(f"[INFO] Solo se construirán índices: {', '.join(using_indexes)}")
        else:
            using_indexes = ["ISAM", "HASH", "RTREE", "AVL", "BTREE"]

        # ======================================================
        # ISAM
        # ======================================================
        if "ISAM" in using_indexes:
            for p in (self.isam_data_path, self.isam_index_path):
                if p.exists():
                    p.unlink(missing_ok=True)
            self.isam = ISAM(
                data_path=str(self.isam_data_path),
                index_path=str(self.isam_index_path)
            )
            print("[INFO] Construyendo ISAM...")
            self.isam.build(recs)

        # ======================================================
        # HASH
        # ======================================================
        if "HASH" in using_indexes:
            print("[INFO] Construyendo índice hash extendible...")
            if self.hash_path.exists():
                shutil.rmtree(self.hash_path, ignore_errors=True)
            self.hash_path.mkdir(parents=True, exist_ok=True)
            self.hash = ExtendibleHashing(
                base_path=str(self.hash_path),
                bucket_capacity=4,
                key_selector=lambda r: r["Restaurant ID"],
                name="restaurants_hash"
            )
            for r in recs:
                try:
                    self.hash.add({
                        "Restaurant ID": r.restaurant_id,
                        "Name": r.name,
                        "City": r.city,
                        "Rating": r.aggregate_rating,
                        "Longitude": r.longitude,
                        "Latitude": r.latitude
                    })
                except Exception as e:
                    print(f"[WARN] HASH insert: {e}")

        # ======================================================
        # RTREE
        # ======================================================
        if "RTREE" in using_indexes:
            print("[INFO] Construyendo índice espacial (R-Tree)...")

            if hasattr(self, "rtree") and self.rtree is not None:
                try:
                    self.rtree.close()
                    self.rtree = None
                    print("[INFO] R-Tree anterior cerrado correctamente.")
                except Exception as e:
                    print(f"[WARN] No se pudo cerrar R-Tree anterior: {e}")

            for ext in (".data", ".index", ".meta"):
                f = self.rtree_path.with_suffix(ext)
                if f.exists():
                    try:
                        os.remove(f)
                        print(f"[INFO] Archivo antiguo eliminado: {f.name}")
                    except PermissionError:
                        print(f"[WARN] {f.name} bloqueado, se omitirá.")

            df = pd.DataFrame([{
                "Restaurant ID": r.restaurant_id,
                "Restaurant Name": r.name,
                "City": r.city,
                "Longitude": r.longitude,
                "Latitude": r.latitude,
                "Aggregate rating": r.aggregate_rating
            } for r in recs])

            self.rtree = RTreePoints.from_dataframe(
                df,
                x_col="Longitude",
                y_col="Latitude",
                keep_cols=["Restaurant ID", "Restaurant Name", "City", "Aggregate rating"],
                index_name=str(self.rtree_path)
            )

        # ======================================================
        # AVL
        # ======================================================
        if "AVL" in using_indexes:
            print("[INFO] Construyendo índice AVL...")
            for ext in (".avl", ".dat"):
                f = Path(str(self.avl_path) + ext)
                if f.exists():
                    f.unlink(missing_ok=True)
            self.avl = AVLFile(str(self.avl_path))
            for r in recs:
                try:
                    self.avl.insert({
                        "restaurant_id": r.restaurant_id,
                        "restaurant_name": r.name,
                        "city": r.city,
                        "longitude": r.longitude,
                        "latitude": r.latitude,
                        "average_cost_for_two": getattr(r, "avg_cost_for_two", 0),
                        "aggregate_rating": r.aggregate_rating,
                        "votes": getattr(r, "votes", 0)
                    })
                except Exception as e:
                    print(f"[WARN] AVL insert: {e}")

        # ======================================================
        # BTREE
        # ======================================================
        if "BTREE" in using_indexes or "B+TREE" in using_indexes:
            print("[INFO] Construyendo índice B+Tree...")
            self.bpt = BPTree()
            try:
                rows = BPRecord.load_from_csv(csv_path)
            except Exception:
                rows = None
            it = rows[:limit] if rows and limit else (rows or recs)
            for rec in it:
                try:
                    key = int(getattr(rec, "restaurant_id", rec.data.get("restaurant_id")))
                    val = getattr(rec, "name", None) or rec.data.get("restaurant_name", "")
                    self.bpt.insert(key, val)
                except Exception as e:
                    print(f"[WARN] B+Tree insert: {e}")

        # ======================================================
        # FIN
        # ======================================================
        print("[OK] Índices construidos correctamente.")
        print(f"[OK] Índices creados: {', '.join(using_indexes)}.")
        return using_indexes  # ← permite al llamador saber qué índices se construyeron

    def rebuild_from_csv(self, csv_path: str, limit: int | None = 50, using_indexes: list[str] | None = None):
        """
        Limpia artefactos locales (excepto /data del B+Tree persistente)
        y reconstruye de forma segura.
        Permite reconstruir solo índices seleccionados mediante using_indexes.
        """
        print("[INFO] Limpiando entorno previo...")

        # Cierre seguro de RTree para evitar locks (especialmente en Windows)
        try:
            if self.rtree:
                self.rtree.close()
                self.rtree = None
        except Exception as e:
            print(f"[WARN] No se pudo cerrar RTree previo: {e}")

        # Pequeño retraso para asegurar liberación del handle
        time.sleep(0.5)

        # Limpieza controlada del directorio base
        for child in self.base_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                try:
                    child.unlink(missing_ok=True)
                except PermissionError:
                    print(f"[WARN] {child.name} bloqueado, se omitirá.")
        self.base_dir.mkdir(parents=True, exist_ok=True)

        # Reconstrucción completa (solo índices seleccionados)
        built_indexes = self.build_from_csv(csv_path, limit, using_indexes)

        # Cerrar nuevamente el RTree después del rebuild (previene locks futuros)
        if hasattr(self, "rtree") and self.rtree:
            try:
                self.rtree.close()
            except Exception:
                pass

        return built_indexes

    # ======================================================
    #  BÚSQUEDAS
    # ======================================================
    def search_by_name(self, name: str = "", city: str = ""):
        name, city = name.strip(), city.strip()
        if city:
            return self.isam.search(name, city)
        results = []
        for _, page in self.isam.data.iter_pages():
            for rec in page.records:
                if normalize_text(rec.name) == normalize_text(name):
                    results.append(rec)
        return results
# BUSQUEDA
    def search_comparison(self, attr: str, op: str, value: float):
        """
        Delegado principal: usa el AVL para comparar atributos numéricos
        (>, <, >=, <=, =) sobre registros completos.
        """
        try:
            results = self.avl.search_comparison(attr, op, value)
            print(f"[DEBUG] IndexManager.search_comparison('{attr}', '{op}', {value}) → {len(results)} resultado(s)")
            return results
        except Exception as e:
            print(f"[WARN] Fallback en search_comparison(): {e}")
            # Fallback opcional si mantienes un data_cache en memoria
            if hasattr(self, "data_cache"):
                out = []
                a = attr.strip().lower()
                for rec in self.data_cache:
                    if a not in rec:
                        continue
                    try:
                        rv = float(rec[a]);
                        vv = float(value)
                    except Exception:
                        continue
                    ok = ((op == "=" and rv == vv) or
                          (op == ">" and rv > vv) or
                          (op == ">=" and rv >= vv) or
                          (op == "<" and rv < vv) or
                          (op == "<=" and rv <= vv))
                    if ok:
                        out.append(rec)
                return out
            return []

    def search_between_general(self, attr: str, low, high):
        """
        BETWEEN genérico:
          - Si es ID → usa B+Tree (rangeSearch).
          - Para otros atributos numéricos → usa AVL.search_between().
        """
        a = attr.strip().lower()
        if "id" in a:
            try:
                lo = int(low);
                hi = int(high)
                return self.search_range_id(lo, hi)
            except Exception as e:
                print(f"[WARN] search_between_general(ID) → {e}")
                return []
        try:
            results = self.avl.search_between(attr, low, high)
            print(f"[DEBUG] IndexManager.search_between_general('{attr}', {low}, {high}) → {len(results)} resultado(s)")
            return results
        except Exception as e:
            print(f"[WARN] search_between_general() → {e}")
            return []

    def search_by_id(self, restaurant_id: int):
        """Búsqueda exacta por ID (AVL → B+Tree → Hash)"""
        try:
            found = self.avl.search(int(restaurant_id))
            if found:
                return [found]
        except Exception as e:
            print(f"[AVL-ERROR] search: {e}")
        try:
            val = self.bpt.search(int(restaurant_id))
            if val is not None:
                return [{"restaurant_id": int(restaurant_id), "restaurant_name": val}]
        except Exception as e:
            print(f"[BPT-ERROR] search: {e}")
        try:
            h = self.hash.search(int(restaurant_id))
            if h:
                return [h]
        except Exception as e:
            print(f"[HASH-ERROR] search: {e}")
        return []

    def search_range_id(self, begin_id: int, end_id: int):
        """Rango de IDs en B+Tree"""
        try:
            pairs = self.bpt.rangeSearch(int(begin_id), int(end_id))
            return [{"restaurant_id": k, "restaurant_name": v} for (k, v) in pairs]
        except Exception as e:
            print(f"[BPT-ERROR] range: {e}")
            return []

    def search_near(self, lon: float, lat: float, radius_km: float = 3.0):
        """Búsqueda espacial por radio (km)"""
        try:
            return self.rtree.range_search_km((float(lon), float(lat)), float(radius_km))
        except Exception as e:
            print(f"[RTree-ERROR] range_search_km: {e}")
            return []

    # ======================================================
    #  INSERCIÓN / ELIMINACIÓN
    # ======================================================
    def insert(self, record: Record):
        print(f"[INSERT] {record.name} ({record.city})")

        # ISAM
        self.isam.insert(record)

        # HASH
        try:
            self.hash.add({
                "Restaurant ID": record.restaurant_id,
                "Name": record.name,
                "City": record.city,
                "Rating": record.aggregate_rating,
                "Longitude": record.longitude,
                "Latitude": record.latitude
            })
        except Exception as e:
            print(f"[HASH-ERROR] insert: {e}")

        # RTREE
        try:
            self.rtree.add_point(record.longitude, record.latitude, {
                "Restaurant_ID": record.restaurant_id,
                "Restaurant_Name": record.name,
                "City": record.city,
                "Aggregate_rating": record.aggregate_rating
            })
            self.rtree.save()
        except Exception as e:
            print(f"[RTree-ERROR] insert: {e}")

        # AVL
        try:
            self.avl.insert({
                "restaurant_id": record.restaurant_id,
                "restaurant_name": record.name,
                "city": record.city,
                "longitude": record.longitude,
                "latitude": record.latitude,
                "average_cost_for_two": getattr(record, "avg_cost_for_two", 0),
                "aggregate_rating": record.aggregate_rating,
                "votes": getattr(record, "votes", 0)
            })
        except Exception as e:
            print(f"[AVL-ERROR] insert: {e}")

        # B+TREE
        try:
            self.bpt.insert(int(record.restaurant_id), record.name)
        except Exception as e:
            print(f"[BPT-ERROR] insert: {e}")

    def delete(self, name: str = "", city: str = "", restaurant_id: int | None = None):
        """
        Elimina en TODAS las estructuras.
        Si no se pasa ID, lo intenta resolver con ISAM (por nombre/ciudad).
        """
        print(f"[DELETE] name='{name}' city='{city}' id={restaurant_id}")

        ids = [int(restaurant_id)] if restaurant_id else [
            int(rec.restaurant_id) for rec in (self.search_by_name(name, city) or [])
        ]

        for rid in ids:
            # --- ISAM ---
            try:
                # Intentar versión de 3 argumentos
                self.isam.delete(name, city, rid)
            except TypeError:
                # Compatibilidad con versión que solo recibe (name, city)
                try:
                    self.isam.delete(name, city)
                except Exception as e:
                    print(f"[ISAM-ERROR] delete (fallback): {e}")
            except Exception as e:
                print(f"[ISAM-ERROR] delete: {e}")

            # --- HASH ---
            try:
                self.hash.remove(rid)
            except Exception as e:
                print(f"[HASH-ERROR] delete: {e}")

            # --- RTREE ---
            try:
                self.rtree.remove_point_by_id(rid)
                self.rtree.save()
            except Exception as e:
                print(f"[RTree-ERROR] delete: {e}")

            # --- AVL ---
            try:
                self.avl.remove(rid)
            except Exception as e:
                print(f"[AVL-ERROR] delete: {e}")

            # --- B+TREE ---
            try:
                self.bpt.delete(rid)
            except Exception as e:
                print(f"[BPT-ERROR] delete: {e}")

        print(f"[OK] Eliminados {len(ids)} registros.")

    # ======================================================
    #  UTILIDADES
    # ======================================================
    def summary(self):
        print("\n=== INDEX MANAGER SUMMARY ===")
        print(f" Base dir : {self.base_dir}")
        try:
            print(f" ISAM     : {self.isam.data.page_count()} páginas")
        except Exception:
            print(" ISAM     : (no disponible)")
        try:
            print(f" HASH     : {len(self.hash.directory)} entradas")
        except Exception:
            print(" HASH     : (no disponible)")
        try:
            print(f" RTree    : {len(self.rtree._rows)} puntos")
        except Exception:
            print(" RTree    : (no disponible)")
        try:
            print(f" AVL      : {len(self.avl.inorder_ids())} registros")
        except Exception:
            print(" AVL      : (no disponible)")
        print(" B+Tree   : persistente (ver print_tree())")

    def get_stats(self):
        stats = {}
        try:
            stats["hash"] = {
                "global_depth": self.hash.global_depth,
                "dir_size": len(self.hash.directory),
                "reads": self.hash.reads,
                "writes": self.hash.writes,
            }
        except Exception:
            pass
        try:
            stats["rtree"] = self.rtree.stats()
        except Exception:
            pass
        try:
            stats["avl_count"] = len(self.avl.inorder_ids())
        except Exception:
            pass
        return stats

    def close(self):
        """Cierra estructuras y guarda metadatos."""
        try:
            if hasattr(self, "rtree") and self.rtree:
                self.rtree.close()
        except Exception:
            print("[WARN] Fallo al cerrar RTree.")

        try:
            if hasattr(self, "hash") and hasattr(self.hash, "flush"):
                self.hash.flush()
        except Exception:
            pass

        print("[INFO] Índices cerrados correctamente.")

