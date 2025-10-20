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
from test_parser.indexes.isam_s.isam import normalize_text as _norm
from test_parser.indexes.bmas.bplustree import BPlusTreeIndex

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
        # === Carpeta base centralizada ===
        self.base_dir = Path(__file__).resolve().parent.parent / "data"
        self.base_dir.mkdir(parents=True, exist_ok=True)

        # === Archivo base principal ===
        self.base_table_path = self.base_dir / "restaurants_base.csv"

        self.all_columns = [
            "Restaurant ID", "Restaurant Name", "Country Code", "City", "Address", "Locality",
            "Locality Verbose", "Longitude", "Latitude", "Cuisines", "Average Cost for two",
            "Currency", "Has Table booking", "Has Online delivery", "Is delivering now",
            "Switch to order menu", "Price range", "Aggregate rating", "Rating color",
            "Rating text", "Votes"
        ]

        # === Paths individuales de estructuras ===
        self.isam_data_path = self.base_dir / "restaurants.dat"
        self.isam_index_path = self.base_dir / "restaurants.idx"
        self.hash_path = self.base_dir / "restaurants_hash"
        self.avl_path = self.base_dir / "restaurants_avl"
        self.bpt_path = self.base_dir / "bptree_index"
        self.rtree_path = self.base_dir / "rtree_index"

        # === ISAM ===
        try:
            self.isam = ISAM(
                data_path=str(self.isam_data_path),
                index_path=str(self.isam_index_path)
            )
            print("[INIT] ISAM inicializado correctamente.")
        except Exception as e:
            print(f"[WARN] No se pudo inicializar ISAM: {e}")
            self.isam = None

        # === Extendible Hashing ===
        # === Extendible Hashing ===
        try:
            hash_dir = self.base_dir / "restaurants_hash" / "restaurants_hash_dir.json"
            hash_data = self.base_dir / "restaurants_hash" / "restaurants_hash_data.dat"

            if hash_dir.exists() and hash_data.exists():
                self.hash = ExtendibleHashing(
                    base_path=str(self.base_dir / "restaurants_hash"),
                    name="restaurants_hash"
                )
                print("[INIT] Extendible Hash reabierto correctamente.")
            else:
                print("[WARN] Archivos de Hash no encontrados.")
                self.hash = None
        except Exception as e:
            print(f"[WARN] No se pudo inicializar Extendible Hash: {e}")
            self.hash = None

        # === AVL ===
        try:
            avl_file = self.avl_path
            if avl_file.exists() or Path(str(avl_file) + ".avl").exists():
                self.avl = AVLFile(str(avl_file))
                print("[INIT] AVL reabierto correctamente.")
            else:
                print("[WARN] Archivo de AVL no encontrado.")
                self.avl = None
        except Exception as e:
            print(f"[WARN] No se pudo inicializar AVL: {e}")
            self.avl = None

        # === B+ Tree ===
        # === B+ Tree ===
        try:
            # Rutas correctas (según build_from_csv)
            bpt_data = self.base_dir / "bptree_index.dat"
            bpt_meta = self.base_dir / "bptree_meta.json"

            if bpt_data.exists() and bpt_meta.exists():
                self.bpt = BPlusTreeIndex(
                    data_file=str(bpt_data),
                    meta_file=str(bpt_meta)
                )
                print("[INIT] B+Tree reabierto correctamente.")
            else:
                # Si no existen, crear automáticamente el índice vacío
                print("[WARN] Archivos de B+Tree no encontrados. Se creará uno nuevo.")
                self.bpt = BPlusTreeIndex(
                    data_file=str(bpt_data),
                    meta_file=str(bpt_meta)
                )
        except Exception as e:
            print(f"[WARN] No se pudo inicializar B+Tree: {e}")
            self.bpt = None

        # === R-Tree ===
        try:
            base_path = self.rtree_path
            data_file = base_path.with_suffix(".data")
            index_file = base_path.with_suffix(".index")

            if data_file.exists() and index_file.exists():
                self.rtree = RTreePoints(index_name=str(base_path))
                print("[INIT] R-Tree reabierto correctamente.")
            else:
                print("[WARN] Archivos del R-Tree no encontrados.")
                self.rtree = None
        except Exception as e:
            print(f"[WARN] No se pudo inicializar R-Tree: {e}")
            self.rtree = None
    def _rec_to_dict(self, r) -> dict:
        return {
            "restaurant_id": r.restaurant_id,
            "name": r.name,
            "country_code": r.country_code,
            "city": r.city,
            "address": r.address,
            "locality": r.locality,
            "locality_verbose": r.locality_verbose,
            "longitude": r.longitude,
            "latitude": r.latitude,
            "cuisines": r.cuisines,
            "average_cost_for_two": r.avg_cost_for_two,
            "currency": r.currency,
            "has_table_booking": r.has_table_booking,
            "has_online_delivery": r.has_online_delivery,
            "is_delivering_now": r.is_delivering_now,
            "switch_to_order_menu": r.switch_to_order_menu,
            "price_range": r.price_range,
            "aggregate_rating": r.aggregate_rating,
            "rating_color": r.rating_color,
            "rating_text": r.rating_text,
            "votes": r.votes
        }

    def search_text(self, field: str, value: str, op: str = "=") -> list[dict]:
        fld = field.strip().lower()
        valn = _norm(str(value))
        out = []
        attr_name = {
            "restaurant_name": "name"
        }.get(fld, fld)
        use_like = op and op.upper() == "LIKE" and "%" in valn
        if use_like:
            import re
            pattern = "^" + re.escape(valn).replace(r"\%", ".*") + "$"
            rgx = re.compile(pattern)

            for _, page in self.isam.data.iter_pages():
                for rec in page.records:
                    txt = getattr(rec, attr_name, "")
                    txtn = _norm(str(txt))
                    match = (rgx.fullmatch(txtn) is not None) if use_like else (txtn == valn)
                    if match:
                        out.append(self._rec_to_dict(rec))

            return out

        # === ISAM ===
        self.isam_data_path = self.base_dir / "restaurants.dat"
        self.isam_index_path = self.base_dir / "restaurants.idx"
        self.isam = ISAM(
            data_path=str(self.isam_data_path),
            index_path=str(self.isam_index_path)
        )

        # === HASH ===
        self.hash_path = self.base_dir / "hash"
        self.hash = ExtendibleHashing(
            base_path=str(self.hash_path),
            bucket_capacity=4,
            key_selector=lambda r: r["Restaurant ID"],
            name="restaurants_hash"
        )

        # === R-TREE ===
        self.rtree_path = self.base_dir / "rtree_index"
        self.rtree = RTreePoints(index_name=str(self.rtree_path), max_children=50)

        # === AVL ===
        self.avl_path = self.base_dir / "restaurants_avl"
        self.avl = AVLFile(str(self.avl_path))

        # === B+TREE ===
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

            # Rutas dentro del directorio centralizado /data
            bpt_data = self.base_dir / "bptree_index.dat"
            bpt_meta = self.base_dir / "bptree_meta.json"

            # Elimina previos si existen
            for f in [bpt_data, bpt_meta]:
                if f.exists():
                    f.unlink(missing_ok=True)

            # Crear instancia de B+Tree con rutas explícitas
            self.bpt = BPlusTreeIndex(
                data_file=str(bpt_data),
                meta_file=str(bpt_meta)
            )

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
    def insert_full(self, record_dict: dict):
        import csv, os
        from test_parser.indexes.isam_s.isam import Record  # usa tu Record actualizado

        # ----------------------------
        # 1) Normalizar / mapear campos
        # ----------------------------
        # mapeo de nombres CSV → atributos del Record
        mapping = {
            "Average Cost for two": "avg_cost_for_two",
            "Has Table booking": "has_table_booking",
            "Has Online delivery": "has_online_delivery",
            "Is delivering now": "is_delivering_now",
            "Switch to order menu": "switch_to_order_menu",
            "Aggregate rating": "aggregate_rating",
            "Rating color": "rating_color",
            "Rating text": "rating_text",
            "Price range": "price_range",
        }
        normalized = dict(record_dict)  # copia
        for old, new in mapping.items():
            if old in normalized:
                normalized[new] = normalized.pop(old)

        # ----------------------------
        # 2) Construir el Record
        # ----------------------------
        # Nota: Ajusta estos campos a tu dataclass Record actual.
        rec = Record(
            restaurant_id=self._to_int(normalized.get("Restaurant ID")),
            name=normalized.get("Restaurant Name", ""),
            country_code=self._to_int(normalized.get("Country Code")),
            city=normalized.get("City", ""),
            address=normalized.get("Address", ""),
            cuisines=normalized.get("Cuisines", ""),
            avg_cost_for_two=self._to_int(normalized.get("avg_cost_for_two")),
            currency=normalized.get("Currency", ""),
            has_table_booking=self._to_bool_yesno(normalized.get("has_table_booking")),
            has_online_delivery=self._to_bool_yesno(normalized.get("has_online_delivery")),
            is_delivering_now=self._to_bool_yesno(normalized.get("is_delivering_now")),
            price_range=self._to_int(normalized.get("price_range")),
            aggregate_rating=self._to_float(normalized.get("aggregate_rating")),
            rating_text=normalized.get("rating_text", ""),
            votes=self._to_int(normalized.get("Votes")),
            longitude=self._to_float(normalized.get("Longitude")),
            latitude=self._to_float(normalized.get("Latitude")),
        )

        # 🔒 3) Chequeo de duplicado por ID (global)
        rid = int(rec.restaurant_id)
        if self._id_exists(rid):
            print(f"[DUPLICATE] Restaurant ID={rid} ya existe. No se insertará ni en índices ni en CSV.")
            return

        # ----------------------------
        # 4) Insertar en índices primero
        # ----------------------------
        ok = self.insert(rec)
        if not ok:
            print("[ERROR] El INSERT falló en alguna estructura. No se persistirá en CSV.")
            return

        # ----------------------------
        # 5) Persistir en CSV (si todo OK arriba)
        # ----------------------------
        os.makedirs(self.base_table_path.parent, exist_ok=True)
        is_new = not os.path.exists(self.base_table_path)

        # Reconstruir dict con los nombres EXACTOS del CSV (self.all_columns)
        # y sus valores originales (no los normalizados internos)
        row = {k: record_dict.get(k, "") for k in self.all_columns}

        with open(self.base_table_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.all_columns)
            if is_new:
                writer.writeheader()
            writer.writerow(row)

        print("[OK] Registro insertado en índices y CSV base.")

    def insert(self, record: Record):
        """
        Inserta en todas las estructuras con trazas detalladas de depuración.
        Si alguna inserción falla, aborta y muestra el traceback completo.
        """
        import traceback

        rid = int(record.restaurant_id)
        print(f"\n[INSERT DEBUG] === Iniciando inserción global para ID={rid} ({record.name}, {record.city}) ===")

        # 🔒 Verificar duplicado global por Restaurant ID
        if self._id_exists(rid):
            print(f"[DUPLICATE] Restaurant ID={rid} ya existe. Se omite la inserción.")
            return False

        try:
            print("[1] → Insertando en ISAM...")
            self.isam.insert(record)
            print("[OK] ISAM completado.")

            print("[2] → Insertando en HASH...")
            self.hash.add({
                "Restaurant ID": record.restaurant_id,
                "Name": record.name,
                "City": record.city,
                "Rating": record.aggregate_rating,
                "Longitude": record.longitude,
                "Latitude": record.latitude
            })
            print("[OK] HASH completado.")

            print("[3] → Insertando en RTREE...")
            self.rtree.add_point(record.longitude, record.latitude, {
                "Restaurant_ID": record.restaurant_id,
                "Restaurant_Name": record.name,
                "City": record.city,
                "Aggregate_rating": record.aggregate_rating
            })
            self.rtree.save()
            print("[OK] RTREE completado.")

            print("[4] → Insertando en AVL...")
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
            print("[OK] AVL completado.")

            print("[5] → Insertando en B+TREE...")
            self.bpt.insert(int(record.restaurant_id), record.name)
            print("[OK] B+TREE completado.")

            print("[INSERT DEBUG] ✅ Todas las estructuras insertadas correctamente.\n")
            return True

        except Exception as e:
            print(f"[ERROR] Fallo durante la inserción: {type(e).__name__} → {e}")
            print("".join(traceback.format_exc()))
            print("[ABORT] Cancelando inserción global tras error.")
            return False

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

    def force_search(self, forced_index, cond):
        """
        Ejecuta una búsqueda forzada según el índice especificado en la consulta.
        Retorna un diccionario con formato JSON amigable para frontend:
        {
            "status": "success" | "error" | "warning",
            "index": "<nombre del índice>",
            "message": "<texto descriptivo>",
            "results": [ ... ]
        }
        """
        attr = getattr(cond, "attribute", "").lower()
        op = getattr(cond, "operator", "=")
        val = getattr(cond, "value", None)

        print(f"[DEBUG] Ejecutando búsqueda forzada con índice {forced_index} en atributo '{attr}'")

        textual_fields = {"name", "city"}
        numeric_fields = {"rating", "aggregate_rating", "votes", "average_cost_for_two"}
        spatial_fields = {"coords", "longitude", "latitude"}
        id_fields = {"id", "restaurant_id"}

        # === VALIDACIONES DE COMPATIBILIDAD =================================
        if forced_index == "AVL" and attr not in numeric_fields:
            msg = f"❌ El índice AVL solo puede aplicarse a campos numéricos: {', '.join(numeric_fields)}."
            print("[ERROR]", msg)
            return {"status": "error", "index": forced_index, "message": msg, "results": []}

        if forced_index == "ISAM" and attr not in textual_fields:
            msg = f"❌ El índice ISAM solo puede aplicarse a campos textuales: {', '.join(textual_fields)}."
            print("[ERROR]", msg)
            return {"status": "error", "index": forced_index, "message": msg, "results": []}

        if forced_index == "HASH" and not any(f in attr for f in id_fields):
            msg = "❌ El índice HASH solo puede aplicarse a campos ID (por ejemplo 'restaurant_id')."
            print("[ERROR]", msg)
            return {"status": "error", "index": forced_index, "message": msg, "results": []}

        if forced_index == "RTREE" and attr not in spatial_fields:
            msg = f"❌ El índice R-Tree requiere coordenadas espaciales ({', '.join(spatial_fields)})."
            print("[ERROR]", msg)
            return {"status": "error", "index": forced_index, "message": msg, "results": []}

        # === EJECUCIÓN SEGÚN ÍNDICE ========================================
        try:
            if forced_index == "ISAM":
                results = self.search_by_name(
                    val if attr == "name" else "",
                    val if attr == "city" else ""
                )
                return {
                    "status": "success",
                    "index": "ISAM",
                    "message": f"✅ Búsqueda ISAM completada para {attr}='{val}'.",
                    "results": results
                }

            elif forced_index == "AVL":
                results = self.search_comparison(attr, op, float(val))
                return {
                    "status": "success",
                    "index": "AVL",
                    "message": f"✅ Búsqueda AVL completada ({attr} {op} {val}).",
                    "results": results
                }


            elif forced_index == "HASH":
                result = self.hash.search(int(val))
                if result:
                    rid = int(result.get("Restaurant ID", val))
                    # Buscar la versión completa en los otros índices
                    full_info = self.search_by_id(rid)
                    results = full_info if full_info else [result]
                else:
                    results = []
                return {
                    "status": "success",
                    "index": "HASH",
                    "message": f"✅ Búsqueda HASH completada para ID={val}.",
                    "results": results
                }


            elif forced_index == "RTREE":
                if hasattr(cond, "point") and hasattr(cond, "radius"):
                    x, y = cond.point
                    results = self.search_near(x, y, cond.radius)
                    return {
                        "status": "success",
                        "index": "RTREE",
                        "message": f"✅ Búsqueda R-Tree completada en entorno ({x}, {y}) ± {cond.radius}.",
                        "results": results
                    }
                msg = "❌ Faltan coordenadas o radio para búsqueda espacial con R-Tree."
                print("[ERROR]", msg)
                return {"status": "error", "index": forced_index, "message": msg, "results": []}



            elif forced_index == "BTREE":

                value = self.bpt.search(int(val))

                if value is None:

                    results = []

                else:

                    # Buscar información completa en otros índices

                    full = self.search_by_id(int(val))

                    if full:

                        results = full

                    else:

                        results = [{"restaurant_id": int(val), "restaurant_name": value}]

                return {

                    "status": "success",

                    "index": "BTREE",

                    "message": f"✅ Búsqueda B+Tree completada para ID={val}.",

                    "results": results

                }



            else:
                msg = f"⚠️ Índice '{forced_index}' no reconocido o no implementado."
                print("[WARN]", msg)
                return {"status": "warning", "index": forced_index, "message": msg, "results": []}

        except Exception as e:
            msg = f"❌ Error interno al ejecutar búsqueda con {forced_index}: {e}"
            print("[ERROR]", msg)
            return {"status": "error", "index": forced_index, "message": msg, "results": []}

    # ==========================
    # Helpers de normalización
    # ==========================
    @staticmethod
    def _to_int(value, default=0):
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _to_float(value, default=0.0):
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _to_bool_yesno(value, default=False):
        """
        Acepta Yes/No, True/False, 1/0, 'y'/'n', etc.
        """
        if value is None:
            return default
        s = str(value).strip().lower()
        if s in {"yes", "y", "true", "t", "1"}:
            return True
        if s in {"no", "n", "false", "f", "0"}:
            return False
        return default

    # ==========================
    # Políticas de unicidad
    # ==========================
    def _id_exists(self, rid: int) -> bool:
        """
        Chequeo rápido de existencia global por ID usando índices que ya lo tienen:
         - B+Tree (rápido y persistente)
         - si falla, Hash
         - como fallback, AVL
        """
        try:
            if self.bpt.search(rid) is not None:
                return True
        except Exception:
            pass
        try:
            if self.hash.search(rid):
                return True
        except Exception:
            pass
        try:
            if self.avl.search(rid):
                return True
        except Exception:
            pass
        return False

