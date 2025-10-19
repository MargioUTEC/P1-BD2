from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, List, Tuple, Optional, Dict
import math, pandas as pd, time, json
from pathlib import Path

try:
    from rtree import index as rindex
except ImportError as e:
    raise ImportError("Instala con: pip install rtree") from e


Coord = Tuple[float, float]


@dataclass
class PointRec:
    id: int
    coords: Coord
    payload: Optional[Dict] = None


class RTreePoints:
    def __init__(self, index_name: Optional[str] = None, max_children: int = 50):
        self.index_name = index_name
        self.max_children = max_children
        self._rows: Dict[int, PointRec] = {}
        self._next_id = 0
        self._created_time = time.strftime("%Y-%m-%d %H:%M:%S")

        p = rindex.Property()
        p.dimension = 2
        p.leaf_capacity = max(4, int(max_children))
        p.index_capacity = max(8, int(max_children * 2))
        p.near_minimum_overlap_factor = min(3, p.leaf_capacity - 1)
        p.fill_factor = 0.7
        p.dat_extension = "data"
        p.idx_extension = "index"
        self._prop = p

        # ------------------------------------------------------------------
        # Persistente → abrir o crear archivos .index/.data/.meta
        # ------------------------------------------------------------------
        if index_name:
            base = Path(index_name).resolve()
            base.parent.mkdir(parents=True, exist_ok=True)
            self._meta_path = base.with_suffix(".meta")

            data_file = base.with_suffix(".data")
            index_file = base.with_suffix(".index")

            # Reabrir si existe
            if data_file.exists() and index_file.exists() and self._meta_path.exists():
                try:
                    self._idx = rindex.Index(str(base), properties=p)
                    with open(self._meta_path, "r", encoding="utf8") as f:
                        raw = json.load(f)
                    for rid, info in raw.items():
                        self._rows[int(rid)] = PointRec(
                            id=int(rid),
                            coords=tuple(info["coords"]),
                            payload=info["payload"],
                        )
                    self._next_id = max(self._rows.keys(), default=-1) + 1
                    print(f"[INFO] R-Tree reabierto desde {base} con {len(self._rows)} registros.")
                    return
                except Exception:
                    print(f"[WARN] Falló reapertura, recreando índice desde cero...")

            # Si no existe → limpiar archivos corruptos
            for f in (data_file, index_file, self._meta_path):
                if f.exists():
                    f.unlink(missing_ok=True)

            self._idx = rindex.Index(str(base), properties=p)
        else:
            # En memoria
            self._idx = rindex.Index(properties=p)
            self._meta_path = None

    # ==============================================================
    # Inserción y cierre
    # ==============================================================

    def add_point(self, x: float, y: float, payload: Optional[Dict] = None) -> int:
        """Inserta un punto. Si el Restaurant_ID ya existe, lo reemplaza."""
        payload = {k.strip().replace(" ", "_"): v for k, v in (payload or {}).items()}
        restaurant_id = payload.get("Restaurant_ID")

        # Si ya existe ese Restaurant_ID, reemplazar
        if restaurant_id is not None:
            duplicates = [
                pid for pid, rec in self._rows.items()
                if rec.payload and rec.payload.get("Restaurant_ID") == restaurant_id
            ]
            for pid in duplicates:
                rec = self._rows.pop(pid)
                try:
                    self._idx.delete(pid, (rec.coords[0], rec.coords[1], rec.coords[0], rec.coords[1]))
                except Exception:
                    pass

        # Insertar nuevo punto
        pid = self._next_id
        self._next_id += 1
        rec = PointRec(id=pid, coords=(float(x), float(y)), payload=payload)
        self._rows[pid] = rec
        self._idx.insert(pid, (x, y, x, y))
        self.save()
        return pid

    def close(self):
        """Cierra y guarda metadata si es persistente."""
        if self._meta_path:
            meta = {
                rid: {"coords": rec.coords, "payload": rec.payload}
                for rid, rec in self._rows.items()
            }
            with open(self._meta_path, "w", encoding="utf8") as f:
                json.dump(meta, f, indent=2, ensure_ascii=False)

        try:
            del self._idx
        except Exception:
            pass

        if self.index_name:
            self._idx = rindex.Index(str(Path(self.index_name)), properties=self._prop)

    def save(self):
        """Guarda metadata sin cerrar el índice."""
        if self._meta_path:
            meta = {
                rid: {"coords": rec.coords, "payload": rec.payload}
                for rid, rec in self._rows.items()
            }
            with open(self._meta_path, "w", encoding="utf8") as f:
                json.dump(meta, f, indent=2, ensure_ascii=False)

    # ==============================================================
    # Consultas
    # ==============================================================

    @staticmethod
    def _dist(a: Coord, b: Coord) -> float:
        return math.hypot(a[0] - b[0], a[1] - b[1])

    @staticmethod
    def _haversine_km(a: Coord, b: Coord) -> float:
        R = 6371.0088
        lon1, lat1 = map(math.radians, a)
        lon2, lat2 = map(math.radians, b)
        dlon, dlat = lon2 - lon1, lat2 - lat1
        h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        return 2 * R * math.asin(math.sqrt(h))

    def range_search_km(self, point: Coord, radio_km: float) -> List[Dict]:
        lon, lat = point
        dlat = radio_km / 111.0
        dlon = radio_km / (111.0 * max(math.cos(math.radians(lat)), 1e-9))
        bbox = (lon - dlon, lat - dlat, lon + dlon, lat + dlat)
        out = []
        for pid in self._idx.intersection(bbox):
            rec = self._rows.get(pid)
            if not rec:
                continue
            dkm = self._haversine_km(point, rec.coords)
            if dkm <= radio_km:
                out.append({"id": pid, "dist_km": dkm, **rec.payload})
        return sorted(out, key=lambda r: r["dist_km"])

    def knn(self, point: Coord, k: int = 5) -> List[Dict]:
        qx, qy = map(float, point)
        out = []
        for pid in self._idx.nearest((qx, qy, qx, qy), k):
            rec = self._rows.get(pid)
            if not rec:
                continue
            d = self._dist(rec.coords, (qx, qy))
            out.append({"id": pid, "dist": d, **rec.payload})
        return sorted(out, key=lambda r: r["dist"])

    # ==============================================================
    # Carga desde CSV / DataFrame
    # ==============================================================

    @classmethod
    def from_dataframe(
        cls,
        df: pd.DataFrame,
        x_col: str = "Longitude",
        y_col: str = "Latitude",
        keep_cols: Optional[Iterable[str]] = None,
        index_name: Optional[str] = None,
        max_children: int = 50,
    ) -> RTreePoints:
        keep_cols = list(keep_cols or [])
        rt = cls(index_name=index_name, max_children=max_children)
        for _, row in df.iterrows():
            payload = {c: row[c] for c in keep_cols if c in row}
            rt.add_point(row[x_col], row[y_col], payload)
       # rt.close()  # guardar metadata
        return rt

    # ==============================================================
    # Utilidades / Debug
    # ==============================================================

    def stats(self) -> Dict:
        return {
            "total_points": len(self._rows),
            "max_children": self.max_children,
            "mode": "persistente" if self.index_name else "memoria",
            "created": self._created_time,
        }

    def debug_dump(self, limit: Optional[int] = None):
        mode = "persistente" if self.index_name else "en memoria"
        print("\n=== RTree Debug Dump ===")
        print(f"Modo             : {mode}")
        print(f"Número de puntos : {len(self._rows)}")
        print(f"Capacidad hojas  : {self.max_children}")
        if self.index_name:
            print(f"Ruta base        : {self.index_name}")
        if self._rows:
            xs = [r.coords[0] for r in self._rows.values()]
            ys = [r.coords[1] for r in self._rows.values()]
            print(f"Bounding Box     : ({min(xs):.3f}, {min(ys):.3f}) → ({max(xs):.3f}, {max(ys):.3f})")
        if limit:
            print(f"Primeros {limit} registros:")
            for i, (pid, rec) in enumerate(self._rows.items()):
                if i >= limit:
                    break
                print(f"  → id={pid}, coords={rec.coords}, payload={rec.payload}")
        print("==========================\n")

    def remove_point_by_id(self, restaurant_id: int):
        """Elimina TODOS los puntos cuya payload['Restaurant_ID'] == restaurant_id."""
        matches = []
        for pid, rec in list(self._rows.items()):
            if rec.payload and rec.payload.get("Restaurant_ID") == restaurant_id:
                matches.append((pid, rec.coords))

        if not matches:
            print(f"[WARN] R-Tree: Restaurant_ID={restaurant_id} no encontrado.")
            return

        for pid, (x, y) in matches:
            try:
                if hasattr(self, "_idx"):
                    self._idx.delete(pid, (x, y, x, y))
            except Exception as e:
                print(f"[WARN] Falló eliminación en índice espacial (pid={pid}): {e}")
            self._rows.pop(pid, None)

        # Persistir sin cerrar (más estable)
        self.save()
        print(f"[OK] {len(matches)} punto(s) con Restaurant_ID={restaurant_id} eliminado(s) del R-Tree.")


