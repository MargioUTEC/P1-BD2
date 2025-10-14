from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, List, Tuple, Optional, Union, Dict
import math
import pandas as pd

try:
    from rtree import index as rindex
except ImportError as e:
    raise ImportError(
        "Falta la librería 'rtree'. Instálala con: pip install rtree\n"
        "En Linux puede requerir libspatialindex."
    ) from e


Coord = Tuple[float, float]  # asumimos 2D para consultas


@dataclass
class PointRec:

    id: int
    coords: Coord
    payload: Optional[Dict] = None  # columnas extra del CSV


class RTreePoints:

    def __init__(self, max_children: int = 50):
        p = rindex.Property()

        p.dimension = 2
        p.dat_extension = 'data'
        p.idx_extension = 'index'
        p.leaf_capacity = max_children
        p.fill_factor = 0.7
        self._prop = p
        self._idx = rindex.Index(properties=p)
        self._rows: Dict[int, PointRec] = {}
        self._next_id = 0


    def add_point(self, x: float, y: float, payload: Optional[Dict] = None) -> int:
        pid = self._next_id
        self._next_id += 1
        self._rows[pid] = PointRec(id=pid, coords=(float(x), float(y)), payload=payload or {})

        self._idx.insert(pid, (x, y, x, y))
        return pid

    @staticmethod
    def _haversine_km(a, b):
        R = 6371.0088
        lon1, lat1 = map(math.radians, a)
        lon2, lat2 = map(math.radians, b)
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        return 2 * R * math.asin(math.sqrt(h))

    def rangeSearch_km(self, point, radio_km):
        lon, lat = point

        dlat = radio_km / 111.0
        dlon = radio_km / (111.0 * max(math.cos(math.radians(lat)), 1e-9))
        bbox = (lon - dlon, lat - dlat, lon + dlon, lat + dlat)
        candidates = list(self._idx.intersection(bbox))
        out = []
        for pid in candidates:
            rec = self._rows[pid]
            dkm = self._haversine_km(point, rec.coords)
            if dkm <= radio_km:
                out.append({"id": rec.id, "x": rec.coords[0], "y": rec.coords[1], "dist_km": dkm, **rec.payload})
        out.sort(key=lambda r: r["dist_km"])
        return out

    @classmethod
    def from_dataframe(
        cls,
        df: pd.DataFrame,
        x_col: str,
        y_col: str,
        keep_cols: Optional[Iterable[str]] = None,
        max_children: int = 50
    ) -> "RTreePoints":
        keep_cols = list(keep_cols or [])
        rt = cls(max_children=max_children)
        for _, row in df.iterrows():
            payload = {c: row[c] for c in keep_cols if c in row}
            rt.add_point(row[x_col], row[y_col], payload)
        return rt


    @staticmethod
    def _dist(a: Coord, b: Coord) -> float:
        dx = a[0] - b[0]
        dy = a[1] - b[1]
        return math.hypot(dx, dy)


    def rangeSearch(self, point: Coord, radio: Optional[float] = None, k: Optional[int] = None):

        if (radio is None) == (k is None):
            raise ValueError("Usa exactamente uno: 'radio' (range) o 'k' (k-NN).")

        qx, qy = float(point[0]), float(point[1])

        if radio is not None:

            mins = (qx - radio, qy - radio, qx + radio, qy + radio)
            candidates = list(self._idx.intersection(mins))

            out: List[Dict] = []
            for pid in candidates:
                rec = self._rows[pid]
                d = self._dist(rec.coords, (qx, qy))
                if d <= radio:
                    out.append({
                        "id": rec.id,
                        "x": rec.coords[0],
                        "y": rec.coords[1],
                        "dist": d,
                        **rec.payload
                    })

            out.sort(key=lambda r: r["dist"])
            return out

        else:

            k = int(k)
            ids = list(self._idx.nearest((qx, qy, qx, qy), k))
            out = []
            for pid in ids:
                rec = self._rows[pid]
                d = self._dist(rec.coords, (qx, qy))
                out.append({
                    "id": rec.id,
                    "x": rec.coords[0],
                    "y": rec.coords[1],
                    "dist": d,
                    **rec.payload
                })

            out.sort(key=lambda r: r["dist"])
            return out



def load_points_csv(
    csv_path: str,
    x_col: Optional[str] = None,
    y_col: Optional[str] = None,
    keep_cols: Optional[Iterable[str]] = None,
    max_children: int = 50
) -> Tuple[RTreePoints, str, str]:

    df = pd.read_csv(csv_path)

    if x_col is None or y_col is None:
        candidates = [
            ("x", "y"),
            ("X", "Y"),
            ("lon", "lat"),
            ("longitude", "latitude"),
            ("Lon", "Lat"),
        ]
        pick = None
        for xc, yc in candidates:
            if xc in df.columns and yc in df.columns:
                pick = (xc, yc)
                break
        if pick is None:
            raise ValueError(
                f"No pude deducir columnas de coordenadas. Columnas disponibles: {list(df.columns)}.\n"
                "Pásame x_col e y_col explícitamente."
            )
        x_col, y_col = pick

    rt = RTreePoints.from_dataframe(df, x_col=x_col, y_col=y_col, keep_cols=keep_cols or [], max_children=max_children)
    return rt, x_col, y_col

