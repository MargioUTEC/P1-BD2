# test_parser/indexes/avl/avl_file.py
import os, struct
from dataclasses import dataclass
from typing import Optional, List

# ---------- NODOS (archivo .avl) ----------
# header: root_pos (int32, -1 si vacío)
# nodo:   id:int32, left:int32, right:int32, height:int32, data_off:int64
NODE_FMT = struct.Struct("<iiiiq")  # 5 campos
ROOT_FMT = struct.Struct("<i")

@dataclass
class AVLNode:
    id: int
    left: int
    right: int
    height: int
    data_off: int  # offset en .dat


# ---------- REGISTROS (archivo .dat) ----------
# id:int32, name:50s, city:30s, lon:float, lat:float, avg_cost:int32, agg_rating:float, votes:int32
REC_FMT = struct.Struct("<i50s30sffifi")

def _pad(s: str, n: int) -> bytes:
    b = s.encode("utf-8", errors="ignore")[:n]
    return b + b"\x00" * (n - len(b))

def _unpad(b: bytes) -> str:
    return b.decode("utf-8", errors="ignore").rstrip("\x00").strip()


# ============================================================
# MANEJO DE ARCHIVOS DE DATOS Y NODOS
# ============================================================
class AVLDataFile:
    """Archivo heap para payload de restaurantes."""
    def __init__(self, filename_dat: str):
        self.filename = filename_dat
        if not os.path.exists(self.filename):
            with open(self.filename, "wb"):
                pass

    def write_record(self, rec: dict) -> int:
        packed = REC_FMT.pack(
            int(rec["restaurant_id"]),
            _pad(rec["restaurant_name"], 50),
            _pad(rec["city"], 30),
            float(rec["longitude"]),
            float(rec["latitude"]),
            int(rec.get("average_cost_for_two", 0)),
            float(rec.get("aggregate_rating", 0.0)),
            int(rec.get("votes", 0)),
        )
        with open(self.filename, "ab") as f:
            off = f.tell()
            f.write(packed)
        return off

    def read_record(self, off: int) -> dict:
        with open(self.filename, "rb") as f:
            f.seek(off)
            raw = f.read(REC_FMT.size)
        (rid, name_b, city_b, lon, lat, avg_cost, agg, votes) = REC_FMT.unpack(raw)
        return {
            "restaurant_id": rid,
            "restaurant_name": _unpad(name_b),
            "city": _unpad(city_b),
            "longitude": lon,
            "latitude": lat,
            "average_cost_for_two": avg_cost,
            "aggregate_rating": agg,
            "votes": votes,
        }


class AVLNodesFile:
    """Archivo de nodos AVL (índice)."""
    def __init__(self, filename_avl: str):
        self.filename = filename_avl
        if not os.path.exists(self.filename):
            with open(self.filename, "wb") as f:
                f.write(ROOT_FMT.pack(-1))  # raíz vacía
        with open(self.filename, "rb") as f:
            self.root_pos = ROOT_FMT.unpack(f.read(ROOT_FMT.size))[0]

    def _node_offset(self, pos: int) -> int:
        return ROOT_FMT.size + pos * NODE_FMT.size

    def read_node(self, pos: int) -> AVLNode:
        with open(self.filename, "rb") as f:
            f.seek(self._node_offset(pos))
            data = f.read(NODE_FMT.size)
        rid, left, right, height, data_off = NODE_FMT.unpack(data)
        return AVLNode(rid, left, right, height, data_off)

    def write_node(self, pos: int, node: AVLNode):
        with open(self.filename, "r+b") as f:
            f.seek(self._node_offset(pos))
            f.write(NODE_FMT.pack(node.id, node.left, node.right, node.height, node.data_off))

    def append_node(self, node: AVLNode) -> int:
        with open(self.filename, "ab") as f:
            pos = (f.tell() - ROOT_FMT.size) // NODE_FMT.size
            f.write(NODE_FMT.pack(node.id, node.left, node.right, node.height, node.data_off))
        return pos

    def count_nodes(self) -> int:
        sz = os.path.getsize(self.filename)
        return (sz - ROOT_FMT.size) // NODE_FMT.size

    def save_root(self, pos: int):
        with open(self.filename, "r+b") as f:
            f.seek(0)
            f.write(ROOT_FMT.pack(pos))
        self.root_pos = pos


# ============================================================
# AVL PRINCIPAL (persistente)
# ============================================================
class AVLFile:
    """AVL persistente con 2 archivos: nodos (.avl) y datos (.dat)."""
    def __init__(self, base_path: str):
        self.nodes = AVLNodesFile(base_path + ".avl")
        self.data = AVLDataFile(base_path + ".dat")

    # ============================================================
    # Normalización universal de registros (CSV, parser, frontend)
    # ============================================================
    def normalize_record(self, raw: dict) -> dict:
        if not raw:
            raise ValueError("Registro vacío recibido en normalize_record()")

        clean = {}
        for k, v in raw.items():
            key = k.strip().replace(" ", "_").replace("\ufeff", "").lower()
            clean[key] = v

        mapping = {
            "restaurantid": "restaurant_id",
            "restaurant_id": "restaurant_id",
            "restaurant_name": "restaurant_name",
            "name": "restaurant_name",
            "city": "city",
            "longitude": "longitude",
            "latitude": "latitude",
            "average_cost_for_two": "average_cost_for_two",
            "averagecostfortwo": "average_cost_for_two",
            "aggregate_rating": "aggregate_rating",
            "aggregaterating": "aggregate_rating",
            "votes": "votes",
        }

        record = {}
        for old, new in mapping.items():
            if old in clean:
                record[new] = clean[old]

        # Conversión segura
        record["restaurant_id"] = int(float(record.get("restaurant_id", 0)))
        record["restaurant_name"] = str(record.get("restaurant_name", "")).strip()
        record["city"] = str(record.get("city", "")).strip()
        record["longitude"] = float(record.get("longitude", 0.0))
        record["latitude"] = float(record.get("latitude", 0.0))
        record["average_cost_for_two"] = int(float(record.get("average_cost_for_two", 0)))
        record["aggregate_rating"] = float(record.get("aggregate_rating", 0.0))
        record["votes"] = int(float(record.get("votes", 0)))
        return record

    # ---- helpers ----
    def _height(self, pos: int) -> int:
        if pos == -1:
            return -1
        n = self.nodes.read_node(pos)
        return n.height

    def _update_height(self, pos: int) -> None:
        n = self.nodes.read_node(pos)
        hl = self._height(n.left)
        hr = self._height(n.right)
        n.height = max(hl, hr) + 1
        self.nodes.write_node(pos, n)

    def _balance_factor(self, pos: int) -> int:
        n = self.nodes.read_node(pos)
        return self._height(n.left) - self._height(n.right)

    # ---- rotaciones ----
    def _rotate_right_at(self, pos: int) -> int:
        x = self.nodes.read_node(pos)
        y_pos = x.left
        y = self.nodes.read_node(y_pos)

        x.left = y.right
        y.right = pos
        self.nodes.write_node(pos, x)
        self.nodes.write_node(y_pos, y)

        self._update_height(pos)
        self._update_height(y_pos)
        return y_pos

    def _rotate_left_at(self, pos: int) -> int:
        x = self.nodes.read_node(pos)
        y_pos = x.right
        y = self.nodes.read_node(y_pos)

        x.right = y.left
        y.left = pos
        self.nodes.write_node(pos, x)
        self.nodes.write_node(y_pos, y)

        self._update_height(pos)
        self._update_height(y_pos)
        return y_pos

    def _rebalance_at(self, pos: int) -> int:
        bf = self._balance_factor(pos)
        n = self.nodes.read_node(pos)

        if bf > 1:
            if self._balance_factor(n.left) < 0:
                new_left = self._rotate_left_at(n.left)
                n.left = new_left
                self.nodes.write_node(pos, n)
            return self._rotate_right_at(pos)

        if bf < -1:
            if self._balance_factor(n.right) > 0:
                new_right = self._rotate_right_at(n.right)
                n.right = new_right
                self.nodes.write_node(pos, n)
            return self._rotate_left_at(pos)

        return pos

    # ---- insertar ----
    def insert(self, rec: dict) -> None:
        rec = self.normalize_record(rec)
        data_off = self.data.write_record(rec)
        node = AVLNode(
            id=int(rec["restaurant_id"]),
            left=-1,
            right=-1,
            height=0,
            data_off=data_off,
        )
        new_root = self._insert_rec(self.nodes.root_pos, node)
        if new_root != self.nodes.root_pos:
            self.nodes.save_root(new_root)

    def _insert_rec(self, pos: int, node: AVLNode) -> int:
        if pos == -1:
            return self.nodes.append_node(node)

        n = self.nodes.read_node(pos)
        if node.id < n.id:
            n.left = self._insert_rec(n.left, node)
            self.nodes.write_node(pos, n)
        elif node.id > n.id:
            n.right = self._insert_rec(n.right, node)
            self.nodes.write_node(pos, n)
        else:
            return pos  # duplicado

        self._update_height(pos)
        return self._rebalance_at(pos)

    # ---- buscar ----
    def search(self, rid: int) -> Optional[dict]:
        pos = self.nodes.root_pos
        while pos != -1:
            n = self.nodes.read_node(pos)
            if rid == n.id:
                return self.data.read_record(n.data_off)
            pos = n.left if rid < n.id else n.right
        return None

    # ---- mínimo (pos) ----
    def _min_pos(self, pos: int) -> int:
        cur = pos
        while True:
            n = self.nodes.read_node(cur)
            if n.left == -1:
                return cur
            cur = n.left

    # ---- eliminar ----
    def remove(self, rid: int) -> None:
        new_root = self._remove_rec(self.nodes.root_pos, rid)
        if new_root != self.nodes.root_pos:
            self.nodes.save_root(new_root)

    def _remove_rec(self, pos: int, rid: int) -> int:
        if pos == -1:
            return -1
        n = self.nodes.read_node(pos)
        if rid < n.id:
            n.left = self._remove_rec(n.left, rid)
            self.nodes.write_node(pos, n)
        elif rid > n.id:
            n.right = self._remove_rec(n.right, rid)
            self.nodes.write_node(pos, n)
        else:
            if n.left == -1:
                return n.right
            if n.right == -1:
                return n.left
            succ_pos = self._min_pos(n.right)
            succ = self.nodes.read_node(succ_pos)
            n.id = succ.id
            n.data_off = succ.data_off
            n.right = self._remove_rec(n.right, succ.id)
            self.nodes.write_node(pos, n)

        self._update_height(pos)
        return self._rebalance_at(pos)

    # ---- recorrido / rango ----
    def inorder_ids(self) -> list[int]:
        out: List[int] = []
        self._inorder(self.nodes.root_pos, out)
        return out

    def _inorder(self, pos: int, out: List[int]):
        if pos == -1:
            return
        n = self.nodes.read_node(pos)
        self._inorder(n.left, out)
        out.append(n.id)
        self._inorder(n.right, out)

    def range_search(self, lo: int, hi: int) -> list[dict]:
        out: List[dict] = []
        self._range_rec(self.nodes.root_pos, lo, hi, out)
        return out

    def _range_rec(self, pos: int, lo: int, hi: int, out: List[dict]):
        if pos == -1:
            return
        n = self.nodes.read_node(pos)
        if lo < n.id:
            self._range_rec(n.left, lo, hi, out)
        if lo <= n.id <= hi:
            out.append(self.data.read_record(n.data_off))
        if n.id < hi:
            self._range_rec(n.right, lo, hi, out)

    # ---- recorrido de registros (generador) ----
    def _iter_records(self):
        """Genera todos los registros activos (in-order por ID)."""
        stack = []
        pos = self.nodes.root_pos
        while stack or pos != -1:
            # bajar por la izquierda
            while pos != -1:
                stack.append(pos)
                pos = self.nodes.read_node(pos).left
            pos = stack.pop()
            n = self.nodes.read_node(pos)
            yield self.data.read_record(n.data_off)
            # ir a la derecha
            pos = n.right

    # ---- exportar todos los registros ----
    def export_all(self) -> List[dict]:
        """Devuelve todos los registros (in-order) como lista."""
        return list(self._iter_records())

    # ---- normalización de nombre de atributo ----
    def _normalize_attr(self, attr: str) -> str:
        a = attr.strip().lower().replace(" ", "_")
        alias = {
            "id": "restaurant_id",
            "restaurantid": "restaurant_id",
            "name": "restaurant_name",
            "avg_cost": "average_cost_for_two",
            "avg_cost_for_two": "average_cost_for_two",
            "average_cost": "average_cost_for_two",
            "averagecostfortwo": "average_cost_for_two",
            "rating": "aggregate_rating",
            "aggregate": "aggregate_rating",
            "aggregate_rating": "aggregate_rating",
            "votes": "votes",
        }
        return alias.get(a, a)

    # ---- comparación genérica por atributo numérico ----
    def search_comparison(self, attr: str, op: str, value) -> List[dict]:
        """
        Filtra registros por atributo con operadores: >, <, >=, <=, =.
        Nota: el árbol indexa por ID; esto hace full-scan in-order del .dat.
        """
        attr = self._normalize_attr(attr)
        op = op.strip()
        out: List[dict] = []

        def _ok(a, b) -> bool:
            try:
                # intenta comparar numéricamente si aplica
                fa = float(a)
                fb = float(b)
                a, b = fa, fb
            except Exception:
                # cae a comparación de igualdad como texto si no numérico
                if op != "=":
                    return False
            if op == "=":  return a == b
            if op == ">":  return a >  b
            if op == "<":  return a <  b
            if op == ">=": return a >= b
            if op == "<=": return a <= b
            return False

        for rec in self._iter_records():
            if attr not in rec:
                continue
            if _ok(rec[attr], value):
                out.append(rec)
        return out

    # ---- between genérico por atributo numérico ----
    def search_between(self, attr: str, low, high) -> List[dict]:
        """
        Filtra registros por attr en [low, high] (inclusive).
        """
        attr = self._normalize_attr(attr)
        out: List[dict] = []
        try:
            lowf = float(low); highf = float(high)
        except Exception:
            # si no es numérico, no aplicamos between
            return out

        for rec in self._iter_records():
            if attr not in rec:
                continue
            try:
                v = float(rec[attr])
                if lowf <= v <= highf:
                    out.append(rec)
            except Exception:
                pass
        return out
