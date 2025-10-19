from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
import json
import os
import struct
from pathlib import Path


@dataclass
class Bucket:
    capacity: int
    local_depth: int
    items: Dict[Any, Any] = field(default_factory=dict)

    def is_full(self) -> bool:
        return len(self.items) >= self.capacity

    def can_merge_with(self, other: "Bucket") -> bool:
        return self.local_depth == other.local_depth and len(self.items) == 0 and len(other.items) == 0


class ExtendibleHashing:
    """
    Implementación persistente de Extendible Hashing.
    Guarda buckets en binario y el directorio en JSON.
    """

    _REC_HEADER_FMT = "<II"
    _REC_HEADER_SIZE = struct.calcsize(_REC_HEADER_FMT)

    def __init__(
        self,
        base_path: str = "data",
        bucket_capacity: int = 4,
        key_selector=lambda r: r["Restaurant ID"],
        hash_fn=lambda k: k,
        name: str = "restaurants_hash"
    ):
        if bucket_capacity <= 0:
            raise ValueError("La capacidad del bucket debe ser mayor a 0")

        # ===  Directorio donde se guardará el índice ===
        absolute_base = Path(os.getcwd()) / base_path
        absolute_base.mkdir(parents=True, exist_ok=True)

        # === Rutas absolutas del índice ===
        self.base_path = str(absolute_base)
        self.dir_path = str(absolute_base / f"{name}_dir.json")
        self.data_path = str(absolute_base / f"{name}_data.dat")

        self.bucket_capacity = bucket_capacity
        self.key_selector = key_selector
        self.hash_fn = hash_fn

        self.reads = 0
        self.writes = 0

        # Cargar si existe o inicializar desde cero
        if os.path.exists(self.dir_path) and os.path.exists(self.data_path):
            self._load_dir()
        else:
            self.global_depth = 1
            self.next_bucket_id = 1
            root_id = self._alloc_bucket_id()
            root_bucket = Bucket(self.bucket_capacity, local_depth=1)
            with open(self.data_path, "wb") as _:
                pass
            self.directory: List[int] = [root_id, root_id]
            self.bucket_offsets: Dict[str, int] = {}
            self._write_bucket(root_id, root_bucket)
            self._save_dir()

    # ------------------------------
    # Persistencia de directorio
    # ------------------------------
    def _load_dir(self) -> None:
        with open(self.dir_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        self.reads += 1
        self.global_depth = meta["global_depth"]
        self.bucket_capacity = meta["bucket_capacity"]
        self.next_bucket_id = meta["next_bucket_id"]
        self.directory = meta["directory"]
        self.bucket_offsets = {str(k): int(v) for k, v in meta["bucket_offsets"].items()}

    def _save_dir(self) -> None:
        meta = {
            "global_depth": self.global_depth,
            "bucket_capacity": self.bucket_capacity,
            "next_bucket_id": self.next_bucket_id,
            "directory": self.directory,
            "bucket_offsets": self.bucket_offsets,
        }
        with open(self.dir_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)
        self.writes += 1

    # ------------------------------
    # Buckets
    # ------------------------------
    def _alloc_bucket_id(self) -> int:
        bid = self.next_bucket_id
        self.next_bucket_id += 1
        return bid

    def _read_bucket(self, bucket_id: int) -> Bucket:
        key = str(bucket_id)
        if key not in self.bucket_offsets:
            raise KeyError(f"Bucket id {bucket_id} no encontrado")
        offset = self.bucket_offsets[key]

        with open(self.data_path, "rb") as f:
            f.seek(offset)
            header = f.read(self._REC_HEADER_SIZE)
            rec_id, size = struct.unpack(self._REC_HEADER_FMT, header)
            payload = f.read(size)
        self.reads += 1

        data = json.loads(payload.decode("utf-8"))
        ld = int(data["ld"])
        items = {k: v for (k, v) in data["items"]}
        return Bucket(self.bucket_capacity, ld, items)

    def _write_bucket(self, bucket_id: int, bucket: Bucket) -> None:
        payload = json.dumps({
            "ld": bucket.local_depth,
            "items": list(bucket.items.items())
        }).encode("utf-8")

        with open(self.data_path, "ab") as f:
            offset = f.tell()
            f.write(struct.pack(self._REC_HEADER_FMT, int(bucket_id), len(payload)))
            f.write(payload)
        self.writes += 1

        self.bucket_offsets[str(bucket_id)] = offset
        self._save_dir()

    # ------------------------------
    # Helpers de hashing y split
    # ------------------------------
    def _index(self, key_hash: int, depth: Optional[int] = None) -> int:
        d = self.global_depth if depth is None else depth
        return key_hash & ((1 << d) - 1)

    def _double_directory(self) -> None:
        self.directory += self.directory
        self.global_depth += 1
        self._save_dir()

    def _all_indexes_of_bucket_id(self, bucket_id: int) -> List[int]:
        return [i for i, b in enumerate(self.directory) if b == bucket_id]

    def _buddy_index(self, idx: int, ld: int) -> int:
        return idx ^ (1 << (ld - 1))

    def _split_bucket(self, idx: int) -> None:
        old_id = self.directory[idx]
        old_bucket = self._read_bucket(old_id)

        if old_bucket.local_depth == self.global_depth:
            self._double_directory()

        new_ld = old_bucket.local_depth + 1
        b0_id = self._alloc_bucket_id()
        b1_id = self._alloc_bucket_id()
        b0 = Bucket(self.bucket_capacity, new_ld)
        b1 = Bucket(self.bucket_capacity, new_ld)

        # Reasignar punteros del directorio
        for i in self._all_indexes_of_bucket_id(old_id):
            bit = (i >> (new_ld - 1)) & 1
            self.directory[i] = b1_id if bit else b0_id

        # Redistribuir registros
        for k, reg in old_bucket.items.items():
            try:
                h = int(self.hash_fn(int(k)))  # si es string numérico
            except Exception:
                try:
                    h = int(self.hash_fn(k))
                except Exception:
                    h = abs(hash(str(k)))

            bit = (self._index(h, new_ld) >> (new_ld - 1)) & 1
            (b1 if bit else b0).items[k] = reg

        # Guardar nuevos buckets
        self._write_bucket(b0_id, b0)
        self._write_bucket(b1_id, b1)
        self._save_dir()

    # ------------------------------
    # Operaciones públicas
    # ------------------------------
    def search(self, key: Any) -> Optional[Any]:
        h = self.hash_fn(key)
        idx = self._index(h)
        bid = self.directory[idx]
        bucket = self._read_bucket(bid)
        return bucket.items.get(str(key))

    def add(self, registro: Any) -> None:
        # Clave real
        key_raw = self.key_selector(registro)

        # Guardar clave como string en disco
        key = str(key_raw)

        # Hash numérico (forzamos a int siempre)
        try:
            h = int(self.hash_fn(key_raw))
        except Exception:
            # Si hash_fn devuelve string o algo raro, forzamos a hash nativo
            h = abs(hash(str(key_raw)))

        while True:
            idx = self._index(h)
            bid = self.directory[idx]
            bucket = self._read_bucket(bid)

            # Actualizar si ya existe
            if key in bucket.items:
                bucket.items[key] = registro
                self._write_bucket(bid, bucket)
                return

            # Insertar si hay espacio
            if not bucket.is_full():
                bucket.items[key] = registro
                self._write_bucket(bid, bucket)
                return

            # Si el bucket está lleno, dividirlo
            self._split_bucket(idx)

    def remove(self, key: Any) -> bool:
        h = self.hash_fn(int(key))
        idx = self._index(h)
        bid = self.directory[idx]
        bucket = self._read_bucket(bid)

        key = str(key)
        if key in bucket.items:
            del bucket.items[key]
            self._write_bucket(bid, bucket)
            return True
        return False

    # ------------------------------
    # Debug e informes
    # ------------------------------
    def debug_dump(self) -> None:
        print("=== ExtendibleHashing Debug Dump ===")
        print(f"Base path       : {self.base_path}")
        print(f"Global depth    : {self.global_depth}")
        print(f"Bucket capacity : {self.bucket_capacity}")
        print(f"Directory size  : {len(self.directory)}")
        print(f"Reads/Writes    : {self.reads}/{self.writes}")

        groups: Dict[int, List[int]] = {}
        for i, bid in enumerate(self.directory):
            groups.setdefault(bid, []).append(i)
        for bid, idxs in groups.items():
            b = self._read_bucket(bid)
            print(f"Bucket id={bid} ld={b.local_depth} dir-idx={idxs} keys={list(b.items.keys())}")
