from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

@dataclass
class Bucket:
    capacity: int
    local_depth: int
    items: Dict[Any, Any] = field(default_factory=dict)

    def is_full(self) -> bool:
        return len(self.items) >= self.capacity


class ExtendibleHashing:
    def __init__(
        self,
        bucket_capacity: int = 4,
        key_selector: Callable[[Any], Any] = lambda r: r["key"],
        hash_fn: Callable[[Any], int] = hash,
    ):
        if bucket_capacity <= 0:
            raise ValueError("La capacidad del bucket debe ser mayor a 0")
        self.bucket_capacity = bucket_capacity
        self.key_selector = key_selector
        self.hash_fn = hash_fn

        self.global_depth = 1
        root = Bucket(capacity=bucket_capacity, local_depth=1)
        self.directory: List[Bucket] = [root, root]

    # Seccion de Helpers para manejo interno de los buckets
    def _index(self, key_hash: int, depth: Optional[int] = None) -> int:
        d = self.global_depth if depth is None else depth
        return key_hash & ((1 << d) - 1)

    def _double_directory(self) -> None:
        self.directory += self.directory
        self.global_depth += 1

    def _all_indexes_of_bucket(self, bucket: Bucket) -> List[int]:
        return [i for i, b in enumerate(self.directory) if b is bucket]

    def _split_bucket(self, idx: int) -> None:
        old = self.directory[idx]
        if old.local_depth == self.global_depth:
            self._double_directory()

        new_ld = old.local_depth + 1
        b0 = Bucket(self.bucket_capacity, new_ld)
        b1 = Bucket(self.bucket_capacity, new_ld)

        # Reasignar punteros del directorio que apuntaban a old
        for i in self._all_indexes_of_bucket(old):
            bit = (i >> (new_ld - 1)) & 1
            self.directory[i] = b1 if bit else b0

        # Redistribuir elementos
        for k, reg in old.items.items():
            h = self.hash_fn(k)
            bit = (self._index(h, new_ld) >> (new_ld - 1)) & 1
            (b1 if bit else b0).items[k] = reg


# Operaciones para archivos

    def search(self, key: Any) -> Optional[Any]: #función de búsqueda
        h = self.hash_fn(key)
        idx = self._index(h)
        return self.directory[idx].items.get(key)

    def add(self, registro: Any) -> None: #función de inserción 
        key = self.key_selector(registro)
        h = self.hash_fn(key)

        while True:
            idx = self._index(h)
            bucket = self.directory[idx]

            # update si ya existe
            if key in bucket.items:
                bucket.items[key] = registro
                return

            # inserción si hay espacio
            if not bucket.is_full():
                bucket.items[key] = registro
                return

            # split y reintento
            self._split_bucket(idx)

    def remove(self, key: Any) -> bool: #Borra por key de registro
        h = self.hash_fn(key)
        idx = self._index(h)
        bucket = self.directory[idx]

        if key in bucket.items:
            del bucket.items[key]
            return True
        
        return False


#Testeo de mis funciones, quedan pendientes auuuun!!!