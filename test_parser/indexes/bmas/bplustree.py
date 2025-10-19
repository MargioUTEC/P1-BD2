from __future__ import annotations
import os
import csv
import json
import pickle
import time
from math import ceil

# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DATA_FILE = os.path.join(DATA_DIR, "bplustree_index.dat")
META_FILE = os.path.join(DATA_DIR, "bplustree_meta.json")

ORDER = 4               # Máximo nº de claves por nodo
BLOCK_SIZE = 4096       # Tamaño fijo de bloque en disco


# ============================================================
# CLASE NODO
# ============================================================

class BPlusNode:
    """
    Nodo de un B+Tree.
    - Para hojas: children almacena 'valores' (punteros/registros) y next_leaf enlaza con la siguiente hoja.
    - Para internos: children almacena posiciones de hijos en el archivo.
    """
    def __init__(self, is_leaf=False, keys=None, children=None, next_leaf=-1):
        self.is_leaf = is_leaf
        self.keys = keys or []
        self.children = children or []
        self.next_leaf = next_leaf

    def serialize(self) -> bytes:
        return pickle.dumps({
            "is_leaf": self.is_leaf,
            "keys": self.keys,
            "children": self.children,
            "next_leaf": self.next_leaf
        })

    @staticmethod
    def deserialize(binary_data: bytes) -> "BPlusNode":
        data = pickle.loads(binary_data)
        return BPlusNode(
            is_leaf=data["is_leaf"],
            keys=data["keys"],
            children=data["children"],
            next_leaf=data["next_leaf"]
        )


# ============================================================
# ARCHIVO BINARIO (manejo de páginas/bloques)
# ============================================================

class BPlusTreeFile:
    """
    Encapsula las operaciones de bajo nivel sobre el archivo binario de nodos.
    Lleva contadores de lecturas/escrituras.
    """
    def __init__(self, filename=DATA_FILE):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        self.filename = filename
        self.reads = 0
        self.writes = 0
        if not os.path.exists(filename):
            open(filename, "wb").close()

    def write_node(self, node: BPlusNode, position: int | None = None) -> int:
        """Escribe un nodo en disco y devuelve la posición lógica (número de bloque)."""
        data = node.serialize()
        if len(data) > BLOCK_SIZE:
            raise ValueError("Nodo excede el tamaño máximo de bloque.")
        data += b'\x00' * (BLOCK_SIZE - len(data))

        with open(self.filename, "r+b") as f:
            if position is None:
                f.seek(0, os.SEEK_END)
                position = f.tell() // BLOCK_SIZE
            else:
                f.seek(position * BLOCK_SIZE)
            f.write(data)

        self.writes += 1
        return position

    def read_node(self, position: int) -> BPlusNode:
        """Lee un nodo desde su posición lógica (número de bloque)."""
        with open(self.filename, "rb") as f:
            f.seek(position * BLOCK_SIZE)
            data = f.read(BLOCK_SIZE)
        self.reads += 1
        return BPlusNode.deserialize(data)


# ============================================================
# IMPLEMENTACIÓN DEL B+ TREE
# ============================================================

class BPlusTreeIndex:
    """
    B+Tree persistente en disco.
    - Inserción con división de nodos (leaf/internal).
    - Búsqueda exacta y por rango.
    - Eliminación básica (sin rebalanceo).
    - Persistencia de metadatos (posición de la raíz, parámetros).
    - Contadores de I/O y tiempos.
    """
    def __init__(self, data_file: str = DATA_FILE, meta_file: str = META_FILE):
        self.file = BPlusTreeFile(data_file)
        self.meta_file = meta_file
        self.start_time = time.time()

        # Intentar cargar metadatos existentes
        if os.path.exists(self.meta_file) and os.path.getsize(data_file) > 0:
            self._load_meta()
        else:
            # Crear raíz nueva (hoja)
            root = BPlusNode(is_leaf=True)
            self.root_pos = self.file.write_node(root)  # normalmente retorna 0 en archivo vacío
            self._save_meta()

    # ------------------------------
    # Persistencia de metadatos
    # ------------------------------
    def _save_meta(self):
        meta = {
            "root_pos": self.root_pos,
            "order": ORDER,
            "block_size": BLOCK_SIZE,
            "data_file": self.file.filename
        }
        os.makedirs(os.path.dirname(self.meta_file), exist_ok=True)
        with open(self.meta_file, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)

    def _load_meta(self):
        with open(self.meta_file, "r", encoding="utf-8") as f:
            meta = json.load(f)
        # Validaciones mínimas
        if meta.get("order") != ORDER or meta.get("block_size") != BLOCK_SIZE:
            # Si cambiaste parámetros, podrías reconstruir. Aquí asumimos consistencia.
            pass
        self.root_pos = meta["root_pos"]

    # ------------------------------
    # Búsqueda exacta
    # ------------------------------
    def search(self, key):
        node = self.file.read_node(self.root_pos)
        while not node.is_leaf:
            i = 0
            while i < len(node.keys) and key >= node.keys[i]:
                i += 1
            node = self.file.read_node(node.children[i])
        # En hoja: búsqueda lineal (puedes optimizar con bisect)
        for i, k in enumerate(node.keys):
            if k == key:
                return node.children[i]
        return None

    # ------------------------------
    # Búsqueda por rango
    # ------------------------------
    def range_search(self, start_key, end_key):
        # Baja hasta la hoja donde podría comenzar start_key
        node = self.file.read_node(self.root_pos)
        while not node.is_leaf:
            i = 0
            while i < len(node.keys) and start_key >= node.keys[i]:
                i += 1
            node = self.file.read_node(node.children[i])

        results = []
        # Recorre hojas enlazadas hacia la derecha
        while node:
            for k, v in zip(node.keys, node.children):
                if start_key <= k <= end_key:
                    results.append((k, v))
                elif k > end_key:
                    return results
            node = self.file.read_node(node.next_leaf) if node.next_leaf != -1 else None
        return results

    # ------------------------------
    # Inserción
    # ------------------------------
    def insert(self, key, value):
        new_pos, new_child, split_key = self._insert_recursive(self.root_pos, key, value)
        if new_child is not None:
            # Crear nueva raíz
            new_root = BPlusNode(is_leaf=False, keys=[split_key],
                                 children=[self.root_pos, new_child])
            self.root_pos = self.file.write_node(new_root)
            self._save_meta()

    # Alias para parser
    def add(self, record):
        """Interfaz genérica: recibe registro y extrae clave/valor estándar."""
        # Asume nombres normalizados si vienen como dict pythonic
        # o estilo CSV normalizado a snake_case.
        if isinstance(record, dict):
            # Intenta variantes comunes:
            key = record.get("restaurant_id") or record.get("Restaurant ID")
            value = record.get("restaurant_name") or record.get("Restaurant Name")
            if key is None:
                raise KeyError("No se encontró la clave 'restaurant_id' en el registro.")
            self.insert(int(key), value)
        else:
            raise TypeError("add(record) espera un dict con restaurant_id / restaurant_name.")

    def _insert_recursive(self, pos, key, value):
        node = self.file.read_node(pos)

        # Caso hoja
        if node.is_leaf:
            # Evitar duplicados (puedes optar por actualizar el valor si existe)
            if key in node.keys:
                idx = node.keys.index(key)
                node.children[idx] = value
                self.file.write_node(node, position=pos)
                return pos, None, None

            node.keys.append(key)
            node.children.append(value)
            combined = sorted(zip(node.keys, node.children))
            node.keys, node.children = map(list, zip(*combined))

            if len(node.keys) > ORDER:
                return self._split_leaf(pos, node)

            self.file.write_node(node, position=pos)
            return pos, None, None

        # Caso nodo interno
        i = 0
        while i < len(node.keys) and key >= node.keys[i]:
            i += 1
        child_pos = node.children[i]
        child_pos, new_child, split_key = self._insert_recursive(child_pos, key, value)

        if new_child is not None:
            node.keys.insert(i, split_key)
            node.children.insert(i + 1, new_child)

            if len(node.keys) > ORDER:
                return self._split_internal(pos, node)

            self.file.write_node(node, position=pos)

        return pos, None, None

    def _split_leaf(self, pos, node: BPlusNode):
        mid = len(node.keys) // 2
        right = BPlusNode(is_leaf=True,
                          keys=node.keys[mid:],
                          children=node.children[mid:])
        node.keys = node.keys[:mid]
        node.children = node.children[:mid]

        right_pos = self.file.write_node(right)
        right.next_leaf = node.next_leaf
        node.next_leaf = right_pos

        self.file.write_node(node, position=pos)
        self.file.write_node(right, position=right_pos)
        # La clave de separación para el padre es la primera del nuevo derecho
        return pos, right_pos, right.keys[0]

    def _split_internal(self, pos, node: BPlusNode):
        mid = len(node.keys) // 2
        split_key = node.keys[mid]

        right = BPlusNode(is_leaf=False,
                          keys=node.keys[mid + 1:],
                          children=node.children[mid + 1:])
        node.keys = node.keys[:mid]
        node.children = node.children[:mid + 1]

        right_pos = self.file.write_node(right)
        self.file.write_node(node, position=pos)
        return pos, right_pos, split_key

    # ------------------------------
    # Eliminación (básica: elimina en hoja, sin rebalancear)
    # ------------------------------
    def remove(self, key) -> bool:
        """
        Eliminación básica:
        - Localiza la hoja, elimina la pareja (k,v) si existe.
        - No hace redistribución ni merge (los nodos pueden quedar sub-ocupados).
        Retorna True si eliminó, False si no encontró.
        """
        removed = self._remove_in_leaf(self.root_pos, key)
        if removed:
            self._save_meta()
        return removed

    # Alias para parser
    def delete(self, key) -> bool:
        return self.remove(key)

    def _remove_in_leaf(self, pos, key) -> bool:
        node = self.file.read_node(pos)
        if node.is_leaf:
            if key in node.keys:
                i = node.keys.index(key)
                del node.keys[i]
                del node.children[i]
                self.file.write_node(node, position=pos)
                return True
            return False

        # Nodo interno: bajar al hijo adecuado
        i = 0
        while i < len(node.keys) and key >= node.keys[i]:
            i += 1
        return self._remove_in_leaf(node.children[i], key)

    # ------------------------------
    # Utilidades
    # ------------------------------
    def print_tree(self, pos=None, level=0):
        if pos is None:
            pos = self.root_pos
        node = self.file.read_node(pos)
        indent = "  " * level
        if node.is_leaf:
            print(f"{indent}Hoja[{pos}] -> {list(zip(node.keys, node.children))} (next={node.next_leaf})")
        else:
            print(f"{indent}Nodo[{pos}] -> Keys: {node.keys} | Children: {node.children}")
            for child in node.children:
                self.print_tree(child, level + 1)

    def stats(self):
        elapsed_ms = (time.time() - self.start_time) * 1000.0
        print(f"[B+Tree] Reads={self.file.reads} | Writes={self.file.writes} | Tiempo={elapsed_ms:.2f} ms")

    # Aliases útiles para el parser / estilo del enunciado
    def rangeSearch(self, begin_key, end_key):
        return self.range_search(begin_key, end_key)


# ============================================================
# CLASE RECORD (CSV robusto con normalización)
# ============================================================

class Record:
    def __init__(self, **kwargs):
        self.data = kwargs

    @staticmethod
    def normalize(text: str) -> str:
        """Normaliza nombres de columnas: quita BOM, comillas, espacios; minúscula con _."""
        return (text.strip()
                .replace("\ufeff", "")
                .replace('"', "")
                .replace("'", "")
                .replace(" ", "_")
                .lower())

    @staticmethod
    def load_from_csv(csv_path: str):
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"No se encontró el CSV: {csv_path}")
        records = []
        with open(csv_path, encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            reader.fieldnames = [Record.normalize(h) for h in reader.fieldnames]
            for row in reader:
                normalized = {Record.normalize(k): (v.strip() if isinstance(v, str) else v)
                              for k, v in row.items()}
                records.append(Record(**normalized))
        if not records:
            raise ValueError("El CSV está vacío o mal formateado.")
        return records
