import os
import json
import struct
import re
from typing import List, Any, Optional
from test_parser.core.parser.ast_nodes import ConditionNode, BetweenConditionNode

CATALOG_FILE = "data/catalog.json"
HEADER_FMT = "<IqI"
HEADER_SIZE = struct.calcsize(HEADER_FMT)
FREE_PTR_SIZE = 8

class StorageManager:
    def __init__(self, base_path="data/"):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)
        self.catalog_path = os.path.join(base_path, "catalog.json")

        if os.path.exists(self.catalog_path):
            with open(self.catalog_path, "r", encoding="utf-8") as f:
                self.tables = json.load(f)
        else:
            self.tables = {}
            self._save_catalog()

    def _save_catalog(self):
        with open(self.catalog_path, "w", encoding="utf-8") as f:
            json.dump(self.tables, f, indent=2)

    def _table_path(self, name):
        return os.path.join(self.base_path, f"{name}.dat")

    # ======================================================
    # Crear tabla
    # ======================================================
    def create_table(self, table_name: str, columns: List[dict]):
        if table_name in self.tables:
            raise ValueError(f"La tabla '{table_name}' ya existe.")

        path = self._table_path(table_name)
        fmt = self._build_struct_format(columns)
        body_size = max(struct.calcsize(fmt), FREE_PTR_SIZE)  # aseguro espacio para next_free
        rec_size = 1 + body_size  # 1 byte flag + cuerpo

        with open(path, "wb") as f:
            header = struct.pack(HEADER_FMT, 0, -1, rec_size)
            f.write(header)

        self.tables[table_name] = {
            "columns": columns,
            "file": path,
            "fmt": fmt,
            "rec_size": rec_size
        }
        self._save_catalog()
        print(f"[Storage] Tabla '{table_name}' creada binaria: fmt={fmt}, rec_size={rec_size}")

    def _build_struct_format(self, columns: List[dict]) -> str:
        fmt = "<"
        for col in columns:
            t = col["type"].upper()
            if t.startswith("INT"):
                fmt += "i"
            elif t.startswith("FLOAT"):
                fmt += "f"
            elif t.startswith("VARCHAR"):
                size = int(re.findall(r"\[(\d+)\]", t)[0])
                fmt += f"{size}s"
            elif t.startswith("ARRAY"):
                # Por ahora ARRAY[FLOAT] = 2 floats
                fmt += "ff"
            else:
                raise ValueError(f"Tipo no soportado: {t}")
        return fmt

    def _read_header(self, f):
        f.seek(0)
        data = f.read(HEADER_SIZE)
        if len(data) != HEADER_SIZE:
            raise IOError("Header corrupto o incompleto")
        count, free_head, rec_size = struct.unpack(HEADER_FMT, data)
        return count, free_head, rec_size

    def _write_header(self, f, count: int, free_head: int, rec_size: int):
        f.seek(0)
        f.write(struct.pack(HEADER_FMT, count, free_head, rec_size))

    # ======================================================
    # Insertar registro
    # ======================================================

    def insert_record(self, table_name, values):
        if table_name not in self.tables:
            raise KeyError(f"La tabla '{table_name}' no existe.")

        info = self.tables[table_name]
        path = info["file"]
        fmt = info["fmt"]
        rec_size = info["rec_size"]
        columns = info["columns"]

        with open(path, "r+b") as f:
            count, free_head, record_size = self._read_header(f)

            record_bytes = self._pack_record(columns, fmt, values)

            if free_head != -1:
                # Reutilizar hueco libre
                print(f"[Storage] Reutilizando hueco en offset {free_head}")
                f.seek(free_head)
                next_free = struct.unpack("<q", f.read(8))[0]  # siguiente libre
                f.seek(free_head)
                f.write(record_bytes)

                # Actualizar puntero de free list en el header
                f.seek(struct.calcsize("<i"))  # saltar count
                f.write(struct.pack("<q", next_free))
            else:
                # Insertar al final del archivo
                f.seek(0, os.SEEK_END)
                f.write(record_bytes)

            # Actualizar contador de registros activos
            f.seek(0)
            f.write(struct.pack("<i", count + 1))

        print(f"[Storage] Insertado en '{table_name}': {values}")

    def _pack_record(self, columns: List[dict], fmt: str, values: List[Any]) -> bytes:
        """
        Devuelve un bloque binario: flag (0 activo) + cuerpo.
        """
        flag = struct.pack("<B", 0)  # 0 = activo
        body = self._pack_body(columns, fmt, values)
        return flag + body

    # ======================================================
    # Empaquetar / Desempaquetar
    # ======================================================
    def _pack_body(self, columns: List[dict], fmt: str, values: List[Any]) -> bytes:
        packed_vals = []
        for col, val in zip(columns, values):
            t = col["type"].upper()
            if t.startswith("INT"):
                packed_vals.append(int(val))
            elif t.startswith("FLOAT"):
                packed_vals.append(float(val))
            elif t.startswith("VARCHAR"):
                size = int(re.findall(r"\[(\d+)\]", t)[0])
                s = str(val).encode("utf-8")[:size]
                s += b"\x00" * (size - len(s))
                packed_vals.append(s)
            elif t.startswith("ARRAY"):
                # por simplicidad, 2 floats
                if isinstance(val, list) and len(val) == 2:
                    packed_vals.extend([float(val[0]), float(val[1])])
                else:
                    packed_vals.extend([0.0, 0.0])
            else:
                raise ValueError(f"Tipo no soportado en pack: {t}")

        body = struct.pack(fmt, *packed_vals)
        return body

    def _unpack_body(self, columns: List[dict], fmt: str, body: bytes) -> List[Any]:
        vals = list(struct.unpack(fmt, body[:struct.calcsize(fmt)]))
        out = []
        i = 0
        for col in columns:
            t = col["type"].upper()
            if t.startswith("VARCHAR"):
                s = vals[i].decode("utf-8", errors="ignore").rstrip("\x00")
                out.append(s)
                i += 1
            elif t.startswith("ARRAY"):
                out.append([vals[i], vals[i+1]])
                i += 2
            else:
                out.append(vals[i])
                i += 1
        return out

    # ======================================================
    # Recorrido de registros (y utilidades)
    # ======================================================
    def _iter_slots(self, f, rec_size: int):
        """Itera (offset_slot, flag) de cada slot desde después del header hasta EOF."""
        f.seek(0, os.SEEK_END)
        file_end = f.tell()
        pos = HEADER_SIZE
        while pos + rec_size <= file_end:
            f.seek(pos)
            flag = struct.unpack("<B", f.read(1))[0]
            yield pos, flag
            pos += rec_size

    # ======================================================
    # SELECT *
    # ======================================================
    def select_all(self, table_name: str):
        if table_name not in self.tables:
            raise KeyError(f"La tabla '{table_name}' no existe.")

        info = self.tables[table_name]
        path = info["file"]
        fmt = info["fmt"]
        rec_size = info["rec_size"]
        columns = info["columns"]

        out = []
        with open(path, "rb") as f:
            count, free_head, rec_size_hdr = self._read_header(f)
            for off, flag in self._iter_slots(f, rec_size_hdr):
                if flag != 0:  # borrado
                    continue
                f.seek(off + 1)
                body = f.read(rec_size_hdr - 1)
                out.append(self._unpack_body(columns, fmt, body))
        return out

    # ======================================================
    # Búsqueda exacta (key = value)
    # ======================================================
    def search_exact(self, table_name: str, key: str, value: Any):
        if table_name not in self.tables:
            raise KeyError(f"La tabla '{table_name}' no existe.")
        info = self.tables[table_name]
        names = [c["name"] for c in info["columns"]]
        if key not in names:
            raise ValueError(f"La columna '{key}' no existe en {table_name}.")
        idx = names.index(key)

        results = []
        for row in self.select_all(table_name):
            if str(row[idx]) == str(value):
                results.append(row)
        return results

    # ======================================================
    # DELETE (eliminación lógica + free list)
    # ======================================================
    def delete_records(self, table_name: str, condition):
        """
        Por ahora soportamos:
         - ConditionNode (col op val) con op "="
         - BetweenConditionNode (no-op: retorna 0 por ahora)
        """
        if table_name not in self.tables:
            raise KeyError(f"La tabla '{table_name}' no existe.")

        info = self.tables[table_name]
        path = info["file"]
        fmt = info["fmt"]
        rec_size = info["rec_size"]
        columns = info["columns"]
        names = [c["name"] for c in columns]

        def match_row(row):
            if isinstance(condition, ConditionNode):
                if condition.operator != "=":
                    return False
                if condition.attribute not in names:
                    return False
                col_idx = names.index(condition.attribute)
                return str(row[col_idx]) == str(condition.value)
            elif isinstance(condition, BetweenConditionNode):
                return False
            return False

        deleted = 0
        with open(path, "r+b") as f:
            count, free_head, rec_size_hdr = self._read_header(f)

            for off, flag in self._iter_slots(f, rec_size_hdr):
                if flag != 0:
                    continue
                f.seek(off + 1)
                body = f.read(rec_size_hdr - 1)
                row = self._unpack_body(columns, fmt, body)
                if match_row(row):
                    f.seek(off)
                    f.write(struct.pack("<B", 1))
                    f.write(struct.pack("<q", free_head))
                    free_head = off
                    deleted += 1

            if deleted:
                count = max(0, count - deleted)
                self._write_header(f, count, free_head, rec_size_hdr)

        print(f"[Storage] DELETE: {deleted} registro(s) marcados como borrados en '{table_name}'")
        return deleted


    def search_range(self, table, key, v1, v2):
        info = self.tables[table]
        names = [c["name"] for c in info["columns"]]
        if key not in names:
            return []
        idx = names.index(key)
        out = []
        for row in self.select_all(table):
            try:
                val = row[idx]
                if str(v1) <= str(val) <= str(v2):
                    out.append(row)
            except Exception:
                pass
        return out

    def search_comparison(self, table, key, op, value):
        info = self.tables[table]
        names = [c["name"] for c in info["columns"]]
        if key not in names:
            return []
        idx = names.index(key)
        out = []
        for row in self.select_all(table):
            try:
                val = row[idx]
                if op == ">" and val > value: out.append(row)
                if op == "<" and val < value: out.append(row)
                if op == ">=" and val >= value: out.append(row)
                if op == "<=" and val <= value: out.append(row)
            except Exception:
                pass
        return out

    def search_spatial(self, table, column, point, radius):
        return []

    def debug_dump_table(self, table_name: str):
        """
        Muestra información interna de la tabla:
        - Header (count, free_head, rec_size)
        - Offsets, flags y contenido bruto de cada slot
        Muy útil para depurar la free list.
        """
        if table_name not in self.tables:
            raise KeyError(f"La tabla '{table_name}' no existe.")

        info = self.tables[table_name]
        path = info["file"]
        rec_size = info["rec_size"]
        fmt = info["fmt"]
        columns = info["columns"]

        print(f"\n================ DEBUG DUMP: {table_name} ================")
        with open(path, "rb") as f:
            count, free_head, rec_size_hdr = self._read_header(f)
            print(f"[HEADER] count={count}, free_head={free_head}, rec_size={rec_size_hdr}")

            for off, flag in self._iter_slots(f, rec_size_hdr):
                f.seek(off + 1)
                body = f.read(rec_size_hdr - 1)

                if flag == 1:
                    next_free = struct.unpack("<q", body[:8])[0]
                    print(f"[SLOT] off={off:04d} | flag=1 (BORRADO) | next_free={next_free}")
                else:
                    row = self._unpack_body(columns, fmt, body)
                    print(f"[SLOT] off={off:04d} | flag=0 (ACTIVO)  | data={row}")

        print("==========================================================\n")
