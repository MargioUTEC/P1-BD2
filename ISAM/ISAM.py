import os
import struct
from dataclasses import dataclass
from typing import List, Tuple, Optional
from bisect import bisect_right
from bisect import bisect_left
import csv
import unicodedata

# =========================
# CONFIGURACIÓN / CONSTANTES
# =========================

BLOCK_FACTOR = 8  # tamaño de página de datos (registros por página)

# para el índice: (name, city, id)
NAME_BYTES = 64
CITY_BYTES = 48
ID_DIGITS = 10
KEY_SIZE = NAME_BYTES + CITY_BYTES + ID_DIGITS  # 122 bytes

PAGE_HEADER_FORMAT = "<iq"
PAGE_HEADER_SIZE   = struct.calcsize(PAGE_HEADER_FORMAT)

# =========================
# HELPERS
# =========================

def _pad(s: str, n: int) -> bytes:
    return s[:n].ljust(n).encode("utf-8", errors="ignore")


def normalize_text(s: str) -> str:
    """
    Normaliza un texto: pasa a minúsculas, elimina acentos y
    caracteres especiales invisibles (como guiones largos).
    """
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    return ''.join(c for c in s if not unicodedata.combining(c))


def make_key(name: str, city: str, restaurant_id: int) -> str:
    """
    Genera una clave segura y reproducible basada en bytes,
    insensible a tildes, mayúsculas y símbolos invisibles.
    """

    def fix_field(s: str, nbytes: int) -> str:
        s = normalize_text(s)
        b = s.encode('utf-8', errors='ignore')[:nbytes]
        return b.decode('utf-8', errors='ignore').ljust(nbytes)

    name_field = fix_field(name, NAME_BYTES)
    city_field = fix_field(city, CITY_BYTES)
    id_field = f"{int(restaurant_id):0{ID_DIGITS}d}"
    return name_field + city_field + id_field


# =========================
# RECORD (REGISTRO DE RESTAURANTE)
# =========================

@dataclass
class Record:
    restaurant_id: int
    name: str
    country_code: int
    city: str
    address: str
    cuisines: str
    avg_cost_for_two: int
    currency: str
    has_table_booking: bool
    has_online_delivery: bool
    is_delivering_now: bool
    price_range: int
    aggregate_rating: float
    rating_text: str
    votes: int
    longitude: float
    latitude: float

    # name (64), city (48), address (96), cuisines (96), currency (16), rating_text (16)
    FORMAT = (
        "<i"                        # restaurant_id
        f"{NAME_BYTES}s"            # name
        "i"                         # country_code
        f"{CITY_BYTES}s"            # city
        "96s"                       # address
        "96s"                       # cuisines
        "i"                         # avg_cost_for_two
        "16s"                       # currency
        "?" "?" "?"                 # has_table_booking, has_online_delivery, is_delivering_now
        "i"                         # price_range
        "d"                         # aggregate_rating
        "16s"                       # rating_text
        "i"                         # votes
        "d" "d"                     # longitude, latitude
    )
    SIZE = struct.calcsize(FORMAT)

    @staticmethod
    def from_csv_row(row: List[str]) -> "Record":
        # Columnas:
        # 0 id, 1 name, 2 country_code, 3 city, 4 address,
        # 7 longitude, 8 latitude, 9 cuisines,
        # 10 avg_cost, 11 currency, 12 has_table_booking, 13 has_online_delivery,
        # 14 is_delivering_now, 16 price_range, 17 aggregate_rating,
        # 19 rating_text, 20 votes
        def yesno(x: str) -> bool: return x.strip().lower() == "yes"
        return Record(
            restaurant_id = int(row[0]),
            name = row[1],
            country_code = int(row[2]),
            city = row[3],
            address = row[4],
            cuisines = row[9],
            avg_cost_for_two = int(row[10]),
            currency = row[11],
            has_table_booking = yesno(row[12]),
            has_online_delivery = yesno(row[13]),
            is_delivering_now = yesno(row[14]),
            price_range = int(row[16]),
            aggregate_rating = float(row[17]),
            rating_text = row[19],
            votes = int(row[20]),
            longitude = float(row[7]),
            latitude = float(row[8]),
        )

    def pack(self) -> bytes:
        return struct.pack(
            self.FORMAT,
            self.restaurant_id,
            _pad(self.name, NAME_BYTES),
            self.country_code,
            _pad(self.city, CITY_BYTES),
            _pad(self.address, 96),
            _pad(self.cuisines, 96),
            self.avg_cost_for_two,
            _pad(self.currency, 16),
            self.has_table_booking,
            self.has_online_delivery,
            self.is_delivering_now,
            self.price_range,
            self.aggregate_rating,
            _pad(self.rating_text, 16),
            self.votes,
            self.longitude,
            self.latitude,
        )

    @staticmethod
    def unpack(data: bytes) -> "Record":
        (rid, name_b, ccode, city_b, addr_b, cuis_b, avgc, curr_b,
         tb, od, dn, pr, ar, rt_b, votes, lon, lat) = struct.unpack(Record.FORMAT, data)

        def dec(b: bytes, n: Optional[int] = None) -> str:
            return b.decode("utf-8", errors="ignore").rstrip("\x00 ").strip()

        return Record(
            restaurant_id=rid,
            name=dec(name_b),
            country_code=ccode,
            city=dec(city_b),
            address=dec(addr_b),
            cuisines=dec(cuis_b),
            avg_cost_for_two=avgc,
            currency=dec(curr_b),
            has_table_booking=tb,
            has_online_delivery=od,
            is_delivering_now=dn,
            price_range=pr,
            aggregate_rating=ar,
            rating_text=dec(rt_b),
            votes=votes,
            longitude=lon,
            latitude=lat,
        )

    def key(self) -> str:
        return make_key(self.name, self.city, self.restaurant_id)

    def __repr__(self) -> str:
        return (f"#{self.restaurant_id} | {self.name} ({self.city}) | "
                f"rating={self.aggregate_rating:.1f} | price_range={self.price_range}")

# =========================
# PÁGINAS DE DATOS
# =========================

class Page:
    HEADER_FORMAT = PAGE_HEADER_FORMAT
    HEADER_SIZE   = PAGE_HEADER_SIZE
    SIZE_OF_PAGE  = HEADER_SIZE + BLOCK_FACTOR * Record.SIZE

    def __init__(self, records: Optional[List[Record]] = None, next_page: int = -1):
        self.records: List[Record] = records or []
        self.next_page = next_page

    def pack(self) -> bytes:
        header = struct.pack(self.HEADER_FORMAT, len(self.records), self.next_page)
        body = b"".join(r.pack() for r in self.records)
        body += b"\x00" * (BLOCK_FACTOR * Record.SIZE - len(body))
        return header + body

    @staticmethod
    def unpack(buf: bytes) -> "Page":
        size, nxt = struct.unpack(Page.HEADER_FORMAT, buf[:Page.HEADER_SIZE])
        recs, off = [], Page.HEADER_SIZE
        for _ in range(size):
            recs.append(Record.unpack(buf[off:off+Record.SIZE]))
            off += Record.SIZE
        return Page(recs, nxt)

    def first_key(self) -> Optional[str]:
        return self.records[0].key() if self.records else None

class DataFile:
    def __init__(self, filename: str):
        self.filename = filename

    def _ensure(self):
        if not os.path.exists(self.filename):
            with open(self.filename, "wb"): pass

    def page_count(self) -> int:
        if not os.path.exists(self.filename): return 0
        return os.path.getsize(self.filename) // Page.SIZE_OF_PAGE

    def read_page_at(self, offset: int) -> Page:
        with open(self.filename, "rb") as f:
            f.seek(offset)
            return Page.unpack(f.read(Page.SIZE_OF_PAGE))

    def write_page_at(self, offset: int, page: Page) -> None:
        with open(self.filename, "r+b") as f:
            f.seek(offset)
            f.write(page.pack())

    def append_page(self, page: Page) -> int:
        self._ensure()
        with open(self.filename, "ab") as f:
            off = f.tell()
            f.write(page.pack())
            return off

    def iter_pages(self) -> List[Tuple[int, Page]]:
        out: List[Tuple[int, Page]] = []
        if not os.path.exists(self.filename): return out
        with open(self.filename, "rb") as f:
            total = self.page_count()
            for i in range(total):
                off = i * Page.SIZE_OF_PAGE
                f.seek(off)
                out.append((off, Page.unpack(f.read(Page.SIZE_OF_PAGE))))
        return out

# =========================
# ÍNDICE MULTINIVEL (3+ niveles)
# =========================

INDEX_FANOUT = 64  # número máx de hijos por nodo = F (claves F-1). Ajusta según memoria/página índice.

NODE_HEADER_FMT = "B i q q"
NODE_KEYS_FMT = f"{(INDEX_FANOUT-1)*KEY_SIZE}s"
NODE_PTRS_FMT = f"{(INDEX_FANOUT-1)}q"
NODE_FMT = f"<{NODE_HEADER_FMT}{NODE_KEYS_FMT}{NODE_PTRS_FMT}"
NODE_SIZE = struct.calcsize(NODE_FMT)


def _pack_keys(keys: list[str]) -> bytes:
    out = []
    for k in keys:
        b = k.encode("utf-8")[:KEY_SIZE]
        out.append(b + b" "*(KEY_SIZE - len(b)))
    for _ in range((INDEX_FANOUT-1) - len(keys)):
        out.append(b" " * KEY_SIZE)
    return b"".join(out)

def _unpack_keys(buf: bytes, kcount: int) -> list[str]:
    out = []
    for i in range(INDEX_FANOUT-1):
        s = buf[i * KEY_SIZE:(i + 1) * KEY_SIZE].decode("utf-8", errors="ignore").rstrip()
        if i < kcount:
            out.append(s)
    return out


class IndexNode:
    __slots__ = ("is_leaf","key_count","p0","next_sib","keys","ptrs","offset")

    def __init__(self, is_leaf: bool, p0: int=-1, keys=None, ptrs=None, next_sib: int=-1):
        self.is_leaf = 1 if is_leaf else 0
        self.key_count = len(keys or [])
        self.p0 = p0
        self.next_sib = next_sib
        self.keys = keys or []
        self.ptrs = ptrs or []
        self.offset = -1

    def pack(self) -> bytes:
        keys_blob = _pack_keys(self.keys)
        # rellena los punteros faltantes con -1
        ptrs_full = (self.ptrs + [-1]*((INDEX_FANOUT-1)-len(self.ptrs)))[:(INDEX_FANOUT-1)]
        header = struct.pack("<Biq q", self.is_leaf, self.key_count, self.p0, self.next_sib)
        body = keys_blob + struct.pack(f"<{(INDEX_FANOUT-1)}q", *ptrs_full)
        return header + body

    @staticmethod
    def unpack(buf: bytes) -> "IndexNode":
        header_size = struct.calcsize("<Biq q")
        is_leaf, key_count, p0, next_sib = struct.unpack("<Biq q", buf[:header_size])
        keys_blob_size = (INDEX_FANOUT-1)*KEY_SIZE
        keys_blob = buf[header_size:header_size+keys_blob_size]
        keys = _unpack_keys(keys_blob, key_count)
        ptrs_start = header_size + keys_blob_size
        ptrs_vals = list(struct.unpack(f"<{(INDEX_FANOUT-1)}q", buf[ptrs_start:ptrs_start+8*(INDEX_FANOUT-1)]))
        ptrs = ptrs_vals[:key_count]
        nd = IndexNode(bool(is_leaf), p0, keys, ptrs, next_sib)
        return nd

class MultiLevelIndex:
    """
    Índice estático multinivel (ISAM 3+ niveles).
    Nivel hoja: apunta a páginas de datos.
    Niveles internos y raíz: apuntan a nodos hijos.
    """
    def __init__(self, filename: str):
        self.filename = filename
        self.root_off: int = -1
        # si existe el archivo de índice, infiere el root como el último nodo
        try:
            if os.path.exists(self.filename):
                sz = os.path.getsize(self.filename)
                if sz >= NODE_SIZE:
                    # el root se escribe de último
                    self.root_off = sz - NODE_SIZE
        except Exception:
            # Si algo sale mal, mantenemos -1 y el caller puede reconstruir
            self.root_off = -1
    def _ensure(self):
        if not os.path.exists(self.filename):
            with open(self.filename, "wb"): pass

    def _append_node(self, node: IndexNode) -> int:
        self._ensure()
        with open(self.filename, "ab") as f:
            off = f.tell()
            f.write(node.pack())
            node.offset = off
            return off

    def _read_node(self, off: int) -> IndexNode:
        with open(self.filename, "rb") as f:
            f.seek(off)
            buf = f.read(NODE_SIZE)
            nd = IndexNode.unpack(buf)
            nd.offset = off
            return nd

    def _write_node(self, off: int, node: IndexNode):
        with open(self.filename, "r+b") as f:
            f.seek(off)
            f.write(node.pack())

    def rebuild_from_data(self, data_file: "DataFile") -> None:
        """
        Reconstruye el índice apuntando SOLO a páginas base (no overflow)
        y garantizando orden global por first_key.
        """
        # 1) Recolectar offsets de todas las páginas y el conjunto de overflows
        all_pages: List[Tuple[int, Page]] = data_file.iter_pages()
        overflow_set = set()
        # Un solo pase: cada página puede apuntar a su next_page; así recolectamos
        # todos los offsets de overflow en el archivo (cadenas completas).
        for off, pg in all_pages:
            nxt = pg.next_page
            if nxt != -1:
                overflow_set.add(nxt)

        # 2) Considerar como "páginas base" aquellas que NO están en overflow_set
        base_offsets: List[int] = []
        for off, pg in all_pages:
            if off not in overflow_set:
                base_offsets.append(off)

        # 3) Construir entradas hoja (first_key, page_off) SOLO de base_pages
        leaves_entries: List[Tuple[str, int]] = []
        for off in base_offsets:
            pg = data_file.read_page_at(off)
            if pg.records:
                leaves_entries.append((pg.records[0].key(), off))

        # Si no hay entradas, índice vacío
        if not leaves_entries:
            if os.path.exists(self.filename):
                os.remove(self.filename)
            self.root_off = -1
            return

        # 4) ORDENAR GLOBALMENTE por first_key
        leaves_entries.sort(key=lambda t: t[0])

        # 5) Empaquetar en nodos hoja
        if os.path.exists(self.filename):
            os.remove(self.filename)

        leaf_nodes: List[IndexNode] = []
        cap_keys = INDEX_FANOUT - 1  # número de claves por nodo
        i = 0
        while i < len(leaves_entries):
            # Cada hoja describe hasta cap_keys+1 punteros: p0 .. pm
            chunk = leaves_entries[i:i + cap_keys + 1]
            p0 = chunk[0][1]
            keys = [k for (k, _) in chunk[1:]]
            ptrs = [p for (_, p) in chunk[1:]]
            leaf_nodes.append(IndexNode(is_leaf=True, p0=p0, keys=keys, ptrs=ptrs))
            i += (cap_keys + 1)

        # 6) Escribir hojas
        child_ptrs = [self._append_node(nd) for nd in leaf_nodes]

        # 7) Subir niveles hasta raíz única
        def node_first_key(nd: IndexNode, data: DataFile) -> str:
            if nd.is_leaf:
                pg = data.read_page_at(nd.p0)
                return pg.first_key() or ""
            else:
                child = self._read_node(nd.p0)
                return node_first_key(child, data)

        current_children = child_ptrs
        while len(current_children) > 1:
            parents: List[IndexNode] = []
            j = 0
            while j < len(current_children):
                kids = current_children[j:j + cap_keys + 1]
                p0 = kids[0]
                keys: List[str] = []
                ptrs: List[int] = []
                for kid_off in kids[1:]:
                    kid = self._read_node(kid_off)
                    keys.append(node_first_key(kid, data_file))
                    ptrs.append(kid_off)
                parents.append(IndexNode(is_leaf=False, p0=p0, keys=keys, ptrs=ptrs))
                j += (cap_keys + 1)
            current_children = [self._append_node(nd) for nd in parents]

        self.root_off = current_children[0]

    def find_page_offset(self, key: str) -> int:
        if self.root_off == -1 or not os.path.exists(self.filename):
            return -1

        off = self.root_off
        while True:
            nd = self._read_node(off)
            # Usamos bisect_right para elegir el hijo correcto cuando key == Ki
            i = bisect_right(nd.keys, key)

            if nd.is_leaf:
                # i=0 -> p0; si no, P_{i-1}
                return nd.p0 if i == 0 else nd.ptrs[i - 1]

            # Nodo interno: descender al hijo elegido por i
            off = nd.p0 if i == 0 else nd.ptrs[i - 1]

    def update_first_key_of_page(self, data: "DataFile", page_off: int, new_first_key: Optional[str]) -> None:
        """Actualiza la clave en el nodo hoja correspondiente si cambió la first_key de una página base."""
        if self.root_off == -1 or new_first_key is None:
            return
        # DFS para recolectar hojas
        stack = [self.root_off]
        leaves: List[IndexNode] = []
        while stack:
            nd = self._read_node(stack.pop())
            if nd.is_leaf:
                leaves.append(nd)
            else:
                # recorrer hijos
                for p in reversed(nd.ptrs):
                    stack.append(p)
                stack.append(nd.p0)
        # 1) caso: page_off coincide con algún P_i (actualiza K_i)
        for leaf in leaves:
            for idx, p in enumerate(leaf.ptrs):
                if p == page_off:
                    leaf.keys[idx] = new_first_key
                    self._write_node(leaf.offset, leaf)
                    return
                """
        # 2) caso: page_off es p0 de una hoja -> actualiza su primera key (si existe) // ver
        for leaf in leaves:
            if leaf.p0 == page_off and leaf.key_count > 0:
                leaf.keys[0] = new_first_key
                self._write_node(leaf.offset, leaf)
                return
"""
# =========================
# ISAM (usa el índice multinivel)
# =========================

class ISAM:
    def __init__(self, data_path="restaurants.dat", index_path="restaurants.idx"):
        self.data = DataFile(data_path)
        self.index = MultiLevelIndex(index_path)

    def _chain_offsets(self, start_off: int) -> List[int]:
        chain, off = [], start_off
        while off != -1:
            chain.append(off)
            off = self.data.read_page_at(off).next_page
        return chain

    # ---------- Build ----------
    def build(self, records: List[Record]) -> None:
        records.sort(key=lambda r: r.key())
        for path in (self.data.filename, self.index.filename):
            if os.path.exists(path): os.remove(path)

        batch: List[Record] = []
        for r in records:
            batch.append(r)
            if len(batch) == BLOCK_FACTOR:
                self.data.append_page(Page(batch)); batch = []
        if batch:
            self.data.append_page(Page(batch))

        self.index.rebuild_from_data(self.data)

    # ---------- Búsqueda ----------
    def search(self, name: str, city: str, restaurant_id: Optional[int] = None):
        """
        Busca un registro por (name, city, restaurant_id) de forma consistente con make_key().
        Tolera UTF-8, tildes y variaciones de mayúsculas/minúsculas.
        """
        # Verifica que el índice exista
        if not os.path.exists(self.index.filename) or self.index.root_off == -1:
            return None

        # --- Normalización ---
        name_norm = normalize_text(name)
        city_norm = normalize_text(city)

        # Clave: usa un ID neutro alto si es None (evita caer en la página anterior)
        rid = restaurant_id if restaurant_id is not None else 9999999999
        k = make_key(name_norm, city_norm, rid)

        # --- Buscar la página base ---
        off = self.index.find_page_offset(k)
        visited = set()

        while off != -1 and off not in visited:
            visited.add(off)
            pg = self.data.read_page_at(off)

            for rec in pg.records:
                # Comparación exacta con normalización simétrica
                if (normalize_text(rec.name) == name_norm and
                        normalize_text(rec.city) == city_norm and
                        (restaurant_id is None or rec.restaurant_id == restaurant_id)):
                    return off, rec

            off = pg.next_page

        # --- Búsqueda secundaria si no se encuentra ---
        # Si no se especificó ID, realiza barrido por prefijo (útil para búsquedas parciales)
        if restaurant_id is None:
            base_prefix = (name_norm[:NAME_BYTES].ljust(NAME_BYTES) +
                           city_norm[:CITY_BYTES].ljust(CITY_BYTES))
            for off, pg in self.data.iter_pages():
                for rec in pg.records:
                    rec_prefix = (normalize_text(rec.name)[:NAME_BYTES].ljust(NAME_BYTES) +
                                  normalize_text(rec.city)[:CITY_BYTES].ljust(CITY_BYTES))
                    if rec_prefix == base_prefix:
                        return off, rec

        return None

    def range_search(self, begin_key: str, end_key: str):
        res = []
        off = self.index.find_page_offset(begin_key)
        while off != -1:
            pg = self.data.read_page_at(off)
            # registros de esta página (en orden)
            for rec in pg.records:
                k = rec.key()
                if k < begin_key:
                    continue
                if k > end_key:
                    return res
                res.append((off, rec))
            # recorrer overflow de esta base
            nxt_off = pg.next_page
            while nxt_off != -1:
                pgo = self.data.read_page_at(nxt_off)
                for rec in pgo.records:
                    k = rec.key()
                    if k > end_key:
                        return res
                    if k >= begin_key:
                        res.append((nxt_off, rec))
                nxt_off = pgo.next_page
            # pasar a la siguiente página base física
            off += Page.SIZE_OF_PAGE
            if off >= os.path.getsize(self.data.filename):
                break
        return res

    # ---------- Inserción ----------
    # Política simple: mantener ISAM clásico con overflow; no alteramos estructura del índice (estático).
    def insert(self, rec: Record) -> None:
        if not os.path.exists(self.index.filename) or self.index.root_off == -1:
            self.build([rec]); return

        base_off = self.index.find_page_offset(rec.key())
        base_pg = self.data.read_page_at(base_off)

        if len(base_pg.records) < BLOCK_FACTOR:
            base_pg.records.append(rec)
            base_pg.records.sort(key=lambda r: r.key())
            self.data.write_page_at(base_off, base_pg)
            # si cambia la first_key de la base, actualiza hoja
            self.index.update_first_key_of_page(self.data, base_off, base_pg.first_key())
            return

        # Overflow: encadenar al final de la cadena
        chain = self._chain_offsets(base_off)
        last_off = chain[-1]
        last_pg = self.data.read_page_at(last_off)
        if len(last_pg.records) < BLOCK_FACTOR:
            last_pg.records.append(rec)
            last_pg.records.sort(key=lambda r: r.key())
            self.data.write_page_at(last_off, last_pg)
        else:
            new_off = self.data.append_page(Page([rec], next_page=-1))
            last_pg.next_page = new_off
            self.data.write_page_at(last_off, last_pg)
            # Siempre asegurar que el índice esté sincronizado en disco
        if hasattr(self.index, "flush"):
            self.index.flush()
        if hasattr(self.data, "flush"):
            self.data.flush()

        self.index.rebuild_from_data(self.data)
    # ---------- Eliminación ----------
    def delete(self, name: str, city: str, restaurant_id: Optional[int] = None) -> bool:
        if not os.path.exists(self.index.filename) or self.index.root_off == -1: return False
        rid = restaurant_id if restaurant_id is not None else 0
        k = make_key(name, city, rid)
        base_off = self.index.find_page_offset(k)

        prev_off, off = None, base_off
        while off != -1:
            pg = self.data.read_page_at(off)
            idx = next((i for i, r in enumerate(pg.records)
                        if r.name.lower().strip() == name.lower().strip() and
                           r.city.lower().strip() == city.lower().strip() and
                           (restaurant_id is None or r.restaurant_id == restaurant_id)), -1)
            if idx == -1:
                prev_off, off = off, pg.next_page
                continue

            # borrar
            del pg.records[idx]

            # caso base
            if off == base_off:
                if pg.records:
                    self.data.write_page_at(off, pg)
                    self.index.update_first_key_of_page(self.data, off, pg.first_key())

                else:
                    # página base quedó vacía
                    if pg.next_page != -1:
                        next_off = pg.next_page
                        nxt = self.data.read_page_at(next_off)
                        if nxt.records:
                            promoted = nxt.records.pop(0)
                            pg.records = [promoted]
                            pg.next_page = nxt.next_page
                            self.data.write_page_at(off, pg)
                            if nxt.records:
                                self.data.write_page_at(next_off, nxt)
                            self.index.update_first_key_of_page(self.data, off, pg.first_key())
                            self.index.update_first_key_of_page(self.data, next_off, nxt.first_key() or "")

                        else:
                            pg.next_page = nxt.next_page
                            self.data.write_page_at(off, pg)
                            self.index.update_first_key_of_page(self.data, off, pg.first_key() or "")
                    else:
                        self.data.write_page_at(off, pg)
                        self.index.update_first_key_of_page(self.data, off, pg.first_key() or "")
                return True

            # caso overflow
            if pg.records:
                self.data.write_page_at(off, pg)
            else:
                if prev_off is not None:
                    prev_pg = self.data.read_page_at(prev_off)
                    prev_pg.next_page = pg.next_page
                    self.data.write_page_at(prev_off, prev_pg)
            return True

        return False

    # ---------- Depuración / Resumen ----------
    def scan_all(self) -> None:
        for i, (off, page) in enumerate(self.data.iter_pages(), 1):
            print(f"--- Page {i} @ {off}  next={page.next_page}")
            for r in page.records:
                print("   ", r)

# =========================
# UTILIDADES
# =========================

def _read_restaurants_csv(csv_path: str) -> List[Record]:
    recs: List[Record] = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader, None)  # descartar encabezado
        for row in reader:
            if not row or not row[0].strip():
                continue
            recs.append(Record.from_csv_row(row))
    return recs

def _print_result(tag: str, res):
    if res is None:
        print(f"{tag}: NOT FOUND")
    else:
        off, r = res
        print(f"{tag}: FOUND @off={off} -> {r}")

def _quick_summary(isam: ISAM, max_pages: int = 6) -> None:
    print("\n== RESUMEN (primeras páginas de datos) ==")
    for i, (off, page) in enumerate(isam.data.iter_pages(), 1):
        if i > max_pages:
            print("...")
            break
        fk = page.records[0].key() if page.records else None
        print(f"Page {i:>2} @ {off} | next={page.next_page} | count={len(page.records)} | first_key={fk}")
        for r in page.records[:min(3, len(page.records))]:
            print("   ", r)

def _index_summary(idx: MultiLevelIndex, data: DataFile) -> None:
    if idx.root_off == -1 or not os.path.exists(idx.filename):
        print("\n== ÍNDICE: vacío =="); return
    print("\n== ÍNDICE (resumen por niveles) ==")
    # BFS por niveles
    level = 0
    frontier = [idx.root_off]
    total_nodes = 0
    while frontier:
        next_frontier = []
        leaf_count = 0
        internal_count = 0
        for off in frontier:
            nd = idx._read_node(off)
            total_nodes += 1
            if nd.is_leaf:
                leaf_count += 1
            else:
                internal_count += 1
                next_frontier.append(nd.p0)
                next_frontier.extend(nd.ptrs)
        kind = []
        if internal_count: kind.append(f"internos={internal_count}")
        if leaf_count:     kind.append(f"hojas={leaf_count}")
        print(f"Nivel {level}: " + ", ".join(kind))
        frontier = next_frontier
        level += 1
    print(f"Altura total: {level} niveles (0 = raíz)")
    print(f"Nodos totales: {total_nodes}")

