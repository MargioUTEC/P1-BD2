from dataclasses import dataclass
from typing import List, Optional, Any

@dataclass
class CreateTableNode:
    """Nodo que representa una sentencia CREATE TABLE."""
    table_name: str
    columns: List[Any]  # lista de ColumnDefNode

    def __repr__(self):
        return f"<CREATE TABLE {self.table_name} ({len(self.columns)} columnas)>"

# ----------------------------------------------------------

@dataclass
class ColumnDefNode:
    """Definición de una columna dentro de CREATE TABLE."""
    name: str
    type_spec: str
    is_key: bool = False
    index_type: Optional[str] = None

    def __repr__(self):
        key_str = " KEY" if self.is_key else ""
        idx_str = f" INDEX {self.index_type}" if self.index_type else ""
        return f"{self.name} {self.type_spec}{key_str}{idx_str}"

# ----------------------------------------------------------

@dataclass
class CreateFromFileNode:
    """Sentencia CREATE TABLE ... FROM FILE ... [USING ...]"""
    def __init__(self, table_name, file_path, using_indexes=None):
        self.table_name = table_name
        self.file_path = file_path
        self.using_indexes = using_indexes or []

    def __repr__(self):
        indexes = ", ".join(self.using_indexes) if self.using_indexes else "ALL"
        return f"<CREATE FROM FILE {self.table_name} USING [{indexes}] FROM '{self.file_path}'>"

# ----------------------------------------------------------

@dataclass
class InsertNode:
    """Sentencia INSERT INTO ... VALUES (...)"""
    table_name: str
    values: List[Any]

    def __repr__(self):
        return f"<INSERT INTO {self.table_name} VALUES {self.values}>"

# ----------------------------------------------------------

@dataclass
class DeleteNode:
    """Sentencia DELETE FROM ... WHERE ..."""
    table_name: str
    condition: Any

    def __repr__(self):
        return f"<DELETE FROM {self.table_name} WHERE {self.condition}>"

# ----------------------------------------------------------

@dataclass
class SelectNode:
    """Sentencia SELECT común."""
    table: str
    columns: List[str]
    condition: Optional[Any] = None

    def __repr__(self):
        cond = f" WHERE {self.condition}" if self.condition else ""
        return f"<SELECT {', '.join(self.columns)} FROM {self.table}{cond}>"

# ----------------------------------------------------------

@dataclass
class SelectSpatialNode:
    """Sentencia SELECT espacial (R-Tree) o condición espacial."""
    table: Optional[str]      # puede ser None si viene de WHERE ... IN (...)
    column: str
    point: List[float]
    radius: float

    def __repr__(self):
        table_str = f" FROM {self.table}" if self.table else ""
        return f"<SELECT{table_str} WHERE {self.column} IN (POINT={self.point}, R={self.radius})>"

# ----------------------------------------------------------
# ⚙️ 2. Condiciones y expresiones
# ----------------------------------------------------------

@dataclass
class ConditionNode:
    """Condición general de tipo A op B (ej. id = 5, edad > 20)."""
    attribute: str
    operator: str
    value: Any

    def __repr__(self):
        return f"{self.attribute} {self.operator} {self.value}"

# ----------------------------------------------------------

@dataclass
class BetweenConditionNode:
    """Condición tipo BETWEEN (ej. nombre BETWEEN 'A' AND 'M')."""
    attribute: str
    value1: Any
    value2: Any

    def __repr__(self):
        return f"{self.attribute} BETWEEN {self.value1} AND {self.value2}"

# ----------------------------------------------------------
#  3. Utilidades
# ----------------------------------------------------------

@dataclass
class ValueNode:
    """Nodo genérico para representar valores (números, strings, arrays)."""
    value: Any

    def __repr__(self):
        return f"'{self.value}'" if isinstance(self.value, str) else str(self.value)

# ----------------------------------------------------------

@dataclass
class ArrayNode:
    """Nodo que representa listas de valores (ej. coordenadas o arrays)."""
    values: List[Any]

    def __repr__(self):
        return f"[{', '.join(map(str, self.values))}]"

# ----------------------------------------------------------
# SELECT con condición WHERE
# ----------------------------------------------------------

@dataclass
class SelectWhereNode:
    """
    Sentencia SELECT ... FROM ... WHERE ...
    Compatible con condiciones simples, BETWEEN, IN (espacial) o compuestas.
    """
    table_name: str
    condition: Optional[Any] = None
    columns: Optional[List[str]] = None

    def __repr__(self):
        cols = ", ".join(self.columns) if self.columns else "*"
        cond = f" WHERE {self.condition}" if self.condition else ""
        return f"<SELECT {cols} FROM {self.table_name}{cond}>"


# ----------------------------------------------------------
# Condiciones compuestas (AND / OR)
# ----------------------------------------------------------

@dataclass
class ConditionComplexNode:
    """
    Representa una condición compuesta con AND / OR.
    Ejemplo:
      City = "Makati" AND Rating > 4
      (Rating > 4 AND Votes > 200) OR City = "Manila"
    """
    left: Any
    operator: str   # "AND" o "OR"
    right: Any

    def __repr__(self):
        return f"({self.left} {self.operator} {self.right})"
