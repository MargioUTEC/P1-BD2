from typing import Any, List
from lark import Tree, Token

def _looks_like_condition(obj) -> bool:
    return hasattr(obj, "attribute") and hasattr(obj, "operator") and hasattr(obj, "value")

def _looks_like_between(obj) -> bool:
    return hasattr(obj, "attribute") and hasattr(obj, "value1") and hasattr(obj, "value2")

def _looks_like_spatial(obj) -> bool:
    return hasattr(obj, "column") and hasattr(obj, "point") and hasattr(obj, "radius")


class Executor:
    """
    Motor de ejecución central.
    Recibe instrucciones del SQLExecutor y las ejecuta en el storage manager.
    """

    def __init__(self, storage_manager):
        self.sm = storage_manager

    # ======================================================
    # CREATE TABLE
    # ======================================================
    def create_table(self, table_name: str, columns: List[dict]):
        self.sm.create_table(table_name, columns)
        print(f"[OK] Tabla '{table_name}' creada correctamente.")

    # ======================================================
    # CREATE TABLE FROM FILE
    # ======================================================
    def create_table_from_file(self, table_name: str, file_path: str, index_type: str, key_column: str):
        data = self.sm.load_from_csv(file_path)
        self.sm.create_table(table_name, self.sm.infer_schema(data))
        self.sm.build_index(table_name, index_type, key_column, data)
        print(f"[OK] Tabla '{table_name}' creada desde archivo '{file_path}' con índice {index_type}.")

    # ======================================================
    # INSERT
    # ======================================================
    def insert(self, table_name: str, values: List[Any]):
        self.sm.insert_record(table_name, values)
        print(f"[OK] Registro insertado en '{table_name}'.")

    # ======================================================
    # DELETE
    # ======================================================
    def delete(self, table_name: str, condition):
        count = self.sm.delete_records(table_name, condition)
        print(f"[OK] {count} registro(s) eliminado(s) de '{table_name}'.")
        return count

    # ======================================================
    # SELECT
    # ======================================================
    def select(self, table: str, columns: List[str], condition=None):
        # 1) Normaliza si llega un Tree de Lark
        if isinstance(condition, Tree):
            if condition.data == "condition_comparison":
                col, op, val = condition.children
                col = col.value if isinstance(col, Token) else str(col)
                op  = op.value  if isinstance(op, Token)  else str(op)
                condition = type("Cond", (), {"attribute": col, "operator": op, "value": val})()

            elif condition.data == "condition_between":
                col, v1, v2 = condition.children
                col = col.value if isinstance(col, Token) else str(col)
                condition = type("Between", (), {"attribute": col, "value1": v1, "value2": v2})()

            elif condition.data == "condition_in":
                col, spatial = condition.children
                col = col.value if isinstance(col, Token) else str(col)
                if isinstance(spatial, Tree) and spatial.data == "spatial_expr":
                    point, radius = spatial.children
                else:
                    point, radius = spatial
                condition = type("Spatial", (), {"column": str(col), "point": point, "radius": float(radius)})()
            else:
                raise ValueError(f"Condición Tree no reconocida: {condition.data}")

        # 2) Sin condición → todo
        if not condition:
            return self.sm.select_all(table)

        # 3) Comparación simple (=, >, <, >=, <=)
        if _looks_like_condition(condition):
            op = condition.operator
            key = condition.attribute
            value = condition.value
            if op == "=":
                return self.sm.search_exact(table, key, value)
            elif op in [">", "<", ">=", "<="]:
                if hasattr(self.sm, "search_comparison"):
                    return self.sm.search_comparison(table, key, op, value)
                return [{"_info": f"mock {key} {op} {value}"}]
            raise ValueError(f"Operador no soportado: {op}")

        # 4) BETWEEN
        if _looks_like_between(condition):
            return self.sm.search_range(table, condition.attribute, condition.value1, condition.value2)

        # 5) Espacial
        if _looks_like_spatial(condition):
            return self.sm.search_spatial(table, condition.column, condition.point, condition.radius)

        # 6) Si llegó algo raro, muéstralo
        raise ValueError(f"Tipo de condición desconocido: {type(condition).__name__} -> {condition!r}")

    # ======================================================
    # SELECT ESPACIAL
    # ======================================================
    def select_spatial(self, table: str, column: str, point: List[float], radius: float):
        return self.sm.search_spatial(table, column, point, radius)
