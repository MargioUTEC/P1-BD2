
from test_parser.core.parser.ast_nodes import (
    CreateTableNode, CreateFromFileNode, InsertNode,
    DeleteNode, SelectNode, SelectSpatialNode
)
from test_parser.core.query_engine.executor import Executor
import inspect
import test_parser.core.query_engine.executor as _qe_mod


def _looks_like_condition(obj) -> bool:
    return hasattr(obj, "attribute") and hasattr(obj, "operator") and hasattr(obj, "value")

def _looks_like_between(obj) -> bool:
    return hasattr(obj, "attribute") and hasattr(obj, "value1") and hasattr(obj, "value2")

def _looks_like_spatial(obj) -> bool:
    return hasattr(obj, "column") and hasattr(obj, "point") and hasattr(obj, "radius")


class SQLExecutor:
    """ Fachada: recibe nodos AST y delega en el Query Engine (Executor).
        Para SELECT aplica una capa de compatibilidad directa con storage_manager. """

    def __init__(self, storage_manager):
        self.executor = Executor(storage_manager)
        self.sm = self.executor.sm

        try:
            print(f"[DEBUG] executor module path: {inspect.getfile(_qe_mod)}")
        except Exception:
            pass

    def execute(self, node):
        if isinstance(node, CreateTableNode):
            return self._exec_create_table(node)
        elif isinstance(node, CreateFromFileNode):
            return self._exec_create_from_file(node)
        elif isinstance(node, InsertNode):
            return self._exec_insert(node)
        elif isinstance(node, DeleteNode):
            return self._exec_delete(node)
        elif isinstance(node, SelectSpatialNode):
            return self._exec_select_spatial(node)
        elif isinstance(node, SelectNode):
            return self._exec_select(node)
        else:
            raise ValueError(f"[ERROR] Tipo de nodo no reconocido: {type(node).__name__}")

    # ======================================================
    # CREATE TABLE
    # ======================================================
    def _exec_create_table(self, node: CreateTableNode):
        columns = [{
            "name": col.name,
            "type": col.type_spec,
            "is_key": col.is_key,
            "index": col.index_type,
        } for col in node.columns]
        self.executor.create_table(node.table_name, columns)
        return f"Tabla '{node.table_name}' creada con {len(columns)} columnas."

    # ======================================================
    # CREATE FROM FILE
    # ======================================================
    def _exec_create_from_file(self, node: CreateFromFileNode):
        self.executor.create_table_from_file(
            table_name=node.table_name,
            file_path=node.file_path,
            index_type=node.index_type,
            key_column=node.index_key
        )
        return (f"Tabla '{node.table_name}' creada desde archivo "
                f"{node.file_path} con índice {node.index_type}.")

    # ======================================================
    # INSERT
    # ======================================================
    def _exec_insert(self, node: InsertNode):
        self.executor.insert(node.table_name, node.values)
        return f"Registro insertado en '{node.table_name}': {node.values}"

    # ======================================================
    # DELETE
    # ======================================================
    def _exec_delete(self, node: DeleteNode):
        deleted = self.executor.delete(node.table_name, node.condition)
        return f"{deleted} registro(s) eliminado(s) de '{node.table_name}'."

    def _exec_select(self, node: SelectNode):
        table = node.table
        cond = node.condition

        # Sin condición → devolver todo
        if not cond:
            results = self.sm.select_all(table)
            return results if results else f"No se encontraron resultados en '{table}'."

        # Comparación simple (=, >, <, >=, <=)
        if _looks_like_condition(cond):
            key = cond.attribute
            op = cond.operator
            val = cond.value
            if op == "=":
                results = self.sm.search_exact(table, key, val)
            elif op in [">", "<", ">=", "<="]:
                if hasattr(self.sm, "search_comparison"):
                    results = self.sm.search_comparison(table, key, op, val)
                else:
                    results = [{"_info": f"mock {key} {op} {val}"}]
            else:
                raise ValueError(f"Operador no soportado: {op}")
            return results if results else f"No se encontraron resultados en '{table}'."

        # BETWEEN
        if _looks_like_between(cond):
            results = self.sm.search_range(table, cond.attribute, cond.value1, cond.value2)
            return results if results else f"No se encontraron resultados en '{table}'."

        # Espacial desde WHERE ... IN (...)
        if _looks_like_spatial(cond):
            results = self.sm.search_spatial(table, cond.column, cond.point, cond.radius)
            return results if results else f"No se encontraron resultados en '{table}'."

        # Si llega algo raro, lo indicamos
        raise ValueError(f"Tipo de condición desconocido en SELECT: {type(cond).__name__} -> {cond!r}")


    def _exec_select_spatial(self, node: SelectSpatialNode):
        results = self.executor.select_spatial(
            table=node.table,
            column=node.column,
            point=node.point,
            radius=node.radius
        )
        return results if results else f"No se encontraron puntos dentro del radio {node.radius}."
