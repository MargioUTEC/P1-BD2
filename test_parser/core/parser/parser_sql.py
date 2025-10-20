from lark import Lark, Transformer, Token, Tree
from test_parser.core.parser.ast_nodes import (
    CreateTableNode, ColumnDefNode, CreateFromFileNode,
    InsertNode, DeleteNode, SelectNode, SelectSpatialNode,
    ConditionNode, BetweenConditionNode, SelectWhereNode, ExplainNode
)
from test_parser.core.parser.ast_nodes import ConditionComplexNode


class ParserSQL:
    def __init__(self, grammar_path="test_parser/core/parser/grammar_sql.lark"):
        with open(grammar_path, "r", encoding="utf-8") as f:
            grammar = f.read()

        self.parser = Lark(
            grammar,
            start="start",
            parser="lalr",
            transformer=SQLTransformer()  # <--- 🔥 Esta línea aplica el transformer
        )

    def parse(self, query: str):
        """
        Recibe una consulta SQL-like en texto y devuelve un AST transformado.
        """
        tree = self.parser.parse(query)
        transformer = SQLTransformer()
        return transformer.transform(tree)


def _tokval(x):
    if isinstance(x, Token):
        return x.value
    return x


def _strip_quotes(s: str):
    if isinstance(s, str) and len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        return s[1:-1]
    return s

def _to_str_type(x):
    """Convierte Tree/Token/str a un string final de tipo (INT, VARCHAR[20], ARRAY[FLOAT])."""
    if isinstance(x, Tree):
        if x.children:
            return _to_str_type(x.children[0])
        return str(x.data)
    if isinstance(x, Token):
        return x.value
    return str(x)


class SQLTransformer(Transformer):

    def using_clause_list(self, children):
        # Ejemplo: ['ISAM'] o ['ISAM', 'AVL']
        return [str(c).upper() for c in children if c]


    def where_clause(self, children):
        return children[0]

    def using_all(self, _):
        return ["ALL"]

    def index_type(self, children):
        if not children:
            return ""
        return str(children[0]).upper()

    def using_list(self, children):
        return [str(c).upper() for c in children if c]

    def using_clause(self, children):
        if not children:
            return ["ALL"]
        first = children[0]
        return first if isinstance(first, list) else [str(first).upper()]

    # ---------- CREATE TABLE ----------
    def create_table(self, children):
        table_name = str(_tokval(children[0]))
        columns = [c for c in children[1:] if c is not None]
        return CreateTableNode(table_name, columns)

    def column_def(self, children):
        name = str(_tokval(children[0]))
        type_spec = _to_str_type(children[1]) if len(children) > 1 else None

        is_key = False
        index_type = None

        i = 2
        while i < len(children):
            item = children[i]
            if isinstance(item, Token):
                if item.type == "KEYKW":
                    is_key = True
                    i += 1
                    continue
                if item.type == "INDEXKW":
                    if i + 1 < len(children):
                        idx = str(_tokval(children[i+1])).upper()
                        if idx in ("SEQ", "ISAM", "BTREE", "RTREE", "HASH"):
                            index_type = idx
                            i += 2
                            continue
                i += 1
            else:
                s = str(_tokval(item)).upper()
                if s in ("SEQ", "ISAM", "BTREE", "RTREE", "HASH"):
                    index_type = s
                i += 1


        return ColumnDefNode(name, type_spec, is_key, index_type)

    def type_int(self, _):    return "INT"
    def type_float(self, _):  return "FLOAT"
    def type_date(self, _):   return "DATE"

    def type_varchar(self, children):
        size = _tokval(children[0])
        return f"VARCHAR[{size}]"

    def base_float(self, _):  return "FLOAT"
    def base_int(self, _):    return "INT"

    def type_array(self, children):
        # children: [base_type] como "FLOAT" o "INT"
        base = _tokval(children[0]) if children else ""
        return f"ARRAY[{base}]"

    # ---------- CREATE FROM FILE ----------
    def create_from_file(self, children):
        """
        Maneja sentencias como:
          CREATE TABLE restaurants FROM FILE "Dataset.csv"
          CREATE TABLE restaurants USING ALL FROM FILE "Dataset.csv"
          CREATE TABLE restaurants USING ISAM, HASH, RTREE FROM FILE "Dataset.csv"
        """


        try:
            name = None
            path = None
            using = []

            # --- Caso 1: sin USING ---
            if len(children) == 2:
                name = str(_tokval(children[0]))
                path = _strip_quotes(str(_tokval(children[1])))
                using = ["ALL"]

            # --- Caso 2: con USING ---
            elif len(children) == 3:
                name = str(_tokval(children[0]))
                using_node = children[1]
                path = _strip_quotes(str(_tokval(children[2])))

                # Descomponer el árbol USING
                def extract_index_names(node):
                    result = []
                    if isinstance(node, Token):
                        result.append(str(node).upper())
                    elif isinstance(node, Tree):
                        for c in node.children:
                            result.extend(extract_index_names(c))
                    elif isinstance(node, (list, tuple)):
                        for c in node:
                            result.extend(extract_index_names(c))
                    return result

                using = extract_index_names(using_node)
                if not using:
                    using = ["ALL"]

            else:
                raise ValueError(f"Estructura inesperada en create_from_file(): {children}")

            # --- Limpieza final ---
            using = [u for u in using if u and u != "ALL"]
            if not using:
                using = ["ISAM", "HASH", "AVL", "BTREE", "RTREE"]

            return CreateFromFileNode(
                table_name=name,
                file_path=path,
                using_indexes=using
            )

        except Exception as e:
            print(f"[ERROR] Error trying to process rule 'create_from_file': {e}")
            return None

    def explain_stmt(self, children):
        """
        children = [Token('ANALYZE'), select_stmt] o solo [select_stmt]
        """
        analyze = False
        if len(children) == 2:
            analyze = True
            select_node = children[1]
        else:
            select_node = children[0]
        return ExplainNode(analyze, select_node)

    def analyze_true(self, _):
        return True

    def analyze_false(self, _):
        return False

    def explain_statement(self, children):
        """
        children = [analyze_flag (bool), select_stmt]
        """
        analyze = bool(children[0])
        select_stmt = children[1]
        print(f"[DEBUG] explain_statement → ANALYZE={analyze}")
        return ExplainNode(analyze, select_stmt)

    # ---------- INSERT ----------
    def insert_into(self, children):
        name = str(_tokval(children[0]))
        vals = children[1:]
        return InsertNode(name, vals)

    # ---------- DELETE ----------
    def delete_from(self, children):
        table = str(_tokval(children[0]))
        cond = children[1] if len(children) > 1 else None
        return DeleteNode(table, cond)

    # ---------- SELECT ----------
    def select_stmt(self, children):

        columns = children[0]
        table = str(children[1]) if isinstance(children[1], Token) else str(children[1])
        using_index = None
        condition = None

        for c in children[2:]:
            if isinstance(c, list) and c:
                # ['ISAM'] o ['AVL']
                using_index = c[0].upper()
            else:
                condition = c

        print(f"[DEBUG] select_stmt() → using_index={using_index}")

        return SelectWhereNode(
            table_name=table,
            columns=columns,
            condition=condition,
            using_index=using_index
        )

    # ---------- Espacial dentro de WHERE ----------
    def coord_list(self, children):
        return [float(_tokval(x)) for x in children]

    def point(self, children):
        return children[0]

    def radius(self, children):
        return float(_tokval(children[0]))

    def spatial_expr(self, children):
        return (children[0], children[1])  # (point, radius)

    # ---------- CONDICIONES ----------
    def condition_comparison(self, children):
        col, op, val = children
        return ConditionNode(str(_tokval(col)), str(_tokval(op)), val)

    def condition_between(self, children):
        col, v1, v2 = children
        return BetweenConditionNode(str(_tokval(col)), v1, v2)

    # ==========================================================
    #  Condición compuesta: A AND B / A OR B
    # ==========================================================
    def condition_complex(self, children):
        """
        children = [condition1, 'AND', condition2, 'OR', condition3, ...]
        Se convierte en árbol binario encadenado de ConditionComplexNode.
        """

        if len(children) < 3:
            return children[0]

        node = ConditionComplexNode(children[0], str(_tokval(children[1])).upper(), children[2])

        i = 3
        while i + 1 < len(children):
            op = str(_tokval(children[i])).upper()
            right = children[i + 1]
            node = ConditionComplexNode(node, op, right)
            i += 2

        return node

    def condition_in(self, items):
        """
        items = [CNAME, spatial_expr]
        spatial_expr = (point, radius)
        """
        column = str(items[0])  # ej. coords
        spatial_expr = items[1]  # tuple(point, radius)

        # El spatial_expr viene del subárbol: [ [lon, lat], radius ]
        point_coords = spatial_expr[0]
        radius = spatial_expr[1]
        return SelectSpatialNode(
            table=None,
            column=column,
            point=point_coords,
            radius=radius
        )

    def and_condition_chain(self, children):
        """
        Maneja cadenas de condiciones unidas con AND.
        """

        left = children[0]
        right = children[1] if len(children) > 1 else None
        if right is None:
            return left
        return ConditionComplexNode(left=left, right=right, operator="AND")

    def or_condition_chain(self, children):
        """
        Maneja cadenas de condiciones unidas con OR.
        """

        left = children[0]
        right = children[1] if len(children) > 1 else None
        if right is None:
            return left
        return ConditionComplexNode(left=left, right=right, operator="OR")

    def grouped_condition(self, children):
        """
        Maneja condiciones entre paréntesis: ( ... )
        """
        return children[0]

    # ---------- Columnas ----------
    def column_list(self, children):
        if len(children) == 1 and isinstance(children[0], Token) and children[0].value == "*":
            return ["*"]
        return [str(_tokval(c)) for c in children]

    # ---------- Valores ----------
    def value(self, children):
        v = children[0]
        if isinstance(v, Token):
            if v.type == "ESCAPED_STRING":
                return _strip_quotes(v.value)
            txt = v.value
            try:
                if "." in txt:
                    return float(txt)
                return int(txt)
            except ValueError:
                return txt
        return v

    def array_value(self, children):
        return list(children)

    # ---------- Comentarios ----------
    def COMMENT(self, _):
        return None


