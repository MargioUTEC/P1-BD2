from test_parser.core.parser.parser_sql import ParserSQL
from test_parser.core.parser.sql_executor import SQLExecutor
from test_parser.core.parser.ast_nodes import SelectNode


class MockStorageManager:
    def __init__(self):
        self.tables = {}

    def create_table(self, table_name, columns):
        self.tables[table_name] = {"columns": columns, "data": []}
        print(f"[Storage] Tabla '{table_name}' creada con columnas: {columns}")

    def load_from_csv(self, path):
        print(f"[Storage] Cargando CSV desde {path}")
        return [{"id": 1, "nombre": "Tanta"}]  # Datos simulados

    def infer_schema(self, data):
        print("[Storage] Infiriendo esquema de datos CSV")
        return [{"name": k, "type": "VARCHAR"} for k in data[0].keys()]

    def build_index(self, table_name, index_type, key_column, data):
        print(f"[Storage] Construyendo índice {index_type} sobre '{key_column}' en {table_name}")

    def insert_record(self, table_name, values):
        if table_name not in self.tables:
            raise KeyError(f"La tabla '{table_name}' no existe en StorageManager.")
        self.tables[table_name]["data"].append(values)
        print(f"[Storage] Insertado en '{table_name}': {values}")

    def delete_records(self, table_name, condition):
        print(f"[Storage] Eliminando donde {condition}")
        return 1  # Simula un registro eliminado

    def select_all(self, table_name):
        print(f"[Storage] SELECT * FROM {table_name}")
        return self.tables[table_name]["data"]

    def search_exact(self, table, key, value):
        print(f"[Storage] Buscando {key} = {value} en {table}")
        return [{"id": value, "nombre": "Mock"}]

    def search_range(self, table, key, v1, v2):
        print(f"[Storage] Buscando {key} BETWEEN {v1} AND {v2} en {table}")
        return [{"nombre": "A"}, {"nombre": "M"}]

    def search_comparison(self, table, key, op, value):
        print(f"[Storage] Comparación {key} {op} {value} en {table}")
        return [{"_info": f"mock {key} {op} {value}"}]

    def search_spatial(self, table, column, point, radius):
        print(f"[Storage] Buscando {column} IN (POINT={point}, RADIUS={radius}) en {table}")
        return [{"id": 1, "ubicacion": point}]

    def get_table_metadata(self, table):
        # Puedes ajustar esto si quieres que cambie por tabla
        return {"index_type": "BTREE", "key_column": "id"}


sm = MockStorageManager()
sql_exec = SQLExecutor(sm)
parser = ParserSQL()

# ==========================================================
# Prueba del Parser + Executor
# ==========================================================

def run_test(query):
    print(f"\n QUERY: {query}")
    ast = parser.parse(query)
    print(" [AST]:", ast)
    if isinstance(ast, SelectNode):
        print(" [AST.condition]:", repr(ast.condition), "type:", type(ast.condition).__name__)
    result = sql_exec.execute(ast)
    print(" Resultado:", result)

# ==========================================================
# Ejecución de consultas de prueba
# ==========================================================

if __name__ == "__main__":
    queries = [
        'CREATE TABLE Restaurantes (id INT KEY INDEX SEQ, nombre VARCHAR[20] INDEX BTREE, ubicacion ARRAY[FLOAT] INDEX RTREE)',
        'INSERT INTO Restaurantes VALUES (1, "Tanta", [10.5, 20.3])',
        'SELECT * FROM Restaurantes WHERE id = 1',
        'SELECT * FROM Restaurantes WHERE nombre BETWEEN "A" AND "M"',
        'SELECT * FROM Restaurantes WHERE ubicacion IN (POINT[10.0, 20.0], RADIUS=5)',
        'DELETE FROM Restaurantes WHERE id = 1',
        'CREATE TABLE Restaurantes FROM FILE "C:\\restaurantes.csv" USING INDEX ISAM("id")'
    ]

    for q in queries:
        run_test(q)
