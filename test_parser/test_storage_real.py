import os
from test_parser.core.parser.parser_sql import ParserSQL
from test_parser.core.parser.sql_executor import SQLExecutor
from test_parser.core.parser.ast_nodes import SelectNode
from test_parser.storage.storage_manager import StorageManager

# ==========================================================
# Configuración inicial
# ==========================================================

# Carpeta data al mismo nivel del test
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

# Limpiar la carpeta de datos para pruebas frescas
for file in os.listdir(DATA_DIR):
    os.remove(os.path.join(DATA_DIR, file))

sm = StorageManager(base_path=DATA_DIR + "/")
sql_exec = SQLExecutor(sm)
parser = ParserSQL()
# ==========================================================
# Función de prueba
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
# Ejecución de consultas
# ==========================================================

if __name__ == "__main__":
    queries = [
        'CREATE TABLE Restaurantes (id INT KEY INDEX SEQ, nombre VARCHAR[20] INDEX BTREE, ubicacion ARRAY[FLOAT] INDEX RTREE)',
        'INSERT INTO Restaurantes VALUES (1, "Tanta", [10.5, 20.3])',
        'INSERT INTO Restaurantes VALUES (2, "Pardos", [11.2, 19.8])',
        'SELECT * FROM Restaurantes',
        'SELECT * FROM Restaurantes WHERE id = 1',
        'DELETE FROM Restaurantes WHERE id = 1',
        'INSERT INTO Restaurantes VALUES (3, "Central", [12.0, 22.0])',
        'SELECT * FROM Restaurantes'

    ]

    for q in queries:
        run_test(q)
sm.debug_dump_table("Restaurantes")
