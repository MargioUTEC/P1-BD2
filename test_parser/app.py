from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware
import io
import sys
import re
import json
from test_parser.core.query_engine.queryengine import QueryEngine

app = FastAPI(title="DB2 Query Engine API")

# Permitir conexión desde el frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

qe = QueryEngine()


@app.post("/api/query")
def run_query(sql: str = Body(..., embed=True)):
    """
    Ejecuta la consulta SQL-like y devuelve:
      - output: log textual completo
      - results: lista de filas (diccionarios)
    """
    sql = sql.strip().rstrip(";")

    # Redirigir stdout para capturar logs del motor
    buffer = io.StringIO()
    sys_stdout_backup = sys.stdout
    sys.stdout = buffer

    results = []
    try:
        result = qe.run_query(sql)
        # Si la función devuelve una lista o dict directamente
        if isinstance(result, list):
            results = result
        elif isinstance(result, dict):
            results = [result]
    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        sys.stdout = sys_stdout_backup

    # Capturar log textual
    output = buffer.getvalue()
    buffer.close()

    # Si no devolvió resultados, intentar extraerlos desde el log
    if not results:
        # Buscar líneas tipo {'campo': valor, ...}
        pattern = r"\{[^\}]+\}"
        matches = re.findall(pattern, output)
        for m in matches:
            try:
                # Convertir formato Python → JSON válido
                s = m.replace("'", '"')
                s = re.sub(r'(\w+):', r'"\1":', s)
                obj = json.loads(s)
                results.append(obj)
            except Exception:
                continue

    return {"output": output, "results": results}


@app.get("/api/ping")
def ping():
    return {"status": "ok", "message": "DB2 Engine activo"}
