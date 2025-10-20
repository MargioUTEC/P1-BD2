# ============================================
# test_parser/app.py  — BACKEND PRINCIPAL
# ============================================

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Dict, Optional, Any, List

import io
from contextlib import redirect_stdout

# Usa tu QueryEngine y tu ParserSQL reales
from test_parser.core.query_engine.queryengine import QueryEngine
from test_parser.core.parser.parser_sql import ParserSQL

# --------------------------------------------
# Inicialización
# --------------------------------------------
app = FastAPI(title="MiniDB Backend", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ajusta si quieres restricción
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Instancias persistentes
qe = QueryEngine()          # ya crea IndexManager adentro
parser = qe.parser          # usa el ParserSQL del engine

# --------------------------------------------
# Modelos de entrada/salida
# --------------------------------------------
class QueryRequest(BaseModel):
    sql: str
    using: Optional[str] = None  # ISAM | AVL | HASH | RTREE | BTREE | AUTO

class SearchRequest(BaseModel):
    table: str
    filters: Dict[str, Any] = {}     # {"City":"Lima","Rating":">=4.5", ...}
    using: Optional[str] = None      # opcional: forzar índice
    limit: Optional[int] = 200       # límite suave para mostrar

class InsertRequest(BaseModel):
    table: str
    values: Dict[str, Any]           # diccionario con columnas del CSV base


# --------------------------------------------
# Helpers: capturar logs y resultados de QueryEngine
# --------------------------------------------
def _execute_and_capture(sql: str):
    """
    Ejecuta una sentencia SQL (o varias separadas por ';') usando tu QueryEngine,
    captura todo lo que imprime como 'log', y devuelve:
    {
      "data":  [...lista de filas... ]  (si SELECT normal)
      "plan":  {...}                    (si EXPLAIN / EXPLAIN ANALYZE)
      "logs":  [lista de líneas de log]
    }
    """
    log_buf = io.StringIO()

    captured_rows: List[dict] = []
    orig_print_results = qe._print_results

    def capture_print_results(results, source=""):
        nonlocal captured_rows
        if isinstance(results, list):
            captured_rows = results
        return orig_print_results(results, source)

    qe._print_results = capture_print_results

    try:
        with redirect_stdout(log_buf):
            result = parser.parse(sql)
            stmts = result if isinstance(result, (list, tuple)) else [result]
            last_plan = None

            for stmt in stmts:
                ret = qe._execute(stmt)
                if isinstance(ret, dict) and "plan" in ret:
                    last_plan = ret
    finally:
        qe._print_results = orig_print_results

    # --- Limpiar y estructurar logs ---
    raw_log = log_buf.getvalue().strip().replace("\r\n", "\n")
    log_lines = [ln for ln in raw_log.split("\n") if ln.strip()]

    print("\n[DEBUG] === CAPTURA DE LOGS ===")
    if not log_lines:
        print("[WARN] Ninguna línea capturada desde QueryEngine (stdout vacío).")
    else:
        for line in log_lines:
            print(" >", line)
    print("[DEBUG] =========================")

    out = {
        "data": captured_rows,
        "plan": last_plan,
        "logs": log_lines,   # 👈 ahora se devuelve como lista
    }
    return out


def _auto_quote(value: Any) -> str:
    """
    Prepara valores para armar WHERE:
    - numérico: tal cual
    - string: "texto"
    Si el usuario ya puso operadores (>, <, >=, <=, =, BETWEEN, LIKE), lo respeta.
    """
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    txt = str(value).strip()

    # Si el usuario ya incluye operador, lo dejamos
    # (e.g., '>= 4.5', '< 3', 'BETWEEN 1 AND 10')
    ops = ["<=", ">=", "<", ">", "=", " BETWEEN ", " LIKE "]
    if any(op in txt.upper() for op in ops):
        return txt

    # Comillas para strings
    return f"\"{txt}\""


# --------------------------------------------
# Endpoints
# --------------------------------------------
@app.get("/")
async def root():
    return JSONResponse({
        "message": "MiniDB Backend operativo ✅",
        "endpoints": ["/query", "/search", "/insert", "/columns/{table}", "/structures"]
    })

@app.post("/query")
async def query(req: QueryRequest):
    """
    Ejecuta SQL tal cual.
    - Si 'using' viene y el SQL no lo trae, se le añade 'USING <IDX>'.
    """
    sql = (req.sql or "").strip().rstrip(";")
    if not sql:
        raise HTTPException(status_code=400, detail="SQL vacío.")

    if req.using and "USING" not in sql.upper():
        sql = f"{sql} USING {req.using.strip().upper()}"

    out = _execute_and_capture(sql)

    # Responder según sea SELECT normal o EXPLAIN
    if out["plan"]:
        return {
            "status": "success",
            "message": "EXPLAIN ejecutado.",
            "plan": out["plan"],
            "data": out["data"],    # por si EXPLAIN ANALYZE devolvió rows
            "logs": out["logs"]
        }
    else:
        return {
            "status": "success",
            "message": "Consulta ejecutada.",
            "data": out["data"],
            "plan": out["plan"],
            "logs": out["logs"],  # 👈 se envía lista
        }


@app.post("/search")
async def guided_search(req: SearchRequest):
    """
    Arma un SELECT * FROM <tabla> [USING <índice>] WHERE ...
    - Cada filtro produce 'col <op> valor' o 'col = valor' si no hay op explícito.
    - 'using' permite forzar índice (ISAM, AVL, HASH, RTREE, BTREE).
    - Corrige el orden de cláusulas (USING antes del WHERE).
    - Maneja nombres de columnas amigables desde frontend.
    """
    table = (req.table or "").strip()
    if not table:
        raise HTTPException(status_code=400, detail="Se requiere 'table'.")

    # --- Mapeo legible → nombres reales del dataset ---
    column_map = {
        "Restaurant ID": "restaurant_id",
        "Restaurant Name": "restaurant_name",
        "City": "city",
        "Longitude": "longitude",
        "Latitude": "latitude",
        "Aggregate Rating": "aggregate_rating",
        "Rating text": "rating_text",  # 👈 nuevo
        "Rating Text": "rating_text",  # 👈 mayúsculas
        "Rating": "aggregate_rating",  # 👈 alias lógico
        "Average Cost for Two": "average_cost_for_two",
        "Votes": "votes",
        "Country Code": "country_code",
        "Address": "address",
        "Locality": "locality",
        "Locality Verbose": "locality_verbose",
        "Cuisines": "cuisines",
        "Currency": "currency",
    }

    wheres = []
    for k, v in (req.filters or {}).items():
        if v is None or str(v).strip() == "":
            continue

        # Traducir columna si aplica
        col = column_map.get(k, k)
        txt = str(v).strip()
        upper = txt.upper()

        # Si el usuario ya incluye operador (>, <, >=, <=, BETWEEN, LIKE), respetarlo
        if (upper.startswith((">=", "<=", ">", "<", "=")) or
            " BETWEEN " in upper or " LIKE " in upper):
            cond = f'{col} {txt}'
        else:
            cond = f'{col} = {_auto_quote(v)}'

        wheres.append(cond)

    # --- Construir SQL con orden correcto ---
    if req.using:
        sql = f"SELECT * FROM {table} USING {req.using.strip().upper()}"
    else:
        sql = f"SELECT * FROM {table}"

    if wheres:
        sql += " WHERE " + " AND ".join(wheres)

    # --- Ejecutar y capturar ---
    out = _execute_and_capture(sql)
    rows = out["data"] or []
    limited = rows[: (req.limit or 200)]

    return {
        "status": "success",
        "message": f"Búsqueda ejecutada. Filas: {len(rows)} (mostrando {len(limited)}).",
        "query": sql,
        "data": limited,
        "total": len(rows),
        "logs": out["logs"]
    }



@app.post("/insert")
async def insert_record(req: InsertRequest):
    """
    Inserta un registro usando IndexManager.insert_full() a través del QueryEngine.
    'values' debe mapear las columnas del CSV (exactamente como las maneja insert_full).
    """
    if (req.table or "").lower() != "restaurants":
        raise HTTPException(status_code=400, detail="Por ahora solo se soporta tabla 'restaurants'.")

    try:
        # Usa el index_manager del engine para la inserción segura (con validaciones, mapeo, etc.)
        qe.index_manager.insert_full(req.values)
        return {"status": "success", "message": "Registro insertado correctamente."}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/columns/{table}")
async def columns(table: str):
    """
    Devuelve las columnas disponibles del CSV base para construir formularios dinámicos.
    """
    t = (table or "").lower()
    if t != "restaurants":
        raise HTTPException(status_code=404, detail="Tabla no encontrada.")
    try:
        cols = qe.index_manager.all_columns  # ya las tienes definidas en IndexManager
        return {"columns": cols}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/structures")
async def structures():
    """
    Estructuras soportadas para 'USING' en SELECT/SEARCH.
    """
    return {
        "structures": ["AUTO", "ISAM", "AVL", "HASH", "BTREE", "RTREE"]
    }

@app.get("/api/metadata", tags=["status"])
async def api_metadata():
    """
    Devuelve información básica del backend (para comprobar conexión desde frontend).
    """
    return {
        "name": "MiniDB Backend",
        "version": "2.0",
        "status": "online ✅",
        "endpoints": ["/query", "/search", "/insert", "/columns/{table}", "/structures"]
    }

@app.post("/api/run")
async def api_run(req: QueryRequest):
    """Alias de /query para compatibilidad con frontend."""
    return await query(req)


@app.post("/api/search")
async def api_search(req: SearchRequest):
    """Alias de /search para compatibilidad con frontend."""
    return await guided_search(req)
