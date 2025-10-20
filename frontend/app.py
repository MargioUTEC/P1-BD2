# ============================================
# 🔹 frontend/app.py  — FRONTEND FASTAPI (versión corregida y extendida)
# ============================================

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import httpx
from pathlib import Path

app = FastAPI(title="Frontend - MiniDB Studio")

BACKEND_URL = "http://minidb_backend:8000"

# ------------------------------------------------
# Archivos estáticos y CORS
# ------------------------------------------------
static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=static_dir), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------
# 🔹 Páginas principales
# ------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def root():
    """Página principal: consola SQL"""
    index_path = static_dir / "index.html"
    return index_path.read_text(encoding="utf-8")


@app.get("/search", response_class=HTMLResponse)
def search_page():
    """Página secundaria: búsqueda guiada"""
    page = static_dir / "search.html"
    return page.read_text(encoding="utf-8")


@app.get("/explorer", response_class=HTMLResponse)
def explorer_page():
    """Página de explorador de índices"""
    page = static_dir / "explorer.html"
    return page.read_text(encoding="utf-8")

# ------------------------------------------------
# 🔹 API compatible con script.js
# ------------------------------------------------

# === Ejecutar consulta SQL (API Run) ===
@app.post("/api/run")
async def api_run(request: Request):
    data = await request.json()
    sql = data.get("sql", "")
    using = data.get("using", "")
    if not sql:
        return JSONResponse({"error": "Consulta SQL vacía."}, status_code=400)

    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            resp = await client.post(f"{BACKEND_URL}/query", json={"sql": sql, "using": using})
            result = resp.json()
            # adaptamos estructura esperada por script.js
            return JSONResponse({
                "columns": list(result["data"][0].keys()) if result.get("data") else [],
                "rows": result.get("data", []),
                "plan_used": (
                    result.get("plan", {}).get("plan_name", "AUTO")
                    if result.get("plan") else "AUTO"
                ),
                "message": result.get("message", "Consulta completada."),
                "logs": result.get("logs", [])  # 👈 incluimos logs reales
            }, status_code=resp.status_code)

        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)


# === EXPLAIN / EXPLAIN ANALYZE ===
@app.post("/api/explain")
async def api_explain(request: Request):
    data = await request.json()
    sql = data.get("sql", "")
    if not sql:
        return JSONResponse({"error": "Consulta SQL vacía para EXPLAIN."}, status_code=400)

    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            resp = await client.post(f"{BACKEND_URL}/query", json={"sql": f"EXPLAIN {sql}"})
            result = resp.json()
            plan = result.get("plan", {}) or {}
            return JSONResponse({
                "plan": plan.get("plan_name", "Desconocido"),
                "filter": plan.get("filter", ""),
                "index_used": plan.get("index", "AUTO"),
                "estimated_cost": plan.get("cost", 0.0),
                "rows": plan.get("rows", 0),
                "execution_time_ms": plan.get("execution_time", 0.0)
            }, status_code=resp.status_code)
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)


# === Búsqueda guiada ===
@app.post("/api/search")
async def api_search(request: Request):
    data = await request.json()
    filters = data.get("filters", [])
    forced = data.get("forced_index", None)
    sql = data.get("sql", "")

    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            resp = await client.post(f"{BACKEND_URL}/search", json={
                "table": "restaurants",
                "filters": {f["col"]: f["value"] for f in filters},
                "using": forced
            })
            result = resp.json()
            rows = result.get("data", [])
            return JSONResponse({
                "columns": list(rows[0].keys()) if rows else [],
                "rows": rows,
                "plan_used": forced or "AUTO",
                "message": result.get("message", ""),
                "status": "ok"
            }, status_code=resp.status_code)
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)


# === Estadísticas generales de estructuras ===
@app.get("/api/stats")
async def api_stats():
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            # podrías ampliar cuando tu backend tenga endpoint /stats
            return JSONResponse({
                "hash": {"global_depth": 7, "dir_size": 128, "reads": 20, "writes": 4},
                "rtree": {"points": 50},
                "avl_count": 50,
                "isam_pages": 7
            })
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)


# === Metadata de columnas ===
@app.get("/api/metadata")
async def api_metadata():
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.get(f"{BACKEND_URL}/columns/restaurants")
            backend_data = resp.json()
            columns = backend_data.get("columns", [])
            # adaptamos al formato que script.js espera
            cols_formatted = [{"name": c, "type": "string"} for c in columns]
            return JSONResponse({"columns": cols_formatted})
        except Exception:
            # fallback para modo mock
            return JSONResponse({
                "columns": [
                    {"name": "restaurant_id", "type": "int"},
                    {"name": "restaurant_name", "type": "string"},
                    {"name": "city", "type": "string"},
                    {"name": "longitude", "type": "float"},
                    {"name": "latitude", "type": "float"},
                    {"name": "aggregate_rating", "type": "float"},
                    {"name": "average_cost_for_two", "type": "int"},
                    {"name": "votes", "type": "int"},
                ]
            })

# === Healthcheck ===
@app.get("/health")
async def health():
    return {"frontend": "ok", "backend": BACKEND_URL}
