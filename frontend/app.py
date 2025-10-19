from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import httpx
from pathlib import Path

app = FastAPI(title="Frontend - DB2")

BACKEND_URL = "http://127.0.0.1:8000"

static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=static_dir), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:3000",
        "http://localhost:3000",
        "http://127.0.0.1:8000",
        "http://localhost:8000"
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", response_class=HTMLResponse)
def root():
    """
    Carga la página principal (index.html).
    """
    index_path = static_dir / "index.html"
    if not index_path.exists():
        return HTMLResponse("<h1>Error: index.html no encontrado</h1>", status_code=404)
    return index_path.read_text(encoding="utf-8")


@app.post("/api/query")
async def execute_query(request: Request):
    """
    Recibe una consulta SQL desde el frontend (por ejemplo, React)
    y la reenvía al backend principal que ejecuta el parser.
    """
    data = await request.json()
    sql = data.get("sql", "").strip().rstrip(";")

    if not sql:
        return JSONResponse({"error": "No se recibió consulta SQL"}, status_code=400)

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(f"{BACKEND_URL}/query", json={"sql": sql})
            # Propagar la respuesta JSON del backend al frontend
            return JSONResponse(
                content=response.json(),
                status_code=response.status_code
            )
        except httpx.ConnectError:
            return JSONResponse(
                {"error": "No se pudo conectar al backend (verifica que esté en ejecución)"},
                status_code=502
            )
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)
