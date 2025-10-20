# =========================
# Backend Dockerfile
# =========================
FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema necesarias para rtree (libspatialindex)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libspatialindex-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiar los archivos del backend
COPY test_parser/ ./test_parser/

# Instalar dependencias Python mínimas
RUN pip install --no-cache-dir \
    fastapi \
    uvicorn \
    "lark==1.3.0" \
    numpy \
    pandas \
    scikit-learn \
    shapely \
    rtree

# Exponer el puerto
EXPOSE 8000

# Comando para iniciar FastAPI
CMD ["uvicorn", "test_parser.app:app", "--host", "0.0.0.0", "--port", "8000"]
