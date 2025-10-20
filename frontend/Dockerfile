# ===============================
# FRONTEND - MiniDB Studio
# ===============================
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Copiamos el frontend completo
COPY . /app

# Instalamos dependencias mínimas del frontend
RUN pip install --no-cache-dir fastapi uvicorn httpx

EXPOSE 5000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5000"]
