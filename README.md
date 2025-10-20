# 🧩 Mini-DB: Organización e Indexación Eficiente de Archivos con Datos Multidimensionales

## 📘 Descripción del Proyecto

Este proyecto implementa un **mini gestor de bases de datos** diseñado para demostrar técnicas eficientes de **organización, almacenamiento e indexación de archivos físicos**, incluyendo soporte para **datos espaciales (R-Tree)**.

El sistema soporta operaciones fundamentales como **inserción, eliminación y búsqueda** sobre archivos planos con datos reales extraídos del dataset de [Restaurantes en Kaggle](https://www.kaggle.com/datasets/mohdshahnawazaadil/restaurant-dataset).

El proyecto integra un **backend en FastAPI** y un **frontend web interactivo**, ambos completamente desplegables con **Docker Compose**.

---

## 👥 Integrantes del Equipo 8

| Nombre                   | Participación |
| ------------------------ | -------------- |
| Margiory Alvarado Chávez | 100% |
| Yofer Corne Flores       | 100% |
| Diana Ñañez Andrés       | 100% |
| Franco Roque Castillo    | 100% |
| Jesús Velarde Tipte      | 100% |

---

## 🧠 Atributos del Dataset

| Atributo | Descripción |
|-----------|-------------|
| **Restaurant ID** | Identificador único |
| **Restaurant Name** | Nombre del restaurante |
| **City** | Ciudad donde se encuentra |
| **Longitude / Latitude** | Coordenadas geográficas |
| **Cuisines** | Tipo de cocina |
| **Average Cost for Two** | Costo promedio para dos personas |
| **Aggregate Rating** | Calificación promedio |
| **Votes** | Total de votos recibidos |

> El sistema utiliza principalmente estos campos para construir índices de búsqueda eficientes.

---

## ⚙️ Estructuras de Índices Implementadas

| Estructura | Propósito | Tipo de datos |
|-------------|------------|----------------|
| **ISAM** | Búsquedas exactas secuenciales | Cadenas (`city`, `name`) |
| **AVL Tree** | Árbol balanceado para comparaciones | Numéricos (`aggregate_rating`) |
| **B+ Tree** | Árbol optimizado para rangos en disco | Numéricos o alfabéticos |
| **Extendible Hashing** | Acceso directo dinámico | Claves únicas (`restaurant_id`) |
| **R-Tree** | Índice espacial multidimensional | Coordenadas (`longitude`, `latitude`) |

Todos los índices se almacenan en archivos binarios independientes y son gestionados por el módulo `IndexManager`.

---

## 🚀 Guía de Instalación y Ejecución

### 🔧 1. Clonar el repositorio

```bash
git clone https://github.com/MargioUTEC/P1-BD2.git
cd P1-BD2
```

> Asegúrate de tener instalado **Docker Desktop** (Windows/Mac) o **Docker Engine + Docker Compose** (Linux).

---

### 🧱 2. Estructura del proyecto

```
db2_proyecto/
│
├── test_parser/              ← Backend (FastAPI)
│   ├── app.py
│   ├── core/
│   ├── indexes/
│   ├── storage/
│   ├── data/
│   └── Dockerfile
│
├── frontend/                 ← Frontend (FastAPI + HTML)
│   ├── app.py
│   ├── static/
│   └── Dockerfile
│
└── docker-compose.yml
```

---

### ▶️ 3. Construir y levantar los contenedores

Desde la raíz del proyecto, ejecutar:

```bash
docker compose up --build
```

Esto:
- Construye las imágenes de backend y frontend.
- Inicia ambos contenedores en red compartida.
- Expone los servicios en:

| Servicio | Puerto | Descripción |
|-----------|--------|-------------|
| **Backend (FastAPI)** | `8000` | API que ejecuta consultas SQL e interactúa con los índices |
| **Frontend (FastAPI)** | `5000` | Interfaz web para enviar consultas y visualizar resultados |

---

### 🌐 4. Acceso a la aplicación

- **Frontend:**  
  👉 [http://localhost:5000](http://localhost:5000)

- **Backend (API):**  
  👉 [http://localhost:8000](http://localhost:8000)

> Si todo funciona correctamente, deberías ver:  
> `{"message":"MiniDB Backend operativo ✅"}`

---

### 🧩 5. Ejecución manual (sin Docker, opcional)

Si deseas probar el proyecto localmente:

#### 🖥️ Backend
```bash
cd test_parser
pip install -r requirements.txt
uvicorn app:app --reload --port 8000
```

#### 🌐 Frontend
```bash
cd frontend
pip install fastapi uvicorn httpx
uvicorn app:app --reload --port 5000
```

---

## 🔄 Flujo de Ejecución

### 1️⃣ Frontend
El usuario ingresa una consulta SQL en la consola web, por ejemplo:

```sql
SELECT restaurant_name, city, aggregate_rating 
FROM restaurants 
WHERE city = "Lima" AND aggregate_rating > 4.0;
```

El `frontend/app.py` envía esta consulta a:
```
POST /api/run → http://backend:8000/query
```

---

### 2️⃣ Parser SQL (backend)
El módulo `parser_sql.py`:
- Tokeniza y valida la sintaxis SQL.
- Construye un **árbol sintáctico (AST)** con `lark`.
- Devuelve un objeto `SelectStmtNode`.

Ejemplo de log:
```
[SELECT FROM restaurants]
[DEBUG] Nodo SELECT detectado → using_index=None
```

---

### 3️⃣ Planificador de ejecución
El **Executor** determina la estructura adecuada según el tipo de campo:

| Tipo de campo | Estructura usada |
|----------------|------------------|
| Texto (`city`, `name`) | ISAM |
| Numérico (`aggregate_rating`) | AVL o B+Tree |
| Clave (`restaurant_id`) | Extendible Hashing |
| Coordenadas (`longitude`, `latitude`) | R-Tree |

---

### 4️⃣ Ejemplo de Resultado

```json
{
  "status": "OK",
  "plan": "Usando ISAM y AVL",
  "data": [
    {"restaurant_name": "La Lucha", "city": "Lima", "aggregate_rating": 4.5},
    {"restaurant_name": "Panchita", "city": "Lima", "aggregate_rating": 4.2}
  ]
}
```

El frontend renderiza los resultados en una tabla con scroll horizontal.

---

## 🧠 Consideraciones Técnicas

- Los índices son **persistentes** y se guardan en `test_parser/data/`.
- `docker-compose.yml` monta los volúmenes locales para conservar los índices generados.
- `Dockerfile` del backend instala librerías especializadas (`rtree`, `shapely`, `lark==1.3.0`).

---

## 🧰 Comandos Útiles

| Acción | Comando |
|--------|----------|
| Reconstruir contenedores | `docker compose build` |
| Iniciar servicios | `docker compose up` |
| Detener servicios | `docker compose down` |
| Ver logs de backend | `docker logs minidb_backend` |
| Entrar al contenedor backend | `docker exec -it minidb_backend bash` |

---

## ✅ Estado Final Esperado

```
[INIT] ISAM inicializado correctamente.
[INIT] Extendible Hash reabierto correctamente.
[INIT] AVL reabierto correctamente.
[INIT] B+-Tree reabierto correctamente.
[INIT] R-Tree reabierto correctamente.
INFO: Uvicorn running on http://0.0.0.0:8000
```

Navega a:
- `http://localhost:5000` → Frontend operativo  
- `http://localhost:8000` → Backend operativo ✅

---
