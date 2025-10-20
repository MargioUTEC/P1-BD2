#  Mini-DB: Organización e Indexación Eficiente de Archivos con Datos Multidimensionales

##  Descripción del Proyecto

El objetivo de este proyecto es diseñar y desarrollar un **mini gestor de bases de datos** capaz de aplicar técnicas eficientes de **organización, almacenamiento e indexación de archivos físicos**, incluyendo soporte para **datos espaciales**.  

El sistema implementa operaciones fundamentales como **inserción, eliminación y búsqueda**, trabajando sobre archivos planos con datos reales extraídos de un [dataset de restaurantes](https://www.kaggle.com/datasets/mohdshahnawazaadil/restaurant-dataset) disponible en Kaggle.  

Este proyecto busca demostrar eficiencia en las operaciones, claridad en la estructura del código y una adecuada documentación técnica que detalle las decisiones de diseño y los resultados obtenidos.

---

##  Integrantes del Equipo 8

| Nombre                   | Participación |
| ------------------------ | -------------- |
| Margiory Alvarado Chávez | 100% |
| Yofer Corne Flores       | 100% |
| Diana Ñañez Andrés       | 100% |
| Franco Roque Castillo    | 100% |
| Jesús Velarde Tipte      | 100% |

---

##  Atributos del Dataset Utilizado

* **Restaurant ID:** Identificador único para cada restaurante.  
* **Restaurant Name:** Nombre del restaurante.  
* **Country Code:** Código del país donde se encuentra.  
* **City:** Ciudad donde está ubicado.  
* **Address:** Dirección exacta.  
* **Locality:** Localidad general.  
* **Locality Verbose:** Descripción detallada.  
* **Longitude / Latitude:** Coordenadas geográficas.  
* **Cuisines:** Tipo de cocina ofrecida.  
* **Average Cost for Two:** Costo promedio para dos personas.  
* **Currency:** Moneda de los precios.  
* **Has Table Booking:** Indica si acepta reservas.  
* **Has Online Delivery:** Indica si ofrece delivery.  
* **Is Delivering Now:** Si entrega actualmente.  
* **Switch to Order Menu:** Si permite pedidos online.  
* **Price Range:** Nivel de precios.  
* **Aggregate Rating:** Calificación promedio.  
* **Rating Color:** Color del nivel de calificación.  
* **Rating Text:** Nivel de calificación textual.  
* **Votes:** Total de votos recibidos.  



##  Estructuras de Índices Implementadas

| Estructura | Propósito | Tipo de datos |
|-------------|------------|----------------|
| **ISAM** | Índice secuencial para búsquedas exactas | Cadenas (`city`, `name`) |
| **AVL Tree** | Árbol balanceado para comparaciones | Numéricos (`aggregate_rating`) |
| **B+-Tree** | Árbol de búsqueda por rangos optimizado para disco | Numéricos o alfabéticos |
| **Extendible Hashing** | Acceso directo dinámico mediante hashing | Claves únicas (`restaurant_id`) |
| **R-Tree** | Índice espacial multidimensional | Coordenadas (`longitude`, `latitude`) |

Cada estructura se guarda en archivos binarios independientes y es administrada por `IndexManager`.

------------------------------------------------------------

##  Ejecución del Sistema

 Iniciar los índices y preparar el entorno
------------------------------------------------------------
Antes de ejecutar la interfaz, inicializa o reabre los índices persistentes.  
Desde la raíz del proyecto, ejecutar:

python -m tiempo.test_tiempo

Este comando:
- Carga el dataset Dataset.csv.
- Construye o reabre todos los índices físicos.
- Verifica integridad y persistencia de datos.

Salida esperada:
[INIT] Cargando IndexManager...
[INIT] ISAM inicializado correctamente.
[INIT] Extendible Hash reabierto correctamente.
[INIT] AVL reabierto correctamente.
[INIT] B+-Tree reabierto correctamente.
[INFO] R-Tree reabierto con 50 registros.

------------------------------------------------------------

 Levantar el servidor backend
------------------------------------------------------------
Ejecutar el servidor Flask:

python server.py

El backend se ejecutará en:
http://127.0.0.1:8000

Responsabilidades del backend:
- Recibir las consultas SQL enviadas por el frontend.
- Interpretarlas mediante el parser SQL.
- Generar el plan de ejecución.
- Delegar búsquedas al IndexManager.
- Combinar resultados y devolverlos en formato JSON.

------------------------------------------------------------

 Acceder al frontend
------------------------------------------------------------
Abrir en el navegador:
http://127.0.0.1:8000

La interfaz presenta tres secciones:
- Consola SQL: permite escribir consultas directamente.
- Búsqueda guiada: filtra registros por campos comunes.
- Explorador de índices: visualiza estructuras internas (ISAM, AVL, etc.).

------------------------------------------------------------

##  Flujo de Ejecución Completo

Cuando el usuario ejecuta una consulta SQL desde el frontend, el flujo es:

Etapa 1 — Frontend
------------------------------------------------------------
1. El usuario ingresa la consulta:
SELECT restaurant_name, city, aggregate_rating 
FROM restaurants 
WHERE city = "Lima" AND aggregate_rating > 4.0;

2. La consulta se envía al backend mediante una solicitud HTTP (POST).

Etapa 2 — Parser SQL (backend)
------------------------------------------------------------
1. parser_sql convierte la cadena SQL en tokens (SELECT, FROM, WHERE).
2. Valida la estructura según grammar_sql.lark.
3. Construye un árbol sintáctico (AST).
4. Devuelve un objeto SelectStmtNode al executor.

Etapa 3 — Executor (plan de ejecución)
------------------------------------------------------------
El executor analiza el AST y define qué índice usar:

- Texto → ISAM  
- Numérico o rango → AVL o B+Tree  
- Clave hash → Extendible Hashing  
- Coordenadas → R-Tree

Ejemplo:
[PLAN] Usando ISAM para búsqueda por texto (city = 'Lima')
[PLAN] Usando AVL para búsqueda por comparación (aggregate_rating > 4.0)

Etapa 4 — IndexManager
------------------------------------------------------------
El IndexManager coordina la ejecución:
- Reabre los índices desde disco.
- Ejecuta las búsquedas.
- Devuelve los resultados parciales al executor.

Etapa 5 — Combinación de Resultados
------------------------------------------------------------
- Operador AND → Intersección de resultados.
- Operador OR → Unión sin duplicados.

Etapa 6 — Proyección de Columnas
------------------------------------------------------------
Se devuelven únicamente las columnas del SELECT.

Etapa 7 — Envío de respuesta al frontend
------------------------------------------------------------
El backend genera una respuesta JSON:

{
  "status": "OK",
  "plan": "Usando ISAM y AVL",
  "results": [
    {"restaurant_name": "La Lucha", "city": "Lima", "aggregate_rating": 4.5},
    {"restaurant_name": "Panchita", "city": "Lima", "aggregate_rating": 4.2}
  ]
}

El frontend renderiza esta respuesta en una tabla HTML con desplazamiento horizontal.

------------------------------------------------------------

##  Ejemplo de Ejecución Completa

Consulta:
SELECT * 
FROM restaurants 
WHERE city = "Taguig City" AND aggregate_rating > 4.0;

Salida esperada:
[PLAN] Usando ISAM para búsqueda por texto (city = 'Taguig City')
[PLAN] Usando AVL para búsqueda por comparación (aggregate_rating > 4.0)
[OK] 4 resultado(s) encontrados vía condición compuesta (AND)



