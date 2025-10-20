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

---

PASOS DE EJECUCIÓN

1. Inicializar índices y cargar dataset
Ejecutar en la terminal:
python -m tiempo.test_tiempo

Este comando carga el dataset principal (Dataset.csv) y crea o reabre todos los índices del sistema.
Salida esperada:
[INIT] Cargando IndexManager...
[INIT] ISAM inicializado correctamente.
[INIT] Extendible Hash reabierto correctamente.
[INIT] AVL reabierto correctamente.
[INIT] B+-Tree reabierto correctamente.
[INFO] R-Tree reabierto con 50 registros.

Cada estructura se guarda en su archivo binario dentro de test_parser/data/, manteniendo persistencia entre ejecuciones.

------------------------------------------------------------

2. Iniciar el servidor web
Ejecutar:
python server.py

Esto inicia el servidor backend (Flask o FastAPI).
Luego abrir en el navegador:
http://127.0.0.1:8000

En esta interfaz se encuentran tres secciones:
- Consola SQL para ejecutar consultas.
- Búsqueda guiada para seleccionar filtros.
- Explorador de índices para visualizar la estructura interna.

------------------------------------------------------------

3. Ejecutar consultas SQL
Ejemplos de uso:

Consulta simple (ISAM):
SELECT * FROM restaurants WHERE city = "Lima";

Comparación numérica (AVL):
SELECT restaurant_name, city, aggregate_rating
FROM restaurants
WHERE aggregate_rating > 4.5;

Consulta espacial (R-Tree):
SELECT restaurant_id, restaurant_name, longitude, latitude
FROM restaurants
WHERE city = "Taguig City" USING RTREE;

Condición compuesta (ISAM + AVL):
SELECT * FROM restaurants
WHERE city = "Taguig City" AND aggregate_rating > 4.0;

------------------------------------------------------------

ORDEN DE EJECUCIÓN INTERNA

Etapa 1: frontend
El usuario ingresa la consulta SQL.

Etapa 2: parser_sql
Se tokeniza y genera el árbol sintáctico (AST).

Etapa 3: executor
Se construye el plan de ejecución.

Etapa 4: index_manager
Se selecciona la estructura de índice más adecuada.

Etapa 5: isam.py, avl.py, bplustree.py, extendible_hash.py, rtree.py
Se ejecutan las búsquedas en los archivos correspondientes.

Etapa 6: executor
Se combinan los resultados con operadores AND y OR.

Etapa 7: executor
Se filtran las columnas indicadas en el SELECT.

Etapa 8: server.py
Se devuelve la respuesta en formato JSON.

Etapa 9: frontend.html, style.css, script.js
Se muestra la tabla con los resultados en la interfaz web.

------------------------------------------------------------

EJEMPLO COMPLETO

Consulta:
SELECT *
FROM restaurants
WHERE city = "Taguig City" AND aggregate_rating > 4.0;

Flujo de ejecución:
1. El parser genera un nodo AND con dos condiciones.
2. El executor selecciona ISAM (para texto) y AVL (para comparación).
3. IndexManager ejecuta ambas búsquedas y combina los resultados.
4. Se aplica la proyección SELECT *.
5. El servidor envía los resultados en formato JSON al frontend.

Salida esperada:
[PLAN] Usando ISAM para búsqueda por texto (city = 'Taguig City')
[PLAN] Usando AVL para búsqueda por comparación (aggregate_rating > 4.0)
[OK] 4 resultado(s) encontrados vía condición compuesta (AND)

------------------------------------------------------------

OBSERVACIONES

- Los índices son persistentes y se mantienen entre ejecuciones.
- Los archivos .dat, .idx y .json se crean automáticamente.
- El sistema puede ampliarse con operaciones de inserción y eliminación.
- La arquitectura modular permite integrar nuevos índices fácilmente.

------------------------------------------------------------

CONCLUSIÓN

Mini-DB demuestra cómo las técnicas de organización e indexación de archivos pueden integrarse en un motor funcional.
Gracias a la combinación de ISAM, AVL, B+-Tree, Extendible Hashing y R-Tree, el sistema soporta consultas textuales, numéricas y espaciales con operadores lógicos (AND, OR), ofreciendo una arquitectura completa, escalable y extensible.



