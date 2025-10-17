# Organización e Indexación Eficiente de Archivos con Datos Multidimensionales

## Descripción del Proyecto

El objetivo de este proyecto es diseñar y desarrollar un mini gestor de bases de datos capaz de aplicar técnicas eficientes de **organización, almacenamiento e indexación de archivos físicos**, incluyendo soporte para datos espaciales.
El sistema implementa operaciones fundamentales como **inserción, eliminación y búsqueda**, trabajando sobre archivos planos con datos reales extraídos de un [dataset de restaurantes](https://www.kaggle.com/datasets/mohdshahnawazaadil/restaurant-dataset) disponible en Kaggle.
Este proyecto busca demostrar eficiencia en las operaciones, claridad en la estructura del código y una adecuada documentación técnica que detalle las decisiones de diseño y los resultados obtenidos.

---

## Integrantes del Equipo 8

| Nombre                   | Participación |
| ------------------------ | ----------------- |
| Margiory Alvarado Chávez | 100%    |
| Yofer Corne Flores            | 100%                   |
| Diana Ñañez Andrés             | 100%                     |
| Franco Roque Castillo             | 100%                        |
| Jesús Velarde Tipte             | 100%                     |

---

## Atributos del Dataset Utilizado

* **Restaurant ID:** Identificador único para cada restaurante.
* **Restaurant Name:** Nombre del restaurante.
* **Country Code:** Código del país donde se encuentra el restaurante.
* **City:** Ciudad donde está ubicado el restaurante.
* **Address:** Dirección del restaurante.
* **Locality:** Localidad general del restaurante.
* **Locality Verbose:** Descripción detallada de la localidad.
* **Longitude:** Coordenada de longitud de la ubicación del restaurante.
* **Latitude:** Coordenada de latitud de la ubicación del restaurante.
* **Cuisines:** Tipo de cocina ofrecida por el restaurante (variable objetivo).
* **Average Cost for Two:** Costo promedio para dos personas al comer en el restaurante.
* **Currency:** Moneda utilizada para los precios.
* **Has Table Booking:** Variable binaria que indica si el restaurante acepta reservas de mesa.
* **Has Online Delivery:** Variable binaria que indica si el restaurante ofrece entrega en línea.
* **Is Delivering Now:** Variable binaria que indica si el restaurante está realizando entregas actualmente.
* **Switch to Order Menu:** Variable binaria que indica si el restaurante tiene opción de pedido en línea desde el menú.
* **Price Range:** Rango que indica el nivel de precios de los ítems del menú.
* **Aggregate Rating:** Calificación promedio del restaurante basada en reseñas de clientes.
* **Rating Color:** Código de color que representa el nivel de calificación.
* **Rating Text:** Representación textual del nivel de calificación.
* **Votes:** Número total de votos recibidos por el restaurante.