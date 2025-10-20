# Análisis exploratorio y sugerencias de índices para nuestro dataset

import pandas as pd
from itertools import combinations

# Ruta de tu dataset
ruta = "Dataset-restaurantes.csv"
# Cargar dataset
df = pd.read_csv(ruta)

# Información general
print("=== Información General ===")
print(df.info())
print("\n=== Primeras filas ===")
print(df.head())

# Valores nulos
print("\n=== Valores nulos por columna ===")
print(df.isnull().sum())

# Estadísticas descriptivas
print("\n=== Estadísticas descriptivas ===")
print(df.describe(include='all'))

# Sugerencias de índices según tipo de datos
print("\n=== Sugerencias de índices ===")
numericas = []
for col in df.columns:
    dtype = df[col].dtype
    if "int" in str(dtype) or "float" in str(dtype):
        numericas.append(col)
        print(f"- {col} ({dtype}): B+ Tree, ISAM o Hashing (búsqueda por rango y clave).")
    elif "object" in str(dtype):
        print(f"- {col} ({dtype}): B+ Tree (para texto).")
    else:
        print(f"- {col} ({dtype}): Verificar utilidad.")

# Detectar posibles combinaciones de columnas numéricas para R-Tree
print("\n=== Posibles combinaciones para R-Tree ===")
if len(numericas) >= 2:
    for combo in combinations(numericas, 2):
        print(f"- {combo[0]} + {combo[1]}")
else:
    print("No hay suficientes columnas numéricas para crear combinaciones 2D para R-Tree.")