# Análisis exploratorio y sugerencias de índices para nuestro dataset

import pandas as pd
from itertools import combinations

ruta = "Dataset-restaurantes.csv"
df = pd.read_csv(ruta)

print("=== Información General ===")
print(df.info())
print("\n=== Primeras filas ===")
print(df.head())

print("\n=== Valores nulos por columna ===")
print(df.isnull().sum())

print("\n=== Estadísticas descriptivas ===")
print(df.describe(include='all'))

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

print("\n=== Posibles combinaciones para R-Tree ===")
if len(numericas) >= 2:
    for combo in combinations(numericas, 2):
        print(f"- {combo[0]} + {combo[1]}")
else:
    print("No hay suficientes columnas numéricas para crear combinaciones 2D para R-Tree.")