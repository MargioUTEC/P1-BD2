import os
import pandas as pd
from pathlib import Path
from test_parser.indexes.rtree_point.rtree_points import RTreePoints

script_dir = Path(__file__).resolve().parent
csv_path = script_dir / "Dataset.csv"
INDEX_PATH = script_dir / "data" / "rtree_index"

def load_dataset():
    df = pd.read_csv(csv_path)
    print(f"[INFO] Columnas detectadas: {list(df.columns)}")
    print(f"[INFO] Total de filas en CSV: {len(df)}")
    df = df.dropna(subset=["Longitude", "Latitude"])
    df = df.head(15)
    print(f"[INFO] Cargadas {len(df)} filas válidas del dataset.")
    return df

def run_basic_tests():
    # Limpieza previa
    for f in INDEX_PATH.parent.glob("rtree_index.*"):
        f.unlink(missing_ok=True)

    df = load_dataset()
    print("\n[INFO] Construyendo índice R-Tree persistente...")
    rt = RTreePoints.from_dataframe(
        df,
        x_col="Longitude",
        y_col="Latitude",
        keep_cols=["Restaurant ID", "Restaurant Name", "City", "Cuisines", "Aggregate rating"],
        index_name=str(INDEX_PATH),
        max_children=20,
    )

    rt.debug_dump(limit=3)

    print("\n=== CONSULTAS ESPACIALES ===")
    punto_ref = (121.0275, 14.56)
    radio_km = 3.0

    print(f"\n[TEST] Restaurantes en {radio_km} km de {punto_ref}")
    for r in rt.range_search_km(punto_ref, radio_km)[:5]:
        print(f" - {r.get('Restaurant_Name', 'N/A')} ({r.get('City', 'N/A')}) [{r.get('dist_km', 0):.2f} km]")

    print("\n[TEST] 3 restaurantes más cercanos:")
    for n in rt.knn(punto_ref, k=3):
        print(f" - {n.get('Restaurant_Name', 'N/A')} ({n.get('City', 'N/A')}) → Dist={n.get('dist', 0):.4f}")

    # ------------------------------------------------------
    # Reapertura del índice
    # ------------------------------------------------------
    print("\n=== REAPERTURA DEL ÍNDICE ===")
    rt.close()  #  liberar archivos y guardar metadata
    rt2 = RTreePoints(index_name=str(INDEX_PATH))
    rt2.debug_dump(limit=3)
    print("\nEstadísticas:", rt2.stats())
    rt2.close()


if __name__ == "__main__":
    run_basic_tests()
