import pandas as pd
from record import Record
from bplus_tree import BPlusTree

class BPlusTreeManager:
    def __init__(self, orden=4):
        self.tree = BPlusTree(orden)
        self.records_map = {}
    
    def loadFromCSV(self, csv_path, num_records=20):
        try:
            df = pd.read_csv(csv_path)
            print(f"Dataset cargado: {len(df)} registros")
            
            test_records = df.head(num_records)
            
            print("Insertando registros en el B+ Tree...")
            for _, row in test_records.iterrows():
                record = Record(
                    restaurant_id=row['Restaurant ID'],
                    restaurant_name=row['Restaurant Name'],
                    country_code=row['Country Code'],
                    city=row['City'],
                    address=row['Address'],
                    locality=row['Locality'],
                    locality_verbose=row['Locality Verbose'],
                    longitude=row['Longitude'],
                    latitude=row['Latitude'],
                    cuisines=row['Cuisines'],
                    average_cost_for_two=row['Average Cost for two'],
                    currency=row['Currency'],
                    has_table_booking=row['Has Table booking'],
                    has_online_delivery=row['Has Online delivery'],
                    is_delivering_now=row['Is delivering now'],
                    switch_to_order_menu=row['Switch to order menu'],
                    price_range=row['Price range'],
                    aggregate_rating=row['Aggregate rating'],
                    rating_color=row['Rating color'],
                    rating_text=row['Rating text'],
                    votes=row['Votes']
                )
                
                self.tree.insertar(record.restaurant_id, record)
                self.records_map[record.restaurant_id] = record
                
                print(f"Insertado: ID {record.restaurant_id}")
            
            stats = self.tree.getDiskStats()
            print(f"\nMétricas de disco:")
            print(f"  Lecturas: {stats['disk_reads']}")
            print(f"  Escrituras: {stats['disk_writes']}")
            print(f"  Total: {stats['total_operations']}")
            
            return True
            
        except Exception as e:
            print(f"Error cargando CSV: {e}")
            return False
    
    def searchRecord(self, restaurant_id):
        record = self.tree.buscar(restaurant_id)
        if record:
            print(f"✓ Registro encontrado - ID: {record.restaurant_id}")
            print(f"  Nombre: {record.restaurant_name}")
            print(f"  Ciudad: {record.city}")
            print(f"  Rating: {record.aggregate_rating}")
            return record
        else:
            print(f"✗ Registro con ID {restaurant_id} no encontrado")
            return None
    
    def rangeSearch(self, begin_id, end_id):
        results = self.tree.buscarPorRango(begin_id, end_id)
        print(f"Búsqueda en rango [{begin_id} - {end_id}]: {len(results)} registros")
        
        for record in results:
            print(f"  - ID: {record.restaurant_id}, Nombre: {record.restaurant_name[:30]}...")
        
        return results
    
    def deleteRecord(self, restaurant_id):
        success = self.tree.eliminar(restaurant_id)
        if success:
            print(f"✓ Registro {restaurant_id} eliminado del índice")
            if restaurant_id in self.records_map:
                del self.records_map[restaurant_id]
        else:
            print(f"✗ Error eliminando registro {restaurant_id}")
        return success

def main():
    manager = BPlusTreeManager(orden=4)
    
    if manager.loadFromCSV("Dataset.csv", num_records=20):
        print("\n" + "="*50)
        
        print("1. PRUEBA DE BÚSQUEDA INDIVIDUAL:")
        if manager.records_map:
            test_id = list(manager.records_map.keys())[0]
            manager.searchRecord(test_id)
        
        print("\n2. PRUEBA DE BÚSQUEDA POR RANGO:")
        if manager.records_map:
            min_id = min(manager.records_map.keys())
            max_id = max(manager.records_map.keys())
            manager.rangeSearch(min_id, max_id)
        
        print("\n3. PRUEBA DE ELIMINACIÓN:")
        if manager.records_map:
            remove_id = list(manager.records_map.keys())[0]
            manager.deleteRecord(remove_id)
            
            print("\n4. VERIFICACIÓN POST-ELIMINACIÓN:")
            manager.searchRecord(remove_id)

if __name__ == "__main__":
    main()