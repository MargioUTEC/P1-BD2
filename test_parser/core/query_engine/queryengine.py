from test_parser.core.parser.parser_sql import ParserSQL
from test_parser.core.parser.ast_nodes import (
    CreateFromFileNode, InsertNode, DeleteNode,
    SelectNode, SelectWhereNode, ConditionComplexNode, SelectSpatialNode
)
from test_parser.core.index_manager import IndexManager
from test_parser.indexes.isam_s.isam import Record


class QueryEngine:
    """
    Interpreta y ejecuta consultas SQL-like utilizando las 5 estructuras:
    - ISAM para búsquedas textuales (nombre/ciudad)
    - Hash Extensible para accesos directos por ID
    - AVL como índice principal persistente
    - B+Tree para rangos y orden
    - R-Tree para búsquedas espaciales
    """

    def __init__(self):
        self.parser = ParserSQL()
        self.index_manager = IndexManager()

    # ------------------------------------------------------
    # Ejecutar múltiples sentencias (separadas por líneas)
    # ------------------------------------------------------
    def run_script(self, script: str):
        """
        Ejecuta múltiples sentencias SQL en un mismo bloque.
        Separa por ';' en lugar de por líneas para evitar errores
        cuando WHERE o AND están en líneas distintas.
        """
        # Separar sentencias completas por ';'
        statements = [s.strip() for s in script.strip().split(";") if s.strip()]

        for stmt in statements:
            if stmt.startswith("--") or not stmt:
                continue
            try:
                self.run_query(stmt)
            except Exception as e:
                print(f"[ERROR] {e}")

    def run_query(self, query: str):
        result = self.parser.parse(query)
        stmts = result if isinstance(result, (list, tuple)) else [result]
        for stmt in stmts:
            self._execute(stmt)

    def _execute(self, stmt):
        # ======================================================
        # CREATE TABLE ... FROM FILE ...
        # ======================================================
        if isinstance(stmt, CreateFromFileNode):
            print(f"\n[CREATE FROM FILE] {stmt.file_path}")
            self.index_manager.rebuild_from_csv(
                stmt.file_path,
                limit=50,
                using_indexes=stmt.using_indexes
            )
            built = stmt.using_indexes or ["ISAM", "HASH", "AVL", "B+TREE", "RTREE"]
            print(f"[OK] Índices creados: {', '.join(built)}.")
            self.index_manager.summary()
            return

        # ======================================================
        # INSERT INTO ...
        # ======================================================
        if isinstance(stmt, InsertNode):
            print(f"\n[INSERT INTO {stmt.table_name}]")
            try:
                vals = stmt.values
                rec = Record.from_minimal(
                    restaurant_id=int(vals[0]),
                    name=str(vals[1]),
                    city=str(vals[2]),
                    longitude=float(vals[3]),
                    latitude=float(vals[4]),
                    aggregate_rating=float(vals[5])
                )
                self.index_manager.insert(rec)
                print("[OK] Registro insertado en todas las estructuras.")
            except Exception as e:
                print(f"[ERROR] Fallo al insertar: {e}")
            return

        # ======================================================
        # DELETE FROM ...
        # ======================================================
        if isinstance(stmt, DeleteNode):
            print(f"\n[DELETE FROM {stmt.table_name}]")
            cond = stmt.condition
            if cond is None:
                print("[WARN] DELETE sin condición no permitido.")
                return

            # Por ID → eliminar en todas las estructuras
            if hasattr(cond, "attribute") and "id" in cond.attribute.lower():
                rid = int(cond.value)
                print(f"[DEBUG] Eliminando registro con ID={rid}...")
                self.index_manager.delete(restaurant_id=rid)
                print("[OK] Eliminación completada en ISAM, AVL, HASH, B+Tree y R-Tree.")
            else:
                print(f"[WARN] Condición no soportada para DELETE: {cond}")
            return

        # ======================================================
        # SELECT ...
        # ======================================================

        if isinstance(stmt, (SelectNode, SelectWhereNode)):
            table_name = getattr(stmt, "table_name", getattr(stmt, "table", ""))
            print(f"\n[SELECT FROM {table_name}]")

            cond = getattr(stmt, "condition", None)
            if cond is None:
                print("[INFO] SELECT * no implementado. Usa WHERE.")
                return

            if isinstance(cond, ConditionComplexNode):
                print("[DEBUG] Evaluando condición compuesta (AND / OR)...")
                results = self._evaluate_condition(cond)
                return self._print_results(results, "Combinado (AND/OR)")

            if isinstance(cond, SelectSpatialNode):
                x, y = cond.point
                r = cond.radius
                print(f"[DEBUG] Búsqueda espacial con R-Tree: ({x}, {y}) ± {r} km")
                results = self.index_manager.search_near(x, y, r)
                return self._print_results(results, "R-Tree")

            results = self._evaluate_condition(cond)
            return self._print_results(results, "Condición simple")

        if isinstance(stmt, SelectSpatialNode):
            print(f"\n[SELECT SPATIAL WHERE {stmt.column}]")
            x, y = stmt.point
            r = stmt.radius
            results = self.index_manager.search_near(x, y, r)
            return self._print_results(results, "R-Tree Direct")
        print(f"[WARN] Nodo no soportado: {stmt}")

    def _evaluate_condition(self, cond):
        """
        Evalúa recursivamente condiciones simples y compuestas (AND / OR),
        combinando resultados de distintos índices (ISAM, AVL, B+Tree, R-Tree).
        """
        from test_parser.core.parser.ast_nodes import (
            ConditionComplexNode, ConditionNode, BetweenConditionNode, SelectSpatialNode
        )

        # ==============================
        # (A AND B), (A OR B), o anidada
        # ==============================
        if isinstance(cond, ConditionComplexNode):
            left_results = self._evaluate_condition(cond.left)
            right_results = self._evaluate_condition(cond.right)

            # --- Función auxiliar para extraer el ID de distintos tipos de registro
            def extract_id(record):
                if isinstance(record, dict):
                    return record.get('restaurant_id')
                elif isinstance(record, tuple):
                    # Ejemplo: (6152, #18255654 | Hobing ...)
                    return record[0]
                elif hasattr(record, 'restaurant_id'):
                    return getattr(record, 'restaurant_id', None)
                return None

            # --- Crear conjuntos de IDs
            left_ids = {extract_id(r) for r in left_results if extract_id(r) is not None}
            right_ids = {extract_id(r) for r in right_results if extract_id(r) is not None}

            print("[DEBUG] LEFT sample:", left_results[:3])
            print("[DEBUG] RIGHT sample:", right_results[:3])

            # ==============================
            # AND -> intersección lógica
            # ==============================
            if cond.operator == "AND":
                combined_ids = left_ids & right_ids
                print(
                    f"[DEBUG] AND combinó {len(left_results)} ∩ {len(right_results)} → {len(combined_ids)} resultado(s)")

                # Crear mapa para acceder rápido por ID
                all_results = {}
                for r in left_results + right_results:
                    rid = extract_id(r)
                    if rid is not None:
                        all_results[rid] = r

                # Filtrar resultados finales por los IDs combinados
                filtered = [all_results[rid] for rid in combined_ids if rid in all_results]
                return filtered

            # ==============================
            # OR -> unión lógica
            # ==============================
            elif cond.operator == "OR":
                all_results = {}
                for r in left_results + right_results:
                    rid = extract_id(r)
                    if rid is not None:
                        all_results[rid] = r
                print(
                    f"[DEBUG] OR combinó {len(left_results)} ∪ {len(right_results)} → {len(all_results)} resultado(s)")
                return list(all_results.values())

            else:
                print(f"[WARN] Operador lógico desconocido: {cond.operator}")
                return []

        # ==============================
        # espacial -> R-Tree
        # ==============================
        if isinstance(cond, SelectSpatialNode):
            x, y = cond.point
            r = cond.radius
            print(f"[PLAN] Usando R-Tree para búsqueda espacial ({x}, {y}) ± {r} km")
            return self.index_manager.search_near(x, y, r)

        # ==============================
        # simple → textual o numérica
        # ==============================
        if isinstance(cond, ConditionNode):
            attr = cond.attribute.lower()
            op = cond.operator
            val = cond.value

            # ISAM -> búsqueda textual
            if attr in ["name", "city"]:
                name = val if attr == "name" else ""
                city = val if attr == "city" else ""
                print(f"[PLAN] Usando ISAM para búsqueda por texto ({attr} = '{val}')")
                return self.index_manager.search_by_name(name.strip(), city.strip())

            # AVL -> atributos numéricos
            elif attr in ["rating", "votes", "average_cost_for_two"]:
                try:
                    value = float(val)
                    print(f"[PLAN] Usando AVL.search_comparison() para {attr} {op} {value}")
                    return self.index_manager.search_comparison(attr, op, value)
                except Exception as e:
                    print(f"[WARN] Error en búsqueda numérica ({attr} {op} {val}): {e}")
                    return []

            # ID -> AVL -> Hash -> B+Tree
            elif "id" in attr:
                try:
                    rid = int(val)
                    print(f"[PLAN] Búsqueda jerárquica ID={rid} → AVL → Hash → B+Tree")
                    res = self.index_manager.search_by_id(rid)
                    if res:
                        return res
                    row = self.index_manager.hash.search(rid)
                    if row:
                        return [row]
                    valb = self.index_manager.bpt.search(rid)
                    if valb:
                        return [{"restaurant_id": rid, "restaurant_name": valb}]
                except Exception as e:
                    print(f"[WARN] Error en búsqueda por ID: {e}")
                return []

        if isinstance(cond, BetweenConditionNode):
            attr = cond.attribute.lower()
            v1, v2 = cond.value1, cond.value2
            print(f"[PLAN] Usando rango BETWEEN ({v1}, {v2}) para '{attr}'")
            try:
                return self.index_manager.search_between_general(attr, v1, v2)
            except Exception as e:
                print(f"[WARN] Error en búsqueda BETWEEN: {e}")
                return []

        print(f"[WARN] Condición no reconocida: {cond}")
        return []

    def _print_results(self, results, source=""):
        if not results:
            print(f"[INFO] Sin resultados ({source}).")
            return

        print(f"[OK] {len(results)} resultado(s) encontrados vía {source}:")
        for r in results[:8]:
            print(" ", r)
        if len(results) > 8:
            print(f" ... ({len(results) - 8} más omitidos)")
        print()

    def close(self):
        print("\n[CLOSE] Cerrando estructuras...")
        self.index_manager.close()
        print("[OK] Todas las estructuras cerradas correctamente.")


if __name__ == "__main__":
    qe = QueryEngine()
    script = """
    CREATE TABLE restaurants USING ISAM, HASH, RTREE, AVL, BTREE FROM FILE "Dataset.csv";
    SELECT * FROM restaurants
    WHERE city = "Taguig City" AND rating > 4.0 AND votes BETWEEN 100 AND 500;
    """
    qe.run_script(script)
    qe.close()
