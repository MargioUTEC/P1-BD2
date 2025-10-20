from test_parser.core.parser.parser_sql import ParserSQL
from test_parser.core.parser.ast_nodes import (
    CreateFromFileNode, InsertNode, DeleteNode,
    SelectNode, SelectWhereNode, ConditionComplexNode, SelectSpatialNode, ExplainNode
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
    # para explain
    # ------------------------------------------------------
    def _execute_explain(self, select_stmt, analyze=False):
        """
        Genera un plan de ejecución similar a PostgreSQL.
        Si analyze=True, ejecuta realmente la consulta y mide el tiempo.
        """
        import time
        cond = getattr(select_stmt, "condition", None)
        forced_index = getattr(select_stmt, "using_index", None)
        table_name = getattr(select_stmt, "table_name", "")

        # --- PLAN lógico: elegir índice apropiado ---
        plan_info = {
            "plan": None,
            "filter": str(cond) if cond else "N/A",
            "index_used": None,
            "estimated_cost": 0.0,
            "rows": 0,
            "execution_time_ms": 0.0
        }

        start_time = time.time()

        # --- Determinar índice usado ---
        if forced_index:
            plan_info["index_used"] = forced_index
            plan_info["plan"] = f"Index Scan using {forced_index} on {table_name}"
        elif cond and hasattr(cond, "attribute"):
            attr = cond.attribute.lower()
            if attr in ["city", "name"]:
                plan_info["index_used"] = "ISAM"
                plan_info["plan"] = f"Index Scan using ISAM on {table_name}"
            elif attr in ["rating", "aggregate_rating", "votes", "average_cost_for_two"]:
                plan_info["index_used"] = "AVL"
                plan_info["plan"] = f"Index Scan using AVL on {table_name}"
            elif "id" in attr:
                plan_info["index_used"] = "B+Tree"
                plan_info["plan"] = f"Index Scan using B+Tree on {table_name}"
            elif attr in ["coords", "longitude", "latitude"]:
                plan_info["index_used"] = "R-Tree"
                plan_info["plan"] = f"Spatial Index Scan using R-Tree on {table_name}"
            else:
                plan_info["index_used"] = "Sequential"
                plan_info["plan"] = f"Seq Scan on {table_name}"
        else:
            plan_info["plan"] = f"Seq Scan on {table_name}"
            plan_info["index_used"] = "Sequential"

        # --- Si es EXPLAIN ANALYZE, ejecutar realmente ---
        results = []
        if analyze:
            results = self._evaluate_condition(cond) if cond else []
            plan_info["rows"] = len(results)
            plan_info["execution_time_ms"] = (time.time() - start_time) * 1000
            plan_info["estimated_cost"] = round(plan_info["execution_time_ms"] * 0.02, 4)  # ejemplo simple

        # --- Salida tipo PostgreSQL ---
        print("\nQUERY PLAN")
        print("-" * 60)
        print(plan_info["plan"])
        print(f"  Filter: {plan_info['filter']}")
        print(f"  Index Used: {plan_info['index_used']}")
        if analyze:
            print(f"  Estimated Cost: {plan_info['estimated_cost']} ms")
            print(f"  Rows Returned: {plan_info['rows']}")
            print(f"  Execution Time: {plan_info['execution_time_ms']:.2f} ms")
        else:
            print("  (Analysis not executed)")
        print("-" * 60)

        # --- Retornar formato JSON para frontend ---
        return plan_info


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
        elif isinstance(stmt, InsertNode):
            print(f"[INSERT] Ejecutando INSERT en tabla {stmt.table_name}...")
            if hasattr(self.index_manager, "insert_full"):
                # Los valores vienen ya parseados como lista en stmt.values
                values = stmt.values
                if not values or len(values) != len(self.index_manager.all_columns):
                    print(
                        f"[WARN] INSERT con {len(values)} valores, se esperaban {len(self.index_manager.all_columns)}")
                record_dict = dict(zip(self.index_manager.all_columns, values))
                self.index_manager.insert_full(record_dict)
                print("[OK] Registro insertado exitosamente en archivo base e índices.")
            else:
                print("[ERROR] insert_full() no disponible en IndexManager.")

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
        # EXPLAIN [ANALYZE]
        # ======================================================
        if isinstance(stmt, ExplainNode):
            print(f"\n[EXPLAIN MODE ACTIVATED] → ANALYZE={stmt.analyze}")
            select_stmt = stmt.select_stmt
            analyze = stmt.analyze
            return self._execute_explain(select_stmt, analyze)


        if isinstance(stmt, ExplainNode):
            print("\n[EXPLAIN MODE ACTIVATED]")
            select_stmt = stmt.select_stmt
            analyze = stmt.analyze
            return self._execute_explain(select_stmt, analyze)

        # ======================================================
        # SELECT ...
        # ======================================================

        if isinstance(stmt, (SelectNode, SelectWhereNode)):
            self._last_select_stmt = stmt
            table_name = getattr(stmt, "table_name", getattr(stmt, "table", ""))
            print(f"\n[SELECT FROM {table_name}]")

            cond = getattr(stmt, "condition", None)
            # Detectar índice forzado por el usuario
            forced_index = getattr(stmt, "using_index", None)
            print(f"[DEBUG] Nodo SELECT detectado → using_index={getattr(stmt, 'using_index', None)}")

            if forced_index:
                self.index_manager.forced_index = forced_index
                print(f"[FORCE INDEX] Usuario especificó usar {forced_index}")
            else:
                self.index_manager.forced_index = None

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

        # Sobrescribir elección si el usuario forzó un índice
        forced = getattr(self.index_manager, "forced_index", None)
        if forced:
            print(f"[FORCED EXECUTION] Redirigiendo búsqueda a {forced} manualmente.")
            res = self.index_manager.force_search(forced, cond)

            # --- Si se devuelve un dict JSON desde force_search ---
            if isinstance(res, dict):
                status = res.get("status", "error")
                msg = res.get("message", "")
                print(msg)
                if status != "success":
                    return []  # evita romper flujo
                return res.get("results", [])

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

            # 🔹 TEXTO genérico (sin índice) → scan secuencial sobre páginas ISAM
            elif attr in ["rating_text", "cuisines", "currency", "rating_color", "address", "locality",
                          "locality_verbose"]:
                print(f"[PLAN] Búsqueda secuencial (texto) para {attr} {op} '{val}'")
                try:
                    return self.index_manager.search_text(attr, str(val), op or "=")
                except Exception as e:
                    print(f"[WARN] Error en búsqueda textual genérica: {e}")
                    return []

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
        if isinstance(results, dict):
            ...
        if not results:
            print(f"[INFO] Sin resultados ({source}).")
            return

        # 🔹 Recuperar columnas seleccionadas (si existen en el último stmt)
        current_stmt = getattr(self, "_last_select_stmt", None)
        selected_cols = getattr(current_stmt, "columns", None)
        if selected_cols and selected_cols != ["*"]:
            filtered_results = [
                {k: v for k, v in r.items() if k in selected_cols} for r in results
            ]
        else:
            filtered_results = results

        print(f"[OK] {len(filtered_results)} resultado(s) encontrados vía {source}:")
        for r in filtered_results[:8]:
            print(" ", r)
        if len(filtered_results) > 8:
            print(f" ... ({len(filtered_results) - 8} más omitidos)")
        print()

    def close(self):
        print("\n[CLOSE] Cerrando estructuras...")
        self.index_manager.close()
        print("[OK] Todas las estructuras cerradas correctamente.")

    def _estimate_cost(self, index_type: str, attr: str, total_rows: int = 10000, matched_rows: int = 0):
        """
        Simula el cálculo de costos como hace PostgreSQL.
        Retorna un diccionario con startup_cost, total_cost, selectivity y estimated_time.
        """
        import random

        # 1. Costos base (I/O + CPU)
        startup_costs = {
            "ISAM": 0.10,
            "AVL": 0.20,
            "HASH": 0.05,
            "BTREE": 0.15,
            "RTREE": 0.25,
            "AUTO": 0.12,
        }

        io_cost_per_page = 0.002  # ms por página (simulado)
        cpu_cost_per_tuple = 0.0005  # ms por fila

        # 2. Selectividad estimada según atributo
        selectivity_map = {
            "id": 0.01,
            "restaurant_id": 0.01,
            "name": 0.05,
            "city": 0.10,
            "rating": 0.25,
            "votes": 0.25,
            "average_cost_for_two": 0.15,
        }

        selectivity = selectivity_map.get(attr.lower(), 0.20)

        # 3. Estimar filas esperadas
        estimated_rows = int(total_rows * selectivity)

        # Si ya se conoce el número de filas reales (ANALYZE)
        if matched_rows:
            estimated_rows = matched_rows

        # 4. Costos proporcionales
        startup = startup_costs.get(index_type, 0.10)
        total = startup + (estimated_rows * cpu_cost_per_tuple) + (estimated_rows * io_cost_per_page)
        estimated_time = round(random.uniform(total * 0.5, total * 1.5), 4)

        return {
            "startup_cost": round(startup, 4),
            "total_cost": round(total, 4),
            "selectivity": round(selectivity, 3),
            "estimated_rows": estimated_rows,
            "estimated_time": estimated_time,
        }

    def run_query_with_options(self, sql: str, options: dict):
        """
        Ejecuta una query con opciones adicionales (índice forzado, modo, límites).
        """
        print("\n=== Ejecutando consulta con opciones ===")
        print(f"SQL: {sql}")
        print(f"Options: {options}")

        force_index = (options.get("force_index") or "").upper()
        field = options.get("force_field", "").lower()
        mode = options.get("mode", "NORMAL").upper()

        if mode == "EXPLAIN":
            result = self.parser.parse(sql)
            stmt = result[0] if isinstance(result, list) else result
            if not isinstance(stmt, ExplainNode):
                stmt = ExplainNode(analyze=False, select_stmt=stmt)
            return self._execute_explain(stmt.select_stmt, analyze=False)

        # En modo normal, ejecuta la query
        result = self.parser.parse(sql)
        stmts = result if isinstance(result, (list, tuple)) else [result]

        for stmt in stmts:
            # Si hay índice forzado, podemos sobreescribir temporalmente la selección automática
            if force_index:
                print(f"[FORCE INDEX] Se usará {force_index} para ejecutar la consulta.")
                self.index_manager.forced_index = force_index
            else:
                self.index_manager.forced_index = None

            self._execute(stmt)

        return []


if __name__ == "__main__":
    qe = QueryEngine()
    script = """

INSERT INTO restaurants VALUES (
    9999991,
    "Test Restaurant",
    162,
    "Lima",
    "Av. Universitaria 1234",
    "San Miguel",
    "Lima Metropolitana",
    77.032,
    -12.046,
    "Peruvian, Fast Food",
    120,
    "Peruvian Soles(S/)",
    "Yes",
    "No",
    "No",
    "No",
    2,
    4.5,
    "Green",
    "Very Good",
    120
);
"""
    qe.run_script(script)
    qe.close()
