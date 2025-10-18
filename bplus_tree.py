from structs import Casilla, Node
import os
import struct

class BPlusTree:
    def __init__(self, orden=4, index_file="bptree_index.dat", data_file="bptree_data.dat"):
        self.orden = orden
        self.raiz = None
        self.min_claves = max(1, (self.orden // 2) - 1)
        self.index_file = index_file
        self.data_file = data_file
        self.next_node_id = 0
        self.nodes_map = {}
        self.disk_reads = 0
        self.disk_writes = 0
        self.inicializarArchivos()

    # ===CARGA DE NODOS DESDE DISCO                   
    def inicializarArchivos(self):
        try:
            if not os.path.exists(self.index_file) or os.path.getsize(self.index_file) == 0:
                with open(self.index_file, 'wb') as f:
                    f.write(struct.pack('i', self.orden))
                    f.write(struct.pack('i', 0))
                    f.write(struct.pack('i', -1))
                self.raiz = None
                return

            with open(self.index_file, 'rb') as f:
                self.orden = struct.unpack('i', f.read(4))[0]
                self.next_node_id = struct.unpack('i', f.read(4))[0]
                raiz_id = struct.unpack('i', f.read(4))[0]
                if raiz_id != -1:
                    self.raiz = self.cargarNodo(raiz_id)
                else:
                    self.raiz = None
        except Exception as e:
            print(f"Error inicializando archivos: {e}")
            self.raiz = None

    def guardarNodo(self, nodo):
        try:
            if nodo.node_id is None:
                nodo.node_id = self.next_node_id
                self.next_node_id += 1
            self.nodes_map[nodo.node_id] = nodo
            with open(self.index_file, 'r+b') as f:
                f.seek(0, 2)
                f.write(struct.pack('i', nodo.node_id))
                f.write(struct.pack('?', nodo.es_hoja))
                f.write(struct.pack('i', nodo.cantidad))
                for casilla in nodo.keys:
                    f.write(struct.pack('i', int(casilla.key)))
                for _ in range(self.orden - nodo.cantidad):
                    f.write(struct.pack('i', -1))
                if not nodo.es_hoja:
                    for hijo_id in nodo.hijos:
                        f.write(struct.pack('i', hijo_id))
                    for _ in range(self.orden + 1 - len(nodo.hijos)):
                        f.write(struct.pack('i', -1))
                else:
                    siguiente = nodo.siguiente if nodo.siguiente else -1
                    f.write(struct.pack('i', siguiente))
            self.disk_writes += 1
            return True
        except Exception as e:
            print(f"Error guardando nodo {nodo.node_id}: {e}")
            return False

    def cargarNodo(self, node_id):
        try:
            if node_id in self.nodes_map:
                return self.nodes_map[node_id]
            with open(self.index_file, 'rb') as f:
                f.seek(12)
                while True:
                    data = f.read(4)
                    if not data:
                        break
                    current_node_id = struct.unpack('i', data)[0]
                    es_hoja = struct.unpack('?', f.read(1))[0]
                    cantidad = struct.unpack('i', f.read(4))[0]
                    nodo = Node(self.orden, es_hoja, current_node_id)
                    nodo.cantidad = cantidad
                    for _ in range(self.orden):
                        key = struct.unpack('i', f.read(4))[0]
                        if key != -1:
                            nodo.keys.append(Casilla(int(key)))
                    if not es_hoja:
                        for _ in range(self.orden + 1):
                            child_id = struct.unpack('i', f.read(4))[0]
                            if child_id != -1:
                                nodo.hijos.append(child_id)
                    else:
                        siguiente = struct.unpack('i', f.read(4))[0]
                        if siguiente != -1:
                            nodo.siguiente = siguiente
                    self.disk_reads += 1
                    self.nodes_map[node_id] = nodo
                    return nodo
            return None
        except Exception as e:
            print(f"Error cargando nodo {node_id}: {e}")
            return None

    def guardarMetadata(self):
        try:
            with open(self.index_file, 'r+b') as f:
                raiz_id = self.raiz.node_id if self.raiz else -1
                f.seek(0)
                f.write(struct.pack('i', self.orden))
                f.write(struct.pack('i', self.next_node_id))
                f.write(struct.pack('i', raiz_id))
            return True
        except Exception as e:
            print(f"Error guardando metadata: {e}")
            return False

    # ===BÚSQUEDA   

    def buscar(self, clave):
        if self.raiz is None:
            return None
        clave = int(clave)
        nodo_actual = self.raiz
        while not nodo_actual.es_hoja:
            encontrado = False
            for i in range(len(nodo_actual.keys)):
                if clave < int(nodo_actual.keys[i].key):
                    nodo_actual = self.cargarNodo(nodo_actual.hijos[i])
                    encontrado = True
                    break
            if not encontrado:
                nodo_actual = self.cargarNodo(nodo_actual.hijos[-1])
        for casilla in nodo_actual.keys:
            if int(casilla.key) == clave:
                return self.cargarRegistro(clave)
        return None

    def buscarPorRango(self, claveInicio, claveFin):
        if self.raiz is None:
            return []
        claveInicio, claveFin = int(claveInicio), int(claveFin)
        nodo_actual = self.raiz
        while not nodo_actual.es_hoja:
            encontrado = False
            for i in range(len(nodo_actual.keys)):
                if claveInicio <= int(nodo_actual.keys[i].key):
                    nodo_actual = self.cargarNodo(nodo_actual.hijos[i])
                    encontrado = True
                    break
            if not encontrado:
                nodo_actual = self.cargarNodo(nodo_actual.hijos[-1])
        resultados = []
        while nodo_actual is not None:
            for casilla in nodo_actual.keys:
                k = int(casilla.key)
                if claveInicio <= k <= claveFin:
                    registro = self.cargarRegistro(k)
                    if registro:
                        resultados.append(registro)
                elif k > claveFin:
                    return resultados
            if nodo_actual.siguiente is not None:
                nodo_actual = self.cargarNodo(nodo_actual.siguiente)
            else:
                nodo_actual = None
        return resultados


    # ===INSERCIÓN 

    def insertar(self, clave, valor=None):
        clave = int(clave)
        if self.raiz is None:
            self.raiz = Node(self.orden, True, self.next_node_id)
            self.next_node_id += 1
            self.raiz.insertar_casilla(Casilla(clave, valor))
            self.guardarNodo(self.raiz)
            self.guardarMetadata()
            if valor:
                self.guardarRegistro(valor)
            return True
        self.insertarRecursivo(self.raiz, None, clave, valor)
        if self.raiz.esta_lleno():
            self.dividirRaiz()
        self.guardarMetadata()
        return True

    def insertarRecursivo(self, nodoActual, nodoPadre, clave, valor):
        if nodoActual.es_hoja:
            nodoActual.insertar_casilla(Casilla(clave, valor))
            self.guardarNodo(nodoActual)
            if nodoActual.esta_lleno():
                self.dividirNodo(nodoActual, nodoPadre)
            if valor:
                self.guardarRegistro(valor)
            return
        indiceHijo = self.encontrarIndiceHijo(nodoActual, clave)
        hijo = self.cargarNodo(nodoActual.hijos[indiceHijo])
        self.insertarRecursivo(hijo, nodoActual, clave, valor)
        if hijo.esta_lleno():
            self.dividirNodo(hijo, nodoActual)

    def encontrarIndiceHijo(self, nodo, clave):
        for i in range(len(nodo.keys)):
            if clave < int(nodo.keys[i].key):
                return i
        return len(nodo.keys)

    def dividirRaiz(self):
        raizVieja = self.raiz
        nuevaRaiz = Node(self.orden, False, self.next_node_id)
        self.next_node_id += 1
        if raizVieja.es_hoja:
            nuevaHoja = self.dividirHoja(raizVieja)
            clavePromocion = nuevaHoja.keys[0].key
            nuevaRaiz.insertar_casilla(Casilla(clavePromocion))
            nuevaRaiz.hijos = [raizVieja.node_id, nuevaHoja.node_id]
        else:
            nuevoNodo, clavePromocion = self.dividirInterno(raizVieja)
            nuevaRaiz.insertar_casilla(Casilla(clavePromocion))
            nuevaRaiz.hijos = [raizVieja.node_id, nuevoNodo.node_id]
        self.raiz = nuevaRaiz
        self.guardarNodo(nuevaRaiz)
        self.guardarNodo(raizVieja)

    def dividirNodo(self, nodo, padre):
        if nodo.es_hoja:
            nuevoNodo = self.dividirHoja(nodo)
            clavePromocion = nuevoNodo.keys[0].key
        else:
            nuevoNodo, clavePromocion = self.dividirInterno(nodo)
        if padre is not None:
            padre.insertar_casilla(Casilla(clavePromocion))
            self.insertarHijo(padre, nuevoNodo, clavePromocion)
            self.guardarNodo(padre)

    def dividirHoja(self, hoja):
        puntoMedio = len(hoja.keys) // 2
        nuevaHoja = Node(self.orden, True, self.next_node_id)
        self.next_node_id += 1
        nuevaHoja.keys = hoja.keys[puntoMedio:]
        nuevaHoja.cantidad = len(nuevaHoja.keys)
        hoja.keys = hoja.keys[:puntoMedio]
        hoja.cantidad = len(hoja.keys)
        nuevaHoja.siguiente = hoja.siguiente
        hoja.siguiente = nuevaHoja.node_id
        self.guardarNodo(hoja)
        self.guardarNodo(nuevaHoja)
        return nuevaHoja

    def dividirInterno(self, nodo):
        puntoMedio = len(nodo.keys) // 2
        clavePromocion = nodo.keys[puntoMedio].key
        nuevoNodo = Node(self.orden, False, self.next_node_id)
        self.next_node_id += 1
        nuevoNodo.keys = nodo.keys[puntoMedio + 1:]
        nuevoNodo.hijos = nodo.hijos[puntoMedio + 1:]
        nuevoNodo.cantidad = len(nuevoNodo.keys)
        nodo.keys = nodo.keys[:puntoMedio]
        nodo.hijos = nodo.hijos[:puntoMedio + 1]
        nodo.cantidad = len(nodo.keys)
        self.guardarNodo(nodo)
        self.guardarNodo(nuevoNodo)
        return nuevoNodo, clavePromocion

    def insertarHijo(self, padre, nuevoHijo, clave):
        indice = 0
        while indice < len(padre.keys) and int(padre.keys[indice].key) < clave:
            indice += 1
        padre.hijos.insert(indice + 1, nuevoHijo.node_id)

    # ===ELIMINACIÓN
    def eliminar(self, clave):
        clave = int(clave)
        if self.raiz is None:
            print("✗ Árbol vacío.")
            return False
        eliminado = self._eliminarRecursivo(self.raiz, None, -1, clave)
        if not eliminado:
            print(f"✗ Clave {clave} no encontrada.")
            return False
        if self.raiz and not self.raiz.es_hoja and len(self.raiz.keys) == 0:
            if len(self.raiz.hijos) > 0:
                nueva_raiz = self.cargarNodo(self.raiz.hijos[0])
                self.raiz = nueva_raiz
                self.guardarMetadata()
                self.nodes_map.clear()
            else:
                self.raiz = None
                self.guardarMetadata()
        else:
            self.guardarMetadata()
        return True

    def _eliminarRecursivo(self, nodo, padre, child_index, clave):
        if nodo is None:
            return False
        if nodo.es_hoja:
            for i, cas in enumerate(nodo.keys):
                if int(cas.key) == clave:
                    nodo.keys.pop(i)
                    nodo.cantidad = len(nodo.keys)
                    self.guardarNodo(nodo)
                    if padre is not None:
                        self._balancearNodoDespuesDeEliminar(padre, child_index)
                    return True
            return False
        idx = None
        for i in range(len(nodo.keys)):
            if clave < int(nodo.keys[i].key):
                idx = i
                break
        if idx is None:
            idx = len(nodo.keys)
        hijo_id = nodo.hijos[idx]
        hijo = self.cargarNodo(hijo_id)
        eliminado = self._eliminarRecursivo(hijo, nodo, idx, clave)
        if eliminado:
            self._balancearNodoDespuesDeEliminar(nodo, idx)
            self.guardarNodo(nodo)
        return eliminado

    def _balancearNodoDespuesDeEliminar(self, padre, indice_hijo):
        if indice_hijo < 0 or indice_hijo >= len(padre.hijos):
            return
        hijo = self.cargarNodo(padre.hijos[indice_hijo])
        if hijo is None:
            return
        min_claves = self.min_claves
        if hijo.cantidad >= min_claves:
            return
        left = self.cargarNodo(padre.hijos[indice_hijo - 1]) if indice_hijo > 0 else None
        right = self.cargarNodo(padre.hijos[indice_hijo + 1]) if (indice_hijo + 1) < len(padre.hijos) else None
        if left and left.cantidad > min_claves:
            if hijo.es_hoja:
                prestado = left.keys.pop(-1)
                left.cantidad = len(left.keys)
                hijo.keys.insert(0, prestado)
                hijo.cantidad = len(hijo.keys)
                padre.keys[indice_hijo - 1].key = hijo.keys[0].key
            else:
                prestado = left.keys.pop(-1)
                left.cantidad = len(left.keys)
                clave_padre = padre.keys[indice_hijo - 1]
                padre.keys[indice_hijo - 1] = Casilla(prestado.key)
                hijo.keys.insert(0, Casilla(clave_padre.key))
                if len(left.hijos) > 0:
                    last_child = left.hijos.pop(-1)
                    hijo.hijos.insert(0, last_child)
                hijo.cantidad = len(hijo.keys)
            self.guardarNodo(left)
            self.guardarNodo(hijo)
            self.guardarNodo(padre)
            return
        if right and right.cantidad > min_claves:
            if hijo.es_hoja:
                prestado = right.keys.pop(0)
                right.cantidad = len(right.keys)
                hijo.keys.append(prestado)
                hijo.cantidad = len(hijo.keys)
                padre.keys[indice_hijo].key = right.keys[0].key
            else:
                prestado = right.keys.pop(0)
                right.cantidad = len(right.keys)
                clave_padre = padre.keys[indice_hijo]
                padre.keys[indice_hijo] = Casilla(prestado.key)
                hijo.keys.append(Casilla(clave_padre.key))
                if len(right.hijos) > 0:
                    moved_child = right.hijos.pop(0)
                    hijo.hijos.append(moved_child)
                hijo.cantidad = len(hijo.keys)
            self.guardarNodo(right)
            self.guardarNodo(hijo)
            self.guardarNodo(padre)
            return
        if left:
            self._fusionarNodos(padre, indice_hijo - 1)
        elif right:
            self._fusionarNodos(padre, indice_hijo)
        return

    def _fusionarNodos(self, padre, indice):
        if indice < 0 or indice + 1 >= len(padre.hijos):
            return
        left = self.cargarNodo(padre.hijos[indice])
        right = self.cargarNodo(padre.hijos[indice + 1])
        if left is None or right is None:
            return
        if left.es_hoja:
            left.keys.extend(right.keys)
            left.cantidad = len(left.keys)
            left.siguiente = right.siguiente if right.siguiente is not None else None
        else:
            separadora = padre.keys.pop(indice)
            left.keys.append(Casilla(separadora.key))
            left.keys.extend(right.keys)
            left.hijos.extend(right.hijos)
            left.cantidad = len(left.keys)
        padre.hijos.pop(indice + 1)
        padre.cantidad = len(padre.keys)
        self.guardarNodo(left)
        self.guardarNodo(padre)
        if right.node_id in self.nodes_map:
            del self.nodes_map[right.node_id]
        return

    # ===REGISTROS

    def guardarRegistro(self, record):
        try:
            with open(self.data_file, 'ab') as f:
                f.write(struct.pack('i', int(record.restaurant_id)))
                campos = [
                    record.restaurant_name,
                    record.city,
                    record.cuisines,
                    record.currency,
                    record.rating_color,
                    record.rating_text
                ]
                for campo in campos:
                    campo_str = str(campo) if campo is not None else ""
                    campo_bytes = campo_str.encode('utf-8')
                    f.write(struct.pack('i', len(campo_bytes)))
                    f.write(campo_bytes)
                f.write(struct.pack('i', int(record.country_code) if record.country_code is not None else -1))
                f.write(struct.pack('f', float(record.longitude) if record.longitude is not None else 0.0))
                f.write(struct.pack('f', float(record.latitude) if record.latitude is not None else 0.0))
                f.write(struct.pack('i', int(record.average_cost_for_two) if record.average_cost_for_two is not None else -1))
                f.write(struct.pack('i', int(record.price_range) if record.price_range is not None else -1))
                f.write(struct.pack('f', float(record.aggregate_rating) if record.aggregate_rating is not None else 0.0))
                f.write(struct.pack('i', int(record.votes) if record.votes is not None else 0))
            self.disk_writes += 1
            return True
        except Exception as e:
            print(f"Error guardando registro: {e}")
            return False

    def cargarRegistro(self, restaurant_id):
        try:
            with open(self.data_file, 'rb') as f:
                while True:
                    data = f.read(4)
                    if not data:
                        break
                    id_leido = struct.unpack('i', data)[0]
                    if id_leido == restaurant_id:
                        def leer_string():
                            length = struct.unpack('i', f.read(4))[0]
                            if length > 0:
                                return f.read(length).decode('utf-8')
                            return ""
                        restaurant_name = leer_string()
                        city = leer_string()
                        cuisines = leer_string()
                        currency = leer_string()
                        rating_color = leer_string()
                        rating_text = leer_string()
                        country_code = struct.unpack('i', f.read(4))[0]
                        longitude = struct.unpack('f', f.read(4))[0]
                        latitude = struct.unpack('f', f.read(4))[0]
                        average_cost_for_two = struct.unpack('i', f.read(4))[0]
                        price_range = struct.unpack('i', f.read(4))[0]
                        aggregate_rating = struct.unpack('f', f.read(4))[0]
                        votes = struct.unpack('i', f.read(4))[0]
                        from record import Record
                        return Record(
                            restaurant_id=id_leido,
                            restaurant_name=restaurant_name,
                            country_code=country_code,
                            city=city,
                            address="",
                            locality="",
                            locality_verbose="",
                            longitude=longitude,
                            latitude=latitude,
                            cuisines=cuisines,
                            average_cost_for_two=average_cost_for_two,
                            currency=currency,
                            has_table_booking="No",
                            has_online_delivery="No",
                            is_delivering_now="No",
                            switch_to_order_menu="No",
                            price_range=price_range,
                            aggregate_rating=aggregate_rating,
                            rating_color=rating_color,
                            rating_text=rating_text,
                            votes=votes
                        )
                    else:
                        # saltar 6 strings y 28 bytes numéricos
                        for _ in range(6):
                            l = struct.unpack('i', f.read(4))[0]
                            f.seek(l, 1)
                        f.seek(28, 1)
            return None
        except Exception as e:
            print(f"Error cargando registro {restaurant_id}: {e}")
            return None

    def getDiskStats(self):
        return {
            "disk_reads": self.disk_reads,
            "disk_writes": self.disk_writes,
            "total_operations": self.disk_reads + self.disk_writes
        }
