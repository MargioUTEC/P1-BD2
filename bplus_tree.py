from structs import Casilla, Node
import os
import struct

class BPlusTree:
    def __init__(self, orden=4, index_file="bptree_index.dat", data_file="bptree_data.dat"):
        # Parámetros y estado
        self.orden = orden
        self.raiz = None
        # mínimo de claves permitido por nodo (ajustable según tu convención)
        self.min_claves = max(1, (self.orden // 2) - 1)
        self.index_file = index_file
        self.data_file = data_file
        self.next_node_id = 0
        # cache en memoria de nodos cargados recientemente
        self.nodes_map = {}
        self.disk_reads = 0
        self.disk_writes = 0

        # Inicializa (crea archivo si no existe)
        self.inicializarArchivos()

    
    # Archivos y metadata
    
    def inicializarArchivos(self):
        try:
            # Si no existe o está vacío, crear cabecera
            if not os.path.exists(self.index_file) or os.path.getsize(self.index_file) == 0:
                with open(self.index_file, 'wb') as f:
                    f.write(struct.pack('i', self.orden))     # orden
                    f.write(struct.pack('i', 0))              # next_node_id
                    f.write(struct.pack('i', -1))             # raiz_id (-1 = no raíz)
                self.raiz = None
                return

            # Si existe, leer cabecera
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

    #  Escribe el nodo al final del archivo de índice (append).
    #Nota: para simplicidad la implementación añade una nueva versión del nodo al final. cargarNodo devuelve la última versión encontrada de un node_id.
    # Guardar nodo (append)
    def guardarNodo(self, nodo):
        try:
            if nodo.node_id is None:
                nodo.node_id = self.next_node_id
                self.next_node_id += 1

            # Mantenerlo en cache
            self.nodes_map[nodo.node_id] = nodo

            with open(self.index_file, 'r+b') as f:
                f.seek(0, 2)  # ir al final
                # node_id (4 bytes)
                f.write(struct.pack('i', nodo.node_id))
                # es_hoja (1 byte, '?')
                f.write(struct.pack('?', nodo.es_hoja))
                # cantidad (4 bytes)
                f.write(struct.pack('i', nodo.cantidad))
                # claves (orden * 4 bytes) -> rellenar con -1 si no hay más
                for casilla in nodo.keys:
                    f.write(struct.pack('i', int(casilla.key)))
                for i in range(self.orden - nodo.cantidad):
                    f.write(struct.pack('i', -1))
                # punteros hijos o siguiente
                if not nodo.es_hoja:
                    # escribir orden+1 hijos
                    for hijo_id in nodo.hijos:
                        f.write(struct.pack('i', hijo_id))
                    for i in range(self.orden + 1 - len(nodo.hijos)):
                        f.write(struct.pack('i', -1))
                else:
                    siguiente = nodo.siguiente if nodo.siguiente is not None else -1
                    f.write(struct.pack('i', siguiente))
            self.disk_writes += 1
            return True
        except Exception as e:
            print(f"Error guardando nodo {getattr(nodo, 'node_id', None)}: {e}")
            return False

    #Esta funcion va a leer todo el archivo index_file buscando entradas de nodo, lo que va a devolver la ÚLTIMA versión encontrada con node_id (si existe).
    #Esto permite escribir nodos por append sin invalidar búsquedas.

    # Cargar nodo por id
    def cargarNodo(self, node_id):
        try:
            # si está en cache devolvemos directamente
            if node_id in self.nodes_map:
                return self.nodes_map[node_id]

            found_node = None

            with open(self.index_file, 'rb') as f:
                # saltar cabecera
                f.seek(12)
                while True:
                    data = f.read(4)
                    if not data:
                        break
                    current_node_id = struct.unpack('i', data)[0]

                    # leer es_hoja (1 byte) y cantidad (4 bytes)
                    es_hoja = struct.unpack('?', f.read(1))[0]
                    cantidad = struct.unpack('i', f.read(4))[0]

                    # leer claves (orden enteros)
                    raw_keys = []
                    for i in range(self.orden):
                        k = struct.unpack('i', f.read(4))[0]
                        raw_keys.append(k)

                    # leer hijos o siguiente según tipo
                    raw_hijos = []
                    siguiente = -1
                    if not es_hoja:
                        for i in range(self.orden + 1):
                            child = struct.unpack('i', f.read(4))[0]
                            raw_hijos.append(child)
                    else:
                        siguiente = struct.unpack('i', f.read(4))[0]

                    # Si coincide el id, construir objeto Node
                    if current_node_id == node_id:
                        nodo = Node(self.orden, es_hoja, current_node_id)
                        nodo.cantidad = cantidad
                        # agregar claves válidas
                        for key_val in raw_keys:
                            if key_val != -1:
                                nodo.keys.append(Casilla(int(key_val)))
                        # hijos válidos
                        if not es_hoja:
                            for child_id in raw_hijos:
                                if child_id != -1:
                                    nodo.hijos.append(child_id)
                        else:
                            if siguiente != -1:
                                nodo.siguiente = siguiente
                        found_node = nodo
                        # no retornamos de inmediato: seguir leyendo para obtener la última versión
                        self.disk_reads += 1

                # si encontramos, cachearlo y retornarlo
                if found_node is not None:
                    self.nodes_map[node_id] = found_node
                    return found_node

            return None
        except Exception as e:
            print(f"Error cargando nodo {node_id}: {e}")
            return None

    
    # BÚSQUEDAS 
    def buscar(self, clave):
        if self.raiz is None:
            return None
        clave_int = int(clave)
        nodo_actual = self.raiz
        # bajar hasta hoja
        while not nodo_actual.es_hoja:
            encontrado = False
            # comparar contra claves internas
            for i in range(len(nodo_actual.keys)):
                if clave_int < int(nodo_actual.keys[i].key):
                    nodo_actual = self.cargarNodo(nodo_actual.hijos[i])
                    encontrado = True
                    break
            if not encontrado:
                nodo_actual = self.cargarNodo(nodo_actual.hijos[-1])
        # buscar en hoja
        for cas in nodo_actual.keys:
            if int(cas.key) == clave_int:
                return self.cargarRegistro(clave_int)
        return None

    def buscarPorRango(self, clave_inicio, clave_fin):
        if self.raiz is None:
            return []
        start = int(clave_inicio)
        end = int(clave_fin)
        nodo_actual = self.raiz
        # bajar hasta la hoja que puede contener start
        while not nodo_actual.es_hoja:
            encontrado = False
            for i in range(len(nodo_actual.keys)):
                if start <= int(nodo_actual.keys[i].key):
                    nodo_actual = self.cargarNodo(nodo_actual.hijos[i])
                    encontrado = True
                    break
            if not encontrado:
                nodo_actual = self.cargarNodo(nodo_actual.hijos[-1])

        resultados = []
        # recorrer hojas encadenadas
        while nodo_actual is not None:
            for cas in nodo_actual.keys:
                k = int(cas.key)
                if k < start:
                    continue
                if k > end:
                    return resultados
                registro = self.cargarRegistro(k)
                if registro is not None:
                    resultados.append(registro)
            if nodo_actual.siguiente is not None:
                nodo_actual = self.cargarNodo(nodo_actual.siguiente)
            else:
                nodo_actual = None
        return resultados

    # INSERCIÓN
    def insertar(self, clave, valor=None):
        clave_int = int(clave)
        # crear raíz si no existe
        if self.raiz is None:
            self.raiz = Node(self.orden, True, self.next_node_id)
            self.next_node_id += 1
            self.raiz.insertar_casilla(Casilla(clave_int, valor))
            self.guardarNodo(self.raiz)
            self.guardarMetadata()
            if valor is not None:
                self.guardarRegistro(valor)
            return True

        # insertar recursivamente
        self.insertar_recursivo(self.raiz, None, clave_int, valor)

        # si raíz quedó llena, dividirla
        if self.raiz.esta_lleno():
            self.dividirRaiz()

        self.guardarMetadata()
        return True

    def insertar_recursivo(self, nodo_actual, padre, clave, valor):
        # si es hoja insertar y posiblemente dividir
        if nodo_actual.es_hoja:
            nodo_actual.insertar_casilla(Casilla(clave, valor))
            self.guardarNodo(nodo_actual)
            if nodo_actual.esta_lleno():
                self.dividirNodo(nodo_actual, padre)
            if valor is not None:
                self.guardarRegistro(valor)
            return

        # bajar por el hijo correcto
        indice_hijo = self.encontrarIndiceHijo(nodo_actual, clave)
        hijo = self.cargarNodo(nodo_actual.hijos[indice_hijo])
        self.insertar_recursivo(hijo, nodo_actual, clave, valor)

        # si hijo quedó lleno, dividirlo
        if hijo.esta_lleno():
            self.dividirNodo(hijo, nodo_actual)

    def encontrarIndiceHijo(self, nodo, clave):
        # devuelve índice de hijo a seguir
        for i in range(len(nodo.keys)):
            if clave < int(nodo.keys[i].key):
                return i
        return len(nodo.keys)

    def dividirRaiz(self):
        raiz_vieja = self.raiz
        nueva_raiz = Node(self.orden, False, self.next_node_id)
        self.next_node_id += 1

        if raiz_vieja.es_hoja:
            nueva_hoja = self.dividirHoja(raiz_vieja)
            clave_promocion = nueva_hoja.keys[0].key
            nueva_raiz.insertar_casilla(Casilla(clave_promocion))
            nueva_raiz.hijos = [raiz_vieja.node_id, nueva_hoja.node_id]
        else:
            nuevo_nodo, clave_promocion = self.dividirInterno(raiz_vieja)
            nueva_raiz.insertar_casilla(Casilla(clave_promocion))
            nueva_raiz.hijos = [raiz_vieja.node_id, nuevo_nodo.node_id]

        self.raiz = nueva_raiz
        self.guardarNodo(nueva_raiz)
        self.guardarNodo(raiz_vieja)

    def dividirNodo(self, nodo, padre):
        if nodo.es_hoja:
            nuevo_nodo = self.dividirHoja(nodo)
            clave_promocion = nuevo_nodo.keys[0].key
        else:
            nuevo_nodo, clave_promocion = self.dividirInterno(nodo)

        if padre is not None:
            padre.insertar_casilla(Casilla(clave_promocion))
            self.insertarHijo(padre, nuevo_nodo, clave_promocion)
            self.guardarNodo(padre)

    def dividirHoja(self, hoja):
        punto_medio = len(hoja.keys) // 2
        nueva_hoja = Node(self.orden, True, self.next_node_id)
        self.next_node_id += 1

        nueva_hoja.keys = hoja.keys[punto_medio:]
        nueva_hoja.cantidad = len(nueva_hoja.keys)

        hoja.keys = hoja.keys[:punto_medio]
        hoja.cantidad = len(hoja.keys)

        nueva_hoja.siguiente = hoja.siguiente
        hoja.siguiente = nueva_hoja.node_id

        self.guardarNodo(hoja)
        self.guardarNodo(nueva_hoja)
        return nueva_hoja

    def dividirInterno(self, nodo):
        punto_medio = len(nodo.keys) // 2
        clave_promocion = nodo.keys[punto_medio].key

        nuevo_nodo = Node(self.orden, False, self.next_node_id)
        self.next_node_id += 1

        nuevo_nodo.keys = nodo.keys[punto_medio + 1:]
        nuevo_nodo.hijos = nodo.hijos[punto_medio + 1:]
        nuevo_nodo.cantidad = len(nuevo_nodo.keys)

        nodo.keys = nodo.keys[:punto_medio]
        nodo.hijos = nodo.hijos[:punto_medio + 1]
        nodo.cantidad = len(nodo.keys)

        self.guardarNodo(nodo)
        self.guardarNodo(nuevo_nodo)
        return nuevo_nodo, clave_promocion

    def insertarHijo(self, padre, nuevo_hijo, clave):
        indice = 0
        while indice < len(padre.keys) and int(padre.keys[indice].key) < int(clave):
            indice += 1
        padre.hijos.insert(indice + 1, nuevo_hijo.node_id)


    # ELIMINACIÓN 
    def eliminar(self, clave):
        clave_int = int(clave)
        if self.raiz is None:
            print("Árbol vacío.")
            return False

        eliminado = self.eliminar_recursivo(self.raiz, None, -1, clave_int)

        if not eliminado:
            print(f"Clave {clave_int} no encontrada.")
            return False

        # si raíz interna quedó vacía, bajar un nivel
        if self.raiz and not self.raiz.es_hoja and len(self.raiz.keys) == 0:
            if len(self.raiz.hijos) > 0:
                nueva_raiz = self.cargarNodo(self.raiz.hijos[0])
                self.raiz = nueva_raiz
                self.guardarMetadata()
                # limpiar cache para forzar recargas 
                self.nodes_map.clear()
            else:
                self.raiz = None
                self.guardarMetadata()
        else:
            self.guardarMetadata()

        return True
    

#Elimina la clave en el subárbol cuyo nodo raíz es 'nodo'.
    def eliminar_recursivo(self, nodo, padre, child_index, clave):
        if nodo is None:
            return False

        # caso hoja: intentar eliminar en esta hoja
        if nodo.es_hoja:
            for i in range(len(nodo.keys)):
                cas = nodo.keys[i]
                if int(cas.key) == clave:
                    # eliminar la casilla de la hoja
                    nodo.keys.pop(i)
                    nodo.cantidad = len(nodo.keys)
                    self.guardarNodo(nodo)
                    # balancear si es necesario en el padre
                    if padre is not None:
                        self.balancear_despues_eliminar(padre, child_index)
                    return True
            return False

        # caso interno: decidir por cuál hijo bajar
        idx = None
        for i in range(len(nodo.keys)):
            if clave < int(nodo.keys[i].key):
                idx = i
                break
        if idx is None:
            idx = len(nodo.keys)

        hijo_id = nodo.hijos[idx]
        hijo = self.cargarNodo(hijo_id)
        eliminado = self.eliminar_recursivo(hijo, nodo, idx, clave)

        if eliminado:
            # después de eliminar en el hijo, balancear si el hijo quedó con pocas claves
            self.balancear_despues_eliminar(nodo, idx)
            self.guardarNodo(nodo)
        return eliminado


#TENEMOS ESTOS CASOS DE BALANCEO: Balancear el hijo padre.hijos[indice_hijo] si quedó con menos claves que min_claves.
#         1) pedir prestado del hermano izquierdo,
#          2) pedir prestado del hermano derecho,
#         3) si no hay préstamo posible, fusionar con un hermano.
    def balancear_despues_eliminar(self, padre, indice_hijo):
        # validar índice
        if indice_hijo < 0 or indice_hijo >= len(padre.hijos):
            return

        hijo = self.cargarNodo(padre.hijos[indice_hijo])
        if hijo is None:
            return

        if hijo.cantidad >= self.min_claves:
            return  # no hace falta balancear

        # cargar hermanos si existen
        hermano_izq = None
        hermano_der = None
        if indice_hijo > 0:
            hermano_izq = self.cargarNodo(padre.hijos[indice_hijo - 1])
        if (indice_hijo + 1) < len(padre.hijos):
            hermano_der = self.cargarNodo(padre.hijos[indice_hijo + 1])

        #intentar préstamo del hermano izquierdo
        if hermano_izq is not None and hermano_izq.cantidad > self.min_claves:
            if hijo.es_hoja:
                # mover última clave del hermano izquierdo al inicio del hijo
                prestado = hermano_izq.keys.pop(-1)
                hermano_izq.cantidad = len(hermano_izq.keys)
                hijo.keys.insert(0, prestado)
                hijo.cantidad = len(hijo.keys)
                # actualizar clave separadora en el padre
                padre.keys[indice_hijo - 1].key = hijo.keys[0].key
            else:
                # nodo interno: rotación hacia la derecha desde hermano izquierdo
                prestado = hermano_izq.keys.pop(-1)
                hermano_izq.cantidad = len(hermano_izq.keys)
                clave_padre = padre.keys[indice_hijo - 1]
                padre.keys[indice_hijo - 1] = Casilla(prestado.key)
                hijo.keys.insert(0, Casilla(clave_padre.key))
                # también mover el último puntero hijo del hermano izquierdo
                if len(hermano_izq.hijos) > 0:
                    ultimo_hijo = hermano_izq.hijos.pop(-1)
                    hijo.hijos.insert(0, ultimo_hijo)
                hijo.cantidad = len(hijo.keys)

            # persistir cambios
            self.guardarNodo(hermano_izq)
            self.guardarNodo(hijo)
            self.guardarNodo(padre)
            return

        #intentar préstamo del hermano derecho
        if hermano_der is not None and hermano_der.cantidad > self.min_claves:
            if hijo.es_hoja:
                # mover primera clave del hermano derecho al final del hijo
                prestado = hermano_der.keys.pop(0)
                hermano_der.cantidad = len(hermano_der.keys)
                hijo.keys.append(prestado)
                hijo.cantidad = len(hijo.keys)
                # actualizar clave separadora en el padre
                padre.keys[indice_hijo].key = hermano_der.keys[0].key
            else:
                # nodo interno: rotación desde la derecha
                prestado = hermano_der.keys.pop(0)
                hermano_der.cantidad = len(hermano_der.keys)
                clave_padre = padre.keys[indice_hijo]
                padre.keys[indice_hijo] = Casilla(prestado.key)
                hijo.keys.append(Casilla(clave_padre.key))
                # también mover el primer puntero hijo del hermano derecho
                if len(hermano_der.hijos) > 0:
                    primer_hijo = hermano_der.hijos.pop(0)
                    hijo.hijos.append(primer_hijo)
                hijo.cantidad = len(hijo.keys)

            self.guardarNodo(hermano_der)
            self.guardarNodo(hijo)
            self.guardarNodo(padre)
            return

        #si no se pudo redistribuir: fusionar con un hermano
        if hermano_izq is not None:
            self.fusionar_nodos(padre, indice_hijo - 1)
        elif hermano_der is not None:
            self.fusionar_nodos(padre, indice_hijo)
        # si no hay hermanos no hay nada que hacer


#Fusionara padre.hijos[indice] (left) y padre.hijos[indice+1] (right).
#El resultado queda en el nodo izquierdo.
    def fusionar_nodos(self, padre, indice):
        if indice < 0 or indice + 1 >= len(padre.hijos):
            return

        left = self.cargarNodo(padre.hijos[indice])
        right = self.cargarNodo(padre.hijos[indice + 1])
        if left is None or right is None:
            return

        if left.es_hoja:
            # para hojas, concatenar claves y ajustar puntero siguiente
            left.keys.extend(right.keys)
            left.cantidad = len(left.keys)
            left.siguiente = right.siguiente if right.siguiente is not None else None
        else:
            # para internos, insertar la clave separadora y concatenar
            separadora = padre.keys.pop(indice)
            left.keys.append(Casilla(separadora.key))
            left.keys.extend(right.keys)
            left.hijos.extend(right.hijos)
            left.cantidad = len(left.keys)

        # eliminar el hijo derecho del padre
        padre.hijos.pop(indice + 1)
        padre.cantidad = len(padre.keys)

        # persistir cambios
        self.guardarNodo(left)
        self.guardarNodo(padre)

        # eliminar versión cacheada del nodo derecho si existe
        if right.node_id in self.nodes_map:
            del self.nodes_map[right.node_id]

    
    # REGISTROS
    def guardarRegistro(self, record):
        try:
            with open(self.data_file, 'ab') as f:
                # id
                f.write(struct.pack('i', int(record.restaurant_id)))
                # escribir 6 strings (tamaño + bytes)
                campos = [
                    record.restaurant_name,
                    record.city,
                    record.cuisines,
                    record.currency,
                    record.rating_color,
                    record.rating_text
                ]
                for campo in campos:
                    texto = str(campo) if campo is not None else ""
                    bytes_texto = texto.encode('utf-8')
                    f.write(struct.pack('i', len(bytes_texto)))
                    f.write(bytes_texto)
                # campos numéricos fijos
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


# Esta funcion lee secuencialmente el archivo de datos buscando el restaurant_id.
    def cargarRegistro(self, restaurant_id):
        try:
            with open(self.data_file, 'rb') as f:
                while True:
                    data = f.read(4)
                    if not data:
                        break
                    id_leido = struct.unpack('i', data)[0]
                    if id_leido == restaurant_id:
                        # helper para leer un string (length + bytes)
                        def leer_string():
                            length_bytes = f.read(4)
                            if not length_bytes:
                                return ""
                            length = struct.unpack('i', length_bytes)[0]
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
                        # saltar las 6 strings y los 28 bytes numéricos
                        for i in range(6):
                            len_bytes = f.read(4)
                            if not len_bytes:
                                return None
                            l = struct.unpack('i', len_bytes)[0]
                            if l > 0:
                                f.seek(l, 1)
                        # saltar 28 bytes: int + float + float + int + int + float + int = 4+4+4+4+4+4+4 = 28
                        f.seek(28, 1)
            return None
        except Exception as e:
            print(f"Error cargando registro {restaurant_id}: {e}")
            return None
            
    # Estadísticas

    def getDiskStats(self):
        return {
            "disk_reads": self.disk_reads,
            "disk_writes": self.disk_writes,
            "total_operations": self.disk_reads + self.disk_writes
        }
