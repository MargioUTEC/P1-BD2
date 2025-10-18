class Casilla:
    def __init__(self, key, value=None):
        self.key = key
        self.value = value

class Node:
    def __init__(self, orden, es_hoja=False, node_id=None):
        self.keys = []
        self.hijos = []
        self.siguiente = None
        self.cantidad = 0
        self.es_hoja = es_hoja
        self.orden = orden
        self.node_id = node_id
    
    def esta_lleno(self):
        return self.cantidad >= self.orden
    
    def tiene_pocas_claves(self):
        min_claves = max(1, (self.orden // 2) - 1)
        return self.cantidad < min_claves
    
    def insertar_casilla(self, casilla):
        if not self.keys:
            self.keys.append(casilla)
            self.cantidad += 1
            return
        
        indice = 0
        while indice < len(self.keys) and self.keys[indice].key < casilla.key:
            indice += 1
        
        self.keys.insert(indice, casilla)
        self.cantidad += 1
    
    def eliminar_clave(self, clave):
        for i in range(len(self.keys)):
            if self.keys[i].key == clave:
                self.keys.pop(i)
                self.cantidad -= 1
                return True
        return False