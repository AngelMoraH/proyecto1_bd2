import struct
import os
import csv
from typing import List
import time

RECORD_FORMAT = "32s50s30sf100s200s"
RECORD_SIZE = struct.calcsize(RECORD_FORMAT)
DATA_FILE = "productos_secuencial.bin"
AUX_FILE = "auxiliar.bin"
CSV_FILE = "/Users/obed/Desktop/proyecto1_bd2/backend/productos_amazon.csv"
K = 5


class Producto:
    def __init__(self, id, name, category, price, image, description, eliminado=False):
        self.id = id
        self.name = name[:50]
        self.category = category[:30]
        self.price = price
        self.image = image[:100]
        self.description = description[:200]
        self.eliminado = eliminado


    def to_bytes(self):
        return struct.pack(
            RECORD_FORMAT,
            self.id.encode("utf-8")[:32].ljust(32, b" "),
            self.name.encode("utf-8").ljust(50, b" "),
            self.category.encode("utf-8").ljust(30, b" "),
            self.price,
            self.image.encode("utf-8").ljust(100, b" "),
            self.description.encode("utf-8").ljust(200, b" "),
        )

    @staticmethod
    def from_bytes(data):
        id, name, category, price, image, description = struct.unpack(
            RECORD_FORMAT, data
        )
        id_str = id.decode("utf-8", errors="replace").strip()
        eliminado = id_str.startswith("__")
        return Producto(
            id_str,
            name.decode("utf-8", errors="replace").strip(),
            category.decode("utf-8", errors="replace").strip(),
            price,
            image.decode("utf-8", errors="replace").strip(),
            description.decode("utf-8", errors="replace").strip(),
            eliminado,
        )

    def __str__(self):
        return f"{'[X]' if self.eliminado else ''} ID: {self.id}, Producto: {self.name}, Category: {self.category}, Precio: {self.price}, Image: {self.image}, Description: {self.description}"

class SequentialFileManager:
    def __init__(self):
        for file in [DATA_FILE, AUX_FILE]:
            if not os.path.exists(file):
                open(file, "wb").close()

    def _read_all(self, filename):
        with open(filename, "rb") as f:
            return [
                Producto.from_bytes(f.read(RECORD_SIZE))
                for _ in range(os.path.getsize(filename) // RECORD_SIZE)
            ]

    def insert(self, producto: Producto):
        if self.search(producto.id):
            print(f"[WARN] Ya existe un registro con ID {producto.id}")
            return
        with open(AUX_FILE, "ab") as aux:
            aux.write(producto.to_bytes())

        if os.path.getsize(AUX_FILE) // RECORD_SIZE >= K:
            self.reorganize()

    def reorganize(self):
        registros = [v for v in self._read_all(DATA_FILE) if not v.eliminado]
        registros += [v for v in self._read_all(AUX_FILE)]
        registros.sort(key=lambda v: v.id)

        with open(DATA_FILE, "wb") as f:
            for v in registros:
                f.write(v.to_bytes())

        open(AUX_FILE, "wb").close()

    def search(self, id):
        for file in [DATA_FILE, AUX_FILE]:
            with open(file, "rb") as f:
                while True:
                    chunk = f.read(RECORD_SIZE)
                    if len(chunk) < RECORD_SIZE:
                        break
                    producto = Producto.from_bytes(chunk)
                    if not producto.eliminado and producto.id == id:
                        return producto
        return None

    def delete(self, id):
        for file in [DATA_FILE, AUX_FILE]:
            productos = self._read_all(file)
            modified = False
            for i, producto in enumerate(productos):
                if not producto.eliminado and producto.id == id:
                    productos[i].eliminado = True
                    modified = True
                    break
            if modified:
                with open(file, "wb") as f:
                    for v in productos:
                        f.write(v.to_bytes())
                return True
        return False

    def range_search(self, id_inicio, id_fin) -> List[Producto]:
        result = []
        for file in [DATA_FILE, AUX_FILE]:
            with open(file, "rb") as f:
                while chunk := f.read(RECORD_SIZE):
                    producto = Producto.from_bytes(chunk)
                    if (
                        not producto.eliminado
                        and id_inicio <= producto.id <= id_fin
                    ):
                        result.append(producto)
        return sorted(result, key=lambda v: v.id)


def limpiar_precio(valor):
        if not valor:
            return 0.0
        valor = valor.replace("$", "").strip()
        try:
            return float(valor.split()[0])
        except ValueError:
            return 0.0

def cargar_productos_csv(ruta_csv, manager):
    with open(ruta_csv, newline="", encoding="utf-8") as archivo:
        lector = csv.DictReader(archivo)
        for fila in lector:
            producto = Producto(
                fila["id"],
                fila["name"],
                fila["category"],
                limpiar_precio(fila["price"]),
                fila["image"],
                fila["description"],
            )
            manager.insert(producto)


def demo_operaciones_secuencial():
    manager = SequentialFileManager()

    # Inserción desde CSV
    print("\n📥 Cargando productos desde CSV...")
    start = time.time()
    cargar_productos_csv(CSV_FILE, manager)
    print(f"✅ Inserción completada en {time.time() - start:.6f} segundos")

    # Búsqueda individual
    print("\n🔍 Buscar producto con ID = 18018b6bc416dab347b1b7db79994afa")
    start = time.time()
    result = manager.search("18018b6bc416dab347b1b7db79994afa")
    print(f"Resultado: {result if result else 'No encontrada'}")
    print(f"⏱️ Tiempo búsqueda: {time.time() - start:.6f} segundos")

    # Eliminación
    """print("\n🗑️ Eliminando producto con ID = 5")
    start = time.time()
    eliminado = manager.delete("4c69b61db1fc16e7013b43fc926e502d")
    print("Resultado:", "Eliminada" if eliminado else "No encontrada")
    print(f"⏱️ Tiempo eliminación: {time.time() - start:.6f} segundos")"""

    # Búsqueda nuevamente
    """print("\n🔍 Buscar nuevamente ID = 5")
    start = time.time()
    result = manager.search("4c69b61db1fc16e7013b43fc926e502d")
    print(f"Resultado: {result if result else 'No encontrada'}")
    print(f"⏱️ Tiempo búsqueda posterior: {time.time() - start:.6f} segundos")"""

    # Búsqueda por rango
    """print("\n📊 Búsqueda por rango ID 3 a 8")
    start = time.time()
    resultados = manager.range_search(3, 8)
    for v in resultados:
        print(v)
    print(f"⏱️ Tiempo búsqueda por rango: {time.time() - start:.6f} segundos")"""


if __name__ == "__main__":
    demo_operaciones_secuencial()
