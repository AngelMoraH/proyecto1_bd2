import struct
import os
import time


# RECORD FORMAT = "32s50s30sf100s200s"
# RECORD SIZE = struct.calcsize(RECORD FORMAT)
# DATA FILE = "productos_secuencial.bin"
# AUX FILE = "auxiliar.bin"
CSV_FILE =  "/Users/lvera/OneDrive/Escritorio/proyecto_bd/proyecto1_bd2-main (1)/proyecto1_bd2-main/backend/productos_amazon.csv"
K = 5


def build_producto_class(fields, record_format):
    class Producto:
        def __init__(self, *args):
            for field, value in zip(fields, args):
                name = field["name"]
                setattr(self, name, value)
            self.eliminado = False

        def to_bytes(self):
            valores = []
            for field in fields:
                val = getattr(self, field["name"])
                fmt = field["type"]
                if fmt.startswith("VARCHAR"):
                    size = int(fmt[8:-1])
                    val = val.encode("utf-8")[:size].ljust(size, b" ")
                    valores.append(val)
                elif fmt == "FLOAT":
                    valores.append(float(val))
                elif fmt == "INT":
                    valores.append(int(val))
                elif fmt == "DATE":
                    val = str(val)[:10].encode("utf-8").ljust(10, b" ")
                    valores.append(val)
            valores.append(self.eliminado)
            return struct.pack(record_format, *valores)

        @staticmethod
        def from_bytes(data):
            unpacked = struct.unpack(record_format, data)
            parsed = []
            for val, field in zip(unpacked[:-1], fields):
                tipo = field["type"]
                if tipo.startswith("VARCHAR") or tipo == "DATE":
                    parsed.append(val.decode("utf-8", errors="replace").strip())
                elif tipo == "FLOAT":
                    parsed.append(float(val))
                elif tipo == "INT":
                    parsed.append(int(val))
            eliminado = unpacked[-1]
            obj = Producto(*parsed)
            obj.eliminado = eliminado
            return obj

        def __str__(self):
            values = [f"{f['name']}={getattr(self, f['name'])}" for f in fields]
            return (
                f"[{'X' if self.eliminado else ' '}] Producto("
                + ", ".join(values)
                + ")"
            )

    return Producto


class SequentialFileManager:
    _instances = {}  # almacena una instancia por nombre de tabla

    def __init__(self, record_size, record_format, data_file, aux_file, ProductoClass):
        self.record_size = int(record_size)
        self.record_format = record_format
        self.ProductoClass = ProductoClass
        self.data_file = data_file
        self.aux_file = aux_file
        os.makedirs(os.path.dirname(self.data_file), exist_ok=True)

        for file in [self.data_file, self.aux_file]:
            if not os.path.exists(file):
                open(file, "wb").close()

    @classmethod
    def get_or_create(cls, table_name, record_format, record_size, ProductoClass):
        if table_name in cls._instances:
            return cls._instances[table_name]

        data_file = os.path.join("tables", f"{table_name}.bin")
        aux_file = os.path.join("tables", f"{table_name}_aux.bin")
        for file in [data_file, aux_file]:
            if not os.path.exists(file):
                open(file, "wb").close()

        instance = cls(record_size, record_format, data_file, aux_file, ProductoClass)
        cls._instances[table_name] = instance
        return instance

    def _read_all(self, filename, aux_filename=None):

        productos = []
        files_to_read = []
        if filename:
            files_to_read.append(filename)
        if aux_filename:
            files_to_read.append(aux_filename)

        for file in files_to_read:
            if not os.path.exists(file):
                continue
            with open(file, "rb") as f:
                while True:
                    chunk = f.read(self.record_size)
                    if len(chunk) < self.record_size:
                        break
                    producto = self.ProductoClass.from_bytes(chunk)
                    if not producto.eliminado:
                        productos.append(producto)
        return productos

    def insert(self, producto):
        if self.search(producto.id):
            return {
                "message": f"Ya existe un registro con ID {producto.id}",
                "status": 400,
            }
        with open(self.aux_file, "ab") as aux:
            aux.write(producto.to_bytes())

        if os.path.getsize(self.aux_file) // self.record_size >= K:
            self.reorganize()
        return {"message": "Registro insertado", "status": 200}

    def reorganize(self):
        registros = [v for v in self._read_all(self.data_file) if not v.eliminado]
        registros += list(self._read_all(self.aux_file))
        registros.sort(key=lambda v: v.id)

        with open(self.data_file, "wb") as f:
            for v in registros:
                f.write(v.to_bytes())

        open(self.aux_file, "wb").close()

    def search(self, id):
        for file in [self.data_file, self.aux_file]:
            with open(file, "rb") as f:
                while True:
                    chunk = f.read(self.record_size)
                    if len(chunk) < self.record_size:
                        break
                    producto = self.ProductoClass.from_bytes(chunk)
                    if not producto.eliminado and producto.id == id:
                        return producto
        return None

    def delete(self, id):
        for file in [self.data_file, self.aux_file]:
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


    def range_search(self, id_inicio, id_fin):
        result = []
        for file in [self.data_file, self.aux_file]:
            with open(file, "rb") as f:
                while chunk := f.read(self.record_size):
                    producto = self.ProductoClass.from_bytes(chunk)
                    if not producto.eliminado and id_inicio <= producto.id <= id_fin:
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


"""def cargar_productos_csv(ruta_csv, manager):
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
            manager.insert(producto)"""


def demo_operaciones_secuencial():
    manager = SequentialFileManager()

    # Inserción desde CSV
    print("\n📥 Cargando productos desde CSV...")
    start = time.time()
    # cargar_productos_csv(CSV_FILE, manager)
    print(f"✅ Inserción completada en {time.time() - start:.6f} segundos")

    # Búsqueda individual
    print("\n🔍 Buscar producto con ID = 18018b6bc416dab347b1b7db79994afa")
    start = time.time()
    result = manager.search("18018b6bc416dab347b1b7db79994afa")
    print(f"Resultado: {result or 'No encontrada'}")
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


"""if __name__ == "__main__":
    demo_operaciones_secuencial()
"""
