import os
import json
from algoritmos.sequential import SequentialFileManager, build_producto_class

tables_dir = "tables"
os.makedirs(tables_dir, exist_ok=True)

global_tables = {}

def load_all_tables():
    for file in os.listdir(tables_dir):
        print(f"file: {file}")
        if file.endswith(".meta.json"):
            table_name = file.replace(".meta.json", "")
            meta_path = os.path.join(tables_dir, file)
            with open(meta_path, "r") as f:
                meta = json.load(f)
            fields = meta["columns"]
            record_format = meta["record_format"]
            record_size = int(meta["record_size"])
            Producto = build_producto_class(fields, record_format)
            manager = SequentialFileManager.get_or_create(
                table_name, record_format, record_size, Producto
            )
            global_tables[table_name] = {
                "manager": manager,
                "producto_class": Producto,
                "record_format": record_format,
                "record_size": record_size,
                "index": meta.get("index"),
                "table_name": table_name,
            }

def limpiar_precio(valor):
    if not valor:
        return 0.0
    valor = valor.replace("$", "").strip()
    try:
        return float(valor.split()[0])
    except ValueError:
        return 0.0

def map_type_to_format(typename):
    if typename == "INT":
        return "i"
    elif typename == "FLOAT":
        return "f"
    elif typename == "DATE":
        return "10s"
    elif typename.startswith("VARCHAR["):
        n = int(typename[len("VARCHAR[") : -1])
        return f"{n}s"
    else:
        raise ValueError(f"Tipo no soportado: {typename}")