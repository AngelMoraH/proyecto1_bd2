import os
import json
from algoritmos.bplus_tree import BPlusTree
from algoritmos.sequential import SequentialFileManager, build_producto_class
from algoritmos.rtree_in import RTreeIndex, build_city_class

tables_dir = "tables"
os.makedirs(tables_dir, exist_ok=True)

global_tables = {}


def load_all_tables():
    for file in os.listdir(tables_dir):
        if file.endswith(".meta.json"):
            table_name = file.replace(".meta.json", "")
            meta_path = os.path.join(tables_dir, file)
            with open(meta_path, "r") as f:
                    meta = json.load(f)
            fields = meta["columns"]
            index_info = meta.get("index")
            if index_info and index_info.get("type") == "rtree":#diferencia de índices
                table_name = file.replace(".meta.json", "")
                meta_path = os.path.join(tables_dir, file)
                with open(meta_path, "r") as f:
                    meta = json.load(f)
                fields = meta["columns"]
                record_format = meta["record_format"]
                record_size = int(meta["record_size"])
                City = build_city_class(fields, record_format)
                manager = RTreeIndex.get_or_create(
                    table_name, record_format, record_size, City
                )
                global_tables[table_name] = {
                    "manager": manager,
                    "city_class": City,
                    "record_format": record_format,
                    "record_size": record_size,
                    "index": meta.get("index"),
                    "table_name": table_name,
                }

                index_info = meta.get("index")
                if index_info and index_info.get("type") == "rtree":
                    col = index_info["x_column"]
                    index_path = os.path.join(
                        tables_dir, f"index_rtree_{table_name}_{col}.dat"
                    )
                    if os.path.exists(index_path):
                        r_tree = RTreeIndex.load_from_file(index_path)
                        global_tables[table_name]["r_tree"] = r_tree
#SELECT * FROM ciudades WHERE KNN ( ( 1 , 1  ) , 3 )
            else:
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

                index_info = meta.get("index")
                if index_info and index_info.get("type") == "bplustree":
                    col = index_info["column"]
                    index_path = os.path.join(
                        tables_dir, f"index_bplustree_{table_name}_{col}.dat"
                    )
                    if os.path.exists(index_path):
                        bplus_tree = BPlusTree.load_from_file(index_path)
                        global_tables[table_name]["bplus_tree"] = bplus_tree


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