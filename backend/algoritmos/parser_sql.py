from lark import Transformer, Lark
import struct
import json
import os
import csv
import pathlib
import time

from algoritmos.table_manager import (
    limpiar_precio,
    map_type_to_format,
    global_tables,
    tables_dir,
    load_all_tables,
)
from algoritmos.bplus_tree import BPlusTree
from algoritmos.sequential import SequentialFileManager, build_producto_class
from algoritmos.query_handlers import _handle_delete, _handle_insert, _handle_select


load_all_tables()

bplus_tree = BPlusTree(t=3)


class SQLTransformer(Transformer):
    def create_stmt(self, items):
        if len(items) == 3:
            # CREATE TABLE <name> FROM FILE <path> USING INDEX <index_info>
            table_name = str(items[0])
            file_path = items[1]  # eliminar comillas
            index_info = items[2]

            with open(file_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                if not rows:
                    raise ValueError("CSV vacío.")

                sample = rows[0]
                fields = []
                record_format = ""

                for key, value in sample.items():
                    if key.lower() == "id":
                        tipo = "VARCHAR[32]"
                    elif key.lower() == "price":
                        tipo = "FLOAT"
                    elif key.lower() == "date" or "fecha" in key.lower():
                        tipo = "DATE"
                    elif len(value) > 100:
                        tipo = "VARCHAR[200]"
                    elif len(value) > 50:
                        tipo = "VARCHAR[100]"
                    elif len(value) > 30:
                        tipo = "VARCHAR[50]"
                    else:
                        tipo = "VARCHAR[30]"
                    fields.append({"name": key, "type": tipo})
                    record_format += map_type_to_format(tipo)
                record_format += "?"

            # 2. Calcular RECORD_SIZE
            record_size = struct.calcsize(record_format)

            # Guardar metadata
            os.makedirs("tables", exist_ok=True)
            meta_path = os.path.join("tables", f"{table_name}.meta.json")
            with open(meta_path, "w") as f:
                json.dump(
                    {
                        "table": table_name,
                        "columns": fields,
                        "index": index_info,
                        "record_format": record_format,
                        "record_size": record_size,
                    },
                    f,
                    indent=2,
                )

            # 3. Crear archivos
            table_bin = os.path.join(tables_dir, f"{table_name}.bin")
            # meta_path = os.path.join(tables_dir, f"{table_name}.meta.json")

            open(table_bin, "wb").close()

            Producto = build_producto_class(fields, record_format)

            manager = SequentialFileManager.get_or_create(
                table_name, record_format, record_size, Producto
            )

            # 5. Insertar al archivo binario
            for fila in rows:
                producto = Producto(
                    fila.get("id", ""),
                    fila.get("name", "")[:50],
                    fila.get("category", "")[:30],
                    limpiar_precio(fila.get("price", "")),
                    fila.get("image", "")[:100],
                    fila.get("description", "")[:200],
                )
                manager.insert(producto)

            global_tables[table_name] = {
                "manager": manager,
                "producto_class": Producto,
                "record_format": record_format,
                "record_size": record_size,
                "bplus_tree": None,
            }
            aux_file = os.path.join(tables_dir, f"{table_name}_aux.bin")
            print(aux_file)
            for producto in manager._read_all(table_bin, aux_file):
                bplus_tree.add(getattr(producto, index_info["column"]), producto.id)

            index_filename = (
                f"tables/index_bplustree_{table_name}_{index_info["column"]}.dat"
            )
            bplus_tree.save_to_file(index_filename)
            global_tables[table_name]["bplus_tree"] = bplus_tree

            return {
                "action": "create_from_file",
                "table": table_name,
                "file": file_path,
                "index": index_info,
                "record_format": record_format,
                "record_size": record_size,
                "bplus_tree": None,
            }

        else:
            table_name = str(items[0])
            columns = items[1:]

            fields = []
            record_format = ""
            for col in columns:
                fields.append({"name": col["name"], "type": col["type"]})
                record_format += map_type_to_format(col["type"])
            record_format += "?"

            record_size = struct.calcsize(record_format)
            table_path = os.path.join(tables_dir, f"{table_name}.bin")
            meta_path = os.path.join(tables_dir, f"{table_name}.meta.json")

            with open(table_path, "wb") as f:
                pass

            with open(meta_path, "w") as f:
                json.dump(
                    {
                        "table": table_name,
                        "columns": fields,
                        "record_format": record_format,
                        "record_size": record_size,
                    },
                    f,
                    indent=2,
                )

            return {
                "action": "create",
                "table": table_name,
                "columns": fields,
                "record_format": record_format,
                "record_size": record_size,
            }

    def column_def(self, items):
        name = str(items[0])
        dtype = items[1]
        return {"name": name, "type": dtype}

    def select_all(self, items):
        return {"action": "select", "table": str(items[0])}

    def select_eq(self, items):
        return {
            "action": "select",
            "table": str(items[0]),
            "where": {"column": str(items[1]), "operator": "=", "value": items[2]},
        }

    def select_between(self, items):
        return {
            "action": "select",
            "table": str(items[0]),
            "where": {
                "column": str(items[1]),
                "operator": "BETWEEN",
                "from": items[2],
                "to": items[3],
            },
        }

    def insert_stmt(self, items):
        table = str(items[0])
        values = items[1:]
        return {"action": "insert", "table": table, "values": values}

    def delete_stmt(self, items):
        return {
            "action": "delete",
            "table": str(items[0]),
            "where": {"column": str(items[1]), "value": items[2]},
        }

    def index_bplustree(self, items):
        col = items[0]
        if isinstance(col, str) and col.startswith('"') and col.endswith('"'):
            col = col[1:-1]
        return {"type": "bplustree", "column": str(col)}

    def index_isam(self, items):
        return {"type": "isam", "column": str(items[0])}

    def base_type(self, items):
        return str(items[0])

    def type(self, items):
        return items[0] if len(items) == 1 else f"ARRAY[{items[1]}]"

    def value(self, items):
        return items[0]

    def INT(self, tok):
        return "INT"

    def TEXT(self, tok):
        return "TEXT"

    def FLOAT(self, tok):
        return "FLOAT"

    def DATE(self, tok):
        return "DATE"

    def NAME(self, tok):
        return str(tok)

    def ESCAPED_STRING(self, tok):
        return tok[1:-1]

    def varchar_type(self, items):
        return f"VARCHAR[{items[0]}]"

    def INT_VALUE(self, tok):
        return int(tok)

    def SIGNED_NUMBER(self, tok):
        s = str(tok)
        return int(s) if s.isdigit() else float(s)


sql_grammar = pathlib.Path(
    os.path.join(os.path.dirname(__file__), "sql_grammar.lark")
).read_text()


def execute_query(parsed):
    action = parsed["action"]
    table = parsed["table"]
    if table not in global_tables:
        return {
            "status": 400,
            "message": f"Tabla '{table}' no encontrada.",
        }
    print(f"Ejecutando acción: {action} en la tabla: {table}")
    if action == "delete":
        return _handle_delete(parsed, table)
    elif action == "insert":
        return _handle_insert(parsed, table)
    elif action == "select":
        return _handle_select(parsed, table)
    elif action in ["create", "create_from_file"]:
        return {
            "status": 200,
            "message": f"Tabla '{table}' creada con éxito.",
        }


def timed_execute_query(parsed):
    start_time = time.time()
    result = execute_query(parsed)
    end_time = time.time()
    elapsed = end_time - start_time
    return {"result": result, "execution_time_seconds": round(elapsed, 6)}
