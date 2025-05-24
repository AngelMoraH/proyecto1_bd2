from lark import Transformer, Lark
from sequential import SequentialFileManager
from bplus_tree import BPlusTree
import os
import struct
import json
import csv

tables_dir = "tables"
os.makedirs(tables_dir, exist_ok=True)

global_tables = {}


def load_all_tables():
    for file in os.listdir("tables"):
        if file.endswith(".meta.json"):
            table_name = file.replace(".meta.json", "")
            meta_path = os.path.join("tables", file)

            with open(meta_path, "r") as f:
                meta = json.load(f)

            fields = meta["columns"]
            record_format = meta["record_format"]
            record_size = int(meta["record_size"])

            def build_producto_class(fields, record_format):
                class Producto:
                    def __init__(self, *args):
                        for field, value in zip(fields, args):
                            setattr(self, field["name"], value)
                        self.eliminado = False

                    def to_bytes(self):
                        valores = []
                        for field in fields:
                            val = getattr(self, field["name"])
                            tipo = field["type"]
                            if tipo.startswith("VARCHAR"):
                                size = int(tipo[8:-1])
                                val = val.encode("utf-8")[:size].ljust(size, b" ")
                                valores.append(val)
                            elif tipo == "FLOAT":
                                valores.append(float(val))
                            elif tipo == "INT":
                                valores.append(int(val))
                            elif tipo == "DATE":
                                val = str(val)[:10].encode("utf-8").ljust(10, b" ")
                                valores.append(val)
                        return struct.pack(record_format, *valores)

                    @staticmethod
                    def from_bytes(data):
                        unpacked = struct.unpack(record_format, data)
                        parsed = []
                        for val, field in zip(unpacked, fields):
                            tipo = field["type"]
                            if tipo.startswith("VARCHAR") or tipo == "DATE":
                                parsed.append(
                                    val.decode("utf-8", errors="replace").strip()
                                )
                            elif tipo == "FLOAT":
                                parsed.append(float(val))
                            elif tipo == "INT":
                                parsed.append(int(val))
                        return Producto(*parsed)

                    def __str__(self):
                        return (
                            f"[{'X' if self.eliminado else ' '}] Producto("
                            + ", ".join(
                                f"{f['name']}={getattr(self, f['name'])}"
                                for f in fields
                            )
                            + ")"
                        )

                return Producto

            Producto = build_producto_class(fields, record_format)

            manager = SequentialFileManager.get_or_create(
                table_name,
                record_format,
                record_size,
                Producto,
            )

            global_tables[table_name] = {
                "manager": manager,
                "producto_class": Producto,
                "record_format": record_format,
                "record_size": record_size,
                "index": meta.get("index"),
            }


load_all_tables()

print("Tablas cargadas:")
for table_name, table_info in global_tables.items():
    print(
        f" - {table_name}: {table_info['record_format']} ({table_info['record_size']} bytes)"
    )
    print(f"   Index: {table_info['index']}")
    print(f"   Producto Class: {table_info['producto_class'].__name__}")
    print(f"   Manager: {table_info['manager'].__class__.__name__}")
    print()
    #     cargar_productos_csv("productos_amazon.csv", manager)


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


bplus_tree = BPlusTree(t=3)
"""sequential_file = SequentialFileManager()

for producto in sequential_file._read_all("productos_secuencial.bin"):
    bplus_tree.add(producto.price, producto.id)
"""


class SQLTransformer(Transformer):
    def create_stmt(self, items):
        print(f"items: {items}", len(items))
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

            # 4. Crear clase Producto dinámica
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
                        return struct.pack(record_format, *valores)

                    @staticmethod
                    def from_bytes(data):
                        unpacked = struct.unpack(record_format, data)
                        parsed = []
                        for val, field in zip(unpacked, fields):
                            tipo = field["type"]
                            if tipo.startswith("VARCHAR") or tipo == "DATE":
                                parsed.append(
                                    val.decode("utf-8", errors="replace").strip()
                                )
                            elif tipo == "FLOAT":
                                parsed.append(float(val))
                            elif tipo == "INT":
                                parsed.append(int(val))
                        return Producto(*parsed)

                    def __str__(self):
                        values = [
                            f"{f['name']}={getattr(self, f['name'])}" for f in fields
                        ]
                        return (
                            f"[{'X' if self.eliminado else ' '}] Producto("
                            + ", ".join(values)
                            + ")"
                        )

                return Producto

            Producto = build_producto_class(fields, record_format)

            """manager = SequentialFileManager(
                record_format=record_format,
                record_size=record_size,
                data_file=os.path.join("tables", f"{table_name}.bin"),
                aux_file=os.path.join("tables", f"{table_name}_aux.bin"),
                ProductoClass=Producto,
            )"""

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
            }

            return {
                "action": "create_from_file",
                "table": table_name,
                "file": file_path,
                "index": index_info,
                "record_format": record_format,
                "record_size": record_size,
            }

        else:
            table_name = str(items[0])
            columns = items[1:]

            fields = []
            record_format = ""
            for col in columns:
                fields.append({"name": col["name"], "type": col["type"]})
                record_format += map_type_to_format(col["type"])

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


images_table = [
    {"id": 1, "name": "dog", "features": [0.2, 0.3]},
    {"id": 2, "name": "cat", "features": [0.9, 0.5]},
    {"id": 5, "name": "bird", "features": [0.1, 0.2]},
    {"id": 7, "name": "fish", "features": [0.4, 0.4]},
]


sql_grammar = r"""
    start: statement

    statement: create_stmt
             | select_stmt
             | insert_stmt
             | delete_stmt

    create_stmt: "CREATE" "TABLE" NAME "(" column_def ("," column_def)* ")"
                | "CREATE" "TABLE" NAME "FROM" "FILE" ESCAPED_STRING "USING" "INDEX" index_stmt
    
    column_def: NAME type ["INDEX" NAME]

    index_stmt: "bplustree" "(" NAME ")"      -> index_bplustree
                | "isam" "(" NAME ")"           -> index_isam

    select_stmt: "SELECT" "*" "FROM" NAME                        -> select_all
               | "SELECT" "*" "FROM" NAME "WHERE" NAME "=" value -> select_eq
               | "SELECT" "*" "FROM" NAME "WHERE" NAME "BETWEEN" value "AND" value -> select_between

    insert_stmt: "INSERT" "INTO" NAME "VALUES" "(" value ("," value)* ")"
    delete_stmt: "DELETE" "FROM" NAME "WHERE" NAME "=" value

    type: base_type
        | "ARRAY" "[" base_type "]"

    base_type: INT | TEXT | FLOAT | DATE | varchar_type
    value: ESCAPED_STRING | SIGNED_NUMBER | NAME

    INT: "INT"
    TEXT: "TEXT"
    FLOAT: "FLOAT"
    DATE: "DATE"
    AND: "AND"
    varchar_type: "VARCHAR" "[" INT_VALUE "]"
    INT_VALUE: /[0-9]+/

    NAME: /[a-zA-Z_][a-zA-Z0-9_]*/

    %import common.ESCAPED_STRING
    %import common.SIGNED_NUMBER
    %import common.WS
    %ignore WS
"""


# bplus_tree = BPlusTree(t=3)


def execute_query(parsed, images_table):
    action = parsed["action"]
    table = parsed["table"]
    if action == "delete":
        return _extracted_from_execute_query_47(parsed, images_table)
    elif action == "insert":
        values = parsed["values"]
        keys = list(images_table[0].keys()) if images_table else []
        new_row = dict(zip(keys, values))
        images_table.append(new_row)
        return f"Inserted: {new_row}"

    elif action == "select":
        if "where" not in parsed:
            if table in global_tables:
                manager = global_tables[table]["manager"]
                return manager._read_all(manager.data_file)
            else:
                print(f"La tabla '{table}' no está registrada.")

        cond = parsed["where"]
        col = cond["column"]

        if cond["operator"] == "=":
            if col == "price":
                return bplus_tree.search(float(cond["value"]))

            if table in global_tables:
                manager = global_tables[table]["manager"]
            return [
                r
                for r in manager._read_all(manager.data_file)
                if str(getattr(r, col)).strip() == str(cond["value"]).strip()
            ]

        elif cond["operator"] == "BETWEEN":
            if table in global_tables:
                manager = global_tables[table]["manager"]
            return (
                bplus_tree.range_search(float(cond["from"]), float(cond["to"]))
                if col == "price"
                else [
                    r
                    for r in manager._read_all(
                        "productos_secuencial.bin"
                    )
                    if cond["from"] <= getattr(r, col) <= cond["to"]
                ]
            )


# TODO Rename this here and in `execute_query`
def _extracted_from_execute_query_47(parsed, images_table):
    col = parsed["where"]["column"]
    val = parsed["where"]["value"]
    before = len(images_table)
    images_table[:] = [r for r in images_table if str(r.get(col)) != str(val)]
    after = len(images_table)
    return f"Deleted {before - after} row(s)"


if __name__ == "__main__":
    from pprint import pprint

    parser = Lark(sql_grammar, parser="lalr", start="start")
    transformer = SQLTransformer()

    test_queries = [
        """
    CREATE TABLE Restaurantes (
        id INT,
        nombre VARCHAR[20],
        fecha DATE,
        precio FLOAT
    )
    """,
        """CREATE TABLE Productos FROM FILE "/Users/obed/Desktop/proyecto1_bd2/backend/productos_amazon.csv" USING INDEX bplustree(precio)""",
        "SELECT * FROM Productos","""SELECT * FROM Productos WHERE id = "4c69b61db1fc16e7013b43fc926e502d" """,
    ]
    # "SELECT * FROM productos WHERE price BETWEEN 100.00 AND 101.00",

    for query in test_queries:
        print(f"\nQuery:\n{query.strip()}")
        tree = parser.parse(query)
        parsed = transformer.transform(tree)

        # Desempaquetar el árbol hasta llegar al dict real
        if hasattr(parsed, "children") and parsed.children:
            parsed = parsed.children[0]
        if hasattr(parsed, "children") and parsed.children:
            parsed = parsed.children[0]

        print("Parsed:")
        pprint(parsed)
        result = execute_query(parsed, images_table)
        if result is None:
            print("No se obtuvo resultado.")
        else:
            for r in result:
                print(r)
                print()
