from lark import Transformer, Lark
from sequential import SequentialFileManager
from bplus_tree import BPlusTree


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
    column_def: NAME type ["INDEX" NAME]

    select_stmt: "SELECT" "*" "FROM" NAME                        -> select_all
               | "SELECT" "*" "FROM" NAME "WHERE" NAME "=" value -> select_eq
               | "SELECT" "*" "FROM" NAME "WHERE" NAME "BETWEEN" value "AND" value -> select_between

    insert_stmt: "INSERT" "INTO" NAME "VALUES" "(" value ("," value)* ")"
    delete_stmt: "DELETE" "FROM" NAME "WHERE" NAME "=" value

    type: base_type
        | "ARRAY" "[" base_type "]"

    base_type: INT | TEXT | FLOAT | DATE
    value: ESCAPED_STRING | SIGNED_NUMBER | NAME

    INT: "INT"
    TEXT: "TEXT"
    FLOAT: "FLOAT"
    DATE: "DATE"
    AND: "AND"

    NAME: /[a-zA-Z_][a-zA-Z0-9_]*/

    %import common.ESCAPED_STRING
    %import common.SIGNED_NUMBER
    %import common.WS
    %ignore WS
"""

bplus_tree = BPlusTree(t=3)
sequential_file = SequentialFileManager()

for producto in sequential_file._read_all("productos_secuencial.bin"):
    bplus_tree.add(producto.price, producto.id)


class SQLTransformer(Transformer):
    def create_stmt(self, items):
        table_name = str(items[0])
        columns = items[1:]
        return {"action": "create", "table": table_name, "columns": columns}

    def column_def(self, items):
        name = str(items[0])
        dtype = items[1]
        index = str(items[2]) if len(items) > 2 else None
        return {"name": name, "type": dtype, "index": index}

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

    def type(self, items):
        return items[0] if len(items) == 1 else f"ARRAY[{items[1]}]"

    def base_type(self, items):
        return items[0]

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

    def SIGNED_NUMBER(self, tok):
        s = str(tok)
        return int(s) if s.isdigit() else float(s)



# bplus_tree = BPlusTree(t=3)


def execute_query(parsed, images_table):
    action = parsed["action"]

    if action == "select":
        rows = images_table
        if "where" not in parsed:
            return sequential_file._read_all("productos_secuencial.bin")[:100]

        cond = parsed["where"]
        col = cond["column"]

        if cond["operator"] == "=":
            if col == "price":
                return bplus_tree.search(float(cond["value"]))
            else:
                return [
                    r
                    for r in sequential_file._read_all("productos_secuencial.bin")
                    if getattr(r, col) == cond["value"]
                ]

        elif cond["operator"] == "BETWEEN":
            if col == "price":
                return bplus_tree.range_search(float(cond["from"]), float(cond["to"]))
            else:
                return [
                    r
                    for r in sequential_file._read_all("productos_secuencial.bin")
                    if cond["from"] <= getattr(r, col) <= cond["to"]
                ]

    elif action == "insert":
        values = parsed["values"]
        keys = list(images_table[0].keys()) if images_table else []
        new_row = dict(zip(keys, values))
        images_table.append(new_row)
        return f"Inserted: {new_row}"

    elif action == "delete":
        col = parsed["where"]["column"]
        val = parsed["where"]["value"]
        before = len(images_table)
        images_table[:] = [r for r in images_table if str(r.get(col)) != str(val)]
        after = len(images_table)
        return f"Deleted {before - after} row(s)"


if __name__ == "__main__":
    parser = Lark(sql_grammar, parser="lalr", start="start")
    transformer = SQLTransformer()

    test_queries = [
        "SELECT * FROM productos WHERE price BETWEEN 100.00 AND 200.00",
    ]

    import pprint

    for query in test_queries:
        print(f"\nQuery:\n{query.strip()}")
        tree = parser.parse(query)
        parsed = transformer.transform(tree)

        # Desempaquetar el árbol hasta llegar al dict real
        if hasattr(parsed, "children") and parsed.children:
            parsed = parsed.children[0]
            if hasattr(parsed, "children") and parsed.children:
                parsed = parsed.children[0]
        print(f"Parsed:\n{parsed}")
        result = execute_query(parsed, images_table)
        for r in result[0:10]:
            print(r)
            print()

    print("\nFinal state of images_table:")
    # pprint.pprint(images_table)
