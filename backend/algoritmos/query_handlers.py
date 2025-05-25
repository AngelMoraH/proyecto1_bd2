import json
from algoritmos.table_manager import global_tables
from algoritmos.bplustree_manager import bplus_tree

def _handle_delete(parsed, table):
    if table not in global_tables:
        return {"status": 400, "message": f"Tabla '{table}' no encontrada."}
    col = parsed["where"]["column"]
    val = parsed["where"]["value"]
    manager = global_tables[table]["manager"]
    index_info = global_tables[table].get("index")
    if col != "id":
        return {"status": 400, "message": f"El índice solo se puede eliminar por id, no por {col}"}
    producto = manager.search(val)
    if not producto:
        return {"status": 404, "message": f"Producto con id = {val} no encontrado en '{table}'"}
    eliminado = manager.delete(val)
    if not eliminado:
        return {"status": 400, "message": f"No se pudo eliminar el producto con id = {val}"}
    if index_info and index_info["type"] == "bplustree":
        col_index = index_info["column"]
        key = getattr(producto, col_index)
        bplus_tree.remove(key)
        bplus_tree.save_to_file("tables/bplustree_precio.dat")
    return {"status": 200, "message": f"Producto con id = {val} eliminado de '{table}'"}

def _handle_insert(parsed, table):
    if table not in global_tables:
        return {"status": 400, "message": f"Tabla '{table}' no encontrada."}
    manager = global_tables[table]["manager"]
    Producto = global_tables[table]["producto_class"]
    index_info = global_tables[table].get("index")
    producto = Producto(*parsed["values"])
    response = manager.insert(producto)
    if response["status"] == 400:
        return json.dumps(response)
    if index_info and index_info["type"] == "bplustree":
        col = index_info["column"]
        key = getattr(producto, col)
        bplus_tree.add(key, producto.id)
        bplus_tree.save_to_file("tables/bplustree_precio.dat")
    return {"status": 200, "message": f"Producto con id = {producto.id} insertado en '{table}'"}

def _handle_select(parsed, table):
    if "where" not in parsed and table in global_tables:
        manager = global_tables[table]["manager"]
        return manager._read_all(manager.data_file)
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
                (r, col)
                for r in manager._read_all("productos_secuencial.bin")
                if cond["from"] <= getattr(r, col) <= cond["to"]
            ]
        )