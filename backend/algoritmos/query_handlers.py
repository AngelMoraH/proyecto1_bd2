import json
from algoritmos.table_manager import global_tables


def _handle_delete(parsed, table):
    """Maneja operaciones DELETE para ambos tipos de tabla (sequential y rtree)"""
    if table not in global_tables:
        return {"status": 400, "message": f"Tabla '{table}' no encontrada."}

    table_info = global_tables[table]
    manager = table_info["manager"]
    table_type = table_info["index"]["type"]

    col = parsed["where"]["column"]
    val = parsed["where"]["value"]

    # Para tablas sequential, verificar que sea por id
    if table_type != "rtree" and col != "id":
        return {
            "status": 400,
            "message": f"Las tablas sequential solo se pueden eliminar por id, no por {col}",
        }

    if table_type == "rtree":
        if col == "id":
            return {
                "status": 400,
                "message": f"Para tablas rtree, use la clave compuesta (name_country) en lugar de id",
            }
        elif col == "key":
            # Usar la clave directamente
            key = val
        else:
            return {
                "status": 400,
                "message": f"Para tablas rtree, elimine por 'key' (name_country)",
            }
    else:
        key = val

    # Buscar el registro
    record = manager.search(key)
    if not record:
        entity_name = "Ciudad" if table_type == "rtree" else "Producto"
        return {
            "status": 404,
            "message": f"{entity_name} con clave = {key} no encontrado en '{table}'",
        }

    # Eliminar el registro
    eliminado = manager.delete(key)
    if not eliminado:
        return {
            "status": 400,
            "message": f"No se pudo eliminar el registro con clave = {key}",
        }

    # Actualizar B+ tree si existe (solo para tablas sequential)
    if table_type != "rtree":
        bplus_tree = table_info.get("bplus_tree")
        index_info = table_info.get("index")

        if index_info and index_info["type"] == "bplustree" and bplus_tree:
            col_index = index_info["column"]
            index_key = getattr(record, col_index)
            bplus_tree.remove(index_key)
            # Usar el nombre de la tabla en el archivo del índice
            index_filename = f"tables/index_bplustree_{table}_{col_index}.dat"
            bplus_tree.save_to_file(index_filename)

    entity_name = "Ciudad" if table_type == "rtree" else "Producto"
    return {
        "status": 200,
        "message": f"{entity_name} con clave = {key} eliminado de '{table}'",
    }


def _handle_insert(parsed, table):
    """Maneja operaciones INSERT para ambos tipos de tabla"""
    if table not in global_tables:
        return {"status": 400, "message": f"Tabla '{table}' no encontrada."}

    table_info = global_tables[table]
    manager = table_info["manager"]
    RecordClass = table_info["class"]  # Usar "class" en lugar de "producto_class"
    table_type = table_info["index"]["type"]

    # Crear el objeto registro
    record = RecordClass(*parsed["values"])

    # Insertar el registro
    response = manager.insert(record)
    if response["status"] != 200:
        return response

    # Actualizar B+ tree si existe (solo para tablas sequential)
    if table_type != "rtree":
        bplus_tree = table_info.get("bplus_tree")
        index_info = table_info.get("index")

        if index_info and index_info["type"] == "bplustree" and bplus_tree:
            col = index_info["column"]
            index_key = getattr(record, col)
            record_id = getattr(record, "id") if hasattr(record, "id") else record.key
            bplus_tree.add(index_key, record_id)
            # Usar el nombre de la tabla en el archivo del índice
            index_filename = f"tables/index_bplustree_{table}_{col}.dat"
            bplus_tree.save_to_file(index_filename)

    entity_name = "Ciudad" if table_type == "rtree" else "Producto"
    record_key = record.key if hasattr(record, "key") else getattr(record, "id", "N/A")

    return {
        "status": 200,
        "message": f"{entity_name} con clave = {record_key} insertado en '{table}'",
    }


def _handle_select(parsed, table):
    if table not in global_tables:
        return {"status": 400, "message": f"Tabla '{table}' no encontrada."}

    table_info = global_tables[table]
    manager = table_info["manager"]
    table_type = table_info["index"]["type"]

    # Si no hay WHERE clause, devolver todos los registros
    if "where" not in parsed:
        if table_type != "rtree":
            return manager._read_all(manager.data_file, manager.aux_file)
        else:  # rtree
            # Para rtree, necesitamos leer todos los registros no eliminados
            records = []
            if hasattr(manager, "data_file"):
                import os

                if os.path.exists(manager.data_file):
                    with open(manager.data_file, "rb") as f:
                        record_id = 0
                        while True:
                            chunk = f.read(manager.record_size)
                            if len(chunk) < manager.record_size:
                                break
                            record = manager.CityClass.from_bytes(chunk)
                            if not record.eliminado:
                                records.append(record)
                            record_id += 1
            return records
    cond = parsed["where"]
    col = cond["column"]

    if table_type == "rtree" and col in ["spatial_range", "knn"]:
        if col == "knn":
            params = cond["value"]
        elif col == "spatial_range":
            return _extracted_from__handle_select_35(cond, manager)
        if isinstance(params, str):
            params = json.loads(params)
        knn_results = manager.knn_search(params["point"], params["k"])

        return [city for city, distance in knn_results]
    # Búsquedas por igualdad
    if cond["operator"] == "=":
        # Usar B+ tree si está disponible y es la columna indexada
        bplus_tree = table_info.get("bplus_tree")
        index_info = table_info.get("index")

        if (
            table_type != "rtree"
            and bplus_tree
            and index_info
            and index_info["column"] == col
        ):

            search_value = float(cond["value"]) if col == "price" else cond["value"]
            return bplus_tree.search(search_value)

        if table_info.get("index") and table_info["index"]["type"] == "isam":
            isam: ISAMIndex = table_info["isam"]
            index_column = table_info["index"]["column"]
            if col == index_column:
                key = cond["value"]
                offset = isam.search(key)
                if offset is not None:
                    with open(manager.data_file, "rb") as f:
                        f.seek(offset)
                        chunk = f.read(manager.record_size)
                        producto = manager.ProductoClass.from_bytes(chunk)
                        return [] if producto.eliminado else [producto]
                return []

        if col in ["id", "key"]:
            result = manager.search(cond["value"])
            return [result] if result else []

        if table_type != "rtree":
            all_records = manager._read_all(manager.data_file, manager.aux_file)
        else:  # rtree - obtener todos los registros
            all_records = _get_all_rtree_records(manager)

        return [
            r
            for r in all_records
            if str(getattr(r, col, "")).strip() == str(cond["value"]).strip()
        ]

    # Búsquedas por rango (BETWEEN)
    elif cond["operator"] == "BETWEEN":
        # Usar B+ tree si está disponible
        bplus_tree = table_info.get("bplus_tree")
        index_info = table_info.get("index")

        if (
            table_type != "rtree"
            and bplus_tree
            and index_info
            and index_info["column"] == col
        ):

            from_val = float(cond["from"]) if col == "price" else cond["from"]
            to_val = float(cond["to"]) if col == "price" else cond["to"]

            ids = bplus_tree.range_search(from_val, to_val)
            return [record for id in ids if (record := manager.search(id)) is not None]

        if table_type != "rtree":
            all_records = manager._read_all(manager.data_file, manager.aux_file)
        else:  # rtree
            all_records = _get_all_rtree_records(manager)

        return [
            r for r in all_records if cond["from"] <= getattr(r, col, 0) <= cond["to"]
        ]

    return {"status": 400, "message": f"Operador '{cond['operator']}' no soportado"}


# TODO Rename this here and in `_handle_select`
def _extracted_from__handle_select_35(cond, manager):
    params = cond["value"]
    if isinstance(params, str):
        params = json.loads(params)
    waza = manager.spatial_range_search(params["point"], params["radius"])
    cities_only = [city for city, distance in waza]
    print(cities_only)
    return cities_only


def _get_all_rtree_records(manager):
    records = []
    if hasattr(manager, "data_file"):
        import os

        if os.path.exists(manager.data_file):
            with open(manager.data_file, "rb") as f:
                while True:
                    chunk = f.read(manager.record_size)
                    if len(chunk) < manager.record_size:
                        break

                    record = manager.CityClass.from_bytes(chunk)
                    if not record.eliminado:
                        records.append(record)
    return records


def _handle_spatial_query(table, query_type, params):
    if table not in global_tables:
        return {"status": 400, "message": f"Tabla '{table}' no encontrada."}

    table_info = global_tables[table]

    if table_info["index"] != "rtree":
        return {
            "status": 400,
            "message": f"Las consultas espaciales solo están disponibles para tablas rtree",
        }

    manager = table_info["manager"]

    try:
        if query_type == "spatial_range":
            point = params["point"]  # [longitude, latitude]
            radius = params["radius"]  # en kilómetros
            return {"status": 200, "data": manager.spatial_range_search(point, radius)}

        elif query_type == "knn":
            point = params["point"]  #
            k = params["k"]
            return {"status": 200, "data": manager.knn_search(point, k)}

        else:
            return {
                "status": 400,
                "message": f"Tipo de consulta espacial '{query_type}' no soportado",
            }

    except Exception as e:
        return {"status": 500, "message": f"Error en consulta espacial: {str(e)}"}
