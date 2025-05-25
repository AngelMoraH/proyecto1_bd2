import os
from algoritmos.bplus_tree import BPlusTree
from algoritmos.table_manager import global_tables
from algoritmos.sequential import SequentialFileManager

bplustree_path = "tables/bplustree_precio.dat"

if not os.path.exists(bplustree_path):
    bplus_tree = BPlusTree(t=3)
    if global_tables:
        tables = [k for k, v in global_tables.items() if v["index"]][0]
        sequential_file = SequentialFileManager.get_or_create(
            tables,
            global_tables[tables]["record_format"],
            global_tables[tables]["record_size"],
            global_tables[tables]["producto_class"],
        )
        for producto in sequential_file._read_all(sequential_file.data_file):
            bplus_tree.add(producto.price, producto.id)
        bplus_tree.save_to_file(bplustree_path)
else:
    bplus_tree = BPlusTree.load_from_file(bplustree_path)