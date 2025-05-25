import os
from algoritmos.bplus_tree import BPlusTree
from algoritmos.table_manager import global_tables
from algoritmos.sequential import SequentialFileManager

bplustree_path = "tables/bplustree_precio.dat"


if os.path.exists(bplustree_path):
  bplus_tree = BPlusTree.load_from_file(bplustree_path)
else:
  bplus_tree = BPlusTree(t=3)
  bplus_tree.save_to_file(bplustree_path)
