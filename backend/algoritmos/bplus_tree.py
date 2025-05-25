import csv
import pickle
import os

class BPlusTreeNode:
    def __init__(self, is_leaf=False):
        self.is_leaf = is_leaf
        self.keys = []
        self.children = []
        self.next = None  # for leaf linkage

class BPlusTree:
    def __init__(self, t=3):
        self.root = BPlusTreeNode(is_leaf=True)
        self.t = t

    def search(self, key):
        node = self.root
        while not node.is_leaf:
            i = self._find_index(node.keys, key)
            node = node.children[i]
        return [record for k, record in node.children if k == key]

    def range_search(self, start_key, end_key):
        
        print("start_key",start_key, type(start_key))
        print("end_key",end_key, type(end_key))
        
        node = self.root
        while not node.is_leaf:
            i = self._find_index(node.keys, start_key)
            node = node.children[i]
        result = []
        while node:
            for k, record in node.children:
                print("k",k, type(k))
                print("record",record)
                if start_key <= k <= end_key:
                    result.append(record)
                elif k > end_key:
                    return result
            node = node.next
        return result

    def add(self, key, value):
        root = self.root
        if len(root.keys) == (2 * self.t - 1):
            new_root = BPlusTreeNode()
            new_root.children.append(self.root)
            self._split_child(new_root, 0)
            self.root = new_root
            
        self._insert_non_full(self.root, key, value)

    def remove(self, key):
        self._remove_recursive(self.root, key)
        if not self.root.keys and not self.root.is_leaf:
            self.root = self.root.children[0]

    def _remove_recursive(self, node, key):
        t = self.t
        if node.is_leaf:
            node.children = [(k, v) for (k, v) in node.children if k != key]
            node.keys = [k for (k, _) in node.children]
        else:
            idx = self._find_index(node.keys, key)
            child = node.children[idx]
            self._remove_recursive(child, key)
            if len(child.keys) < t - 1:
                left_sibling = node.children[idx - 1] if idx > 0 else None
                right_sibling = node.children[idx + 1] if idx + 1 < len(node.children) else None

                if left_sibling and len(left_sibling.keys) > t - 1:
                    borrow_key = left_sibling.keys.pop()
                    borrow_child = left_sibling.children.pop()
                    child.keys.insert(0, node.keys[idx - 1])
                    child.children.insert(0, borrow_child)
                    node.keys[idx - 1] = borrow_key

                elif right_sibling and len(right_sibling.keys) > t - 1:
                    borrow_key = right_sibling.keys.pop(0)
                    borrow_child = right_sibling.children.pop(0)
                    child.keys.append(node.keys[idx])
                    child.children.append(borrow_child)
                    node.keys[idx] = borrow_key

                elif left_sibling:
                    left_sibling.keys += [node.keys[idx - 1]] + child.keys
                    left_sibling.children += child.children
                    if child.is_leaf:
                        left_sibling.next = child.next
                    node.keys.pop(idx - 1)
                    node.children.pop(idx)

                elif right_sibling:
                    child.keys += [node.keys[idx]] + right_sibling.keys
                    child.children += right_sibling.children
                    if right_sibling.is_leaf:
                        child.next = right_sibling.next
                    node.keys.pop(idx)
                    node.children.pop(idx + 1)

    def _insert_non_full(self, node, key, value):
        if node.is_leaf:
            i = self._find_index([k for k, _ in node.children], key)
            node.children.insert(i, (key, value))
            #node.children.insert(i, (key, value))
        else:
            i = self._find_index(node.keys, key)
            # Verificar si el hijo está lleno según si es hoja o interno
            child = node.children[i]
            is_full = (
                len(child.children) == (2 * self.t - 1)
                if child.is_leaf else
                len(child.keys) == (2 * self.t - 1)
            )

            if is_full:
                self._split_child(node, i)
                if key > node.keys[i]:
                    i += 1

            self._insert_non_full(node.children[i], key, value)

    def _split_child(self, parent, index):
        t = self.t
        child = parent.children[index]
        new_child = BPlusTreeNode(is_leaf=child.is_leaf)

        #parent.keys.insert(index, child.keys[t - 1])
        parent.children.insert(index + 1, new_child)

        #new_child.keys = child.keys[t:]
        #child.keys = child.keys[:t - 1]

        if child.is_leaf:
            new_child.children = child.children[t:]
            child.children = child.children[:t]
            new_child.next = child.next
            child.next = new_child
            parent.keys.insert(index, new_child.children[0][0])
        else:
            new_child.keys = child.keys[t:]
            child.keys = child.keys[:t - 1]
            new_child.children = child.children[t:]
            child.children = child.children[:t]
            parent.keys.insert(index, child.keys[t - 1])

    def _find_index(self, keys, key):
        for i, item in enumerate(keys):
            if key < item:
                return i
        return len(keys)
    
    def save_to_file(self, filename):
        with open(filename, 'wb') as f:
            pickle.dump(self, f)
    @staticmethod
    def load_from_file(filename):
        with open(filename, 'rb') as f:
            return pickle.load(f)

TREE_FILE = "bplustree_precio.dat"


"""if os.path.exists(TREE_FILE):
    tree = BPlusTree.load_from_file(TREE_FILE)
    print("🌲 Árbol B+ cargado desde archivo.")
else:
    tree = BPlusTree(t=3)
    with open("productos_amazon.csv", newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for fila in reader:
            precio = float(fila["price"].replace("$", "").strip())
            producto_id = fila["id"]
            # Agregar al árbol usando precio como clave
            tree.add(precio, producto_id)
    tree.save_to_file(TREE_FILE)
    print("💾 Árbol B+ construido y guardado.")


# Consulta
print("\n🔍 Productos con precio 100.0:")
for r in tree.search(100.0):
    print(r)

print("\n📊 Productos entre S/100.0 y S/100.36:")
for r in tree.range_search(100.0, 100.36):
    print(r)"""