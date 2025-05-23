import os
import pickle
import hashlib
import csv

DATA_DIR = "extendible_hashing_data"
os.makedirs(DATA_DIR, exist_ok=True)  # Crear la carpeta si no existe

class Bucket:
    def __init__(self, size=4):
        self.records = []
        self.local_depth = 1
        self.size = size

    def is_full(self):
        return len(self.records) >= self.size

    def add(self, key, value):
        self.records.append((key, value))

    def remove(self, key):
        original = len(self.records)
        self.records = [(k, v) for (k, v) in self.records if k != key]
        return len(self.records) < original

    def search(self, key):
        return [v for k, v in self.records if k == key]

    def range_search(self, k1, k2):
        return [v for k, v in self.records if k1 <= k <= k2]

class ExtendibleHashing:
    def __init__(self, bucket_size=4, dir_file="directory.dat"):
        self.bucket_size = bucket_size
        self.directory_file = os.path.join(DATA_DIR, dir_file)
        self.global_depth = 1

        if os.path.exists(self.directory_file):
            with open(self.directory_file, "rb") as f:
                self.directory = pickle.load(f)
        else:
            self.directory = {"0": self._bucket_path("bucket_0.dat"), 
                              "1": self._bucket_path("bucket_1.dat")}
            for b in self.directory.values():
                self._save_bucket(b, Bucket(self.bucket_size))
            self._save_directory()

    def _bucket_path(self, filename):
        return os.path.join(DATA_DIR, filename)

    def _hash(self, key):
        h = hashlib.md5(str(key).encode()).hexdigest()
        return bin(int(h, 16))[2:].zfill(128)[:self.global_depth]

    def _load_bucket(self, filename):
        with open(filename, "rb") as f:
            return pickle.load(f)

    def _save_bucket(self, filename, bucket):
        with open(filename, "wb") as f:
            pickle.dump(bucket, f)

    def _save_directory(self):
        with open(self.directory_file, "wb") as f:
            pickle.dump(self.directory, f)

    def add(self, key, value):
        hash_key = self._hash(key)
        if hash_key not in self.directory:
            self.directory[hash_key] = self._bucket_path(f"bucket_{hash_key}.dat")
            self._save_bucket(self.directory[hash_key], Bucket(self.bucket_size))
        
        filename = self.directory[hash_key]
        bucket = self._load_bucket(filename)

        if not bucket.is_full():
            bucket.add(key, value)
            self._save_bucket(filename, bucket)
            return

        # Split bucket
        old_records = bucket.records + [(key, value)]
        bucket.local_depth += 1

        if bucket.local_depth > self.global_depth:
            self.global_depth += 1
            new_dir = {}
            for k, v in self.directory.items():
                new_dir["0" + k] = v
                new_dir["1" + k] = v
            self.directory = new_dir

        b1 = Bucket(self.bucket_size)
        b2 = Bucket(self.bucket_size)
        b1.local_depth = b2.local_depth = bucket.local_depth

        fname1 = self._bucket_path(f"bucket_{hashlib.md5(os.urandom(6)).hexdigest()[:6]}.dat")
        fname2 = self._bucket_path(f"bucket_{hashlib.md5(os.urandom(6)).hexdigest()[:6]}.dat")

        for k, v in old_records:
            h = self._hash(k)
            if h.endswith("0"):
                b1.add(k, v)
            else:
                b2.add(k, v)

        for k in list(self.directory):
            if self.directory[k] == filename:
                if k[-1] == "0":
                    self.directory[k] = fname1
                else:
                    self.directory[k] = fname2

        os.remove(filename)
        self._save_bucket(fname1, b1)
        self._save_bucket(fname2, b2)
        self._save_directory()

    def search(self, key):
        hash_key = self._hash(key)
        if hash_key not in self.directory:
            return []
        bucket = self._load_bucket(self.directory[hash_key])
        return bucket.search(key)

    def range_search(self, k1, k2):
        seen = set()
        result = []
        for file in set(self.directory.values()):
            if file in seen:
                continue
            seen.add(file)
            b = self._load_bucket(file)
            result.extend(b.range_search(k1, k2))
        return result

    def remove(self, key):
        hash_key = self._hash(key)
        if hash_key not in self.directory:
            return False
        file = self.directory[hash_key]
        b = self._load_bucket(file)
        removed = b.remove(key)
        self._save_bucket(file, b)
        return removed


# ===========================
# CARGA DESDE CSV Y PRUEBAS
# ===========================
hash_index = ExtendibleHashing()

with open("productos_amazon.csv", newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        hash_index.add(row["id"], row)
        print(f"Agregado: {row['id']}")

# Buscar por ID
print(hash_index.search("4c69b61db1fc16e7013b43fc926e502d"))

# Buscar por rango de ID
print(hash_index.range_search("4a", "5f"))

# Eliminar
hash_index.remove("4c69b61db1fc16e7013b43fc926e502d")

# Verificar que fue eliminado
print(hash_index.search("4c69b61db1fc16e7013b43fc926e502d"))