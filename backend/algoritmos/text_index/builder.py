# builder.py

import os
import json
import math
from collections import defaultdict, Counter
from .preprocess import preprocess_text

class SPIMIIndexer:
    def __init__(self, block_dir='blocks', index_dir='index', block_size=1000):
        self.block_dir   = block_dir
        self.index_dir   = index_dir
        self.block_size  = block_size
        os.makedirs(block_dir, exist_ok=True)
        os.makedirs(index_dir, exist_ok=True)
        self.block_count = 0

    def index_documents(self, documents):
        """
        documents: iterable de tuplas (doc_id, texto_completo)
        """
        # 1) Crear bloques parciales
        block = defaultdict(dict)
        for i, (doc_id, text) in enumerate(documents):
            terms    = preprocess_text(text)
            tf_counts = Counter(terms)
            for term, tf in tf_counts.items():
                block.setdefault(term, {})[doc_id] = tf
            if (i + 1) % self.block_size == 0:
                self._write_block(block)
                block.clear()

        if block:
            self._write_block(block)

        # 2) Fusionar bloques en postings individuales
        self._merge_blocks(total_docs=len(documents))

        # 3) Calcular normas de documento desde postings
        self._compute_doc_norms()

    def _write_block(self, block):
        path = os.path.join(self.block_dir, f'block_{self.block_count}.json')
        with open(path, 'w') as f:
            json.dump(block, f)
        self.block_count += 1

    def _merge_blocks(self, total_docs):
        """
        Lee todos los bloques, calcula TF-IDF y escribe
        cada posting list en index/postings/{term}.json,
        además de stats.json con N y df.
        """
        # 1. Merge de bloques en global_postings
        global_postings = {}
        df = defaultdict(int)
        for i in range(self.block_count):
            block_path = os.path.join(self.block_dir, f'block_{i}.json')
            with open(block_path, 'r') as f:
                block = json.load(f)
            for term, post in block.items():
                global_postings.setdefault(term, {}).update(post)

        # 2. Crear carpeta de postings
        postings_dir = os.path.join(self.index_dir, 'postings')
        os.makedirs(postings_dir, exist_ok=True)

        # 3. Calcular y grabar cada posting TF-IDF
        N = total_docs
        for term, raw_post in global_postings.items():
            df[term] = len(raw_post)
            idf = math.log10(N / df[term])
            # Construir posting TF-IDF
            weighted = { str(doc_id): (1 + math.log10(tf)) * idf
                         for doc_id, tf in raw_post.items() }
            safe_term = term.replace('/', '_')
            with open(os.path.join(postings_dir, f'{safe_term}.json'), 'w') as f:
                json.dump(weighted, f)

        # 4. Guardar stats.json
        stats = {'N': N, 'df': df}
        with open(os.path.join(self.index_dir, 'stats.json'), 'w') as f:
            json.dump(stats, f)

    def _compute_doc_norms(self):
        """
        Lee todos los postings en index/postings para
        calcular la norma Euclídea de cada documento y
        guarda doc_norms.json.
        """
        postings_dir = os.path.join(self.index_dir, 'postings')
        norms = defaultdict(float)

        # Recorre cada archivo de término
        for fname in os.listdir(postings_dir):
            path = os.path.join(postings_dir, fname)
            with open(path, 'r') as f:
                posting = json.load(f)  # {doc_id_str: weight, ...}
            for doc_id_str, weight in posting.items():
                doc_id = int(doc_id_str)
                norms[doc_id] += weight ** 2

        # Raíz cuadrada y guardar
        norms = { doc_id: math.sqrt(sum_sq) for doc_id, sum_sq in norms.items() }
        with open(os.path.join(self.index_dir, 'doc_norms.json'), 'w') as f:
            json.dump(norms, f)