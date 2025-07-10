# ------------------- builder.py -------------------
import os, json, math
from collections import defaultdict, Counter
from preprocess import preprocess_text

class SPIMIIndexer:
    def __init__(self, block_dir='blocks', index_dir='index', block_size=1000):
        self.block_dir = block_dir
        self.index_dir = index_dir
        self.block_size = block_size
        os.makedirs(block_dir, exist_ok=True)
        os.makedirs(index_dir, exist_ok=True)
        self.block_count = 0

    def index_documents(self, documents):
        """
        documents: iterable de tuplas (doc_id, texto_completo)
        """
        block = defaultdict(dict)
        for i, (doc_id, text) in enumerate(documents):
            terms = preprocess_text(text)
            tf_counts = Counter(terms)
            for term, tf in tf_counts.items():
                block.setdefault(term, {})[doc_id] = tf
            # Si alcanzamos el block_size, volcamos a disco
            if (i + 1) % self.block_size == 0:
                self._write_block(block)
                block.clear()
        # Último bloque
        if block:
            self._write_block(block)
        # Merge blocks y construir índice final
        self._merge_blocks(total_docs=len(documents))
        # Calcular normas de documentos
        self._compute_doc_norms()

    def _write_block(self, block):
        path = os.path.join(self.block_dir, f'block_{self.block_count}.json')
        with open(path, 'w') as f:
            json.dump(block, f)
        self.block_count += 1

    def _merge_blocks(self, total_docs):
        """
        Lee todos los bloques, une postings y calcula pesos TF-IDF.
        Guarda inverted_index.json y stats.json (N, df).
        """
        global_postings = {}
        df = defaultdict(int)
        # Merge simple: cargar cada bloque
        for i in range(self.block_count):
            path = os.path.join(self.block_dir, f'block_{i}.json')
            with open(path) as f:
                block = json.load(f)
            for term, postings in block.items():
                global_postings.setdefault(term, {}).update(postings)
        # Calcular TF-IDF
        N = total_docs
        index = {}
        for term, postings in global_postings.items():
            df[term] = len(postings)
            idf = math.log10(N / df[term])
            index[term] = {doc_id: (1 + math.log10(tf)) * idf for doc_id, tf in postings.items()}
        # Persistir índice y stats
        with open(os.path.join(self.index_dir, 'inverted_index.json'), 'w') as f:
            json.dump(index, f)
        with open(os.path.join(self.index_dir, 'stats.json'), 'w') as f:
            json.dump({'N': N, 'df': df}, f)

    def _compute_doc_norms(self):
        """
        Lee inverted_index.json y calcula norma Euclídea de cada documento.
        Guarda doc_norms.json.
        """
        path = os.path.join(self.index_dir, 'inverted_index.json')
        with open(path) as f:
            index = json.load(f)
        norms = defaultdict(float)
        for postings in index.values():
            for doc_id, weight in postings.items():
                norms[doc_id] += weight ** 2
        norms = {doc_id: math.sqrt(v) for doc_id, v in norms.items()}
        with open(os.path.join(self.index_dir, 'doc_norms.json'), 'w') as f:
            json.dump(norms, f)
