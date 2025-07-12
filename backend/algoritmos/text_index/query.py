# query.py
import os
import json
import math
import csv
from collections import defaultdict, Counter
from .preprocess import preprocess_text
from .builder import SPIMIIndexer

class SearchEngine:
    def __init__(self, index_dir='index'):
        self.index_dir = index_dir
        # Sólo cargo stats y norms (que suelen ser pequeños)
        self.stats = json.load(open(os.path.join(index_dir, 'stats.json')))
        self.N  = self.stats['N']
        self.df = self.stats['df']
        norms_raw = json.load(open(os.path.join(index_dir, 'doc_norms.json')))
        self.norms = {int(d): v for d, v in norms_raw.items()}

    def _load_postings(self, term: str) -> dict[int, float]:
        """
        Carga y devuelve el posting list TF-IDF de `term` desde disk,
        o {} si no existe.
        """
        safe = term.replace("/", "_")
        path = os.path.join(self.index_dir, 'postings', f"{safe}.json")
        if not os.path.exists(path):
            return {}
        raw = json.load(open(path))
        # raw viene con claves str, convítelas a int
        return {int(doc_id): weight for doc_id, weight in raw.items()}

    def search(self, query: str, k: int = 10) -> list[tuple[int, float]]:
        # 1) Preprocesar consulta...
        terms = preprocess_text(query)
        tf_q  = Counter(terms)

        # 2) Calcular pesos de la consulta
        q_w = {}
        for term, tf in tf_q.items():
            df_t = self.df.get(term, 0)
            if df_t > 0:
                idf     = math.log10(self.N / df_t)
                q_w[term] = (1 + math.log10(tf)) * idf

        # 3) Norma de la consulta
        q_norm_sq = sum(v*v for v in q_w.values())
        if q_norm_sq == 0:
            return []
        q_norm = math.sqrt(q_norm_sq)

        # 4) Producto punto leyendo solo postings necesarios
        scores = defaultdict(float)
        for term, qw in q_w.items():
            postings = self._load_postings(term)
            for doc_id, dw in postings.items():
                scores[doc_id] += qw * dw

        # 5) Normalizar con doc_norms.json
        results = []
        for doc_id, dot in scores.items():
            doc_norm = self.norms.get(doc_id, 0.0)
            if doc_norm > 0:
                results.append((doc_id, dot / (doc_norm * q_norm)))

        # 6) Top-k
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:k]



def build_search(query,top_k):
    row_map = {}
    docs = []
    with open('/Users/angelmora/Desktop/proyecto1_bd2/backend/data/data.csv', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            doc_id = int(row['id'])
            text   = row['text']
            row_map[doc_id] = row.copy()
            docs.append((doc_id, text))

    # 2) Sólo indexar la primera vez (o si borraste el índice)
    stats_path = os.path.join('index', 'stats.json')

    if not os.path.exists(stats_path):
        indexer = SPIMIIndexer(block_size=500)
        indexer.index_documents(docs)


    # 3) Crear la instancia de búsqueda y lanzar consultas
    se     = SearchEngine(index_dir='index')
    hits   = se.search(query, k=top_k)

    print(f"\nResultados para «{query}» (top {top_k}):")
    results=[]
    for doc_id, score in hits:
        row = row_map.get(doc_id, {})
        # Aseguramos copiar para no mutar el row_map original
        entry = row.copy()
        entry['score'] = score
        results.append(entry)

    return {
        "query":query, "response":results
    }