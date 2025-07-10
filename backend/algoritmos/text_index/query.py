# query.py
import os
import json
import math
from collections import defaultdict, Counter
from preprocess import preprocess_text
from builder import SPIMIIndexer

class SearchEngine:
    def __init__(self, index_dir='index'):
        self.index_dir = index_dir
        self._load_index()

    def _load_index(self):
        # Cargo índices y estadísticas
        self.index = json.load(open(os.path.join(self.index_dir, 'inverted_index.json')))
        norms_raw = json.load(open(os.path.join(self.index_dir, 'doc_norms.json')))
        stats = json.load(open(os.path.join(self.index_dir, 'stats.json')))
        self.N = stats['N']
        self.df = stats['df']
        # Convertir las claves de normas de string a int
        self.norms = {int(doc_id): norm for doc_id, norm in norms_raw.items()}

    def search(self, query: str, k: int = 10) -> list:
        """
        Procesa la consulta y devuelve top-k [(doc_id, score), ...].
        """

        # 1) Preprocesar la consulta
        terms = preprocess_text(query)
        tf_q = Counter(terms)

        # 2) Calcular pesos TF-IDF de la consulta
        q_weights = {}
        for term, tf in tf_q.items():
            if term in self.df and self.df[term] > 0:
                idf = math.log10(self.N / self.df[term])
                q_weights[term] = (1 + math.log10(tf)) * idf

        # 3) Norma de la consulta
        q_norm_sq = sum(w * w for w in q_weights.values())
        if q_norm_sq <= 0:
            return []   # consulta vacía o sin coincidencias en el índice
        q_norm = math.sqrt(q_norm_sq)

        # 4) Calcular producto escalar consulta·documento
        scores = defaultdict(float)
        for term, qw in q_weights.items():
            for doc_id_str, dw in self.index.get(term, {}).items():
                # en inverted_index.json los doc_id también son strings
                doc_id = int(doc_id_str)
                scores[doc_id] += qw * dw

        # 5) Normalizar por normas, evitando división por cero
        results = []
        for doc_id, score in scores.items():
            doc_norm = self.norms.get(doc_id, 0.0)
            if doc_norm > 0:
                results.append((doc_id, score / (doc_norm * q_norm)))
            # si doc_norm == 0, descartamos el documento

        # 6) Devolver top-k ordenado
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:k]


if __name__ == '__main__':
    # 1) Defino aquí mis documentos de prueba:
    docs = [
        (1, "Este es un texto de prueba para el primer documento."),
        (2, "Otro ejemplo de documento con texto de prueba."),
        (3, "Un tercer texto que contiene palabras de prueba y ejemplo.")
    ]

    # 2) Indexar esos documentos (sólo la primera vez)
    indexer = SPIMIIndexer(block_size=2)
    indexer.index_documents(docs)
    print("Índice construido ✔️\n")

    # 3) Crear la instancia de búsqueda y lanzar consultas
    se = SearchEngine()
    query = "texto de prueba ejemplo"
    results = se.search(query, k=3)

    print(f"Resultados para la consulta: '{query}'")
    for doc_id, score in results:
        print(f"  Documento {doc_id} → Score: {score:.4f}")