from typing import Union
import pandas as pd
import numpy as np
import cv2
from io import BytesIO
from PIL import Image
import pickle
import os
import math
import heapq
from collections import defaultdict, Counter
from sklearn.cluster import KMeans
from pydantic import BaseModel
from tqdm import tqdm
import sys
sys.modules['knn_images'] = sys.modules[__name__]
# Variables globales
visual_dict = None
knn_sequential = None
visual_search_engine = None
image_metadata = {}

class PickleableVisualWordsDictionary:
    def __init__(self, k=500):
        self.k = k
        self.kmeans = None
        
    def _get_detector(self):
        """Crea detector SIFT cuando sea necesario"""
        return cv2.SIFT_create()
        
    def extract_descriptors(self, image):
        if len(image.shape) == 3:
            image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        
        image = cv2.resize(image, (60, 80))
        detector = self._get_detector()
        keypoints, descriptors = detector.detectAndCompute(image, None)
        return descriptors
    
    def build_dictionary(self, images):
        print("Extrayendo descriptores SIFT...")
        all_descriptors = []
        
        for i, img in enumerate(tqdm(images, desc="Procesando imágenes")):
            descriptors = self.extract_descriptors(img)
            if descriptors is not None:
                all_descriptors.append(descriptors)
        
        if all_descriptors:
            print(f"Concatenando {len(all_descriptors)} conjuntos de descriptores...")
            all_descriptors = np.vstack(all_descriptors)
            print(f"Total de descriptores: {len(all_descriptors)}")
            
            print(f"Aplicando K-means con k={self.k}...")
            self.kmeans = KMeans(n_clusters=self.k, random_state=42, n_init=10)
            self.kmeans.fit(all_descriptors)
            print("Diccionario visual construido exitosamente")
        else:
            print("No se pudieron extraer descriptores de las imágenes")
            
    def get_visual_words(self, image):
        """Obtiene las palabras visuales de una imagen"""
        descriptors = self.extract_descriptors(image)
        if descriptors is None or self.kmeans is None:
            return []
        return self.kmeans.predict(descriptors).tolist()
    
    def get_histogram(self, image):
        """Obtiene histograma de palabras visuales"""
        visual_words = self.get_visual_words(image)
        histogram = np.zeros(self.k)
        word_counts = Counter(visual_words)
        for word_id, count in word_counts.items():
            if 0 <= word_id < self.k:
                histogram[word_id] = count
        return histogram

class PickleableKNNSequential:
    def __init__(self, visual_dict, k_neighbors=5):
        self.visual_dict = visual_dict
        self.k_neighbors = k_neighbors
        self.image_histograms = {}
        self.image_norms = {}
        self.df = None
        self.N = 0
    
    def build_index(self, images_data):
        """Construye el índice KNN secuencial con TF-IDF"""
        print("Construyendo índice KNN secuencial...")
        histograms = {}
        self.N = len(images_data)
        
        # Calcular histogramas para todas las imágenes
        for image_id, image in tqdm(images_data, desc="Calculando histogramas"):
            histogram = self.visual_dict.get_histogram(image)
            histograms[image_id] = histogram
        
        # Calcular document frequency (df)
        vocab_size = self.visual_dict.k
        df = np.zeros(vocab_size)
        for histogram in histograms.values():
            df += (histogram > 0).astype(int)
        
        self.df = df
        
        # Calcular TF-IDF para cada imagen
        print("Calculando vectores TF-IDF...")
        for image_id, histogram in tqdm(histograms.items(), desc="TF-IDF"):
            tfidf_vector = np.zeros(vocab_size)
            for visual_word_id in range(vocab_size):
                tf = histogram[visual_word_id]
                if tf > 0 and df[visual_word_id] > 0:
                    tf_weight = 1 + math.log10(tf)
                    idf = math.log10(self.N / df[visual_word_id])
                    tfidf_vector[visual_word_id] = tf_weight * idf
            
            self.image_histograms[image_id] = tfidf_vector
            self.image_norms[image_id] = np.linalg.norm(tfidf_vector)
        
        print(f"Índice KNN construido para {self.N} imágenes")
    
    def search(self, query_image, k=10):
        """Busca las k imágenes más similares usando similitud coseno"""
        query_histogram = self.visual_dict.get_histogram(query_image)
        vocab_size = self.visual_dict.k
        query_tfidf = np.zeros(vocab_size)
        
        for visual_word_id in range(vocab_size):
            tf = query_histogram[visual_word_id]
            if tf > 0 and self.df[visual_word_id] > 0:
                tf_weight = 1 + math.log10(tf)
                idf = math.log10(self.N / self.df[visual_word_id])
                query_tfidf[visual_word_id] = tf_weight * idf
        
        query_norm = np.linalg.norm(query_tfidf)
        if query_norm == 0:
            return []
        
        # Calcular todas las similitudes
        similarities = []
        for image_id, image_tfidf in self.image_histograms.items():
            dot_product = np.dot(query_tfidf, image_tfidf)
            image_norm = self.image_norms[image_id]
            
            if image_norm > 0:
                similarity = dot_product / (query_norm * image_norm)
                similarities.append((similarity, image_id))
        
        # Ordenar y devolver top k
        similarities.sort(reverse=True)
        return [(image_id, similarity) for similarity, image_id in similarities[:k]]

class PickleableVisualSearchEngine:
    def __init__(self, visual_dict):
        self.visual_dict = visual_dict
        self.postings = {}
        self.df = {}
        self.image_norms = {}
        self.N = 0
    
    def build_index(self, images_data):
        """Construye el índice invertido para búsqueda visual"""
        print("Construyendo índice invertido...")
        global_postings = defaultdict(dict)
        self.N = len(images_data)
        
        # Construir postings lists
        for image_id, image in tqdm(images_data, desc="Construyendo postings"):
            visual_words = self.visual_dict.get_visual_words(image)
            if visual_words:
                tf_counts = Counter(visual_words)
                for visual_word_id, tf in tf_counts.items():
                    global_postings[visual_word_id][image_id] = tf
                    self.df[visual_word_id] = self.df.get(visual_word_id, 0) + 1
        
        # Calcular TF-IDF para cada posting
        print("Calculando pesos TF-IDF...")
        for visual_word_id, raw_posting in tqdm(global_postings.items(), desc="TF-IDF postings"):
            df_term = self.df[visual_word_id]
            idf = math.log10(self.N / df_term)
            
            tfidf_posting = {}
            for image_id, tf in raw_posting.items():
                tf_weight = 1 + math.log10(tf) if tf > 0 else 0
                tfidf_posting[image_id] = tf_weight * idf
            
            self.postings[visual_word_id] = tfidf_posting
        
        self._compute_image_norms()
        print(f"Índice invertido construido para {self.N} imágenes")
    
    def _compute_image_norms(self):
        """Calcula las normas de los vectores de imagen"""
        norms = defaultdict(float)
        for posting in self.postings.values():
            for image_id, weight in posting.items():
                norms[image_id] += weight ** 2
        
        self.image_norms = {img_id: math.sqrt(sum_sq) for img_id, sum_sq in norms.items()}
    
    def search(self, query_image, k=10):
        """Busca usando el índice invertido"""
        visual_words = self.visual_dict.get_visual_words(query_image)
        if not visual_words:
            return []
        
        tf_query = Counter(visual_words)
        query_weights = {}
        
        # Calcular pesos de la consulta
        for visual_word_id, tf in tf_query.items():
            if visual_word_id in self.df:
                tf_weight = 1 + math.log10(tf)
                idf = math.log10(self.N / self.df[visual_word_id])
                query_weights[visual_word_id] = tf_weight * idf
        
        query_norm_sq = sum(w*w for w in query_weights.values())
        if query_norm_sq == 0:
            return []
        query_norm = math.sqrt(query_norm_sq)
        
        # Calcular scores usando el índice invertido
        scores = defaultdict(float)
        for visual_word_id, query_weight in query_weights.items():
            if visual_word_id in self.postings:
                for image_id, doc_weight in self.postings[visual_word_id].items():
                    scores[image_id] += query_weight * doc_weight
        
        # Usar heap para los k mejores resultados
        heap = []
        for image_id, dot_product in scores.items():
            image_norm = self.image_norms.get(image_id, 0.0)
            if image_norm > 0:
                similarity = dot_product / (image_norm * query_norm)
                
                if len(heap) < k:
                    heapq.heappush(heap, (similarity, image_id))
                elif similarity > heap[0][0]:
                    heapq.heapreplace(heap, (similarity, image_id))
        
        results = sorted(heap, reverse=True)
        return [(image_id, similarity) for similarity, image_id in results]

def load_image_from_path(image_path):
    """Carga una imagen desde un path local"""
    try:
        image = cv2.imread(image_path)
        if image is None:
            return None
        return cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
    except Exception as e:
        print(f"Error cargando imagen {image_path}: {e}")
        return None

def build_visual_system_from_scratch(csv_path, images_folder, k_clusters=300, sample_size=None):
    """Construye el sistema visual completo desde cero"""
    print("Cargando datos...")
    df = pd.read_csv(csv_path)
    
    if sample_size:
        df = df.head(sample_size)
        print(f"Usando muestra de {sample_size} imágenes")
    
    # Filtrar solo las imágenes que existen
    existing_images = []
    valid_data = []
    
    print("Verificando imágenes existentes...")
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Verificando archivos"):
        image_path = os.path.join(images_folder, f"{row['id']}.jpg")
        if os.path.exists(image_path):
            img = load_image_from_path(image_path)
            if img is not None:
                existing_images.append(img)
                valid_data.append(row)
    
    print(f"Encontradas {len(existing_images)} imágenes válidas de {len(df)} total")
    
    if len(existing_images) == 0:
        raise ValueError("No se encontraron imágenes válidas")
    
    # Construir diccionario visual
    print("Construyendo diccionario visual...")
    visual_dict = PickleableVisualWordsDictionary(k=k_clusters)
    
    sample_for_dict = existing_images[:min(1000, len(existing_images))]
    visual_dict.build_dictionary(sample_for_dict)
    
    # Preparar datos para los índices
    images_data = []
    metadata = {}
    
    for i, row in enumerate(valid_data):
        image_id = row['id']
        image_path = os.path.join(images_folder, f"{image_id}.jpg")
        img = load_image_from_path(image_path)
        
        if img is not None:
            images_data.append((image_id, img))
            metadata[image_id] = row.to_dict()
    
    # Construir índices
    print("Construyendo KNN secuencial...")
    knn_sequential = PickleableKNNSequential(visual_dict)
    knn_sequential.build_index(images_data)
    
    print("Construyendo índice invertido...")
    visual_search_engine = PickleableVisualSearchEngine(visual_dict)
    visual_search_engine.build_index(images_data)
    
    # Guardar sistema completo
    system = {
        'visual_dict': visual_dict,
        'knn_sequential': knn_sequential,
        'visual_search_engine': visual_search_engine,
        'metadata': metadata
    }
    
    with open('visual_system_complete.pkl', 'wb') as f:
        pickle.dump(system, f)
    
    print(f"Sistema visual guardado exitosamente con {len(metadata)} imágenes")
    return system

def initialize_visual_system():
    """Inicializa el sistema visual (carga o construye)"""
    global visual_dict, knn_sequential, visual_search_engine, image_metadata
    
    if visual_dict is not None:
        return
    
    # Cargar sistema completo preentrenado
    if os.path.exists('visual_system_complete.pkl'):
        print("Cargando sistema visual preentrenado...")
        with open('visual_system_complete.pkl', 'rb') as f:
            system = pickle.load(f)
        
        visual_dict = system['visual_dict']
        knn_sequential = system['knn_sequential'] 
        visual_search_engine = system['visual_search_engine']
        image_metadata = system['metadata']
        
        print(f"Sistema visual cargado: {len(image_metadata)} imágenes")
        return
    
    # Si no existe, construir desde cero
    print("No se encontró sistema preentrenado. Construyendo desde cero...")
    
    # Configurar paths (ajustar según tu estructura)
    csv_path = "data/data.csv"  # Ajustar path
    images_folder = "data/images"  # Ajustar path donde están las imágenes
    
    system = build_visual_system_from_scratch(
        csv_path=csv_path,
        images_folder=images_folder,
        k_clusters=300,
        sample_size=5000  # Usar None para todas las imágenes
    )
    
    visual_dict = system['visual_dict']
    knn_sequential = system['knn_sequential']
    visual_search_engine = system['visual_search_engine']
    image_metadata = system['metadata']

VisualWordsDictionary = PickleableVisualWordsDictionary
KNNSequential = PickleableKNNSequential
VisualSearchEngine = PickleableVisualSearchEngine

