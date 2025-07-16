# 🚀 PROYECTO 2 – Base de Datos II

## 🎯 Objetivo del proyecto

## Construcción del Índice Invertido Textual (Full-Text Search)

## Indexación de descriptores locales (Multimedia Database)

## Frontend

## Experimentación

## 🎥 Presentación

* **Presentación**: [Ver en Google Drive](https://drive.google.com/drive/folders/1eaTNyh7sq1uyJGuJVUF00FDP35Gt_up3?usp=sharing)

# 🔍 Sistema de Búsqueda Visual de Ropa

Un sistema completo de búsqueda por texto e imágenes implementado con técnicas de recuperación de información, incluyendo KNN secuencial, índices invertidos y búsqueda vectorial con PostgreSQL.

## ✨ Características

- 🔤 **Búsqueda por texto** con índices invertidos optimizados
- 🖼️ **Búsqueda por imagen** usando descriptores SIFT y TF-IDF
- ⚡ **Múltiples algoritmos**: KNN secuencial vs índice invertido
- 🐘 **Integración PostgreSQL** con pgVector para búsquedas vectoriales
- 📊 **Comparativas de rendimiento** entre diferentes tamaños de dataset
- 🎨 **Interfaz moderna** con Flet (Python)

## 🏗️ Arquitectura

```
📁 proyecto/
├── 🔧 backend/                    # API FastAPI
│   ├── main.py                   # Endpoints principales
│   ├── algoritmos/
│   │   └── text_index/
│   │       ├── knn_images.py     # Búsqueda de imágenes
│   │       └── query.py          # Búsqueda de texto
│   └── data/
│       ├── data.csv              # Dataset de productos
│       └── images/               # Imágenes locales
├── 🎨 frontend/                   # Interfaz Flet
│   └── main.py                   # Aplicación GUI
└── 📊 visual_system_complete.pkl  # Sistema visual pre-entrenado
```

## 🚀 Instalación

### Prerequisitos

- Python 3.8+
- PostgreSQL 17+ (opcional, para comparativas)
- Docker (opcional, para pgVector)

### 1. Clonar repositorio

```bash
git clone https://github.com/tu-usuario/sistema-busqueda-visual.git
cd sistema-busqueda-visual
```

### 2. Instalar dependencias

```bash
pip install -r requirements.txt
```

**requirements.txt:**
```txt
fastapi
uvicorn[standard]
flet
opencv-python
scikit-learn
pillow
pandas
numpy
requests
psycopg2-binary
matplotlib
tqdm
```

### 3. Preparar datos

```bash
# Colocar el archivo data.csv en backend/data/
# Colocar las imágenes en backend/data/images/
# Formato: 1234.jpg donde 1234 es el ID del producto
```

### 4. Generar sistema visual (primera vez)

```bash
cd backend
python algoritmos/text_index/generate_visual_system.py
```

## 🎯 Ejecución

### Backend (FastAPI)

```bash
cd backend
fastapi dev main.py
```

El servidor estará disponible en: `http://localhost:8000`

### Frontend (Flet)

```bash
cd frontend
flet run main.py
```

## 📋 Endpoints API

### Búsqueda por Texto
```http
POST /search_text
Content-Type: application/json

{
  "query": "camisa azul",
  "top_k": 10
}
```

### Búsqueda por Imagen (KNN)
```http
POST /search_image_knn
Content-Type: multipart/form-data

file: [imagen.jpg]
top_k: 10
```

### Búsqueda por Imagen (Índice Invertido)
```http
POST /search_image_inverted
Content-Type: multipart/form-data

file: [imagen.jpg]
top_k: 10
```

### Comparar Métodos
```http
POST /compare_methods
Content-Type: multipart/form-data

file: [imagen.jpg]
top_k: 10
```

## 🐘 Configuración PostgreSQL (Opcional)

Para comparativas de rendimiento con pgVector:

### Con Docker

```bash
docker run -d \
  --name postgres-vector \
  -e POSTGRES_PASSWORD=tu_password \
  -p 5433:5432 \
  pgvector/pgvector:pg17
```

### Poblar base de datos

```bash
python migrate_to_postgres.py
```

## 🧪 Algoritmos Implementados

### 1. Búsqueda por Texto
- **TF-IDF** con ponderación logarítmica
- **Índice invertido** para búsquedas eficientes
- **Similitud coseno** para ranking

### 2. Búsqueda por Imagen
- **Descriptores SIFT** para extracción de características
- **K-Means clustering** para diccionario visual (300 palabras)
- **Bag of Visual Words** con TF-IDF
- **KNN secuencial** vs **índice invertido**

### 3. Optimizaciones
- **Heap** para top-K eficiente
- **Normalización de imágenes** (224x224)
- **Cache en memoria** para búsquedas rápidas

## 📊 Comparativas de Rendimiento

El sistema permite comparar rendimiento entre:

- **Memoria** (pkl) vs **PostgreSQL** (pgVector)
- **KNN secuencial** vs **índice invertido**
- **Diferentes tamaños** de dataset (1k, 2k, 4k, 8k, 16k, 32k)

### Resultados típicos:
```
Dataset Size | KNN Memory | Inverted Memory | PostgreSQL
1k           | 15ms       | 12ms           | 8ms
8k           | 45ms       | 38ms           | 12ms
32k          | 180ms      | 95ms           | 15ms
```

## 🎨 Interfaz de Usuario

### Características del Frontend
- 📤 **Carga de imágenes** drag-and-drop
- 🔧 **Selector de algoritmo** (KNN vs Invertido)
- ⚙️ **Configuración de K** (número de resultados)
- 📊 **Visualización de resultados** con similitud
- ⏱️ **Métricas de tiempo** en tiempo real
- 🖼️ **Preview de imagen query**

## 🗂️ Formato de Datos

### CSV Structure
```csv
id,filename,link,gender,masterCategory,subCategory,articleType,baseColour,season,year,usage,productDisplayName,text
15970,15970.jpg,http://example.com/image.jpg,Men,Apparel,Topwear,Shirts,Navy Blue,Fall,2011.0,Casual,Turtle Check Men Navy Blue Shirt,Men Apparel...
```

### Imágenes
- Formato: `{id}.jpg`
- Resolución: Cualquiera (se redimensiona automáticamente)
- Ubicación: `backend/data/images/`

## 🔧 Configuración Avanzada

### Parámetros del Sistema Visual
```python
# En generate_visual_system.py
k_clusters = 300        # Número de palabras visuales
sample_size = 5000     # Imágenes para entrenar diccionario
max_images = None      # Límite del dataset (None = todas)
```

### Conexión PostgreSQL
```python
db_config = {
    'host': 'localhost',
    'database': 'fashion_search',
    'user': 'postgres',
    'password': 'tu_password',
    'port': 5433
}
```


