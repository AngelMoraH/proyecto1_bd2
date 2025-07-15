from typing import Union
import time
from algoritmos.parser_sql import SQLTransformer,timed_execute_query,sql_grammar
from fastapi import FastAPI,UploadFile, File, HTTPException
from pydantic import BaseModel
from algoritmos.text_index.query import build_search
from algoritmos.text_index import knn_images  # ← Cambio aquí
import pandas as pd
import numpy as np
import cv2
import requests
from io import BytesIO
from PIL import Image

app = FastAPI()

class SQLQuery(BaseModel):
    query: str
    top_k: int

class ImageSearchQuery(BaseModel):
    top_k: int = 10
    method: str = "knn"

@app.get("/")
def read_root():
    return {"proyecto": "Proyecto 1 BD2"}

@app.post("/search_text")
def search_text(query: SQLQuery):
    
    print("query",query,query.query, query.top_k)
    start_time = time.time()
    response = build_search(query.query, query.top_k)
    end_time = time.time()
    elapsed = (end_time - start_time) * 1000
    
    return {"result": response, "execution_time_ms": round(elapsed, 3)}

@app.post("/search_image_knn")
async def search_image_knn(file: UploadFile = File(...), top_k: int = 10):
    """Búsqueda de imágenes usando KNN secuencial"""
    try:
        # Inicializar sistema visual
        knn_images.initialize_visual_system()
        
        start_time = time.time()
        
        # Procesar imagen de consulta
        contents = await file.read()
        image = Image.open(BytesIO(contents))
        image_array = np.array(image)
        
        # Realizar búsqueda
        results = knn_images.knn_sequential.search(image_array, k=top_k)
        
        # Formatear respuesta COMPATIBLE con el frontend original
        response_data = []
        for image_id, similarity in results:
            metadata = knn_images.image_metadata.get(image_id, {})
            response_data.append({
                "id": image_id,
                 "link": metadata.get("link", ""),  # Para compatibilidad                
                "similarity": float(similarity),
                "productDisplayName": metadata.get("productDisplayName", ""),
                "score": float(similarity)
            })
        
        end_time = time.time()
        elapsed = (end_time - start_time) * 1000
        
        # Formato compatible con frontend original
        return {
            "result": {
                "query": "image_search",  # ← CAMBIO: query genérico
                "response": response_data
            },
            "execution_time_ms": round(elapsed, 3)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en búsqueda KNN: {str(e)}")

@app.post("/search_image_inverted")
async def search_image_inverted(file: UploadFile = File(...), top_k: int = 10):
    """Búsqueda de imágenes usando índice invertido"""
    try:  # ← AÑADIDO: try-except que faltaba
        # Inicializar sistema visual
        knn_images.initialize_visual_system()
        
        start_time = time.time()
        
        # Procesar imagen de consulta
        contents = await file.read()
        image = Image.open(BytesIO(contents))
        image_array = np.array(image)
        
        # Realizar búsqueda
        results = knn_images.visual_search_engine.search(image_array, k=top_k)
        
        # Formatear respuesta COMPATIBLE con el frontend original
        response_data = []
        for image_id, similarity in results:
            metadata = knn_images.image_metadata.get(image_id, {})
            response_data.append({
                "id": image_id,
                "link": metadata.get("link", ""),  # ← CAMBIO: url en lugar de link
                "similarity": float(similarity),
                "productDisplayName": metadata.get("productDisplayName", ""),
                "score": float(similarity)
            })
        
        end_time = time.time()
        elapsed = (end_time - start_time) * 1000
        
        # Formato compatible con frontend original
        return {
            "result": {
                "query": "image_search",  # ← CAMBIO: query genérico
                "response": response_data
            },
            "execution_time_ms": round(elapsed, 3)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en búsqueda con índice invertido: {str(e)}")

# Endpoint adicional para debugging
@app.get("/system_status")
def get_system_status():
    """Endpoint para verificar el estado del sistema visual"""
    try:
        knn_images.initialize_visual_system()
        
        return {
            "status": "ready",
            "total_images": len(knn_images.image_metadata),
            "dictionary_size": knn_images.visual_dict.k if knn_images.visual_dict else 0,
            "knn_ready": knn_images.knn_sequential is not None,
            "inverted_index_ready": knn_images.visual_search_engine is not None
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }

@app.post("/compare_methods")
async def compare_search_methods(file: UploadFile = File(...), top_k: int = 10):
    """Compara ambos métodos de búsqueda"""
    try:
        knn_images.initialize_visual_system()
        
        # Procesar imagen
        contents = await file.read()
        image = Image.open(BytesIO(contents))
        image_array = np.array(image)
        
        # KNN Secuencial
        start_time = time.time()
        knn_results = knn_images.knn_sequential.search(image_array, k=top_k)
        knn_time = (time.time() - start_time) * 1000
        
        # Índice Invertido
        start_time = time.time()
        inverted_results = knn_images.visual_search_engine.search(image_array, k=top_k)
        inverted_time = (time.time() - start_time) * 1000
        
        # Formatear resultados
        def format_results(results, method):
            return [{
                "id": image_id,
                "url": knn_images.image_metadata.get(image_id, {}).get("link", ""),
                "similarity": float(similarity),
                "productDisplayName": knn_images.image_metadata.get(image_id, {}).get("productDisplayName", ""),
                "method": method
            } for image_id, similarity in results]
        
        return {
            "knn_sequential": {
                "results": format_results(knn_results, "knn_sequential"),
                "execution_time_ms": round(knn_time, 3)
            },
            "inverted_index": {
                "results": format_results(inverted_results, "inverted_index"),
                "execution_time_ms": round(inverted_time, 3)
            },
            "performance_comparison": {
                "knn_faster": knn_time < inverted_time,
                "speed_difference_ms": round(abs(knn_time - inverted_time), 3)
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error comparando métodos: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)