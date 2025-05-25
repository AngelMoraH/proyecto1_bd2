import rtree
from rtree import index
import math
import pickle
import os
import pandas as pd
import numpy as np
from typing import List, Tuple, Any, Optional
from collections import defaultdict

class City:
    def __init__(self, name, country, latitude, longitude, population=0, region="", **kwargs):
        self.key = f"{name}_{country}"  
        self.name = name
        self.country = country
        self.x = longitude  
        self.y = latitude   
        self.population = population
        self.region = region
        
        for k, v in kwargs.items():
            setattr(self, k, v)
    
    def __str__(self):
        return f"City({self.name}, {self.country}, lat={self.y:.2f}, lon={self.x:.2f}, pop={self.population:,})"
    
    def __repr__(self):
        return self.__str__()
class RTreeIndex:
    def __init__(self, dimension=2, data_file="rtree_data.bin", index_file="rtree_index"):
        self.data_file = data_file
        self.index_file = index_file
        self.dimension = dimension
        
        properties = index.Property()#confi del índice c:
        properties.dimension = dimension
        
        if self._files_exist():
            print(f"📂 Cargando datos existentes desde {data_file}...")
            self._load_from_files()
        else:
            print(f"🆕 Creando nuevo índice. Se guardará en {data_file}")
            self.spatial_index = index.Index(self.index_file, properties=properties)
            self.data_store = {}
            self.key_index = defaultdict(list)
            self.next_id = 0
    def _files_exist(self) -> bool:
        return os.path.exists(self.data_file)
    def _load_from_files(self):
        try:
            with open(self.data_file, 'rb') as f:
                data = pickle.load(f)
                self.data_store = data['data_store']
                self.key_index = defaultdict(list, data['key_index'])
                self.next_id = data['next_id']
            
            properties = index.Property()
            properties.dimension = self.dimension
            
            if os.path.exists(f"{self.index_file}.idx"):
                self.spatial_index = index.Index(self.index_file, properties=properties)
            else:
                self.spatial_index = index.Index(self.index_file, properties=properties)
                self._rebuild_spatial_index()
            
            print(f"✅ Cargados {len(self.data_store)} registros desde archivo")
            
        except Exception as e:
            print(f" Error cargando archivos: {e}")
            properties = index.Property()
            properties.dimension = self.dimension
            self.spatial_index = index.Index(self.index_file, properties=properties)
            self.data_store = {}
            self.key_index = defaultdict(list)
            self.next_id = 0
    
    def _rebuild_spatial_index(self):
        for record_id, registro in self.data_store.items():
            coords = self._extract_coordinates(registro)
            bbox = coords + coords
            self.spatial_index.insert(record_id, bbox)
    def save_to_file(self):
        try:
            data_to_save = {
                'data_store': self.data_store,
                'key_index': dict(self.key_index),
                'next_id': self.next_id
            }
            
            with open(self.data_file, 'wb') as f:
                pickle.dump(data_to_save, f, protocol=pickle.HIGHEST_PROTOCOL)
            print(f"Datos guardados en {self.data_file}")
            
        except Exception as e:
            print(f" Error guardando: {e}")
    
    def _extract_coordinates(self, registro):
        coords = []
        coord_names = ['x', 'y', 'z', 'w'][:self.dimension]
        
        for coord_name in coord_names:
            if hasattr(registro, coord_name):
                coords.append(getattr(registro, coord_name))
            elif isinstance(registro, dict) and coord_name in registro:
                coords.append(registro[coord_name])
            else:
                coords.append(0.0)
        
        return coords
    def _extract_key(self, registro):
        if hasattr(registro, 'key'):
            return registro.key
        elif isinstance(registro, dict) and 'key' in registro:
            return registro['key']
        else:
            return str(registro)
    
    def search(self, key) -> List[Any]:
        matching_ids = self.key_index.get(key, [])
        return [self.data_store[id_] for id_ in matching_ids if id_ in self.data_store]
    
    def add_batch(self, registros: List[Any]) -> List[int]:
        ids = []
        print(f"📥 Agregando {len(registros)} registros...")
        
        for i, registro in enumerate(registros):
            if i % 1000 == 0 and i > 0:
                print(f"  ⏳ Procesados {i}/{len(registros)} registros...")
            
            record_id = self.next_id
            self.next_id += 1
            
            coords = self._extract_coordinates(registro)
            bbox = coords + coords
            
            self.spatial_index.insert(record_id, bbox)
            self.data_store[record_id] = registro
            
            key = self._extract_key(registro)
            self.key_index[key].append(record_id)
            
            ids.append(record_id)
        
        self.save_to_file()
        print(f"✅ Agregados {len(ids)} registros exitosamente")
        return ids
    
    def spatial_range_search(self, point: List[float], radio_km: float) -> List[Any]:
        radio_deg = radio_km / 111.0
        
        bbox = [
            point[0] - radio_deg,  
            point[1] - radio_deg,  
            point[0] + radio_deg,  
            point[1] + radio_deg  
        ]
        
        candidate_ids = list(self.spatial_index.intersection(bbox))
        
        result = []
        for record_id in candidate_ids:
            if record_id in self.data_store:
                registro = self.data_store[record_id]
                coords = self._extract_coordinates(registro)
                distance_km = self._haversine_distance(point, coords)
                
                if distance_km <= radio_km:
                    result.append((registro, distance_km))
        
        result.sort(key=lambda x: x[1])
        return result
    
    def _haversine_distance(self, point1, point2):
        lon1, lat1 = point1[0], point1[1]
        lon2, lat2 = point2[0], point2[1]
        
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        r = 6371
        return c * r
    def knn_search(self, point: List[float], k: int) -> List[Tuple[Any, float]]:
        distances = []
        for record_id, registro in self.data_store.items():
            coords = self._extract_coordinates(registro)
            distance = self._haversine_distance(point, coords)
            distances.append((registro, distance))
        
        distances.sort(key=lambda x: x[1])
        return distances[:k]
    

    def get_statistics(self):
        """Obtiene estadísticas del dataset"""
        if not self.data_store:
            return "No hay datos"
        
        cities = list(self.data_store.values())
        countries = set()
        populations = []
        
        for city in cities:
            if hasattr(city, 'country'):
                countries.add(city.country)
            if hasattr(city, 'population') and city.population > 0:
                populations.append(city.population)
        
        stats = {
            'total_cities': len(cities),
            'total_countries': len(countries),
            'avg_population': np.mean(populations) if populations else 0,
            'max_population': max(populations) if populations else 0,
            'min_population': min(populations) if populations else 0
        }
        
        return stats
    
    def size(self) -> int:
        return len(self.data_store)

def load_cities_from_csv(csv_file: str, rtree_index: RTreeIndex):
    try:
        print(f" Leyendo archivo CSV: {csv_file}")
        df = pd.read_csv(csv_file)
        print(f"Columnas disponibles: {list(df.columns)}")
        print(f"Total de filas: {len(df)}")
        cities = []
        for _, row in df.iterrows():
            try:
                if 'city' in df.columns and 'country' in df.columns:
                    city = City(
                        name=str(row.get('city', 'Unknown')),
                        country=str(row.get('country', 'Unknown')),
                        latitude=float(row.get('latitude', 0)),
                        longitude=float(row.get('longitude', 0)),
                        population=int(row.get('population', 0)) if pd.notna(row.get('population', 0)) else 0
                    )
                elif 'City' in df.columns and 'State' in df.columns:
                    city = City(
                        name=str(row.get('City', 'Unknown')),
                        country=str(row.get('State', 'Unknown')),
                        latitude=float(row.get('lat', 0)),
                        longitude=float(row.get('lng', 0)),
                        population=int(row.get('Population', 0)) if pd.notna(row.get('Population', 0)) else 0
                    )
                else:
                    cols = list(df.columns)
                    city = City(
                        name=str(row.iloc[0]) if len(row) > 0 else 'Unknown',
                        country=str(row.iloc[1]) if len(row) > 1 else 'Unknown',
                        latitude=float(row.iloc[2]) if len(row) > 2 else 0,
                        longitude=float(row.iloc[3]) if len(row) > 3 else 0,
                        population=int(row.iloc[4]) if len(row) > 4 and pd.notna(row.iloc[4]) else 0
                    )
                
                cities.append(city)
                
            except Exception as e:
                continue
        print(f"Procesadas {len(cities)} ciudades válidas")
        if cities:
            rtree_index.add_batch(cities)
            print(f"¡Dataset cargado exitosamente!")
        return len(cities)
    except Exception as e:
        print(f"Error cargando CSV: {e}")
        return 0
if __name__ == "__main__":
    cities_index = RTreeIndex(dimension=2, data_file="cities_rtree.bin", index_file="cities_rtree")
    # Si no hay datos, mostrar instrucciones para cargar
    if cities_index.size() == 0:
        sample_cities = [
            City("Madrid", "Spain", 40.4168, -3.7038, 3223000),
            City("Barcelona", "Spain", 41.3851, 2.1734, 1620000),
            City("Paris", "France", 48.8566, 2.3522, 2161000),
            City("London", "UK", 51.5074, -0.1278, 8982000),
            City("New York", "USA", 40.7128, -74.0060, 8336817),
            City("Tokyo", "Japan", 35.6762, 139.6503, 13960000),
            City("Mexico City", "Mexico", 19.4326, -99.1332, 9209944),
            City("Buenos Aires", "Argentina", -34.6118, -58.3960, 2890151),
        ]
        cities_index.add_batch(sample_cities)
        print("✅ Agregadas ciudades de ejemplo")
 
    # Ejemplo 2: K vecinos más cercanos
    print(f"\n🎯 Las 3 ciudades más cercanas a París:")
    paris_coords = [2.3522, 48.8566]  # [lon, lat]
    nearest = cities_index.knn_search(paris_coords, 3)
    for city, distance in nearest:
        print(f"  📍 {city.name}, {city.country} - {distance:.0f}km")

    
