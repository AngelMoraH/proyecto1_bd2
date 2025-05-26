import rtree
from rtree import index
import math
import struct
import os
import pandas as pd
import numpy as np
from typing import List, Tuple, Any, Optional
from collections import defaultdict

csv_f = "/Users/lvera/OneDrive/Escritorio/proyecto_bd/proyecto1_bd2-main (1)/proyecto1_bd2-main/backend/worldcities.csv"

def build_city_class(fields, record_format):
    class City:
        __fields__ = fields  # Agregar esta línea para que sea accesible
        
        def __init__(self, *args):
            for field, value in zip(fields, args):
                name = field["name"]
                setattr(self, name, value)
            self.eliminado = False
        
        @property
        def key(self):
            # Crear una clave única basada en nombre y país
            name = getattr(self, 'name', 'Unknown')
            country = getattr(self, 'country', 'Unknown')
            return f"{name}_{country}"
        
        @property
        def x(self):
            # Coordenada X (longitud)
            return float(getattr(self, 'longitude', 0.0))
        
        @property
        def y(self):
            # Coordenada Y (latitud)
            return float(getattr(self, 'latitude', 0.0))

        def to_bytes(self):
            valores = []
            for field in fields:
                val = getattr(self, field["name"])
                fmt = field["type"]
                if fmt.startswith("VARCHAR"):
                    size = int(fmt[8:-1])
                    val = str(val).encode("utf-8")[:size].ljust(size, b" ")
                    valores.append(val)
                elif fmt == "FLOAT":
                    valores.append(float(val))
                elif fmt == "INT":
                    valores.append(int(val))
                elif fmt == "DATE":
                    val = str(val)[:10].encode("utf-8").ljust(10, b" ")
                    valores.append(val)
            valores.append(self.eliminado)
            return struct.pack(record_format, *valores)

        @staticmethod
        def from_bytes(data):
            unpacked = struct.unpack(record_format, data)
            parsed = []
            for val, field in zip(unpacked[:-1], fields):
                tipo = field["type"]
                if tipo.startswith("VARCHAR") or tipo == "DATE":
                    parsed.append(val.decode("utf-8", errors="replace").strip())
                elif tipo == "FLOAT":
                    parsed.append(float(val))
                elif tipo == "INT":
                    parsed.append(int(val))
            eliminado = unpacked[-1]
            obj = City(*parsed)
            obj.eliminado = eliminado
            return obj

        def __str__(self):
            values = [f"{f['name']}={getattr(self, f['name'])}" for f in fields]
            return (
                f"[{'X' if self.eliminado else ' '}] City("
                + ", ".join(values)
                + ")"
            )

    return City


class RTreeIndex:
    _instances = {}  # almacena una instancia por nombre de tabla

    def __init__(self, record_size, record_format, data_file, index_file, CityClass, dimension=2):
        self.record_size = int(record_size)
        self.record_format = record_format
        self.CityClass = CityClass
        self.data_file = data_file
        self.index_file = index_file
        self.dimension = dimension
        
        # Crear directorio si no existe
        os.makedirs(os.path.dirname(self.data_file), exist_ok=True)
        
        # Inicializar archivos si no existen
        if not os.path.exists(self.data_file):
            open(self.data_file, "wb").close()
        
        # Limpiar archivos de índice existentes para evitar corrupción
        self._cleanup_index_files()
        
        # Configurar propiedades del índice espacial
        properties = index.Property()
        properties.dimension = dimension
        properties.buffering_capacity = 10  # Reducir capacidad de buffer
        properties.leaf_capacity = 100     # Reducir capacidad de hojas
        properties.index_capacity = 100    # Reducir capacidad de índice
        
        # Intentos múltiples para crear el índice
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                print(f"🔄 Intento {attempt + 1}/{max_attempts} de crear índice espacial...")
                # Usar índice en memoria si persisten los problemas
                if attempt == max_attempts - 1:
                    print("📝 Creando índice espacial en memoria...")
                    self.spatial_index = index.Index(properties=properties)  # En memoria
                else:
                    self.spatial_index = index.Index(self.index_file, properties=properties)
                
                self.key_index = defaultdict(list)
                self.next_id = 0
                
                print("✅ Índice espacial creado exitosamente")
                break
                
            except Exception as e:
                print(f"❌ Error en intento {attempt + 1}: {e}")
                if attempt < max_attempts - 1:
                    print("🔄 Limpiando archivos e intentando nuevamente...")
                    self._cleanup_index_files()
                    import time
                    time.sleep(0.5)  # Esperar un poco antes del siguiente intento
                else:
                    print("❌ Todos los intentos fallaron. Usando configuración por defecto.")
                    # Fallback final: índice completamente en memoria
                    try:
                        self.spatial_index = index.Index(properties=index.Property())
                        self.key_index = defaultdict(list)
                        self.next_id = 0
                        print("✅ Índice espacial creado en memoria como fallback")
                    except Exception as final_error:
                        print(f"💥 Error crítico creando índice: {final_error}")
                        raise RuntimeError("No se pudo crear el índice espacial") from final_error
        
        # Cargar datos existentes solo si el índice se creó exitosamente
        try:
            self._load_existing_data()
        except Exception as load_error:
            print(f"⚠️ Error cargando datos existentes: {load_error}")
            print("🔄 Continuando con índice vacío...")

    def _cleanup_index_files(self):
        """Limpia archivos de índice potencialmente corruptos"""
        import time
        import uuid
        
        index_extensions = ['.dat', '.idx']
        for ext in index_extensions:
            file_path = f"{self.index_file}{ext}"
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    print(f"🗑️ Archivo de índice eliminado: {file_path}")
                except Exception as e:
                    print(f"⚠️ No se pudo eliminar {file_path}: {e}")
                    # Si no se puede eliminar, intentar renombrar para evitar conflictos
                    try:
                        backup_name = f"{file_path}.backup_{int(time.time())}_{uuid.uuid4().hex[:8]}"
                        os.rename(file_path, backup_name)
                        print(f"📁 Archivo renombrado a: {backup_name}")
                    except Exception as rename_error:
                        print(f"⚠️ Tampoco se pudo renombrar: {rename_error}")
                        # Como último recurso, cambiar el nombre del índice
                        self.index_file = f"{self.index_file}_new_{int(time.time())}"
                        print(f"🔄 Usando nuevo nombre de índice: {self.index_file}")

    @classmethod
    def get_or_create(cls, table_name, record_format, record_size, CityClass):
        # Siempre crear una nueva instancia para evitar problemas de archivos bloqueados
        # if table_name in cls._instances:
        #     return cls._instances[table_name]

        data_file = os.path.join("tables", f"{table_name}.bin")
        index_file = os.path.join("tables", f"{table_name}_rtree")
        
        # Agregar timestamp para evitar conflictos de archivos
        import time
        if table_name in cls._instances:
            # Si ya existe una instancia, usar un nombre diferente para el índice
            index_file = os.path.join("tables", f"{table_name}_rtree_{int(time.time())}")
        
        instance = cls(record_size, record_format, data_file, index_file, CityClass)
        cls._instances[table_name] = instance
        return instance

    def _load_existing_data(self):
        """Carga datos existentes desde el archivo binario"""
        if not os.path.exists(self.data_file) or os.path.getsize(self.data_file) == 0:
            return
            
        print(f"📂 Cargando datos existentes desde {self.data_file}...")
        
        try:
            with open(self.data_file, "rb") as f:
                record_id = 0
                successful_loads = 0
                
                while True:
                    chunk = f.read(self.record_size)
                    if len(chunk) < self.record_size:
                        break
                    
                    try:
                        city = self.CityClass.from_bytes(chunk)
                        if not city.eliminado:
                            # Validar coordenadas antes de insertar
                            coords = self._extract_coordinates(city)
                            if self._validate_coordinates(coords):
                                bbox = coords + coords  # [x, y, x, y] para punto
                                
                                # Insertar en índice espacial con manejo de errores
                                try:
                                    self.spatial_index.insert(record_id, bbox)
                                    
                                    # Agregar al índice de claves
                                    key = self._extract_key(city)
                                    self.key_index[key].append(record_id)
                                    successful_loads += 1
                                    
                                except Exception as spatial_error:
                                    print(f"⚠️ Error insertando en índice espacial ID {record_id}: {spatial_error}")
                                    continue
                            else:
                                print(f"⚠️ Coordenadas inválidas en registro {record_id}: {coords}")
                        
                    except Exception as record_error:
                        print(f"⚠️ Error procesando registro {record_id}: {record_error}")
                        
                    record_id += 1
                
                self.next_id = record_id
                print(f"✅ Cargados {successful_loads}/{record_id} registros exitosamente")
                
        except Exception as e:
            print(f"❌ Error cargando datos existentes: {e}")
            # Reinicializar índices en caso de error
            self.key_index = defaultdict(list)
            self.next_id = 0

    def _validate_coordinates(self, coords):
        """Valida que las coordenadas sean válidas"""
        if len(coords) < 2:
            return False
        
        x, y = coords[0], coords[1]
        
        # Verificar que sean números válidos
        if not (isinstance(x, (int, float)) and isinstance(y, (int, float))):
            return False
        
        # Verificar que no sean NaN o infinitos
        if math.isnan(x) or math.isnan(y) or math.isinf(x) or math.isinf(y):
            return False
        
        # Verificar rangos válidos para coordenadas geográficas
        if not (-180 <= x <= 180 and -90 <= y <= 90):
            return False
        
        return True

    def _extract_coordinates(self, registro):
        coords = []
        coord_names = ['x', 'y', 'z', 'w'][:self.dimension]
        
        for coord_name in coord_names:
            if hasattr(registro, coord_name):
                coord_val = getattr(registro, coord_name)
                coords.append(float(coord_val) if coord_val is not None else 0.0)
            elif isinstance(registro, dict) and coord_name in registro:
                coord_val = registro[coord_name]
                coords.append(float(coord_val) if coord_val is not None else 0.0)
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

    def _read_record_by_position(self, position):
        """Lee un registro específico por su posición en el archivo"""
        try:
            with open(self.data_file, "rb") as f:
                f.seek(position * self.record_size)
                chunk = f.read(self.record_size)
                if len(chunk) == self.record_size:
                    return self.CityClass.from_bytes(chunk)
        except Exception as e:
            print(f"⚠️ Error leyendo registro en posición {position}: {e}")
        return None

    def insert(self, city):
        if self.search(city.key):
            return {
                "message": f"Ya existe un registro con clave {city.key}",
                "status": 400,
            }
        
        # Validar coordenadas antes de insertar
        coords = self._extract_coordinates(city)
        if not self._validate_coordinates(coords):
            return {
                "message": f"Coordenadas inválidas: {coords}",
                "status": 400,
            }
        
        try:
            # Agregar al archivo binario
            with open(self.data_file, "ab") as f:
                f.write(city.to_bytes())
            
            # Agregar al índice espacial
            bbox = coords + coords
            self.spatial_index.insert(self.next_id, bbox)
            
            # Agregar al índice de claves
            key = self._extract_key(city)
            self.key_index[key].append(self.next_id)
            
            self.next_id += 1
            
            return {"message": "Registro insertado", "status": 200}
            
        except Exception as e:
            print(f"❌ Error insertando registro: {e}")
            return {"message": f"Error insertando: {e}", "status": 500}

    def search(self, key):
        matching_ids = self.key_index.get(key, [])
        
        for record_id in matching_ids:
            city = self._read_record_by_position(record_id)
            if city and not city.eliminado and self._extract_key(city) == key:
                return city
        
        return None

    def delete(self, key):
        matching_ids = self.key_index.get(key, [])
        
        if not matching_ids:
            return False
        
        for record_id in matching_ids:
            city = self._read_record_by_position(record_id)
            if city and not city.eliminado and self._extract_key(city) == key:
                try:
                    # Marcar como eliminado
                    city.eliminado = True
                    
                    # Actualizar en el archivo
                    with open(self.data_file, "r+b") as f:
                        f.seek(record_id * self.record_size)
                        f.write(city.to_bytes())
                    
                    # Remover del índice espacial
                    coords = self._extract_coordinates(city)
                    if self._validate_coordinates(coords):
                        bbox = coords + coords
                        self.spatial_index.delete(record_id, bbox)
                    
                    # Limpiar índice de claves
                    if key in self.key_index:
                        self.key_index[key] = [id_ for id_ in self.key_index[key] if id_ != record_id]
                        if not self.key_index[key]:
                            del self.key_index[key]
                    
                    return True
                    
                except Exception as e:
                    print(f"❌ Error eliminando registro: {e}")
                    return False
        
        return False

    def add_batch(self, cities: List[Any]) -> List[int]:
        """Agrega múltiples ciudades en lote"""
        ids = []
        successful_inserts = 0
        print(f"📥 Agregando {len(cities)} ciudades...")
        
        try:
            with open(self.data_file, "ab") as f:
                for i, city in enumerate(cities):
                    if i % 1000 == 0 and i > 0:
                        print(f"  ⏳ Procesadas {i}/{len(cities)} ciudades... (exitosas: {successful_inserts})")
                    
                    try:
                        # Validar coordenadas
                        coords = self._extract_coordinates(city)
                        if not self._validate_coordinates(coords):
                            print(f"⚠️ Saltando ciudad con coordenadas inválidas: {coords}")
                            continue
                        
                        f.write(city.to_bytes())
                        
                        bbox = coords + coords
                        self.spatial_index.insert(self.next_id, bbox)
                        
                        key = self._extract_key(city)
                        self.key_index[key].append(self.next_id)
                        
                        ids.append(self.next_id)
                        self.next_id += 1
                        successful_inserts += 1
                        
                    except Exception as e:
                        print(f"⚠️ Error procesando ciudad {i}: {e}")
                        continue
        
        except Exception as e:
            print(f"❌ Error en inserción por lotes: {e}")
        
        print(f"✅ Agregadas {successful_inserts}/{len(cities)} ciudades exitosamente")
        return ids

    def spatial_range_search(self, point: List[float], radio_km: float) -> List[Tuple[Any, float]]:
        if not self._validate_coordinates(point):
            return []
            
        radio_deg = radio_km / 111.0
        
        bbox = [
            point[0] - radio_deg,  # min_x
            point[1] - radio_deg,  # min_y
            point[0] + radio_deg,  # max_x
            point[1] + radio_deg   # max_y
        ]
        
        try:
            candidate_ids = list(self.spatial_index.intersection(bbox))
            
            result = []
            for record_id in candidate_ids:
                city = self._read_record_by_position(record_id)
                if city and not city.eliminado:
                    coords = self._extract_coordinates(city)
                    if self._validate_coordinates(coords):
                        distance_km = self._haversine_distance(point, coords)
                        
                        if distance_km <= radio_km:
                            result.append((city, distance_km))
            
            result.sort(key=lambda x: x[1])
            return result
            
        except Exception as e:
            print(f"❌ Error en búsqueda espacial: {e}")
            return []

    def knn_search(self, point: List[float], k: int) -> List[Tuple[Any, float]]:
        if not self._validate_coordinates(point):
            return []
            
        distances = []
        try:
            with open(self.data_file, "rb") as f:
                record_id = 0
                while True:
                    chunk = f.read(self.record_size)
                    if len(chunk) < self.record_size:
                        break
                    
                    try:
                        city = self.CityClass.from_bytes(chunk)
                        if not city.eliminado:
                            coords = self._extract_coordinates(city)
                            if self._validate_coordinates(coords):
                                distance = self._haversine_distance(point, coords)
                                distances.append((city, distance))
                    except Exception as e:
                        print(f"⚠️ Error procesando registro {record_id} en KNN: {e}")
                    
                    record_id += 1
            
            distances.sort(key=lambda x: x[1])
            return distances[:k]
            
        except Exception as e:
            print(f"❌ Error en búsqueda KNN: {e}")
            return []

    def _haversine_distance(self, point1, point2):
        try:
            lon1, lat1 = point1[0], point1[1]
            lon2, lat2 = point2[0], point2[1]
            
            lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
            
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
            c = 2 * math.asin(math.sqrt(a))
            r = 6371  # radio tierra en km
            return c * r
        except Exception as e:
            print(f"⚠️ Error calculando distancia: {e}")
            return float('inf')

    def size(self) -> int:
        count = 0
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, "rb") as f:
                    while True:
                        chunk = f.read(self.record_size)
                        if len(chunk) < self.record_size:
                            break
                        
                        try:
                            city = self.CityClass.from_bytes(chunk)
                            if not city.eliminado:
                                count += 1
                        except Exception:
                            continue
            except Exception as e:
                print(f"❌ Error contando registros: {e}")
        return count

    def _read_all(self, include_deleted=False):
        registros = []
        
        if not os.path.exists(self.data_file) or os.path.getsize(self.data_file) == 0:
            return registros
        
        try:
            with open(self.data_file, "rb") as f:
                while True:
                    chunk = f.read(self.record_size)
                    if len(chunk) < self.record_size:
                        break
                    
                    try:
                        city = self.CityClass.from_bytes(chunk)
                        if not city.eliminado or include_deleted:
                            registros.append(city)
                    except Exception:
                        continue
        except Exception as e:
            print(f"❌ Error leyendo todos los registros: {e}")
        
        return registros


def load_cities_from_csv(csv_file: str, rtree_index: RTreeIndex):
    try:
        print(f"📖 Leyendo archivo CSV: {csv_file}")
        df = pd.read_csv(csv_file)
        print(f"Columnas disponibles: {list(df.columns)}")
        print(f"Total de filas: {len(df)}")
        
        cities = []
        # Acceder a los campos de la clase
        fields = rtree_index.CityClass.__fields__
        
        for _, row in df.iterrows():
            try:
                args = []
                for field in fields:
                    field_name = field["name"]
                    if field_name == "name":
                        args.append(str(row.get('city', row.get('name', 'Unknown'))))
                    elif field_name == "country":
                        args.append(str(row.get('country', 'Unknown')))
                    elif field_name == "latitude":
                        lat_val = row.get('lat', row.get('latitude', 0))
                        args.append(float(lat_val) if pd.notna(lat_val) else 0.0)
                    elif field_name == "longitude":
                        lng_val = row.get('lng', row.get('longitude', 0))
                        args.append(float(lng_val) if pd.notna(lng_val) else 0.0)
                    elif field_name == "population":
                        pop_val = row.get('population', 0)
                        args.append(int(pop_val) if pd.notna(pop_val) else 0)
                    else:
                        args.append(row.get(field_name, ""))
                
                city = rtree_index.CityClass(*args)
                
                # Validar coordenadas antes de agregar
                coords = rtree_index._extract_coordinates(city)
                if rtree_index._validate_coordinates(coords):
                    cities.append(city)
                else:
                    print(f"⚠️ Saltando ciudad con coordenadas inválidas: {city.name} {coords}")
                    
            except Exception as e:
                print(f"Error procesando fila: {e}")
                continue
        
        print(f"Procesadas {len(cities)} ciudades válidas")
        if cities:
            rtree_index.add_batch(cities)
            print(f"¡Dataset cargado exitosamente!")
        return len(cities)
    except Exception as e:
        print(f"Error cargando CSV: {e}")
        return 0