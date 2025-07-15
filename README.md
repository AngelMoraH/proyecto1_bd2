# 🚀 PROYECTO 1 – Base de Datos II

## 📚 Introducción

### 🎯 Objetivo del proyecto

- Comprender y aplicar técnicas de indexación para optimizar la gestión, el almacenamiento y la recuperación de datos estructurados dentro de un modelo relacional basado en tablas, integrando también el soporte para datos complejos y multidimensionales.

### 📝 Descripción de la aplicación

- Este proyecto desarrolla un motor de almacenamiento capaz de procesar consultas SQL básicas (SELECT, INSERT, DELETE) sobre archivos en disco, aprovechando estructuras de datos avanzadas para optimizar tiempos de respuesta y accesos físicos. Además, incluye una interfaz gráfica en **Flet** que facilita la interacción con los datos y visualiza el comportamiento de cada índice.

La aplicación consta de dos módulos principales:

1. **Backend**: Implementa las estructuras de almacenamiento (Sequential File, ISAM, B+ Tree, Extendible Hashing) y un parser SQL basado en Lark, que enruta cada consulta a la estructura adecuada.
2. **Frontend**: Proporciona una interfaz de escritorio donde el usuario puede listar, buscar y filtrar productos por atributos como categoría, nombre o rango de precio. También se puede ver los tiempos de ejecucción de cada quey.

### 🤔 ¿Qué esperamos de la aplicación?

* **Rendimiento**: Comparar tiempos de inserción y búsqueda entre las distintas estructuras.
* **Usabilidad**: Ofrecer una GUI intuitiva que refleje en tiempo real los resultados de las consultas.
* **Escalabilidad**: Demostrar que nuestra solución puede adaptarse a tamaños de archivo crecientes sin degradar dramáticamente el desempeño.

---

## 👥 Integrantes

* **Angel Obed Mora Huamanchay**
* **Lando Fabrizio Veramendi Hilario**
* **Mishelle Stephany Villarreal Falcón**

---

## 🧱 Estructuras Implementadas

### 🔹 Sequential File

* **Función**: Almacena de manera secuencial los registros de tipo `Producto` en un archivo binario.
* **Formato de registro**: Campos fijos (`id`, `name`, `category`, `price`, `image`, `description`) serializados con métodos `to_bytes()` de la clase `Producto`.
* **Implementación**: [sequential.py](https://github.com/AngelMoraH/proyecto1_bd2/blob/main/backend/algoritmos/sequential.py)

### 🔹 ISAM (Indexed Sequential Access Method)

* **Índice disperso**: Dos niveles de índices que apuntan a bloques de registros.
* **Búsqueda**: Reduce el número de accesos al disco al localizar primero el bloque y luego el registro dentro de éste.
* **Implementación**: [ISAM implementation](https://github.com/AngelMoraH/proyecto1_bd2/blob/main/backend/algoritmos)

### 🔹 B+ Tree

* **Índice secundario** ordenado por `price`.
* **Operaciones soportadas**: búsqueda exacta, rango, inserción y eliminación con rebalanceo automático.
* **Implementación**: [bplus\_tree.py](https://github.com/AngelMoraH/proyecto1_bd2/blob/main/backend/algoritmos/bplus_tree.py)

### 🔹 Extendible Hashing

* **Hash dinámico**: Crece de forma flexible mediante directorios y buckets persistentes en `.dat`.
* **Ventaja**: Acceso casi constante por `id`, con redistribución automática al desbordarse.
* **Implementación**: [extendible\_hashing.py](https://github.com/AngelMoraH/proyecto1_bd2/blob/main/backend/algoritmos/extendible_hashing.py)
 
### 🔹 Rtree Index

* **Índice Espacial**: Generado por latitud y longitud según la clase City
* **Ventaja**: Permite encontrar de manera eficaz zonas geográficas en un tiempo óptimo
* **Implementación**: [r\_tree.py](https://github.com/AngelMoraH/proyecto1_bd2/blob/main/backend/algoritmos/rtree_in.py)

---

## 🛠️ Parser SQL

### 🏗️ Construcción con Lark

* Define la gramática SQL (CREATE, SELECT, INSERT, DELETE, BETWEEN, índices).
* Genera un árbol de análisis que se traduce a llamadas al **SequentialFileManager**, **BPlusTree**, **ISAM** , **ExtendibleHashing** o **RtreeIndex**, según el índice y la cláusula WHERE.
* **Implementación**: [parser\_sql.py](https://github.com/AngelMoraH/proyecto1_bd2/blob/main/backend/algoritmos/parser_sql.py)

---

## 📊 Resultados experimentales

### ⏱️ Métricas

* **Número de accesos a disco**
* **Tiempo de ejecución** (ms) medido con `time.perf_counter()`

### 🔍 Comparación para inserción

- **INSERT**


| Estructura         | Sin Indice (ms) | B+ Tree (ms) | ISAM (ms) | Rtree Index(ms)
|--------------------|-----------------|--------------|-----------|-----------|
| Sequential File    | 5.23            | 1.562        | 3.15      |240.34     |


### 🔍 Comparación para búsqueda

- **RANGE SEARCH**
  
| Estructura         | Sin Indice (ms) | B+ Tree (ms) |Rtree Index knn(ms) |Rtree Index Radio(ms) |
|--------------------|-----------------|--------------|--------------------|----------------------|
| Sequential File    | 3.23            | 0.98         |19.78                |   168.27                |

- **SEARCH NAME**
  
| Estructura         | Sin Indice (ms) | ISAM (ms) |
|--------------------|-----------------|-----------|
| Sequential File    | 3.21            | 1.15      |


---

## 💡 Discusión y análisis de los resultados

* **Sequential File**:
    Decidimos implementar una estructura de archivo secuencial complementada con un archivo auxiliar. A continuación detallamos nuestro análisis:

    - 📥 **Inserciones eficientes:**  
      Utilizamos un archivo auxiliar (`aux.bin`) que acumula registros hasta que alcanza un umbral definido (`K`). Esto evita reorganizar constantemente el archivo principal y permite insertar rápidamente nuevos datos.

    - 🔁 **Reorganización por lotes:**  
      Una vez superado el umbral `K`, se realiza una reorganización, ordenando nuevamente todos los registros activos (no eliminados) y consolidándolos en el archivo principal (`.bin`).

    - 🔍 **Búsqueda y eliminación costosa:**  
      Para estas operaciones es necesario recorrer completamente ambos archivos, lo que puede impactar el rendimiento a medida que crece el número de registros.

    - ⚖️ **Compensación mediante índices:**  
      Aunque el acceso secuencial no es ideal para consultas dinámicas, este diseño se ve beneficiado al usarse junto a estructuras como `B+ Tree` o `ISAM`, que reducen el número de accesos requeridos.
  
* **ISAM**:
  Decidimos implementar un índice ISAM de dos niveles con páginas almacenadas en disco. Esta estructura nos permitió mejorar la eficiencia en búsquedas puntuales y por rango. A continuación detallamos los resultados observados:

  - 📄 **Estructura jerárquica en disco:**
    Dividimos el índice en páginas de hojas con claves ordenadas y punteros de desbordamiento. Esto facilitó una búsqueda eficiente y mantenible en disco.

  - 📥 **Inserciones con encadenamiento:**
    Al llegar a la capacidad máxima de una página, los nuevos elementos se insertaban en páginas de desbordamiento. Esta decisión nos permitió mantener el orden sin necesidad de reorganización costosa.

  - 🔍 **Búsquedas exactas eficientes:**
    Utilizando las claves de división (`split_keys`), localizamos rápidamente la página hoja correspondiente y luego escaneamos internamente. Esto resultó más eficiente que la búsqueda secuencial directa.

  - 📈 **Soporte para rangos ordenados:**
    Implementamos `range_search` recorriendo páginas consecutivas desde el punto de inicio, incluyendo las páginas de desbordamiento. Esto nos permitió usar ISAM también para consultas tipo `BETWEEN`.

  - 💾 **Persistencia total:**
    Tanto las páginas como los metadatos (`split_keys` y `leaf_offsets`) se serializan con `pickle`, garantizando que el índice pueda restaurarse exactamente como estaba tras reiniciar el sistema.
* **B+ Tree**:
    Para mejorar las búsquedas por rango y por clave específica, incorporamos un índice B+ Tree sobre columnas como `price`. Nuestra implementación:

    - **Enlaza las hojas del árbol:**  
      Esto permitió que la búsqueda por rangos (`BETWEEN`) fuera muy eficiente, ya que bastaba recorrer las hojas adyacentes sin necesidad de volver al nodo raíz.

    - **Almacena directamente los valores (IDs) en las hojas:**  
      Esto simplificó la recuperación de registros, evitando búsquedas adicionales.

    - **Mayor velocidad en búsquedas específicas:**  
      En pruebas prácticas, las búsquedas con B+ Tree fueron notablemente más rápidas que con el archivo secuencial, especialmente cuando el árbol estaba bien balanceado.
* **Extendible Hashing**:
    Decidimos implementar un índice de tipo Extendible Hashing que guarda los buckets en disco y ajusta dinámicamente su profundidad. Nuestro análisis es el siguiente:

    - **Reorganización automática de buckets:**  
      Cuando un bucket se llena, este se divide y, si es necesario, se incrementa la profundidad global. Esto permitió mantener baja la tasa de colisiones incluso con grandes volúmenes de datos.

    - **Persistencia con archivos `.dat`:**  
      Tanto los buckets como el directorio son persistentes gracias al uso de `pickle`, lo que facilita la recuperación del índice incluso después de cerrar el programa.

    - **Búsquedas exactas rápidas:**  
      La función de hash basada en `md5` y binarización permitió acceder directamente al bucket correspondiente, logrando búsquedas muy rápidas para claves exactas (como IDs).

    - **Soporte para búsquedas por rango:**  
      Aunque no tan eficiente como un B+ Tree para rangos, implementamos una función de `range_search` que recorre los buckets sin repetirlos y permite recuperar datos en intervalos.

    - **Eliminación sencilla pero no compacta:**  
      La eliminación borra los elementos del bucket, pero no reorganiza el índice ni compacta los buckets, lo que puede generar fragmentación si se hacen muchas eliminaciones.
* **Rtree Index**:
   - **Datos espaciales en disco**:  
    El R-tree utiliza un archivo `.dat` para almacenar los nodos (hojas y no hojas), donde cada entrada contiene **rectángulos mínimos acotados (MBR)** que agrupan   objetos geográficos.  
  - **Dos columnas indexadas**: Latitud y Longitud, lo que permite búsquedas basadas en ubicación.  
  - **Persistencia**: Los nodos se serializan (ej.: con `pickle` o formato binario) para garantizar consistencia tras reinicios.
  - **Range Query con radio**:  
    Permite buscar todos los puntos dentro de un **radio circular** alrededor de una coordenada central.  
  - **Optimización**: El R-tree filtra rápidamente MBRs que no intersectan el círculo, reduciendo accesos a disco.  
 **K-Nearest Neighbors (KNN)**:  
   - Busca los **K puntos más cercanos** a una coordenada dada (ej.: "3 hospitales más cercanos").  
   - **Algoritmo**: Usa búsqueda heurística por prioridad (ej.: distancia al MBR) para minimizar accesos.
   **Escenarios ideales**:  
  - Aplicaciones de geolocalización (ej.: mapas, logística).  
  - Consultas con filtros espaciales complejos (intersecciones, áreas poligonales).  
- **Alternativas**:  
  - Para datos no espaciales, un **B+ Tree** o **Hashing** puede ser más eficiente. 
---
# 🚀 PROYECTO 2 – Base de Datos II

## 🎯 Objetivo del proyecto

## Construcción del Índice Invertido Textual (Full-Text Search)

## Indexación de descriptores locales (Multimedia Database)

## Frontend

## Experimentación

## 🎥 Presentación

* **Presentación**: [Ver en Google Drive](https://drive.google.com/drive/folders/1eaTNyh7sq1uyJGuJVUF00FDP35Gt_up3?usp=sharing)
