# 🚀 PROYECTO 1 – Base de Datos II

## 📚 Introducción

### 🎯 Objetivo del proyecto

Este proyecto desarrolla un motor de almacenamiento capaz de procesar consultas SQL básicas (SELECT, INSERT, DELETE) sobre archivos en disco, aprovechando estructuras de datos avanzadas para optimizar tiempos de respuesta y accesos físicos. Además, incluye una interfaz gráfica en **Flet** que facilita la interacción con los datos y visualiza el comportamiento de cada índice.

### 📝 Descripción de la aplicación

La aplicación consta de dos módulos principales:

1. **Backend**: Implementa las estructuras de almacenamiento (Sequential File, ISAM, B+ Tree, Extendible Hashing) y un parser SQL basado en Lark, que enruta cada consulta a la estructura adecuada.
2. **Frontend (Flet)**: Proporciona una interfaz de escritorio donde el usuario puede listar, buscar y filtrar productos por atributos como categoría, nombre o rango de precio.

### 🤔 ¿Qué esperamos de la aplicación?

* **Rendimiento**: Comparar tiempos de inserción y búsqueda entre las distintas estructuras.
* **Usabilidad**: Ofrecer una GUI intuitiva que refleje en tiempo real los resultados de las consultas.
* **Escalabilidad**: Demostrar que nuestra solución puede adaptarse a tamaños de archivo crecientes sin degradar dramáticamente el desempeño.

---

## 👥 Integrantes

* **Kevin Abraham Huaman Vega**
* **Angel Obed Mora Huamanchay**
* **Lando Fabrizio Veramendi Hilario**
* **Mishelle Stephany Villarreal Falcón**

---

## 🧱 Estructuras Implementadas

### 🔹 Sequential File

* **Función**: Almacena de manera secuencial los registros de tipo `Producto` en un archivo binario.
* **Formato de registro**: Campos fijos (`id`, `name`, `category`, `price`, `image`, `description`) serializados con métodos `to_bytes()` de la clase `Producto`.
* **Link**: [sequential.py](https://github.com/AngelMoraH/proyecto1_bd2/blob/main/backend/algoritmos/sequential.py)

### 🔹 ISAM (Indexed Sequential Access Method)

* **Índice disperso**: Dos niveles de índices que apuntan a bloques de registros.
* **Búsqueda**: Reduce el número de accesos al disco al localizar primero el bloque y luego el registro dentro de éste.
* **Link**: [ISAM implementation](https://github.com/AngelMoraH/proyecto1_bd2/blob/main/backend/algoritmos)

### 🔹 B+ Tree

* **Índice secundario** ordenado por `price`.
* **Operaciones soportadas**: búsqueda exacta, rango, inserción y eliminación con rebalanceo automático.
* **Link**: [bplus\_tree.py](https://github.com/AngelMoraH/proyecto1_bd2/blob/main/backend/algoritmos/bplus_tree.py)

### 🔹 Extendible Hashing

* **Hash dinámico**: Crece de forma flexible mediante directorios y buckets persistentes en `.dat`.
* **Ventaja**: Acceso casi constante por `id`, con redistribución automática al desbordarse.
* **Link**: [extendible\_hashing.py](https://github.com/AngelMoraH/proyecto1_bd2/blob/main/backend/algoritmos/extendible_hashing.py)

---

## 🛠️ Parser SQL

### 🏗️ Construcción con Lark

* Define la gramática SQL (CREATE, SELECT, INSERT, DELETE, BETWEEN, índices).
* Genera un árbol de análisis que se traduce a llamadas al **SequentialFileManager**, **BPlusTree**, **ISAM** o **ExtendibleHashing**, según el índice y la cláusula WHERE.

---

## 📊 Resultados experimentales

### ⏱️ Métricas

* **Número de accesos a disco**
* **Tiempo de ejecución** (ms) medido con `time.perf_counter()`

### 🔍 Comparación para inserción

| Estructura         | Accesos a disco | Tiempo promedio (ms) |
| ------------------ | --------------- | -------------------- |
| Sequential File    | N₁              | T₁                   |
| ISAM               | N₂              | T₂                   |
| B+ Tree            | N₃              | T₃                   |
| Extendible Hashing | N₄              | T₄                   |

### 🔍 Comparación para búsqueda

| Estructura         | Accesos a disco | Tiempo promedio (ms) |
| ------------------ | --------------- | -------------------- |
| Sequential File    | M₁              | U₁                   |
| ISAM               | M₂              | U₂                   |
| B+ Tree            | M₃              | U₃                   |
| Extendible Hashing | M₄              | U₄                   |

---

## 💡 Discusión y análisis de los resultados

* **Sequential File**: sencillo pero costoso en búsquedas.
* **ISAM**: mejora en búsquedas, penalización ligera en inserciones.
* **B+ Tree**: balance ideal entre inserción y consulta de rango.
* **Extendible Hashing**: sobresale en búsquedas exactas por `id`, pero no soporta rango.

---

## 🖥️ Frontend (Flet)

### 🔎 Funcionalidades

* Listado de productos con paginación.
* Búsqueda por categoría, nombre y rango de precio.
* Visualización de métricas en pantalla (tiempos, accesos).

---

## 🎥 Pruebas de uso y presentación

* **Video explicativo**: [Ver en Google Drive](https://drive.google.com/drive/folders/1eaTNyh7sq1uyJGuJVUF00FDP35Gt_up3?usp=sharing)
* **Guía rápida**:

  1. Iniciar la aplicación con `python main.py`.
  2. Ejecutar consultas desde la interfaz.
  3. Observar los logs de rendimiento en tiempo real.

