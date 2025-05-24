# PROYECTO 1 - Base de Datos II

## 📚 Introducción

### Objetivo del proyecto
Este proyecto implementa un motor de almacenamiento para consultas SQL básicas, incluyendo selección, inserción y eliminación, utilizando estructuras físicas avanzadas. Además, cuenta con una interfaz gráfica construida con **Flet** para facilitar la visualización e interacción con los datos.

### Descripción de la aplicación

### ¿Qué esperamos de la aplicación?

---

## 👥 Integrantes
- Huaman Vega, Kevin Abraham
- Mora Huamanchay, Angel Obed
- Veramendi Hilario, Lando Fabrizio
- Villarreal Falcón, Mishelle Stephany

---

## 🧱 Estructuras Implementadas

### 📌 Sequential File
- Almacenamiento principal de los registros (`Producto`) en disco.
- Cada registro incluye campos como `id`, `name`, `category`, `price`, `image` y `description`.
Se puede visualizar la implementación de la estructura en [este link](https://github.com/AngelMoraH/proyecto1_bd2/blob/main/backend/algoritmos/sequential.py)


### 📌 ISAM (Indexed Sequential Access Method)
- Implementación con índice disperso (sparse index) de dos niveles.
- Permite búsqueda eficiente sobre archivos secuenciales.
Se puede visualizar la implementación de la estructura en [este link](https://github.com/AngelMoraH/proyecto1_bd2/blob/main/backend/algoritmos)

### 📌 B+ Tree
- Índice secundario ordenado por atributo `price`.
- Soporta búsqueda exacta, por rango, inserción y eliminación.
Se puede visualizar la implementación de la estructura en [este link](https://github.com/AngelMoraH/proyecto1_bd2/blob/main/backend/algoritmos/bplus_tree.py)

### 📌 Extendible Hashing
- Índice dinámico basado en hashing para búsquedas por `id`.
- Soporta expansión automática y persistencia en archivos `.dat`.
Se puede visualizar la implementación de la estructura en [este link](https://github.com/AngelMoraH/proyecto1_bd2/blob/main/backend/algoritmos/extendible_hashing.py)

---

## 🛠️ Parser SQL
- Construido con **Lark**.
- El parser redirige automáticamente a la estructura adecuada (secuencial, B+ Tree o hashing) dependiendo del atributo consultado.

---

## Resultados experimentales
- Las métricas utilizadas serán el total de accesos a discos duros y tiempo de ejecución en milisegundos

### Comparación entre índices para inserción

### Comparación entre índices para búsqueda

## Discusión y análisis de los resultados

---

## 🖥️ Frontend (Flet)
- Aplicación de escritorio desarrollada con **Flet**.
- Permite:
  - Visualizar productos.
  - Buscar por categoría, nombre o precio.

---

## Pruebas de uso y presentación
- Video explicativo -> en [este link](https://drive.google.com/drive/folders/1eaTNyh7sq1uyJGuJVUF00FDP35Gt_up3?usp=sharing)

