import pandas as pd
import os

# 1. Ajusta la ruta a tu archivo CSV
csv_path = 'data.csv'  # Cambia esto si tu archivo está en otra carpeta, e.g. 'data/data.csv'

# 2. Verifica que el archivo exista
if not os.path.isfile(csv_path):
    raise FileNotFoundError(f"El archivo '{csv_path}' no fue encontrado. "
                            "Comprueba la ruta o el nombre del archivo.")

# 3. Carga el CSV en un DataFrame
df = pd.read_csv(csv_path)

# 4. Define los tamaños de los subconjuntos
tamanos = [1000, 2000, 4000, 8000, 16000, 32000, 44446,64000]

# 5. Crea y guarda cada subset
for N in tamanos:
    if len(df) >= N:
        subset = df.iloc[:N]
        out_name = f'docs_{N}.csv'
        subset.to_csv(out_name, index=False)
        print(f'→ {out_name} creado con {len(subset)} filas')
    else:
        print(f'⚠️ No hay suficientes filas en el DataFrame ({len(df)}) para N = {N}')