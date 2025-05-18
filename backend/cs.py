import pandas as pd
import json

# 1415fed85fb0f78111c2572fdf57dcd2,"Gorilla Playsets 01-3036-G Malibu Playhouse Wooden Playhouse for Kids, Brown & Green",Toys & Games | Sports & Outdoor Play | Play Sets & Playground Equipment | Play & Swing Sets,"1,265.00"
# Función para limpiar el precio
def limpiar_precio(precio):
    if pd.isna(precio):
        return 0.0
    precio = precio.replace("$", "").replace(",", "").strip()
    if "-" in precio:
        partes = precio.split("-")
        precio = partes[-1].strip()
    try:
        return float(precio)
    except ValueError:
        return 0.0


# Cargar el dataset desde Hugging Face
df = pd.read_csv("hf://datasets/bprateek/amazon_product_description/marketing_sample_for_amazon_com-ecommerce__20200101_20200131__10k_data.csv")

# Filtrar solo las columnas necesarias y renombrarlas
df_filtrado = df[[
    "Uniq Id", "Product Name", "Category", "Selling Price", "Image", "About Product"
]].rename(columns={
    "Uniq Id": "id",
    "Product Name": "name",
    "Category": "category",
    "Selling Price": "price",
    "Image": "image",
    "About Product": "description"
})

df_filtrado["price"] = df_filtrado["price"].apply(limpiar_precio)
# Guardar en un archivo CSV local
df_filtrado.to_csv("productos_amazon.csv", index=False)

# Agrupar por precio para el JSON
agrupado = df_filtrado.groupby("price")["id"].apply(list).to_dict()

# Guardar como JSON
with open("precio_a_ids.json", "w") as f:
    json.dump(agrupado, f, indent=2)

print("✅ Dataset guardado como productos_amazon.csv con solo los campos necesarios")