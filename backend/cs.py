import pandas as pd

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

# Guardar en un archivo CSV local
df_filtrado.to_csv("productos_amazon.csv", index=False)

print("✅ Dataset guardado como productos_amazon.csv con solo los campos necesarios")