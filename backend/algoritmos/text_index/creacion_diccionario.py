
import os
import sys
from knn_images import build_visual_system_from_scratch
#ya se creó el pkl, no es necesario volver a crearlo, ignorar esta parte 
def main():
    csv_path = "C:/Users/lvera/OneDrive/Escritorio/bd_musica/proyecto1_bd2-main (2)/proyecto1_bd2-main/backend/data/data.csv"
    images_folder = "C:/Users/lvera/OneDrive/Escritorio/bd_musica/archive/images"  
    
    if not os.path.exists(csv_path):
        print(f"Error: No se encontró el archivo CSV en {csv_path}")
        return
    
    if not os.path.exists(images_folder):
        print(f"Error: No se encontró la carpeta de imágenes en {images_folder}")
        return
    
    print(f"CSV: {csv_path}")
    print(f"Imágenes: {images_folder}")
    
    use_sample = input("¿Usar muestra de 5000 imágenes para pruebas? (y/n): ").lower().strip()
    
    if use_sample == 'y':
        sample_size = 5000
        print("Construyendo sistema con muestra de 5000 imágenes...")
    else:
        sample_size = None
        print("Construyendo sistema con todas las imágenes...")
    
    try:
        system = build_visual_system_from_scratch(
            csv_path=csv_path,
            images_folder=images_folder,
            k_clusters=300,  # Número de clusters para el diccionario visual
            sample_size=sample_size
        )
        
        print("\n✅ Sistema visual construido exitosamente!")
        print(f"📁 Archivo guardado: visual_system_complete.pkl")
        print(f"📊 Imágenes procesadas: {len(system['metadata'])}")
        print(f"🎯 Clusters en diccionario: {system['visual_dict'].k}")
        
        # Mostrar estadísticas
        print("\n📈 Estadísticas del sistema:")
        print(f"  - KNN secuencial: {len(system['knn_sequential'].image_histograms)} imágenes indexadas")
        print(f"  - Índice invertido: {len(system['visual_search_engine'].postings)} términos visuales")
        print(f"  - Metadatos: {len(system['metadata'])} registros")
        
    except Exception as e:
        print(f"❌ Error construyendo el sistema: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()