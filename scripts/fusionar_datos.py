import json
import pandas as pd
import os

# Nombres de tus archivos (ajusta si tienen otros nombres)
archivo_viejo = 'data/copia_respaldo.json' 
archivo_nuevo = 'data/peliculas_raw.json'
archivo_final_csv = 'data/peliculas.csv'

def fusionar():
    try:
        # 1. Leer ambos archivos
        with open(archivo_viejo, 'r', encoding='utf-8') as f:
            datos_viejos = json.load(f)
        
        with open(archivo_nuevo, 'r', encoding='utf-8') as f:
            datos_nuevos = json.load(f)
        
        # 2. Combinar listas
        lista_total = datos_viejos + datos_nuevos
        
        # 3. Convertir a DataFrame para limpiar duplicados por ID de IMDb
        df = pd.DataFrame(lista_total)
        df_limpio = df.drop_duplicates(subset=['id_imdb'])
        
        # 4. Guardar de nuevo en CSV y JSON
        df_limpio.to_csv(archivo_final_csv, index=False, encoding='utf-8')
        
        with open(archivo_nuevo, 'w', encoding='utf-8') as f:
            json.dump(df_limpio.to_dict('records'), f, indent=2, ensure_ascii=False)
            
        print(f"✅ ¡Fusión exitosa! Ahora tienes {len(df_limpio)} películas únicas.")
        
    except Exception as e:
        print(f"❌ Error al fusionar: {e}")

if __name__ == "__main__":
    fusionar()