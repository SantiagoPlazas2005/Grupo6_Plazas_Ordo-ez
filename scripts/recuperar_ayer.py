import pandas as pd
import json
import os
from sqlalchemy import create_engine

# --- CONFIGURACIÓN DE CONEXIÓN ---
DB_URL = "postgresql://santiago:plazas2005@localhost:5432/base_datos_peliculas"

def recuperar_datos_json():
    ruta_json = 'data/peliculas_raw.json'
    
    if not os.path.exists(ruta_json):
        print(f"❌ No se encontró el archivo: {ruta_json}")
        return

    try:
        engine = create_engine(DB_URL)
        
        print(f"📖 Leyendo datos desde {ruta_json}...")
        
        # Leemos como texto para limpiar posibles errores de pegado manual
        with open(ruta_json, 'r', encoding='utf-8') as f:
            contenido = f.read()
        
        # Reparamos el formato si es necesario
        contenido_limpio = contenido.replace('][', ',').replace('] [', ',')
        
        # Cargamos el JSON
        datos = json.loads(contenido_limpio)
        df = pd.DataFrame(datos)
        
        # Limpieza de duplicados interna para no subir basura
        df = df.drop_duplicates(subset=['id_imdb'])
        
        print(f"🚀 Subiendo {len(df)} películas del JSON a PostgreSQL (Modo APPEND)...")
        
        # EL PASO MÁS IMPORTANTE: Usar 'append' para que NO borre lo que ya hay
        df.to_sql('peliculas', engine, if_exists='append', index=False)
        
        print("✅ ¡LOGRADO! Las películas del JSON se han sumado a la base de datos.")

    except Exception as e:
        print(f"❌ Error al recuperar datos: {e}")

if __name__ == "__main__":
    recuperar_datos_json()