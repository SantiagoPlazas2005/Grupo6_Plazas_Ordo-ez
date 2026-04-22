#!/usr/bin/env python3
import os
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import logging
import time

# Cargar variables de entorno
load_dotenv()

# Configurar logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('logs/etl_omdb.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class OMDbExtractor:
    def __init__(self):
        self.api_key = os.getenv('API_KEY')
        self.base_url = os.getenv('OMDB_BASE_URL', 'http://www.omdbapi.com/')
        
        if not self.api_key:
            raise ValueError("⚠️ API_KEY no configurada en el .env")

    def buscar_por_termino(self, termino, paginas=20):
        ids_encontrados = []
        logger.info(f"🔍 Buscando: '{termino}'...")
        for page in range(1, paginas + 1):
            try:
                params = {'s': termino, 'apikey': self.api_key, 'page': page, 'type': 'movie'}
                response = requests.get(self.base_url, params=params, timeout=10)
                data = response.json()
                if data.get('Response') == 'True':
                    for movie in data.get('Search', []):
                        if movie.get('imdbID'): ids_encontrados.append(movie.get('imdbID'))
                else: break
            except: break
        return ids_encontrados

    def ejecutar_etl_masivo(self, busquedas_lista):
        todos_los_ids = []
        for busqueda in busquedas_lista:
            todos_los_ids.extend(self.buscar_por_termino(busqueda))
        
        total_ids = list(set(todos_los_ids))
        logger.info(f"📊 {len(total_ids)} IDs únicos. Iniciando descarga con AUTOGUARDADO...")

        resultados = []
        for i, mid in enumerate(total_ids):
            try:
                params = {'i': mid, 'apikey': self.api_key, 'plot': 'short'}
                res = requests.get(self.base_url, params=params, timeout=10)
                d = res.json()
                
                if d.get('Response') == 'True' and d.get('imdbRating') != 'N/A':
                    pelicula = {
                        'titulo': d.get('Title'),
                        'anio': d.get('Year'),
                        'genero': d.get('Genre'),
                        'director': d.get('Director'),
                        'actores': d.get('Actors'),
                        'poster': d.get('Poster'),            # <--- Agregada
                        'calificacion_imdb': d.get('imdbRating'),
                        'votos_imdb': d.get('imdbVotes'),
                        'id_imdb': d.get('imdbID'),
                        'plot': d.get('Plot'),                # <--- Agregada
                        'tipo': d.get('Type'),
                        'fecha_extraccion': datetime.now().isoformat()
                    }
                    resultados.append(pelicula)
                    
                    # --- GUARDADO PREVENTIVO CADA 15 PELÍCULAS ---
                    if len(resultados) % 15 == 0:
                        pd.DataFrame(resultados).to_csv('data/peliculas.csv', index=False)
                
                if i % 50 == 0:
                    logger.info(f"⏳ Progreso: {i}/{len(total_ids)} procesadas (Guardadas: {len(resultados)})")
                
                if i >= 980: # Límite para no quemar la llave de 1000 hoy
                    logger.warning("⚠️ Límite de seguridad alcanzado.")
                    break
            except Exception as e:
                logger.error(f"❌ Error crítico: {e}")
                break
        
        # Guardado final definitivo
        # Busca el guardado final y cámbialo por esto:
        if resultados:
            os.makedirs('data', exist_ok=True)
            nuevo_df = pd.DataFrame(resultados)
            
            # Si el archivo ya existe, los combinamos y quitamos duplicados
            if os.path.exists('data/peliculas.csv'):
                viejo_df = pd.read_csv('data/peliculas.csv')
                df_final = pd.concat([viejo_df, nuevo_df]).drop_duplicates(subset=['id_imdb'])
            else:
                df_final = nuevo_df
                
            df_final.to_csv('data/peliculas.csv', index=False, encoding='utf-8')
            logger.info(f"✅ Dataset actualizado. Total en CSV: {len(df_final)} películas.")
if __name__ == "__main__":
    # Temas variados para asegurar diversidad en la IA
    TEMAS = ['world', 'life', 'love', 'man', 'war', 'star', 'history', 'city', 'night', 'dark', 'blue', 'fire']
    
    try:
        extractor = OMDbExtractor()
        extractor.ejecutar_etl_masivo(TEMAS)
    except Exception as e:
        logger.error(f"💥 Error en el sistema: {e}")