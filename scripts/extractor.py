#!/usr/bin/env python3
import os
import requests
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import logging

# Cargar variables de entorno
load_dotenv()

# Configurar logging profesional
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl_omdb.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class OMDbExtractor:
    def __init__(self):
        # Configuración de credenciales
        self.api_key = os.getenv('API_KEY', '70dd038a')
        self.base_url = os.getenv('OMDB_BASE_URL', 'http://www.omdbapi.com/')
        
        if not self.api_key:
            raise ValueError("⚠️ API_KEY no configurada. Verifica tu archivo .env")

    def buscar_por_termino(self, termino, paginas=2):
        """Busca películas por una palabra clave y devuelve una lista de IDs"""
        ids_encontrados = []
        logger.info(f"🔍 Buscando término: '{termino}'...")
        
        for page in range(1, paginas + 1):
            try:
                params = {
                    's': termino,
                    'apikey': self.api_key,
                    'page': page,
                    'type': 'movie'
                }
                response = requests.get(self.base_url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()

                if data.get('Response') == 'True':
                    for movie in data.get('Search', []):
                        if movie.get('imdbID'):
                            ids_encontrados.append(movie.get('imdbID'))
                    logger.info(f"✅ Página {page} de '{termino}' procesada.")
                else:
                    logger.warning(f"No hay más resultados para '{termino}' en página {page}: {data.get('Error')}")
                    break
            except Exception as e:
                logger.error(f"Error en búsqueda: {str(e)}")
        
        return list(set(ids_encontrados)) 

    def extraer_detalle_pelicula(self, movie_id):
        """Extrae el detalle completo de una película usando su ID"""
        try:
            params = {
                'i': movie_id,
                'apikey': self.api_key,
                'plot': 'short'
            }
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"❌ Error con ID {movie_id}: {str(e)}")
            return None

    def procesar_datos(self, data):
        """Estructura los datos para el DataFrame e INCLUYE EL POSTER"""
        if not data or data.get('Response') == 'False':
            return None
        
        return {
            'titulo': data.get('Title'),
            'anio': data.get('Year'),
            'genero': data.get('Genre'),
            'director': data.get('Director'),
            'actores': data.get('Actors'),
            'poster': data.get('Poster'),  # <--- CRÍTICO PARA EL DASHBOARD
            'calificacion_imdb': data.get('imdbRating'),
            'votos_imdb': data.get('imdbVotes'),
            'id_imdb': data.get('imdbID'),
            'plot': data.get('Plot'),      # <--- Trama de la película
            'tipo': data.get('Type'),
            'fecha_extraccion': datetime.now().isoformat()
        }

    def ejecutar_etl_masivo(self, busquedas_lista):
        """Orquestador del proceso completo"""
        todos_los_ids = []
        for busqueda in busquedas_lista:
            ids = self.buscar_por_termino(busqueda, paginas=2) 
            todos_los_ids.extend(ids)
        
        total_ids = list(set(todos_los_ids))
        
        if not total_ids:
            logger.error("🚫 No se encontró ningún ID. Revisa tu conexión o API Key.")
            return []

        logger.info(f"📊 Se encontraron {len(total_ids)} IDs únicos. Iniciando descarga de detalles...")

        resultados_finales = []
        for i, mid in enumerate(total_ids):
            if i % 5 == 0:
                logger.info(f"⏳ Procesando: {i}/{len(total_ids)}...")
            
            detalle = self.extraer_detalle_pelicula(mid)
            limpio = self.procesar_datos(detalle)
            if limpio:
                resultados_finales.append(limpio)
        
        return resultados_finales

if __name__ == "__main__":
    # Ampliamos los temas para tener una base de datos robusta
    TEMAS_A_BUSCAR = ['Marvel', 'Star Wars', 'Batman', 'Disney', 'Inception', 'Avengers', 'Comedy', 'Horror']
    
    try:
        extractor = OMDbExtractor()
        datos_finales = extractor.ejecutar_etl_masivo(TEMAS_A_BUSCAR)
        
        if datos_finales:
            os.makedirs('data', exist_ok=True)
            
            # Guardar CSV (esto es lo que lee tu dashboard)
            df = pd.DataFrame(datos_finales)
            df.to_csv('data/peliculas.csv', index=False, encoding='utf-8')
            
            # Guardar JSON para respaldo
            with open('data/peliculas_raw.json', 'w', encoding='utf-8') as f:
                json.dump(datos_finales, f, indent=2, ensure_ascii=False)
            
            logger.info(f"🏆 ¡ÉXITO! Se procesaron {len(df)} películas en total con sus carátulas.")
            print(f"\nExtracción finalizada. Archivo guardado en data/peliculas.csv")
            print(df[['titulo', 'anio', 'calificacion_imdb']].head())
        else:
            logger.error("No se pudieron obtener datos.")
            
    except Exception as e:
        logger.error(f"Error crítico en el sistema: {str(e)}")