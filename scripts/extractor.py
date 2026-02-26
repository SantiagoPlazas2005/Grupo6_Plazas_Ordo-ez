#!/usr/bin/env python3
import os
import requests
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import logging

# Cargar variables de entorno (opcional si quieres usar .env)
load_dotenv()

# Configurar logging
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
        # Si quieres usar .env:
        self.api_key = os.getenv('API_KEY', '70dd038a')
        self.base_url = os.getenv('OMDB_BASE_URL', 'http://www.omdbapi.com/')
        # Lista de IMDb IDs que quieras extraer
        self.movie_ids = os.getenv('MOVIE_IDS', 'tt3896198').split(',')
        
        if not self.api_key:
            raise ValueError("API_KEY no configurada")

    def extraer_pelicula(self, movie_id):
        """Extrae datos de una película usando IMDb ID"""
        try:
            params = {
                'i': movie_id.strip(),
                'apikey': self.api_key
            }
            
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('Response') == 'False':
                logger.error(f"❌ Error en API para {movie_id}: {data.get('Error')}")
                return None
            
            logger.info(f"✅ Datos extraídos para {movie_id}")
            return data
        except Exception as e:
            logger.error(f"❌ Error extrayendo datos para {movie_id}: {str(e)}")
            return None

    def procesar_respuesta(self, data):
        """Procesa la respuesta JSON a formato estructurado"""
        try:
            return {
                'titulo': data.get('Title'),
                'anio': data.get('Year'),
                'rated': data.get('Rated'),
                'fecha_lanzamiento': data.get('Released'),
                'duracion': data.get('Runtime'),
                'genero': data.get('Genre'),
                'director': data.get('Director'),
                'escritores': data.get('Writer'),
                'actores': data.get('Actors'),
                'plot': data.get('Plot'),
                'idioma': data.get('Language'),
                'pais': data.get('Country'),
                'premios': data.get('Awards'),
                'calificacion_imdb': data.get('imdbRating'),
                'votos_imdb': data.get('imdbVotes'),
                'tipo': data.get('Type'),
                'fecha_extraccion': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error procesando respuesta: {str(e)}")
            return None

    def ejecutar_extraccion(self):
        """Ejecuta la extracción para todos los IMDb IDs"""
        datos_extraidos = []
        logger.info(f"Iniciando extracción para {len(self.movie_ids)} películas...")
        
        for movie_id in self.movie_ids:
            data = self.extraer_pelicula(movie_id)
            if data:
                datos_extraidos.append(self.procesar_respuesta(data))
        
        return datos_extraidos

if __name__ == "__main__":
    try:
        extractor = OMDbExtractor()
        datos = extractor.ejecutar_extraccion()
        
        # Guardar como JSON
        with open('data/peliculas_raw.json', 'w') as f:
            json.dump(datos, f, indent=2)
        logger.info("📁 Datos guardados en data/peliculas_raw.json")
        
        # Guardar como CSV
        df = pd.DataFrame(datos)
        df.to_csv('data/peliculas.csv', index=False)
        logger.info("📁 Datos guardados en data/peliculas.csv")
        
        print("\n" + "="*50)
        print("RESUMEN DE EXTRACCIÓN")
        print("="*50)
        print(df.to_string())
        print("="*50)
        
    except Exception as e:
        logger.error(f"Error en extracción: {str(e)}")