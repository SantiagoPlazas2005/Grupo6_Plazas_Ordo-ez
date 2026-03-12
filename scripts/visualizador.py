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
        # Usamos tu API KEY que ya sabemos que funciona
        self.api_key = os.getenv('API_KEY', '70dd038a')
        self.base_url = os.getenv('OMDB_BASE_URL', 'http://www.omdbapi.com/')
        
        # DEFINICIÓN DE BÚSQUEDA: Aquí es donde "sacamos todas"
        # Editando esta lista obtendrás diferentes colecciones
        self.search_terms = ['Marvel', 'Star Wars', 'Batman', 'Avengers', 'Disney']

    def buscar_ids_por_termino(self, termino):
        """Busca en la API y retorna una lista de IDs de películas"""
        ids = []
        try:
            # Traeremos las primeras 2 páginas de resultados (20 películas por término)
            for page in range(1, 3): 
                params = {
                    's': termino,
                    'apikey': self.api_key,
                    'page': page,
                    'type': 'movie'
                }
                response = requests.get(self.base_url, params=params, timeout=10)
                data = response.json()
                
                if data.get('Response') == 'True':
                    for item in data.get('Search', []):
                        ids.append(item.get('imdbID'))
            return ids
        except Exception as e:
            logger.error(f"Error buscando término {termino}: {e}")
            return []

    def extraer_detalle(self, movie_id):
        """Obtiene la información completa de un ID específico"""
        try:
            params = {'i': movie_id, 'apikey': self.api_key}
            response = requests.get(self.base_url, params=params, timeout=10)
            return response.json()
        except Exception:
            return None

    def procesar(self, data):
        """Limpia y estructura los datos"""
        if not data or data.get('Response') == 'False': return None
        return {
            'titulo': data.get('Title'),
            'anio': data.get('Year'),
            'rated': data.get('Rated'),
            'genero': data.get('Genre'),
            'director': data.get('Director'),
            'actores': data.get('Actors'),
            'calificacion_imdb': data.get('imdbRating'),
            'votos_imdb': data.get('imdbVotes'),
            'id_imdb': data.get('imdbID'),
            'fecha_extraccion': datetime.now().isoformat()
        }

    def ejecutar(self):
        todos_los_datos = []
        ids_totales = []

        # 1. Recolectar todos los IDs basados en nuestros temas
        for t in self.search_terms:
            logger.info(f"🔍 Buscando películas de: {t}")
            ids_totales.extend(self.buscar_ids_por_termino(t))
        
        # Eliminar duplicados
        ids_unicos = list(set(ids_totales))
        logger.info(f"🚀 Total de películas a procesar: {len(ids_unicos)}")

        # 2. Extraer detalle de cada una
        for i, mid in enumerate(ids_unicos):
            logger.info(f"[{i+1}/{len(ids_unicos)}] Extrayendo detalle de {mid}")
            raw_data = self.extraer_detalle(mid)
            limpio = self.procesar(raw_data)
            if limpio:
                todos_los_datos.append(limpio)
        
        return todos_los_datos

if __name__ == "__main__":
    extractor = OMDbExtractor()
    resultados = extractor.ejecutar()

    if resultados:
        # Guardar JSON
        with open('data/peliculas_raw.json', 'w', encoding='utf-8') as f:
            json.dump(resultados, f, indent=2, ensure_ascii=False)
        
        # Guardar CSV
        df = pd.DataFrame(resultados)
        df.to_csv('data/peliculas.csv', index=False, encoding='utf-8')
        
        logger.info("📁 ETL Finalizado. Archivos actualizados en /data")
        print(f"\nSe han extraído {len(df)} películas exitosamente.")