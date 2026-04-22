#!/usr/bin/env python3
import os
import requests
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import logging
import time

# Cargar variables de entorno
load_dotenv()

# Configurar logging profesional
os.makedirs('logs', exist_ok=True)
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
        # Configuración de credenciales (Usa tu API_KEY del .env o la de respaldo)
        self.api_key = os.getenv('API_KEY', '70dd038a')
        self.base_url = os.getenv('OMDB_BASE_URL', 'http://www.omdbapi.com/')
        
        if not self.api_key:
            raise ValueError("⚠️ API_KEY no configurada. Verifica tu archivo .env")

    def buscar_por_termino(self, termino, paginas=10):
        """Busca películas por una palabra clave y devuelve una lista de IDs únicos"""
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
                else:
                    # Si la API dice que no hay más resultados, rompemos el bucle de páginas
                    break
            except Exception as e:
                logger.error(f"❌ Error en búsqueda de '{termino}' (pág {page}): {str(e)}")
                break
        
        return ids_encontrados

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
        """Estructura los datos para el DataFrame e incluye limpieza básica"""
        if not data or data.get('Response') == 'False':
            return None
        
        return {
            'titulo': data.get('Title'),
            'anio': data.get('Year'),
            'genero': data.get('Genre'),
            'director': data.get('Director'),
            'actores': data.get('Actors'),
            'poster': data.get('Poster'),
            'calificacion_imdb': data.get('imdbRating'),
            'votos_imdb': data.get('imdbVotes'),
            'id_imdb': data.get('imdbID'),
            'plot': data.get('Plot'),
            'tipo': data.get('Type'),
            'fecha_extraccion': datetime.now().isoformat()
        }

    def ejecutar_etl_masivo(self, busquedas_lista, paginas_por_tema=15):
        """Orquestador para obtener miles de películas con salvaguarda de errores"""
        todos_los_ids = []
        
        try:
            # 1. Fase de Recolección de IDs
            for busqueda in busquedas_lista:
                ids = self.buscar_por_termino(busqueda, paginas=paginas_por_tema)
                todos_los_ids.extend(ids)
                time.sleep(0.1)
            
            total_ids = list(set(todos_los_ids)) # Quitar duplicados
            
            if not total_ids:
                logger.error("🚫 No se encontró ningún ID.")
                return []

            logger.info(f"📊 Se encontraron {len(total_ids)} IDs únicos. Iniciando descarga...")

            # 2. Fase de Extracción de Detalles
            resultados_finales = []
            for i, mid in enumerate(total_ids):
                if i % 20 == 0:
                    logger.info(f"⏳ Progreso: {i}/{len(total_ids)} películas procesadas...")
                
                # SI LA API DA ERROR AQUÍ, CAPTURAMOS Y RETORNAMOS LO QUE LLEVAMOS
                try:
                    detalle = self.extraer_detalle_pelicula(mid)
                    limpio = self.procesar_datos(detalle)
                    
                    if limpio and limpio.get('calificacion_imdb') != 'N/A':
                        resultados_finales.append(limpio)
                except Exception as api_err:
                    logger.warning(f"⚠️ Error en ID {mid}, deteniendo y salvando lo obtenido: {api_err}")
                    break # Sale del bucle de películas pero sigue al return

                # Límite de seguridad
                if i >= 1950: 
                    logger.warning("⚠️ Límite alcanzado. Deteniendo para salvar.")
                    break
            
            return resultados_finales

        except Exception as e:
            logger.error(f"💥 Error inesperado en el proceso masivo: {e}")
            return [] # En caso de error total

if __name__ == "__main__":
    TEMAS_A_BUSCAR = [
        'the', 'a', 'in', 'of', 'for', 'with', 'on', 'at', 'by',
        'happy', 'sad', 'angry', 'lost', 'found', 'broken', 'wild', 'dream',
        'london', 'paris', 'tokyo', 'doctor', 'teacher', 'driver', 'space',
        'one', 'two', 'seven', '2020', '2021', '2022', '2023', '2024'
    ]
    
    datos_finales = []
    extractor = None

    try:
        extractor = OMDbExtractor()
        logger.info("🚀 Iniciando extracción masiva...")
        # Guardamos lo que devuelva la función
        datos_finales = extractor.ejecutar_etl_masivo(TEMAS_A_BUSCAR, paginas_por_tema=50)
        
    except Exception as e:
        logger.error(f"💥 Error crítico en el sistema: {str(e)}")
        
    finally:
        # LÓGICA DE SALVAMENTO:
        # Si la función devolvió datos, o si ocurrio un error pero el objeto tiene datos guardados
        if datos_finales and len(datos_finales) > 0:
            os.makedirs('data', exist_ok=True)
            df = pd.DataFrame(datos_finales)
            
            # Guardar CSV (Sobrescribe el de 519 con los nuevos datos)
            df.to_csv('data/peliculas.csv', index=False, encoding='utf-8')
            
            # Guardar JSON de respaldo
            with open('data/peliculas_raw.json', 'w', encoding='utf-8') as f:
                json.dump(datos_finales, f, indent=2, ensure_ascii=False)
            
            logger.info(f"💾 ¡ÉXITO! Se han salvado {len(df)} películas en 'data/peliculas.csv'.")
            print(f"\n✅ Proceso finalizado. Total recuperado: {len(df)} filas.")
        else:
            logger.error("❌ No se generaron datos nuevos. El archivo CSV no fue actualizado.")