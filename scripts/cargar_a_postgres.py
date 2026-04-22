import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# 1. Cargar configuración del .env
load_dotenv()

# Intentamos sacar la URL del .env
db_url = os.getenv('DATABASE_URL')

# --- CORRECCIÓN CRÍTICA PARA DOCKER ---
# Si estamos dentro de Docker, el host DEBE ser 'db', no 'localhost'
if not db_url:
    # Solo como respaldo si el .env falla
    db_url = "postgresql://santiago:plazas2005@localhost:5432/base_datos_peliculas"
# --------------------------------------

try:
    # 2. Conectar a Postgres
    engine = create_engine(db_url)
    
    # 3. Leer el CSV que generó tu extractor
    if os.path.exists('data/peliculas.csv'):
        print("📖 Leyendo datos del CSV...")
        df = pd.read_csv('data/peliculas.csv')
        
        # 4. Enviar a Postgres
        print(f"🚀 Subiendo {len(df)} películas a PostgreSQL nativo...")
        # Usamos if_exists='replace' para que la tabla se actualice con los miles de datos nuevos
        df.to_sql('peliculas', engine, if_exists='replace', index=False)
        
        print("✅ ¡ÉXITO! Los datos ya están en la base de datos nativa.")
    else:
        print("❌ Error: No se encontró el archivo data/peliculas.csv. ¿Corriste el extractor primero?")

except Exception as e:
    print(f"❌ Error de conexión: {e}")
    print("\n💡 Tip: Asegúrate de que el servicio de la base de datos se llame 'db' en tu docker-compose.yml")