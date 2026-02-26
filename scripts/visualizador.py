#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/visualizador.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Cargar datos
df = pd.read_csv('data/peliculas.csv')

# Convertir columnas numéricas
df['calificacion_imdb'] = pd.to_numeric(df['calificacion_imdb'], errors='coerce')
df['votos_imdb'] = df['votos_imdb'].str.replace(',', '').astype(float)
df['anio'] = pd.to_numeric(df['anio'], errors='coerce')
df['duracion_min'] = df['duracion'].str.extract('(\d+)').astype(float)

# Crear figura con 2x2 subplots
fig, axes = plt.subplots(2, 2, figsize=(15, 10))
fig.suptitle('Análisis de Películas', fontsize=16, fontweight='bold')

# Gráfica 1: Calificación IMDb
ax1 = axes[0, 0]
ax1.bar(df['titulo'], df['calificacion_imdb'], color='#ff6b6b')
ax1.set_title('Calificación IMDb')
ax1.set_ylabel('IMDb Rating')
ax1.tick_params(axis='x', rotation=45)
ax1.grid(axis='y', alpha=0.3)

# Gráfica 2: Año de lanzamiento
ax2 = axes[0, 1]
ax2.bar(df['titulo'], df['anio'], color='#4ecdc4')
ax2.set_title('Año de Lanzamiento')
ax2.set_ylabel('Año')
ax2.tick_params(axis='x', rotation=45)
ax2.grid(axis='y', alpha=0.3)

# Gráfica 3: Duración de la película
ax3 = axes[1, 0]
ax3.bar(df['titulo'], df['duracion_min'], color='#95e1d3')
ax3.set_title('Duración (minutos)')
ax3.set_ylabel('Minutos')
ax3.tick_params(axis='x', rotation=45)
ax3.grid(axis='y', alpha=0.3)

# Gráfica 4: Cantidad de votos IMDb
ax4 = axes[1, 1]
ax4.bar(df['titulo'], df['votos_imdb'], color='#ffa07a')
ax4.set_title('Cantidad de Votos IMDb')
ax4.set_ylabel('Votos')
ax4.tick_params(axis='x', rotation=45)
ax4.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig('data/peliculas_analysis.png', dpi=300, bbox_inches='tight')
logger.info("✅ Gráficas guardadas en data/peliculas_analysis.png")
plt.show()