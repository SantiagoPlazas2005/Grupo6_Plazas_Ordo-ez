#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import sys
from datetime import datetime

# Configuración de la página
st.set_page_config(
    page_title="Dashboard de Películas",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Título principal
st.title("🎥 Dashboard de Películas - OMDb API")
st.markdown("---")

# Configuración de la API
API_KEY = "70dd038a"
OMDB_BASE_URL = "http://www.omdbapi.com/"
MOVIE_IDS = ["tt3896198"]  # Puedes agregar más IDs de películas

# Función para obtener datos de la API
def get_movie_data(imdb_id):
    params = {"apikey": API_KEY, "i": imdb_id}
    response = requests.get(OMDB_BASE_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Obtener datos de las películas
movies_data = []
for movie_id in MOVIE_IDS:
    movie = get_movie_data(movie_id)
    if movie and movie.get("Response") == "True":
        movies_data.append({
            "Título": movie.get("Title"),
            "Año": movie.get("Year"),
            "Rating IMDB": float(movie.get("imdbRating", 0)),
            "Metascore": int(movie.get("Metascore", 0)) if movie.get("Metascore", "N/A") != "N/A" else None,
            "Votos IMDB": int(movie.get("imdbVotes", "0").replace(",", "")),
            "BoxOffice": movie.get("BoxOffice", "N/A"),
            "Género": movie.get("Genre"),
            "Director": movie.get("Director"),
            "Actores": movie.get("Actors"),
            "Idioma": movie.get("Language"),
            "País": movie.get("Country"),
            "Poster": movie.get("Poster"),
        })

# Convertir a DataFrame
df = pd.DataFrame(movies_data)

# Sidebar con filtros
st.sidebar.title("🔧 Filtros")
selected_movies = st.sidebar.multiselect(
    "Selecciona películas:",
    options=df['Título'],
    default=df['Título'].tolist()
)

df_filtrado = df[df['Título'].isin(selected_movies)]

# Métricas principales
st.subheader("📊 Métricas Principales")
col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        label="⭐ Rating IMDB Promedio",
        value=f"{df_filtrado['Rating IMDB'].mean():.1f}"
    )

with col2:
    metascore_prom = df_filtrado['Metascore'].dropna().mean() if not df_filtrado['Metascore'].dropna().empty else 0
    st.metric(
        label="📈 Metascore Promedio",
        value=f"{metascore_prom:.1f}"
    )

with col3:
    votos_total = df_filtrado['Votos IMDB'].sum()
    st.metric(
        label="🗳️ Total Votos IMDB",
        value=f"{votos_total:,}"
    )

st.markdown("---")

# Gráficas
st.subheader("📉 Visualizaciones")

col1, col2 = st.columns(2)

with col1:
    fig_rating = px.bar(
        df_filtrado,
        x='Título',
        y='Rating IMDB',
        color='Rating IMDB',
        color_continuous_scale='Blues',
        title="Rating IMDB por Película"
    )
    st.plotly_chart(fig_rating, use_container_width=True)

with col2:
    fig_votes = px.bar(
        df_filtrado,
        x='Título',
        y='Votos IMDB',
        color='Votos IMDB',
        color_continuous_scale='Viridis',
        title="Votos IMDB por Película"
    )
    st.plotly_chart(fig_votes, use_container_width=True)

# Tabla detallada
st.subheader("📋 Datos Detallados")
st.dataframe(df_filtrado, use_container_width=True, height=400)

# Mostrar poster de la película seleccionada
st.subheader("🎬 Posters")
for _, row in df_filtrado.iterrows():
    st.markdown(f"### {row['Título']} ({row['Año']})")
    st.image(row['Poster'], width=200)
    st.markdown(f"**Director:** {row['Director']}")
    st.markdown(f"**Actores:** {row['Actores']}")
    st.markdown(f"**Género:** {row['Género']}")
    st.markdown(f"**BoxOffice:** {row['BoxOffice']}")
    st.markdown("---")