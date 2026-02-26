#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.express as px
import requests
from datetime import datetime, timedelta

# Configuración de la página
st.set_page_config(
    page_title="Dashboard Avanzado de Películas",
    page_icon="🎬",
    layout="wide"
)

st.title("🎥 Dashboard Avanzado - OMDb API")
st.markdown("---")

# Configuración API
API_KEY = "70dd038a"
OMDB_BASE_URL = "http://www.omdbapi.com/"
MOVIE_IDS = ["tt3896198"]  # Puedes agregar más IDs

def get_movie_data(imdb_id):
    params = {"apikey": API_KEY, "i": imdb_id}
    response = requests.get(OMDB_BASE_URL, params=params)
    if response.status_code == 200:
        return response.json()
    return None

# Obtener datos de las películas
movies_data = []
for mid in MOVIE_IDS:
    movie = get_movie_data(mid)
    if movie and movie.get("Response") == "True":
        movies_data.append({
            "Título": movie.get("Title"),
            "Año": movie.get("Year"),
            "Rating IMDB": float(movie.get("imdbRating", 0)),
            "Metascore": int(movie.get("Metascore", 0)) if movie.get("Metascore","N/A")!="N/A" else None,
            "Votos IMDB": int(movie.get("imdbVotes","0").replace(",","")),
            "BoxOffice": movie.get("BoxOffice", "N/A"),
            "Género": movie.get("Genre"),
            "Director": movie.get("Director"),
            "Actores": movie.get("Actors"),
            "Idioma": movie.get("Language"),
            "País": movie.get("Country"),
            "Poster": movie.get("Poster"),
        })

df_movies = pd.DataFrame(movies_data)

# Pestañas principales
tab1, tab2, tab3, tab4 = st.tabs(["📊 Vista General", "📈 Histórico", "🔍 Análisis", "📋 Detalles Películas"])

with tab1:
    st.subheader("Métricas Generales")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("🎬 Total Películas", len(df_movies))
    with col2:
        st.metric("⭐ Rating IMDB Promedio", f"{df_movies['Rating IMDB'].mean():.1f}")
    with col3:
        metascore_avg = df_movies['Metascore'].dropna().mean() if not df_movies['Metascore'].dropna().empty else 0
        st.metric("📈 Metascore Promedio", f"{metascore_avg:.1f}")
    
    st.markdown("---")
    
    # Gráficas rápidas
    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(df_movies, x='Título', y='Rating IMDB', color='Rating IMDB', color_continuous_scale='Blues',
                     title="Rating IMDB por Película")
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.bar(df_movies, x='Título', y='Votos IMDB', color='Votos IMDB', color_continuous_scale='Viridis',
                     title="Votos IMDB por Película")
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.subheader("Análisis Histórico (por Año)")
    df_movies['Año'] = pd.to_numeric(df_movies['Año'], errors='coerce')
    
    fig = px.line(df_movies, x='Año', y='Rating IMDB', markers=True,
                  title="Rating IMDB a lo Largo de los Años")
    st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.subheader("Análisis Detallado por Película")
    
    for _, row in df_movies.iterrows():
        with st.expander(f"🎬 {row['Título']} ({row['Año']})"):
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("⭐ IMDB Rating", f"{row['Rating IMDB']}")
            with col2:
                st.metric("📈 Metascore", f"{row['Metascore'] if row['Metascore'] else 'N/A'}")
            with col3:
                st.metric("🗳️ Votos IMDB", f"{row['Votos IMDB']:,}")
            with col4:
                st.metric("💰 BoxOffice", row['BoxOffice'])
            st.markdown(f"**Director:** {row['Director']}")
            st.markdown(f"**Actores:** {row['Actores']}")
            st.markdown(f"**Género:** {row['Género']}")
            st.markdown(f"**Idioma / País:** {row['Idioma']} / {row['País']}")
            st.image(row['Poster'], width=200)

with tab4:
    st.subheader("📋 Tabla Completa")
    st.dataframe(df_movies, use_container_width=True)