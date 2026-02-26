#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.express as px
import requests
from datetime import datetime

st.set_page_config(
    page_title="Dashboard Interactivo de Películas",
    page_icon="🎬",
    layout="wide"
)

# CSS personalizado
st.markdown("""
    .metric-box {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
""", unsafe_allow_html=True)

st.title("🎛️ Dashboard Interactivo - Películas OMDb")

# Configuración API
API_KEY = "70dd038a"
OMDB_BASE_URL = "http://www.omdbapi.com/"
MOVIE_IDS = ["tt3896198"]  # Puedes agregar más IDs

def get_movie_data(imdb_id):
    params = {"apikey": API_KEY, "i": imdb_id}
    response = requests.get(OMDB_BASE_URL, params=params)
    if response.status_code == 200 and response.json().get("Response") == "True":
        movie = response.json()
        return {
            "Título": movie.get("Title"),
            "Año": int(movie.get("Year", 0)),
            "Rating IMDB": float(movie.get("imdbRating", 0)),
            "Metascore": int(movie.get("Metascore", 0)) if movie.get("Metascore","N/A")!="N/A" else None,
            "Votos IMDB": int(movie.get("imdbVotes","0").replace(",","")),
            "BoxOffice": movie.get("BoxOffice","N/A"),
            "Género": movie.get("Genre"),
            "Director": movie.get("Director"),
            "Actores": movie.get("Actors"),
            "Idioma": movie.get("Language"),
            "País": movie.get("Country"),
            "Poster": movie.get("Poster")
        }
    return None

# Obtener datos
movies_data = [get_movie_data(mid) for mid in MOVIE_IDS]
df = pd.DataFrame([m for m in movies_data if m is not None])

if not df.empty:
    # Sidebar filtros
    st.sidebar.markdown("### 🔧 Controles")
    
    # Filtro por año
    año_min = int(df['Año'].min())
    año_max = int(df['Año'].max())
    if año_min == año_max:  # Ajuste si solo hay un año
        año_max = año_min + 1

    año_rango = st.sidebar.slider("📅 Rango de Año:", año_min, año_max, (año_min, año_max))
    df_filtrado = df[(df['Año'] >= año_rango[0]) & (df['Año'] <= año_rango[1])]
    
    # Filtro por rating
    rating_min = float(df['Rating IMDB'].min())
    rating_max = float(df['Rating IMDB'].max())
    if rating_min == rating_max:  # Ajuste si todos tienen el mismo rating
        rating_max = rating_min + 1

    rating_rango = st.sidebar.slider("⭐ Rango de Rating IMDB:", rating_min, rating_max, (rating_min, rating_max))
    df_filtrado = df_filtrado[(df_filtrado['Rating IMDB'] >= rating_rango[0]) & (df_filtrado['Rating IMDB'] <= rating_rango[1])]
    
    # KPIs
    st.markdown("### 📊 Indicadores Clave")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("🎬 Total Películas", len(df_filtrado))
    with col2:
        st.metric("⭐ Rating Promedio", f"{df_filtrado['Rating IMDB'].mean():.1f}")
    with col3:
        metascore_avg = df_filtrado['Metascore'].dropna().mean() if not df_filtrado['Metascore'].dropna().empty else 0
        st.metric("📈 Metascore Promedio", f"{metascore_avg:.1f}")
    with col4:
        votos_totales = df_filtrado['Votos IMDB'].sum()
        st.metric("🗳️ Total Votos", f"{votos_totales:,}")
    
    st.markdown("---")
    
    # Gráficas
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Comparativa de Rating IMDB")
        fig = px.bar(
            df_filtrado,
            x='Título',
            y='Rating IMDB',
            color='Rating IMDB',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        st.markdown("#### Votos IMDB")
        fig = px.bar(
            df_filtrado,
            x='Título',
            y='Votos IMDB',
            color='Votos IMDB',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Evolución temporal (por año)
    st.markdown("#### 📈 Evolución por Año")
    df_anno = df_filtrado.groupby('Año')['Rating IMDB'].mean().reset_index()
    fig = px.line(
        df_anno,
        x='Año',
        y='Rating IMDB',
        markers=True,
        title='Rating Promedio por Año'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Tabla interactiva
    st.markdown("#### 📋 Datos Detallados")
    columnas_mostrar = st.multiselect(
        "Columnas a mostrar:",
        df_filtrado.columns.tolist(),
        default=['Título', 'Año', 'Rating IMDB', 'Metascore', 'Votos IMDB', 'BoxOffice']
    )
    st.dataframe(df_filtrado[columnas_mostrar], use_container_width=True, height=400)
    
    # Descarga CSV
    csv = df_filtrado.to_csv(index=False)
    st.download_button(
        label="⬇️ Descargar datos como CSV",
        data=csv,
        file_name=f"peliculas_datos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

else:
    st.warning("⚠️ No hay datos disponibles para los filtros seleccionados")