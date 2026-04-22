#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.express as px
import os

# Configuración de la página
st.set_page_config(
    page_title="Dashboard Pro de Películas",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Título principal
st.title("🎥 Dashboard de Películas Masivo - OMDb Data")
st.markdown("---")

# Función para cargar datos locales (Capa de persistencia)
@st.cache_data # Esto optimiza el dashboard para que no recargue el archivo cada segundo
def load_local_data():
    file_path = 'data/peliculas.csv'
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        # Aseguramos que los tipos de datos sean correctos para las gráficas
        df['calificacion_imdb'] = pd.to_numeric(df['calificacion_imdb'], errors='coerce').fillna(0)
        # Limpiamos los votos: quitamos comas y convertimos a número
        if 'votos_imdb' in df.columns:
            df['votos_imdb'] = df['votos_imdb'].astype(str).str.replace(',', '').replace('N/A', '0')
            df['votos_imdb'] = pd.to_numeric(df['votos_imdb'], errors='coerce').fillna(0)
        return df
    else:
        return None

# Intentar cargar los datos
df = load_local_data()

if df is not None:
    # --- SIDEBAR CON FILTROS AVANZADOS ---
    st.sidebar.title("🔧 Filtros de Búsqueda")
    
    # Filtro por Título
    search_query = st.sidebar.text_input("Buscar película por nombre:", "")
    
    # Filtro por Género (Multiselect)
    # Extraemos todos los géneros únicos (manejando que vienen separados por coma)
    todos_generos = set()
    df['genero'].dropna().str.split(', ').apply(todos_generos.update)
    generos_lista = sorted(list(todos_generos))
    
    selected_genres = st.sidebar.multiselect("Filtrar por Géneros:", options=generos_lista)

    # Lógica de filtrado
    df_filtrado = df.copy()
    if search_query:
        df_filtrado = df_filtrado[df_filtrado['titulo'].str.contains(search_query, case=False)]
    if selected_genres:
        # Filtrar si alguno de los géneros seleccionados está en la columna género
        df_filtrado = df_filtrado[df_filtrado['genero'].apply(lambda x: any(g in str(x) for g in selected_genres))]

    # --- MÉTRICAS PRINCIPALES ---
    st.subheader(f"📊 Análisis de {len(df_filtrado)} Películas Filtradas")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="⭐ Rating IMDB Promedio",
            value=f"{df_filtrado['calificacion_imdb'].mean():.1f}"
        )

    with col2:
        # Contamos cuántas películas son "Top Rated" (> 8.0)
        top_movies = len(df_filtrado[df_filtrado['calificacion_imdb'] >= 8.0])
        st.metric(label="🏆 Películas Top (>8.0)", value=top_movies)

    with col3:
        votos_total = int(df_filtrado['votos_imdb'].sum())
        st.metric(label="🗳️ Total Votos en Selección", value=f"{votos_total:,}")

    st.markdown("---")

    # --- VISUALIZACIONES ---
    st.subheader("📉 Comparativas Visuales")
    col_viz1, col_viz2 = st.columns(2)

    with col_viz1:
        # Gráfica de los mejores ratings (Top 10 de la selección)
        top_10_rating = df_filtrado.nlargest(10, 'calificacion_imdb')
        fig_rating = px.bar(
            top_10_rating,
            x='calificacion_imdb',
            y='titulo',
            orientation='h',
            color='calificacion_imdb',
            title="Top 10 Películas por Rating",
            labels={'calificacion_imdb': 'Rating', 'titulo': 'Película'},
            color_continuous_scale='Reds'
        )
        st.plotly_chart(fig_rating, use_container_width=True)

    with col_viz2:
        # Gráfica de distribución por año
        fig_anio = px.histogram(
            df_filtrado,
            x='anio',
            title="Distribución de Películas por Año",
            labels={'anio': 'Año de Lanzamiento'},
            color_discrete_sequence=['#00CC96']
        )
        st.plotly_chart(fig_anio, use_container_width=True)

    # --- TABLA Y DETALLES ---
    st.subheader("📋 Base de Datos Completa")
    st.dataframe(df_filtrado, use_container_width=True)

else:
    st.error("❌ No se encontró el archivo 'data/peliculas.csv'. Por favor, corre primero el extractor: `python scripts/extractor.py`.")