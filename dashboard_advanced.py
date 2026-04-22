#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime

# Configuración de la página
st.set_page_config(
    page_title="CineData Advanced Analytics",
    page_icon="🎬",
    layout="wide"
)

# --- ESTILOS PERSONALIZADOS ---
st.markdown("""
    <style>
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { height: 50px; white-space: pre-wrap; background-color: #1e2130; border-radius: 5px; padding: 10px; color: white; }
    .stTabs [aria-selected="true"] { background-color: #ff4b4b; }
    </style>
""", unsafe_allow_html=True)

# --- LÓGICA DE CARGA DE DATOS ---
PATH_CSV = 'data/peliculas.csv'

def validar_url_poster(url):
    """Evita el error de float y maneja valores nulos de posters."""
    if pd.isna(url) or not isinstance(url, str) or url == "N/A" or url.strip() == "":
        return "https://via.placeholder.com/300x450?text=Sin+Imagen"
    return url

@st.cache_data
def load_full_data():
    if not os.path.exists(PATH_CSV):
        return pd.DataFrame()
    
    df = pd.read_csv(PATH_CSV)
    # Limpieza profunda
    df['calificacion_imdb'] = pd.to_numeric(df['calificacion_imdb'], errors='coerce').fillna(0)
    df['anio_num'] = pd.to_numeric(df['anio'].astype(str).str.extract('(\d{4})')[0], errors='coerce')
    df['votos_imdb'] = df['votos_imdb'].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce').fillna(0)
    return df

df_movies = load_full_data()

# --- INTERFAZ PRINCIPAL ---
st.title("🎥 Dashboard de Análisis Cinematográfico")

if not df_movies.empty:
    # Sidebar para filtros globales
    st.sidebar.header("🔎 Filtros Globales")
    generos = sorted(list(set([g.strip() for sublist in df_movies['genero'].fillna("Sin Género").str.split(',') for g in sublist])))
    generos_sel = st.sidebar.multiselect("Filtrar por Género:", generos)
    
    rating_range = st.sidebar.slider("Rango de Rating:", 0.0, 10.0, (5.0, 10.0))

    # Filtrado del dataframe
    df_filtrado = df_movies[
        (df_movies['calificacion_imdb'] >= rating_range[0]) & 
        (df_movies['calificacion_imdb'] <= rating_range[1])
    ]
    if generos_sel:
        df_filtrado = df_filtrado[df_filtrado['genero'].apply(lambda x: any(g in str(x) for g in generos_sel))]

    # Pestañas
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Vista General", "📈 Histórico", "🖼️ Galería de Posters", "📋 Base de Datos"])

    with tab1:
        st.subheader("Métricas de la Selección")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("🎬 Total", len(df_filtrado))
        c2.metric("⭐ Rating Promedio", f"{df_filtrado['calificacion_imdb'].mean():.1f}")
        c3.metric("📅 Año más antiguo", int(df_filtrado['anio_num'].min()) if not df_filtrado.empty else 0)
        c4.metric("🔥 Año más reciente", int(df_filtrado['anio_num'].max()) if not df_filtrado.empty else 0)

        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            fig = px.histogram(df_filtrado, x='calificacion_imdb', nbins=20, title="Distribución de Ratings", color_discrete_sequence=['#ff4b4b'])
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            top_voted = df_filtrado.nlargest(10, 'votos_imdb')
            fig = px.bar(top_voted, x='votos_imdb', y='titulo', orientation='h', title="Top 10 Más Votadas (Popularidad)", color='votos_imdb')
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.subheader("Evolución del Cine por Año")
        # Agrupar por año para ver tendencia
        df_year = df_filtrado.groupby('anio_num')['calificacion_imdb'].mean().reset_index()
        fig = px.line(df_year, x='anio_num', y='calificacion_imdb', markers=True, 
                     title="Tendencia de Calidad Promedio por Año", line_shape="spline")
        st.plotly_chart(fig, use_container_width=True)

    with tab3:
        st.subheader("Análisis Visual de Carátulas")
        # Selector para ver detalles de una en particular
        pelicula_det = st.selectbox("Selecciona una película para ver trama:", df_filtrado['titulo'].unique())
        
        if pelicula_det:
            movie_row = df_filtrado[df_filtrado['titulo'] == pelicula_det].iloc[0]
            col_p1, col_p2 = st.columns([1, 2])
            with col_p1:
                st.image(validar_url_poster(movie_row['poster']), use_container_width=True)
            with col_p2:
                st.header(movie_row['titulo'])
                st.write(f"**🎭 Género:** {movie_row['genero']}")
                st.write(f"**🎬 Director:** {movie_row['director']}")
                st.write(f"**👥 Actores:** {movie_row['actores']}")
                st.info(f"**📖 Trama (Plot):** {movie_row.get('plot', 'No hay trama disponible.')}")
                st.link_button("🎥 Ver Trailer en YouTube", f"https://www.youtube.com/results?search_query={movie_row['titulo'].replace(' ', '+')}+trailer")

        st.markdown("---")
        st.write("### Rejilla de Posters")
        grid_cols = st.columns(6)
        for i, (idx, row) in enumerate(df_filtrado.head(24).iterrows()):
            with grid_cols[i % 6]:
                st.image(validar_url_poster(row['poster']), use_container_width=True)
                st.caption(f"{row['titulo'][:20]}...")

    with tab4:
        st.subheader("📋 Registro Completo de Datos")
        st.dataframe(df_filtrado, use_container_width=True)
        
        # Botón para descargar los datos filtrados
        csv = df_filtrado.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Descargar Selección en CSV", data=csv, file_name="mi_seleccion.csv", mime="text/csv")

else:
    st.error("🚨 No se encontró el archivo 'data/peliculas.csv'. Por favor, corre tu extractor primero.")
    st.info("El extractor es el que descarga los posters y crea la base de datos necesaria para este dashboard.")