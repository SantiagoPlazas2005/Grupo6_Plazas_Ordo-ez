#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.express as px
import os
import time
from datetime import datetime

# Configuración de la página
st.set_page_config(
    page_title="CineData Pro | Visualizador",
    page_icon="🎬",
    layout="wide"
)

# Estilo CSS
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    .stMetric { background-color: #1e2130; padding: 15px; border-radius: 10px; border: 1px solid #464646; }
    </style>
""", unsafe_allow_html=True)

# --- LÓGICA DE DATOS ---
PATH_CSV = 'data/peliculas.csv'

def get_last_modified_time():
    return os.path.getmtime(PATH_CSV) if os.path.exists(PATH_CSV) else 0

@st.cache_data(show_spinner="Cargando catálogo actualizado...")
def load_data():
    if not os.path.exists(PATH_CSV):
        return pd.DataFrame()
    
    df = pd.read_csv(PATH_CSV)
    # Limpieza de datos
    df['calificacion_imdb'] = pd.to_numeric(df['calificacion_imdb'], errors='coerce').fillna(0)
    df['anio_limpio'] = pd.to_numeric(df['anio'].astype(str).str.extract('(\d{4})')[0], errors='coerce')
    df['votos_imdb'] = df['votos_imdb'].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce').fillna(0)
    return df

# --- FUNCIÓN CRÍTICA: VALIDADOR DE IMAGEN ---
def obtener_url_valida(url):
    """Evita el error de 'float' convirtiendo NaNs o N/A en un placeholder."""
    if pd.isna(url) or not isinstance(url, str) or url == "N/A" or url.strip() == "":
        return "https://via.placeholder.com/300x450?text=Sin+Imagen"
    return url

# Control de auto-refresco
current_mtime = get_last_modified_time()
if "mtime" not in st.session_state:
    st.session_state.mtime = current_mtime

if current_mtime > st.session_state.mtime:
    st.session_state.mtime = current_mtime
    st.cache_data.clear()
    st.rerun()

df = load_data()

st.title("🎬 CineData Pro: Explorador Visual")

if not df.empty:
    # Sidebar
    st.sidebar.header("🎯 Filtros de Búsqueda")
    termino_busqueda = st.sidebar.text_input("Buscar película por título:", "")
    rating_min = st.sidebar.slider("Rating IMDB mínimo ⭐", 0.0, 10.0, 6.5)
    
    # Manejo de géneros (evitar errores si la columna está vacía)
    df['genero'] = df['genero'].fillna("Sin Género")
    generos = sorted(list(set([g.strip() for sublist in df['genero'].str.split(',') for g in sublist])))
    generos_sel = st.sidebar.multiselect("Filtrar por Género:", generos)

    # Filtrado
    df_filtrado = df[
        (df['titulo'].str.contains(termino_busqueda, case=False)) &
        (df['calificacion_imdb'] >= rating_min)
    ]
    
    if generos_sel:
        df_filtrado = df_filtrado[df_filtrado['genero'].apply(lambda x: any(g in str(x) for g in generos_sel))]

    # KPIs
    col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)
    col_kpi1.metric("Total Películas", len(df))
    col_kpi2.metric("Encontradas", len(df_filtrado))
    col_kpi3.metric("Rating Promedio", f"{df_filtrado['calificacion_imdb'].mean():.1f}")
    col_kpi4.metric("Sincronizado", datetime.fromtimestamp(current_mtime).strftime('%H:%M:%S'))

    st.markdown("---")

    # --- GALERÍA DE CARÁTULAS ---
    st.subheader("🖼️ Galería Visual (Top 24)")
    cols = st.columns(6)
    
    for i, (idx, row) in enumerate(df_filtrado.head(24).iterrows()):
        with cols[i % 6]:
            # Aplicamos la validación para evitar el error de float
            img_url = obtener_url_valida(row.get('poster'))
            
            try:
                st.image(img_url, use_container_width=True)
            except:
                st.image("https://via.placeholder.com/300x450?text=Error+Imagen", use_container_width=True)
            
            with st.expander(f"ℹ️ {row['titulo'][:15]}"):
                st.write(f"**Año:** {row['anio']}")
                st.write(f"⭐ {row['calificacion_imdb']}")
                youtube_url = f"https://www.youtube.com/results?search_query={row['titulo'].replace(' ', '+')}+official+trailer"
                st.link_button("🎥 Trailer", youtube_url)

    st.markdown("---")

    # Gráficas
    col_graph1, col_graph2 = st.columns(2)
    with col_graph1:
        st.markdown("#### 📈 Calificación vs Año")
        fig_scatter = px.scatter(df_filtrado, x="anio_limpio", y="calificacion_imdb", 
                                 color="calificacion_imdb", size="votos_imdb", hover_name="titulo")
        st.plotly_chart(fig_scatter, use_container_width=True)

    with col_graph2:
        st.markdown("#### 📊 Top 10 Películas")
        top_10 = df_filtrado.nlargest(10, 'calificacion_imdb')
        fig_bar = px.bar(top_10, x='calificacion_imdb', y='titulo', orientation='h', color='calificacion_imdb')
        fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_bar, use_container_width=True)

    with st.expander("🔍 Ver base de datos completa"):
        st.dataframe(df_filtrado, use_container_width=True)

else:
    st.warning("⚠️ No se encuentran datos. Por favor, ejecuta el extractor primero.")