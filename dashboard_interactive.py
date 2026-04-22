#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime
from sqlalchemy import create_engine

# --- CONFIGURACIÓN DE LA PÁGINA ---
st.set_page_config(
    page_title="CineData Pro | Visualizador",
    page_icon="🎬",
    layout="wide"
)

# Estilo CSS personalizado
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    .stMetric { background-color: #1e2130; padding: 15px; border-radius: 10px; border: 1px solid #464646; }
    </style>
""", unsafe_allow_html=True)

# --- CONFIGURACIÓN DE CONEXIÓN ---
PATH_CSV = 'data/peliculas.csv'

# Intentar obtener la URL de la base de datos desde el entorno (Docker)
# Si no existe, usa la local por defecto
DATABASE_URL = os.getenv(
    'DATABASE_URL', 
    'postgresql://santiago:plazas2005@localhost:5432/base_datos_peliculas'
)

@st.cache_data(ttl=60) # Cache de 1 minuto para ver actualizaciones
def load_data():
    """Carga datos desde PostgreSQL o CSV como respaldo."""
    try:
        # Intentar conexión a Base de Datos
        engine = create_engine(DATABASE_URL)
        query = "SELECT * FROM peliculas"
        df = pd.read_sql(query, engine)
        source = "PostgreSQL"
    except Exception as e:
        # Si falla la BD, intentar con CSV
        if os.path.exists(PATH_CSV):
            df = pd.read_csv(PATH_CSV)
            source = "CSV Local"
        else:
            return pd.DataFrame(), "Sin datos"

    # --- LIMPIEZA DE DATOS ---
    if not df.empty:
        # Asegurar que los nombres de columnas sean consistentes
        df.columns = [c.lower() for c in df.columns]
        
        # Conversión de tipos
        df['calificacion_imdb'] = pd.to_numeric(df['calificacion_imdb'], errors='coerce').fillna(0)
        df['anio_limpio'] = pd.to_numeric(df['anio'].astype(str).str.extract(r'(\d{4})')[0], errors='coerce')
        df['votos_imdb'] = df['votos_imdb'].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce').fillna(0)
        df['genero'] = df['genero'].fillna("Sin Género")
        
    return df, source

def obtener_url_valida(url):
    """Evita iconos rotos convirtiendo N/A o vacíos en un placeholder."""
    if pd.isna(url) or not isinstance(url, str) or url == "N/A" or url.strip() == "":
        return "https://via.placeholder.com/300x450?text=Sin+Imagen"
    return url

# --- EJECUCIÓN ---
df, fuente_datos = load_data()

st.title("🎬 CineData Pro: Explorador Visual")
st.caption(f"Conectado vía: **{fuente_datos}**")

if not df.empty:
    # --- SIDEBAR (FILTROS) ---
    st.sidebar.header("🎯 Filtros de Búsqueda")
    termino_busqueda = st.sidebar.text_input("Buscar película por título:", "")
    rating_min = st.sidebar.slider("Rating IMDB mínimo ⭐", 0.0, 10.0, 6.5)
    
    # Procesar géneros únicos
    generos_set = set()
    for g_list in df['genero'].dropna().unique():
        for g in g_list.split(','):
            generos_set.add(g.strip())
    
    generos_sel = st.sidebar.multiselect("Filtrar por Género:", sorted(list(generos_set)))

    # Aplicar Filtros
    df_filtrado = df[
        (df['titulo'].str.contains(termino_busqueda, case=False, na=False)) &
        (df['calificacion_imdb'] >= rating_min)
    ]
    
    if generos_sel:
        df_filtrado = df_filtrado[df_filtrado['genero'].apply(lambda x: any(g in str(x) for g in generos_sel))]

    # --- KPIs ---
    col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)
    col_kpi1.metric("Total Películas", len(df))
    col_kpi2.metric("Encontradas", len(df_filtrado))
    col_kpi3.metric("Rating Promedio", f"{df_filtrado['calificacion_imdb'].mean():.1f}")
    col_kpi4.metric("Actualizado", datetime.now().strftime('%H:%M:%S'))

    st.markdown("---")

    # --- GALERÍA DE CARÁTULAS ---
    st.subheader("🖼️ Galería Visual (Top 24)")
    
    if len(df_filtrado) > 0:
        # 6 columnas para que se vea ordenado
        cols = st.columns(6)
        for i, (idx, row) in enumerate(df_filtrado.head(24).iterrows()):
            with cols[i % 6]:
                # Validamos la URL del póster
                img_url = obtener_url_valida(row.get('poster'))
                
                # Mostramos la imagen (usando use_column_width para compatibilidad Docker)
                try:
                    st.image(img_url, use_column_width=True)
                except:
                    st.image("https://via.placeholder.com/300x450?text=Error+Carga", use_column_width=True)
                
                # Info simplificada debajo de la imagen
                with st.expander(f"ℹ️ {str(row['titulo'])[:15]}..."):
                    st.write(f"**Año:** {row['anio']}")
                    st.write(f"⭐ {row['calificacion_imdb']}")
                    yt_query = f"https://www.youtube.com/results?search_query={str(row['titulo']).replace(' ', '+')}+trailer"
                    st.link_button("🎥 Trailer", yt_query)
    else:
        st.info("No se encontraron películas con esos filtros.")

    st.markdown("---")

    # --- GRÁFICAS ---
    col_graph1, col_graph2 = st.columns(2)
    with col_graph1:
        st.markdown("#### 📈 Calificación vs Año")
        fig_scatter = px.scatter(df_filtrado, x="anio_limpio", y="calificacion_imdb", 
                                 color="calificacion_imdb", size="votos_imdb", 
                                 hover_name="titulo", template="plotly_dark")
        st.plotly_chart(fig_scatter, use_container_width=True)

    with col_graph2:
        st.markdown("#### 📊 Top 10 Películas")
        top_10 = df_filtrado.nlargest(10, 'calificacion_imdb')
        fig_bar = px.bar(top_10, x='calificacion_imdb', y='titulo', 
                         orientation='h', color='calificacion_imdb', template="plotly_dark")
        fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_bar, use_container_width=True)

    with st.expander("🔍 Ver tabla de datos completa"):
        st.dataframe(df_filtrado, use_container_width=True)

else:
    st.error("⚠️ No se detectaron datos en PostgreSQL ni en el CSV. Verifica que la tabla 'peliculas' tenga registros.")