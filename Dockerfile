# Usar imagen base oficial de Python
FROM python:3.11-slim

# Establecer directorio de trabajo
WORKDIR /app

# Copiar requirements.txt
COPY requirements.txt .

# Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todo el código del proyecto
COPY . .

# Crear directorios necesarios
RUN mkdir -p logs data

# Exponer puerto 8501 (Streamlit)
EXPOSE 8501

# Variable de entorno para Python
ENV PYTHONUNBUFFERED=1

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Comando por defecto: ejecutar Streamlit
CMD ["streamlit", "run", "dashboard_interactive.py", "--server.port=8501", "--server.address=0.0.0.0"]