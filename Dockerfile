# FROM python:3.9-slim

# # Dependencias del sistema
# RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# WORKDIR /app

# # Instalar dependencias Python
# COPY requirements.txt .
# RUN pip install -r requirements.txt

# # Copiar código de la API
# COPY src/api/main.py .

# # Crear directorio para datos y copiar archivo Excel
# RUN mkdir -p /app/data
# COPY src/api/data/data.xlsx /app/data/data.xlsx

# # Verificar que el archivo se copió (para debugging)
# RUN ls -la /app/data/

# # Usuario seguro
# RUN useradd -m basf && chown -R basf:basf /app
# USER basf

# EXPOSE 8000

# # Iniciar API con migración automática integrada
# CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]



###### VErsion 2 #####################

# # Imagen base oficial de Python
# # Usa una imagen base ligera de Python
# FROM python:3.11-slim

# # Establece el directorio de trabajo en el contenedor
# WORKDIR /app

# # Copia los archivos de dependencias
# COPY requirements.txt .

# # Instala las dependencias de Python
# RUN pip install --no-cache-dir -r requirements.txt

# # Copia todo el contenido del proyecto al contenedor
# COPY . .

# # Copia explícitamente la carpeta con el Excel para asegurar su disponibilidad
# COPY src/api/data /app/data

# # Expone el puerto 8000 (usado por Uvicorn)
# EXPOSE 8000

# # Comando para ejecutar la aplicación con Uvicorn apuntando al módulo correcto
# CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

################## Version 3 #############################


FROM python:3.11-slim

# Instalar dependencias del sistema para PostgreSQL
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Establece el directorio de trabajo en el contenedor
WORKDIR /app

# Copia los archivos de dependencias
COPY requirements.txt .

# Instala las dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo el contenido del proyecto al contenedor
COPY . .

# Copia explícitamente la carpeta con el Excel para asegurar su disponibilidad
COPY src/api/data /app/data

# Expone el puerto 8000 (usado por Uvicorn)
EXPOSE 8000

# Comando para ejecutar la aplicación con Uvicorn apuntando al módulo correcto
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]