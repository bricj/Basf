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

FROM python:3.9-slim

RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/api/main.py .
RUN mkdir -p /app/data
COPY src/api/data/data.xlsx /app/data/data.xlsx
RUN ls -la /app/data/

RUN useradd -m basf && chown -R basf:basf /app
USER basf

ENV PATH=/home/basf/.local/bin:/usr/local/bin:/usr/bin:/bin

EXPOSE 8000

CMD ["python3", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
