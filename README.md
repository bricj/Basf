## Proceso para disponibilizar la API

1. En la carpeta data debe guardarse el archivo excel con los datos bajos el nombre "data.xlsx"
2. Levantar el contenedor con: docker compose up --build
3. Ejecutar en otra consola:
    docker run --rm -it --net=host -e NGROK_AUTHTOKEN=**token** ngrok/ngrok:latest http 80
4. El forwarding es la URL de la API que se incluye en Copilot Studio

docker run --rm -it --net=host -e NGROK_AUTHTOKEN=2vQ6NfG3h5nwdJ7NDD7ZUfO593a_27xuQKhoh7gnHm2n141GG ngrok/ngrok:latest http 80
https://TU_SUBDOMINIO.ngrok-free.app/data

docker run --rm -it --net=host -e NGROK_AUTHTOKEN=2vQ6NfG3h5nwdJ7NDD7ZUfO593a_27xuQKhoh7gnHm2n141GG ngrok/ngrok:latest http 80 


![Estructura](imgs/estructura.jpg)

![ngrok](imgs/ngrok.jpg)


## 🛠️ Instalación

### Desarrollo Local

```bash
# Clonar repositorio
git clone https://github.com/tu-usuario/basf-analytics-api.git
cd basf-analytics-api

# Crear entorno virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate     # Windows

# Instalar dependencias
pip install -r requirements.txt

# Ejecutar
uvicorn main:app --reload
```

### Docker

```bash
# Construir imagen
docker build -t basf-api .

# Ejecutar contenedor
docker run -p 8000:8000 basf-api

# O usar docker-compose
docker-compose up --build
```

## 🔗 Endpoints Principales

| Endpoint | Método | Descripción |
|----------|--------|-------------|
| `/esquema-bd` | GET | Metadata completa para IA |
| `/ejecutar-sql` | POST | Ejecutar SQL generado por IA |
| `/validar-sql` | GET | Validar SQL sin ejecutar |
| `/check` | GET | Health check |

## 🤖 Integración con Microsoft Copilot Studio

### 1. Configuración del Conector

```json
{
  "openapi": "3.0.0",
  "servers": [{"url": "https://tu-app.onrender.com"}],
  "paths": {
    "/esquema-bd": {...},
    "/ejecutar-sql": {...}
  }
}
```

### 2. Topic Principal

```yaml
# Obtener esquema para IA
Action: GET /esquema-bd
Save as: {EsquemaBD}

# Generar SQL con IA
Generative AI:
  System Prompt: |
    {EsquemaBD.esquema_sugerido}
    Convierte la pregunta en SQL válido.
  User Input: "{System.Activity.Text}"
  Save as: {SQLGenerado}

# Ejecutar SQL
Action: POST /ejecutar-sql
Body: {"instruccion_sql": "{SQLGenerado}"}
Save as: {Resultados}
```

## 📈 Ejemplos de Consultas

### Consultas de Negocio
```
"¿Cuál es el flete promedio de acetona por proveedor?"
"¿Qué SBU genera más ingresos?"
"¿Cómo ha evolucionado el volumen de importaciones este año?"
```

### Consultas Técnicas
```
"¿Cuál es la eficiencia de cada aduana por kg procesado?"
"¿Qué importadores han diversificado más sus proveedores?"
"¿Cuál es el impacto del gravamen en el costo total por producto?"
```

## 🔒 Seguridad

- **Whitelist SQL**: Solo comandos SELECT permitidos
- **Blacklist**: Prevención de comandos peligrosos  
- **Validación**: Estructura y sintaxis de consultas
- **Timeouts**: Límites de tiempo de ejecución
- **Sanitización**: Limpieza automática de queries

## 🌍 Variables de Entorno

```bash
DATABASE_URL=postgresql://user:pass@host:port/db
PYTHONPATH=/app
```

## 📋 Estructura de Datos

La API analiza 40+ columnas incluyendo:

**📦 Producto**: PRODUCTO, SBU, CLASIFICACION, MERCADO RELEVANTE  
**🏢 Negocio**: IMPORTADOR, PROVEEDOR, GRUPO IMPORTADOR  
**🌍 Geografía**: PAIS ORIGEN, CONTINENTE, ADUANA, CIUDAD INGRESO  
**💰 Financiero**: VALOR FOB, CIF, FLETE, SEGURO, GRAVAMEN  
**📏 Logística**: PESO NETO, PESO BRUTO, VIA, EMPRESA TRANSPORTE  

## 🚀 Despliegue en Render

```bash
# Push a GitHub
git add .
git commit -m "API v4.0 - SQL dinámico con IA"
git push origin main

# En Render:
# 1. Connect GitHub repo
# 2. Build command: pip install -r requirements.txt
# 3. Start command: uvicorn main:app --host 0.0.0.0 --port $PORT
```

## 📞 Soporte

Para consultas técnicas o issues, crear un issue en GitHub o contactar al equipo de desarrollo.

## 📄 Licencia

Propiedad de BASF - Uso interno solamente.