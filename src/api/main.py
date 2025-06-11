# from fastapi import FastAPI, HTTPException
# from fastapi.middleware.cors import CORSMiddleware
# import psycopg2
# import pandas as pd
# from sqlalchemy import create_engine
# import os
# import time
# import logging
# from psycopg2.extras import RealDictCursor

# # Configuración
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# app = FastAPI(title="BASF Data API", version="1.0.0")

# # CORS para Copilot Studio
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# DATABASE_URL = os.getenv("DATABASE_URL")

# def wait_for_postgres():
#     """Esperar a que PostgreSQL esté listo"""
#     for i in range(30):
#         try:
#             conn = psycopg2.connect(DATABASE_URL)
#             conn.close()
#             logger.info("✅ PostgreSQL conectado")
#             return True
#         except:
#             logger.info(f"⏳ Esperando PostgreSQL... ({i+1}/30)")
#             time.sleep(2)
#     return False

# def load_fresh_data():
#     """OPTIMIZADO: Cargar datos frescos por chunks"""
#     try:
#         # Buscar archivo Excel
#         excel_paths = ["/app/data/data.xlsx", "./data/data.xlsx"]
#         excel_file = None
        
#         for path in excel_paths:
#             if os.path.exists(path):
#                 excel_file = path
#                 break
        
#         if not excel_file:
#             logger.error("❌ Excel no encontrado")
#             return False
        
#         # Verificar si hay tabla anterior
#         conn = psycopg2.connect(DATABASE_URL)
#         cursor = conn.cursor()
#         cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'basf_import_data'")
#         table_exists = cursor.fetchone()[0] > 0
        
#         if table_exists:
#             cursor.execute("SELECT COUNT(*) FROM basf_import_data")
#             old_count = cursor.fetchone()[0]
#             logger.info(f"🗑️ Eliminando {old_count} registros anteriores...")
#             cursor.execute("DROP TABLE IF EXISTS basf_import_data CASCADE")
#             conn.commit()
#             logger.info("✅ Datos anteriores eliminados")
#         else:
#             logger.info("ℹ️ No hay datos anteriores")
        
#         conn.close()
        
#         # Cargar Excel fresco
#         logger.info(f"📖 Cargando Excel fresco: {excel_file}")
#         df = pd.read_excel(excel_file, sheet_name="CP_Colombia")
#         logger.info(f"📊 {len(df)} registros, {len(df.columns)} columnas")
        
#         # Limpiar datos
#         logger.info("🧹 Limpiando datos...")
#         df = df.fillna("")
#         df = df.replace([float('inf'), float('-inf')], "")
        
#         # OPTIMIZACIÓN: Cargar en chunks pequeños
#         logger.info("💾 Cargando datos en chunks optimizados...")
#         engine = create_engine(DATABASE_URL)
        
#         chunk_size = 250  # Chunks pequeños para evitar colgarse
#         total_chunks = (len(df) + chunk_size - 1) // chunk_size
        
#         for i in range(0, len(df), chunk_size):
#             chunk_num = (i // chunk_size) + 1
#             chunk = df.iloc[i:i + chunk_size]
            
#             logger.info(f"📤 Cargando chunk {chunk_num}/{total_chunks} ({len(chunk)} registros)...")
            
#             if i == 0:
#                 # Primer chunk: crear tabla
#                 chunk.to_sql("basf_import_data", engine, if_exists="replace", index=False)
#             else:
#                 # Chunks siguientes: agregar datos
#                 chunk.to_sql("basf_import_data", engine, if_exists="append", index=False)
            
#             # Pequeña pausa para no sobrecargar
#             time.sleep(0.1)
        
#         logger.info(f"✅ Datos frescos cargados: {len(df)} registros en {total_chunks} chunks")
#         return True
        
#     except Exception as e:
#         logger.error(f"❌ Error cargando datos frescos: {e}")
#         return False

# @app.on_event("startup")
# async def startup():
#     """Inicializar con datos frescos al arrancar"""
#     logger.info("🚀 Iniciando BASF API con carga optimizada...")
    
#     if not wait_for_postgres():
#         logger.error("❌ No se pudo conectar a PostgreSQL")
#         return
    
#     if load_fresh_data():
#         logger.info("🎉 API lista con datos frescos")
#     else:
#         logger.warning("⚠️ API iniciada sin datos")

# @app.get("/check")
# async def check():
#     """Verificar estado"""
#     try:
#         conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
#         cursor = conn.cursor()
#         cursor.execute("SELECT COUNT(*) as count FROM basf_import_data")
#         result = cursor.fetchone()
#         conn.close()
        
#         return {
#             "status": "ok", 
#             "records": result["count"],
#             "data_source": "optimized_chunk_load"
#         }
#     except Exception as e:
#         return {"status": "error", "error": str(e)}

# @app.get("/Data")
# async def get_data():
#     """Obtener datos frescos para Copilot Studio"""
#     try:
#         conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
#         cursor = conn.cursor()
#         cursor.execute("SELECT * FROM basf_import_data LIMIT 1000")
#         records = cursor.fetchall()
#         conn.close()
        
#         logger.info(f"📊 Datos servidos: {len(records)} registros")
        
#         return {
#             "total": len(records),
#             "source": "basf_optimized_data",
#             "data": [dict(record) for record in records]
#         }
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# @app.get("/refresh")
# async def refresh_data():
#     """Endpoint para recargar datos frescos manualmente"""
#     logger.info("🔄 Recarga manual solicitada...")
    
#     if load_fresh_data():
#         return {
#             "status": "success",
#             "message": "Datos frescos recargados exitosamente con chunks optimizados"
#         }
#     else:
#         raise HTTPException(
#             status_code=500,
#             detail="Error recargando datos frescos"
#         )

# @app.get("/")
# async def root():
#     """Info básica"""
#     return {
#         "service": "BASF Data API", 
#         "version": "optimized_chunks",
#         "endpoints": ["/check", "/data", "/refresh"],
#         "note": "Carga optimizada por chunks para datasets grandes"
#     }

# ----------------------------------------------------

# from fastapi import FastAPI, HTTPException
# from fastapi.middleware.cors import CORSMiddleware
# import psycopg2
# import pandas as pd
# from sqlalchemy import create_engine
# import os
# import time
# import logging
# from psycopg2.extras import RealDictCursor

# # Configuración
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# app = FastAPI(title="BASF Data API", version="1.0.0")

# # CORS para Copilot Studio
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # URL explícita de PostgreSQL en Render
# DATABASE_URL = "postgresql://basf:F8utfvZuhQnp1cHvbOZlgqLOKHhVDkby@dpg-d14ht7muk2gs73at72a0-a.oregon-postgres.render.com/basf_db"

# def wait_for_postgres():
#     for i in range(30):
#         try:
#             conn = psycopg2.connect(DATABASE_URL)
#             conn.close()
#             logger.info("✅ PostgreSQL conectado")
#             return True
#         except:
#             logger.info(f"⏳ Esperando PostgreSQL... ({i+1}/30)")
#             time.sleep(2)
#     return False

# def load_fresh_data():
#     try:
#         excel_paths = ["/app/data/data.xlsx", "./data/data.xlsx"]
#         excel_file = None

#         for path in excel_paths:
#             if os.path.exists(path):
#                 excel_file = path
#                 break

#         if not excel_file:
#             logger.error("❌ Excel no encontrado")
#             return False

#         conn = psycopg2.connect(DATABASE_URL)
#         cursor = conn.cursor()
#         cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'basf_import_data'")
#         table_exists = cursor.fetchone()[0] > 0

#         if table_exists:
#             cursor.execute("SELECT COUNT(*) FROM basf_import_data")
#             old_count = cursor.fetchone()[0]
#             logger.info(f"🗑️ Eliminando {old_count} registros anteriores...")
#             cursor.execute("DROP TABLE IF EXISTS basf_import_data CASCADE")
#             conn.commit()
#             logger.info("✅ Datos anteriores eliminados")
#         else:
#             logger.info("ℹ️ No hay datos anteriores")

#         conn.close()

#         logger.info(f"📖 Cargando Excel fresco: {excel_file}")
#         df = pd.read_excel(excel_file, sheet_name="CP_Colombia")
#         logger.info(f"📊 {len(df)} registros, {len(df.columns)} columnas")

#         logger.info("🧹 Limpiando datos...")
#         df = df.fillna("")
#         df = df.replace([float('inf'), float('-inf')], "")

#         logger.info("💾 Cargando datos en chunks optimizados...")
#         engine = create_engine(DATABASE_URL)

#         chunk_size = 250
#         total_chunks = (len(df) + chunk_size - 1) // chunk_size

#         for i in range(0, len(df), chunk_size):
#             chunk_num = (i // chunk_size) + 1
#             chunk = df.iloc[i:i + chunk_size]
#             logger.info(f"📤 Cargando chunk {chunk_num}/{total_chunks} ({len(chunk)} registros)...")

#             if i == 0:
#                 chunk.to_sql("basf_import_data", engine, if_exists="replace", index=False)
#             else:
#                 chunk.to_sql("basf_import_data", engine, if_exists="append", index=False)

#             time.sleep(0.1)

#         logger.info(f"✅ Datos frescos cargados: {len(df)} registros en {total_chunks} chunks")
#         return True

#     except Exception as e:
#         logger.error(f"❌ Error cargando datos frescos: {e}")
#         return False

# @app.on_event("startup")
# async def startup():
#     logger.info("🚀 Iniciando BASF API con carga optimizada...")

#     if not wait_for_postgres():
#         logger.error("❌ No se pudo conectar a PostgreSQL")
#         return

#     if load_fresh_data():
#         logger.info("🎉 API lista con datos frescos")
#     else:
#         logger.warning("⚠️ API iniciada sin datos")

# @app.get("/check")
# async def check():
#     try:
#         conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
#         cursor = conn.cursor()
#         cursor.execute("SELECT COUNT(*) as count FROM basf_import_data")
#         result = cursor.fetchone()
#         conn.close()

#         return {
#             "status": "ok",
#             "records": result["count"],
#             "data_source": "optimized_chunk_load"
#         }
#     except Exception as e:
#         return {"status": "error", "error": str(e)}

# @app.get("/Data")
# async def get_data():
#     try:
#         conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
#         cursor = conn.cursor()
#         cursor.execute("SELECT * FROM basf_import_data LIMIT 1000")
#         records = cursor.fetchall()
#         conn.close()

#         logger.info(f"📊 Datos servidos: {len(records)} registros")

#         return {
#             # "total": len(records),
#             # "source": "basf_optimized_data",
#              "data": [dict(record) for record in records]
#         }
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# @app.get("/refresh")
# async def refresh_data():
#     logger.info("🔄 Recarga manual solicitada...")

#     if load_fresh_data():
#         return {
#             "status": "success",
#             "message": "Datos frescos recargados exitosamente con chunks optimizados"
#         }
#     else:
#         raise HTTPException(
#             status_code=500,
#             detail="Error recargando datos frescos"
#         )

# @app.get("/")
# async def root():
#     return {
#         "service": "BASF Data API",
#         "version": "optimized_chunks",
#         "endpoints": ["/check", "/data", "/refresh"],
#         "note": "Carga optimizada por chunks para datasets grandes"
#     }

# -----------------------------------------

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import List
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import time

# Configuración general
app = FastAPI()

# Reemplaza con tus variables reales o usa dotenv/env
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@host:port/db")
EXCEL_PATH = "/path/to/your/excel_file.xlsx"  # Cambia a la ruta real del archivo

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Endpoint para cargar el Excel a PostgreSQL
@app.get("/refresh")
async def refresh_database():
    try:
        engine = create_engine(DATABASE_URL)
        chunksize = 250
        total_inserted = 0

        for chunk in pd.read_excel(EXCEL_PATH, sheet_name=0, engine='openpyxl', chunksize=chunksize):
            chunk.dropna(how="all", inplace=True)
            chunk.replace([float('inf'), float('-inf')], pd.NA, inplace=True)
            chunk.to_sql("basf_import_data", engine, if_exists="append", index=False)
            total_inserted += len(chunk)
            time.sleep(0.1)

        return {"message": f"{total_inserted} filas insertadas correctamente en la tabla 'basf_import_data'"}

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# Endpoint de análisis por producto
@app.get("/analisis_flete")
async def analizar_flete(
    productos: List[str] = Query(..., description="Lista de productos en la columna 'PRODUCTO'")
):
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        resultados = {}

        for producto in productos:
            # Flete promedio por país
            cursor.execute("""
                SELECT "PAIS ORIGEN" AS pais_origen,
                       ROUND(AVG(CAST("FLETE" AS FLOAT)), 2) AS flete_promedio_usd,
                       COUNT(*) AS num_importaciones
                FROM basf_import_data
                WHERE LOWER("PRODUCTO") = LOWER(%s)
                  AND "FLETE" IS NOT NULL
                GROUP BY "PAIS ORIGEN"
                ORDER BY flete_promedio_usd ASC
            """, (producto,))
            flete_origen = cursor.fetchall()

            # Mejor proveedor (menor flete total)
            cursor.execute("""
                SELECT "PROVEEDOR", "PAIS ORIGEN",
                       MIN(CAST("FLETE" AS FLOAT)) AS mejor_flete_usd
                FROM basf_import_data
                WHERE LOWER("PRODUCTO") = LOWER(%s)
                  AND "FLETE" IS NOT NULL
                GROUP BY "PROVEEDOR", "PAIS ORIGEN"
                ORDER BY mejor_flete_usd ASC
                LIMIT 1
            """, (producto,))
            mejor_proveedor = cursor.fetchone()

            # Precio por kg por país
            cursor.execute("""
                SELECT "PAIS ORIGEN" AS pais_origen,
                       ROUND(AVG(CAST("CIF (US$)" AS FLOAT) / NULLIF("CANTIDAD TOTAL", 0)), 4) AS precio_cif_kg_usd,
                       ROUND(AVG(CAST("VALOR FOB (US$) TOTAL" AS FLOAT) / NULLIF("CANTIDAD TOTAL", 0)), 4) AS precio_fob_kg_usd,
                       ROUND(AVG(CAST("SEGURO" AS FLOAT) / NULLIF("CANTIDAD TOTAL", 0)), 6) AS seguro_kg_usd
                FROM basf_import_data
                WHERE LOWER("PRODUCTO") = LOWER(%s)
                  AND "CANTIDAD TOTAL" > 0
                GROUP BY "PAIS ORIGEN"
                ORDER BY precio_cif_kg_usd ASC
            """, (producto,))
            precios_por_kg = cursor.fetchall()

            # Métricas por año
            cursor.execute("""
                SELECT EXTRACT(YEAR FROM TO_DATE("FECHA AAAA-MM-DD", 'YYYY-MM-DD'))::INT AS anio,
                       ROUND(AVG(CAST("FLETE" AS FLOAT) / NULLIF("CANTIDAD TOTAL", 0)), 6) AS flete_kg_usd,
                       ROUND(AVG(CAST("CIF (US$)" AS FLOAT) / NULLIF("CANTIDAD TOTAL", 0)), 6) AS cif_kg_usd,
                       ROUND(AVG(CAST("VALOR FOB (US$) TOTAL" AS FLOAT) / NULLIF("CANTIDAD TOTAL", 0)), 6) AS fob_kg_usd
                FROM basf_import_data
                WHERE LOWER("PRODUCTO") = LOWER(%s)
                  AND "CANTIDAD TOTAL" > 0
                GROUP BY anio
                ORDER BY anio ASC
            """, (producto,))
            resumen_anual = cursor.fetchall()

            resultados[producto] = {
                "costo_flete_por_origen": flete_origen,
                "mejor_proveedor": mejor_proveedor,
                "precio_por_kg_por_origen": precios_por_kg,
                "resumen_anual": resumen_anual
            }

        conn.close()
        return resultados

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
