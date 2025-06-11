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

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import os
import time
import logging
from psycopg2.extras import RealDictCursor

# Configuración
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="BASF Flete API", 
    version="2.0.0",
    description="API para consultas de costos de flete y proveedores BASF"
)

# CORS optimizado para Copilot Studio
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    allow_credentials=True,
)

# URL de PostgreSQL
DATABASE_URL = "postgresql://basf:F8utfvZuhQnp1cHvbOZlgqLOKHhVDkby@dpg-d14ht7muk2gs73at72a0-a.oregon-postgres.render.com/basf_db"

# Modelos Pydantic para respuestas estructuradas
class FleteResponse(BaseModel):
    status: str
    mensaje: str
    producto: str
    total_registros: int
    datos: List[Dict[str, Any]]

class MejorProveedorResponse(BaseModel):
    status: str
    mensaje: str
    producto: str
    mejor_proveedor: Dict[str, Any]
    alternativas: List[Dict[str, Any]]

class ComparacionOrigenResponse(BaseModel):
    status: str
    mensaje: str
    producto: str
    comparacion_por_origen: List[Dict[str, Any]]
    resumen_estadistico: Dict[str, Any]

def wait_for_postgres():
    for i in range(30):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.close()
            logger.info("✅ PostgreSQL conectado")
            return True
        except:
            logger.info(f"⏳ Esperando PostgreSQL... ({i+1}/30)")
            time.sleep(2)
    return False

def load_fresh_data():
    try:
        excel_paths = ["/app/data/data.xlsx", "./data/data.xlsx"]
        excel_file = None

        for path in excel_paths:
            if os.path.exists(path):
                excel_file = path
                break

        if not excel_file:
            logger.error("❌ Excel no encontrado")
            return False

        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'basf_import_data'")
        table_exists = cursor.fetchone()[0] > 0

        if table_exists:
            cursor.execute("SELECT COUNT(*) FROM basf_import_data")
            old_count = cursor.fetchone()[0]
            logger.info(f"🗑️ Eliminando {old_count} registros anteriores...")
            cursor.execute("DROP TABLE IF EXISTS basf_import_data CASCADE")
            conn.commit()
            logger.info("✅ Datos anteriores eliminados")

        conn.close()

        logger.info(f"📖 Cargando Excel fresco: {excel_file}")
        df = pd.read_excel(excel_file, sheet_name="CP_Colombia")
        logger.info(f"📊 {len(df)} registros, {len(df.columns)} columnas")

        df = df.fillna("")
        df = df.replace([float('inf'), float('-inf')], "")

        logger.info("💾 Cargando datos en chunks...")
        engine = create_engine(DATABASE_URL)
        chunk_size = 250
        total_chunks = (len(df) + chunk_size - 1) // chunk_size

        for i in range(0, len(df), chunk_size):
            chunk_num = (i // chunk_size) + 1
            chunk = df.iloc[i:i + chunk_size]
            logger.info(f"📤 Chunk {chunk_num}/{total_chunks}")

            if i == 0:
                chunk.to_sql("basf_import_data", engine, if_exists="replace", index=False)
            else:
                chunk.to_sql("basf_import_data", engine, if_exists="append", index=False)

        logger.info(f"✅ Datos cargados: {len(df)} registros")
        return True

    except Exception as e:
        logger.error(f"❌ Error cargando datos: {e}")
        return False

@app.on_event("startup")
async def startup():
    logger.info("🚀 Iniciando BASF Flete API...")
    if not wait_for_postgres():
        logger.error("❌ No se pudo conectar a PostgreSQL")
        return
    if load_fresh_data():
        logger.info("🎉 API lista con datos frescos")
    else:
        logger.warning("⚠️ API iniciada sin datos")

@app.get("/")
async def root():
    return {
        "service": "BASF Flete API",
        "version": "2.0.0",
        "descripcion": "API para consultas de costos de flete y análisis de proveedores",
        "endpoints": {
            "consultas_principales": [
                "/flete-por-origen - Costos de flete por origen para un producto",
                "/mejor-proveedor - Identificar el proveedor con mejor costo de flete",
                "/comparacion-origenes - Comparar precios según origen para un producto"
            ],
            "auxiliares": ["/check", "/productos", "/proveedores", "/origenes"]
        }
    }

@app.get("/check")
async def check():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM basf_import_data")
        result = cursor.fetchone()
        conn.close()
        return {
            "status": "ok",
            "total_registros": result["count"],
            "estado": "Base de datos operativa"
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}

# ENDPOINT 1: Determinación de costos de flete desde distintos orígenes para un producto
@app.get("/flete-por-origen", response_model=FleteResponse)
async def get_flete_por_origen(
    producto: str = Query(..., description="Nombre del producto a consultar"),
    limite: int = Query(100, description="Límite de registros a retornar")
):
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        
        query = """
        SELECT 
            "PAIS ORIGEN",
            "PROVEEDOR",
            "PRODUCTO",
            "FLETE",
            "PESO NETO (KG) TOTAL",
            "VALOR FOB (US$) TOTAL",
            ROUND(("FLETE"::numeric / "PESO NETO (KG) TOTAL"::numeric), 4) as flete_por_kg,
            ROUND(("FLETE"::numeric / "VALOR FOB (US$) TOTAL"::numeric * 100), 2) as porcentaje_flete_fob,
            "VIA",
            "FECHA AAAA-MM-DD",
            "EMPRESA DE TRANSPORTE"
        FROM basf_import_data 
        WHERE UPPER("PRODUCTO") LIKE UPPER(%s)
        AND "FLETE" > 0 
        AND "PESO NETO (KG) TOTAL" > 0
        ORDER BY flete_por_kg ASC
        LIMIT %s
        """
        
        cursor.execute(query, (f'%{producto}%', limite))
        records = cursor.fetchall()
        conn.close()

        if not records:
            return FleteResponse(
                status="sin_datos",
                mensaje=f"No se encontraron registros de flete para el producto '{producto}'",
                producto=producto,
                total_registros=0,
                datos=[]
            )

        datos_procesados = []
        for record in records:
            datos_procesados.append({
                "pais_origen": record["PAIS ORIGEN"],
                "proveedor": record["PROVEEDOR"],
                "flete_usd": float(record["FLETE"]) if record["FLETE"] else 0,
                "peso_kg": float(record["PESO NETO (KG) TOTAL"]) if record["PESO NETO (KG) TOTAL"] else 0,
                "valor_fob_usd": float(record["VALOR FOB (US$) TOTAL"]) if record["VALOR FOB (US$) TOTAL"] else 0,
                "flete_por_kg": float(record["flete_por_kg"]) if record["flete_por_kg"] else 0,
                "porcentaje_flete_fob": float(record["porcentaje_flete_fob"]) if record["porcentaje_flete_fob"] else 0,
                "via_transporte": record["VIA"],
                "fecha": record["FECHA AAAA-MM-DD"],
                "empresa_transporte": record["EMPRESA DE TRANSPORTE"]
            })

        return FleteResponse(
            status="exitoso",
            mensaje=f"Se encontraron {len(records)} registros de flete para '{producto}'",
            producto=producto,
            total_registros=len(records),
            datos=datos_procesados
        )

    except Exception as e:
        logger.error(f"Error en flete-por-origen: {e}")
        raise HTTPException(status_code=500, detail=f"Error consultando flete por origen: {str(e)}")

# ENDPOINT 2: Identificación del proveedor con el mejor costo de flete
@app.get("/mejor-proveedor", response_model=MejorProveedorResponse)
async def get_mejor_proveedor(
    producto: str = Query(..., description="Nombre del producto a consultar"),
    criterio: str = Query("flete_por_kg", description="Criterio: flete_por_kg o porcentaje_flete_fob")
):
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        
        query = """
        SELECT 
            "PROVEEDOR",
            "PAIS ORIGEN",
            AVG("FLETE"::numeric) as flete_promedio,
            AVG("PESO NETO (KG) TOTAL"::numeric) as peso_promedio,
            AVG("VALOR FOB (US$) TOTAL"::numeric) as fob_promedio,
            AVG("FLETE"::numeric / NULLIF("PESO NETO (KG) TOTAL"::numeric, 0)) as flete_por_kg_promedio,
            AVG("FLETE"::numeric / NULLIF("VALOR FOB (US$) TOTAL"::numeric, 0) * 100) as porcentaje_flete_fob_promedio,
            COUNT(*) as total_importaciones,
            MAX("FECHA AAAA-MM-DD") as ultima_importacion
        FROM basf_import_data 
        WHERE UPPER("PRODUCTO") LIKE UPPER(%s)
        AND "FLETE" > 0 
        AND "PESO NETO (KG) TOTAL" > 0
        AND "VALOR FOB (US$) TOTAL" > 0
        GROUP BY "PROVEEDOR", "PAIS ORIGEN"
        HAVING COUNT(*) >= 1
        ORDER BY 
            CASE WHEN %s = 'flete_por_kg' 
                 THEN AVG("FLETE"::numeric / NULLIF("PESO NETO (KG) TOTAL"::numeric, 0))
                 ELSE AVG("FLETE"::numeric / NULLIF("VALOR FOB (US$) TOTAL"::numeric, 0) * 100)
            END ASC
        """
        
        cursor.execute(query, (f'%{producto}%', criterio))
        records = cursor.fetchall()
        conn.close()

        if not records:
            return MejorProveedorResponse(
                status="sin_datos",
                mensaje=f"No se encontraron proveedores para el producto '{producto}'",
                producto=producto,
                mejor_proveedor={},
                alternativas=[]
            )

        mejor = records[0]
        alternativas = records[1:6]  # Top 5 alternativas

        mejor_proveedor_data = {
            "proveedor": mejor["PROVEEDOR"],
            "pais_origen": mejor["PAIS ORIGEN"],
            "flete_promedio_usd": round(float(mejor["flete_promedio"]), 2),
            "flete_por_kg_promedio": round(float(mejor["flete_por_kg_promedio"]), 4),
            "porcentaje_flete_fob_promedio": round(float(mejor["porcentaje_flete_fob_promedio"]), 2),
            "total_importaciones": int(mejor["total_importaciones"]),
            "ultima_importacion": mejor["ultima_importacion"],
            "criterio_seleccion": criterio
        }

        alternativas_data = []
        for alt in alternativas:
            alternativas_data.append({
                "proveedor": alt["PROVEEDOR"],
                "pais_origen": alt["PAIS ORIGEN"],
                "flete_promedio_usd": round(float(alt["flete_promedio"]), 2),
                "flete_por_kg_promedio": round(float(alt["flete_por_kg_promedio"]), 4),
                "porcentaje_flete_fob_promedio": round(float(alt["porcentaje_flete_fob_promedio"]), 2),
                "total_importaciones": int(alt["total_importaciones"])
            })

        return MejorProveedorResponse(
            status="exitoso",
            mensaje=f"Mejor proveedor identificado para '{producto}' según criterio '{criterio}'",
            producto=producto,
            mejor_proveedor=mejor_proveedor_data,
            alternativas=alternativas_data
        )

    except Exception as e:
        logger.error(f"Error en mejor-proveedor: {e}")
        raise HTTPException(status_code=500, detail=f"Error identificando mejor proveedor: {str(e)}")

# ENDPOINT 3: Comparación de precios según el origen para un producto
@app.get("/comparacion-origenes", response_model=ComparacionOrigenResponse)
async def get_comparacion_origenes(
    producto: str = Query(..., description="Nombre del producto a consultar")
):
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        
        query = """
        SELECT 
            "PAIS ORIGEN",
            "CONTINENTE ORIGEN",
            COUNT(*) as total_importaciones,
            AVG("FLETE"::numeric) as flete_promedio,
            MIN("FLETE"::numeric) as flete_minimo,
            MAX("FLETE"::numeric) as flete_maximo,
            AVG("FLETE"::numeric / NULLIF("PESO NETO (KG) TOTAL"::numeric, 0)) as flete_por_kg_promedio,
            AVG("VALOR FOB (US$) TOTAL"::numeric / NULLIF("PESO NETO (KG) TOTAL"::numeric, 0)) as precio_por_kg_promedio,
            AVG("FLETE"::numeric / NULLIF("VALOR FOB (US$) TOTAL"::numeric, 0) * 100) as porcentaje_flete_promedio,
            SUM("PESO NETO (KG) TOTAL"::numeric) as volumen_total_kg,
            MAX("FECHA AAAA-MM-DD") as ultima_importacion
        FROM basf_import_data 
        WHERE UPPER("PRODUCTO") LIKE UPPER(%s)
        AND "FLETE" > 0 
        AND "PESO NETO (KG) TOTAL" > 0
        AND "VALOR FOB (US$) TOTAL" > 0
        GROUP BY "PAIS ORIGEN", "CONTINENTE ORIGEN"
        HAVING COUNT(*) >= 1
        ORDER BY flete_por_kg_promedio ASC
        """
        
        cursor.execute(query, (f'%{producto}%',))
        records = cursor.fetchall()
        conn.close()

        if not records:
            return ComparacionOrigenResponse(
                status="sin_datos",
                mensaje=f"No se encontraron datos de origen para el producto '{producto}'",
                producto=producto,
                comparacion_por_origen=[],
                resumen_estadistico={}
            )

        comparacion_data = []
        for record in records:
            comparacion_data.append({
                "pais_origen": record["PAIS ORIGEN"],
                "continente": record["CONTINENTE ORIGEN"],
                "total_importaciones": int(record["total_importaciones"]),
                "flete_promedio_usd": round(float(record["flete_promedio"]), 2),
                "flete_minimo_usd": round(float(record["flete_minimo"]), 2),
                "flete_maximo_usd": round(float(record["flete_maximo"]), 2),
                "flete_por_kg_promedio": round(float(record["flete_por_kg_promedio"]), 4),
                "precio_por_kg_promedio": round(float(record["precio_por_kg_promedio"]), 2),
                "porcentaje_flete_promedio": round(float(record["porcentaje_flete_promedio"]), 2),
                "volumen_total_kg": round(float(record["volumen_total_kg"]), 0),
                "ultima_importacion": record["ultima_importacion"]
            })

        # Resumen estadístico
        fletes_por_kg = [item["flete_por_kg_promedio"] for item in comparacion_data]
        precios_por_kg = [item["precio_por_kg_promedio"] for item in comparacion_data]
        
        resumen = {
            "total_paises_origen": len(comparacion_data),
            "flete_por_kg_menor": min(fletes_por_kg),
            "flete_por_kg_mayor": max(fletes_por_kg),
            "flete_por_kg_promedio_general": round(sum(fletes_por_kg) / len(fletes_por_kg), 4),
            "precio_por_kg_promedio_general": round(sum(precios_por_kg) / len(precios_por_kg), 2),
            "origen_mas_economico": comparacion_data[0]["pais_origen"],
            "ahorro_potencial_porcentaje": round(((max(fletes_por_kg) - min(fletes_por_kg)) / max(fletes_por_kg)) * 100, 2)
        }

        return ComparacionOrigenResponse(
            status="exitoso",
            mensaje=f"Comparación de {len(comparacion_data)} orígenes para '{producto}'",
            producto=producto,
            comparacion_por_origen=comparacion_data,
            resumen_estadistico=resumen
        )

    except Exception as e:
        logger.error(f"Error en comparacion-origenes: {e}")
        raise HTTPException(status_code=500, detail=f"Error comparando orígenes: {str(e)}")

# ENDPOINTS AUXILIARES

@app.get("/productos")
async def get_productos():
    """Lista los productos únicos disponibles"""
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT "PRODUCTO" 
            FROM basf_import_data 
            WHERE "PRODUCTO" IS NOT NULL AND "PRODUCTO" != ''
            ORDER BY "PRODUCTO"
            LIMIT 50
        """)
        products = [row["PRODUCTO"] for row in cursor.fetchall()]
        conn.close()
        return {"productos": products, "total": len(products)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/proveedores")
async def get_proveedores():
    """Lista los proveedores únicos disponibles"""
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT "PROVEEDOR", "PAIS ORIGEN"
            FROM basf_import_data 
            WHERE "PROVEEDOR" IS NOT NULL AND "PROVEEDOR" != ''
            ORDER BY "PROVEEDOR"
            LIMIT 50
        """)
        proveedores = [{"proveedor": row["PROVEEDOR"], "pais": row["PAIS ORIGEN"]} for row in cursor.fetchall()]
        conn.close()
        return {"proveedores": proveedores, "total": len(proveedores)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/origenes")
async def get_origenes():
    """Lista los países de origen únicos disponibles"""
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT "PAIS ORIGEN", "CONTINENTE ORIGEN"
            FROM basf_import_data 
            WHERE "PAIS ORIGEN" IS NOT NULL AND "PAIS ORIGEN" != ''
            ORDER BY "CONTINENTE ORIGEN", "PAIS ORIGEN"
        """)
        origenes = [{"pais": row["PAIS ORIGEN"], "continente": row["CONTINENTE ORIGEN"]} for row in cursor.fetchall()]
        conn.close()
        return {"origenes": origenes, "total": len(origenes)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/refresh")
async def refresh_data():
    """Recarga los datos desde el archivo Excel"""
    logger.info("🔄 Recarga manual solicitada...")
    if load_fresh_data():
        return {"status": "exitoso", "mensaje": "Datos recargados correctamente"}
    else:
        raise HTTPException(status_code=500, detail="Error recargando datos")