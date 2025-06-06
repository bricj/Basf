from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
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

app = FastAPI(title="BASF Data API", version="1.0.0")

# CORS para Copilot Studio
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv("DATABASE_URL")

def wait_for_postgres():
    """Esperar a que PostgreSQL esté listo"""
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
    """OPTIMIZADO: Cargar datos frescos por chunks"""
    try:
        # Buscar archivo Excel
        excel_paths = ["/app/data/data.xlsx", "./data/data.xlsx"]
        excel_file = None
        
        for path in excel_paths:
            if os.path.exists(path):
                excel_file = path
                break
        
        if not excel_file:
            logger.error("❌ Excel no encontrado")
            return False
        
        # Verificar si hay tabla anterior
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
        else:
            logger.info("ℹ️ No hay datos anteriores")
        
        conn.close()
        
        # Cargar Excel fresco
        logger.info(f"📖 Cargando Excel fresco: {excel_file}")
        df = pd.read_excel(excel_file, sheet_name="CP_Colombia")
        logger.info(f"📊 {len(df)} registros, {len(df.columns)} columnas")
        
        # Limpiar datos
        logger.info("🧹 Limpiando datos...")
        df = df.fillna("")
        df = df.replace([float('inf'), float('-inf')], "")
        
        # OPTIMIZACIÓN: Cargar en chunks pequeños
        logger.info("💾 Cargando datos en chunks optimizados...")
        engine = create_engine(DATABASE_URL)
        
        chunk_size = 250  # Chunks pequeños para evitar colgarse
        total_chunks = (len(df) + chunk_size - 1) // chunk_size
        
        for i in range(0, len(df), chunk_size):
            chunk_num = (i // chunk_size) + 1
            chunk = df.iloc[i:i + chunk_size]
            
            logger.info(f"📤 Cargando chunk {chunk_num}/{total_chunks} ({len(chunk)} registros)...")
            
            if i == 0:
                # Primer chunk: crear tabla
                chunk.to_sql("basf_import_data", engine, if_exists="replace", index=False)
            else:
                # Chunks siguientes: agregar datos
                chunk.to_sql("basf_import_data", engine, if_exists="append", index=False)
            
            # Pequeña pausa para no sobrecargar
            time.sleep(0.1)
        
        logger.info(f"✅ Datos frescos cargados: {len(df)} registros en {total_chunks} chunks")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error cargando datos frescos: {e}")
        return False

@app.on_event("startup")
async def startup():
    """Inicializar con datos frescos al arrancar"""
    logger.info("🚀 Iniciando BASF API con carga optimizada...")
    
    if not wait_for_postgres():
        logger.error("❌ No se pudo conectar a PostgreSQL")
        return
    
    if load_fresh_data():
        logger.info("🎉 API lista con datos frescos")
    else:
        logger.warning("⚠️ API iniciada sin datos")

@app.get("/check")
async def check():
    """Verificar estado"""
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM basf_import_data")
        result = cursor.fetchone()
        conn.close()
        
        return {
            "status": "ok", 
            "records": result["count"],
            "data_source": "optimized_chunk_load"
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.get("/data")
async def get_data():
    """Obtener datos frescos para Copilot Studio"""
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM basf_import_data LIMIT 1000")
        records = cursor.fetchall()
        conn.close()
        
        logger.info(f"📊 Datos servidos: {len(records)} registros")
        
        return {
            "total": len(records),
            "source": "basf_optimized_data",
            "data": [dict(record) for record in records]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/refresh")
async def refresh_data():
    """Endpoint para recargar datos frescos manualmente"""
    logger.info("🔄 Recarga manual solicitada...")
    
    if load_fresh_data():
        return {
            "status": "success",
            "message": "Datos frescos recargados exitosamente con chunks optimizados"
        }
    else:
        raise HTTPException(
            status_code=500,
            detail="Error recargando datos frescos"
        )

@app.get("/")
async def root():
    """Info básica"""
    return {
        "service": "BASF Data API", 
        "version": "optimized_chunks",
        "endpoints": ["/check", "/data", "/refresh"],
        "note": "Carga optimizada por chunks para datasets grandes"
    }