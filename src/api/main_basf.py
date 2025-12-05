from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import asyncio
import logging
import re
import json
from datetime import datetime

# Configuración
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="BASF AI-SQL API", version="4.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    allow_credentials=True,
)

DATABASE_URL = "postgresql://basf:F8utfvZuhQnp1cHvbOZlgqLOKHhVDkby@dpg-d14ht7muk2gs73at72a0-a.oregon-postgres.render.com/basf_db"

# Modelos para la comunicación con Copilot
class SQLRequest(BaseModel):
    instruccion_sql: str
    parametros: Optional[Dict[str, Any]] = {}
    contexto: Optional[str] = ""

class MetadataResponse(BaseModel):
    tablas: List[Dict[str, Any]]
    columnas: List[Dict[str, Any]]
    ejemplos_valores: List[Dict[str, Any]]
    esquema_sugerido: str

class SQLSafeExecutor:
    """Ejecutor SQL seguro que valida y ejecuta queries generadas por IA"""
    
    def __init__(self):
        # Palabras permitidas (whitelist)
        self.palabras_permitidas = {
            'select', 'from', 'where', 'group', 'by', 'order', 'having',
            'sum', 'avg', 'count', 'max', 'min', 'round', 'upper', 'lower',
            'like', 'and', 'or', 'not', 'in', 'between', 'is', 'null',
            'extract', 'year', 'month', 'day', 'date', 'limit', 'distinct',
            'as', 'case', 'when', 'then', 'else', 'end', 'cast', 'nullif',
            'coalesce', 'substring', 'length', 'trim', 'desc', 'asc'
        }
        
        # Palabras prohibidas (blacklist)
        self.palabras_prohibidas = {
            'drop', 'delete', 'insert', 'update', 'create', 'alter', 
            'truncate', 'grant', 'revoke', 'exec', 'execute', 'sp_',
            'xp_', 'sys', 'information_schema', 'pg_', 'admin', 'user'
        }
        
        # Columnas válidas de la tabla (ACTUALIZADAS CON ESQUEMA REAL)
        self.columnas_validas = {
            '"No. DECLARACION"', '"FECHA AAAA-MM-DD"', '"DOCUMENTO DE IDENTIFICACION"',
            '"IMPORTADOR"', '"REPRESENTANTE LEGAL IMPORTADOR"', '"PROVEEDOR"',
            '"PAIS ORIGEN"', '"CONTINENTE ORIGEN"', '"PAIS COMPRA"', '"PAIS PROCEDENCIA"',
            '"ARANCELARIO"', '"DESCRIPCION ARANCEL"', '"GRAVAMEN"', '"CODIGO ARANCELARIO"',
            '"DESC COD ARMONIZADO INGLES"', '"DESCRIPCION COMERCIAL DEL PRODUCTO"',
            '"CANTIDAD TOTAL"', '"UNIDAD COMERCIAL"', '"VALOR FOB (US$) TOTAL"',
            '"CIF (US$)"', '"PESO NETO (KG) TOTAL"', '"TOTAL PESO BRUTO (KG)"',
            '"TOTAL BULTOS"', '"VIA"', '"ADUANA"', '"CIUDAD INGRESO"',
            '"EMPRESA DE TRANSPORTE"', '"FLETE"', '"SEGURO"', '"OD"', '"SBU"', '"PRODUCTO"',
            '"CLASIFICACION"', '"COD"', '"MERCADO RELEVANTE"', '"CHEM_PAIS"',
            '"PROVEEDOR ACTUAL"', '"CLASIFICACION PROVEEDOR"', '"GRUPO IMPORTADOR"',
            '"CLAFICACION IMPORTADOR"'
        }
    
    def validar_sql_seguro(self, sql: str) -> tuple[bool, str]:
        """Valida que el SQL sea seguro para ejecutar"""
        sql_lower = sql.lower().strip()
        
        # 1. Verificar que empiece con SELECT
        if not sql_lower.startswith('select'):
            return False, "Solo se permiten consultas SELECT"
        
        # 2. Verificar palabras prohibidas
        for palabra in self.palabras_prohibidas:
            if palabra in sql_lower:
                return False, f"Palabra prohibida detectada: {palabra}"
        
        # 3. Verificar que solo use la tabla permitida
        if 'basf_import_data' not in sql_lower:
            return False, "Solo se permite consultar la tabla basf_import_data"
        
        # 4. Verificar límite máximo
        if 'limit' not in sql_lower:
            return False, "Debe incluir LIMIT para evitar consultas muy grandes"
        
        # 5. Extraer y validar LIMIT
        limit_match = re.search(r'limit\s+(\d+)', sql_lower)
        if limit_match:
            limit_value = int(limit_match.group(1))
            if limit_value > 500:
                return False, "LIMIT máximo permitido: 500 registros"
        
        # 6. Verificar columnas válidas (básico)
        for columna in self.columnas_validas:
            if columna.lower().replace('"', '') in sql_lower:
                continue  # Columna válida encontrada
        
        return True, "SQL validado correctamente"
    
    def sanitizar_sql(self, sql: str) -> str:
        """Limpia y mejora el SQL generado"""
        # Remover comentarios
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        
        # Añadir LIMIT si no existe
        if 'limit' not in sql.lower():
            sql += ' LIMIT 100'
        
        # Asegurar casting numérico para operaciones
        sql = re.sub(r'"FLETE"(?!\s*::)', '"FLETE"::numeric', sql)
        sql = re.sub(r'"PESO NETO \(KG\) TOTAL"(?!\s*::)', '"PESO NETO (KG) TOTAL"::numeric', sql)
        sql = re.sub(r'"VALOR FOB \(US\$\) TOTAL"(?!\s*::)', '"VALOR FOB (US$) TOTAL"::numeric', sql)
        sql = re.sub(r'"CIF \(US\$\)"(?!\s*::)', '"CIF (US$)"::numeric', sql)
        sql = re.sub(r'"SEGURO"(?!\s*::)', '"SEGURO"::numeric', sql)
        sql = re.sub(r'"GRAVAMEN"(?!\s*::)', '"GRAVAMEN"::numeric', sql)
        sql = re.sub(r'"CANTIDAD TOTAL"(?!\s*::)', '"CANTIDAD TOTAL"::numeric', sql)
        sql = re.sub(r'"TOTAL PESO BRUTO \(KG\)"(?!\s*::)', '"TOTAL PESO BRUTO (KG)"::numeric', sql)
        sql = re.sub(r'"TOTAL BULTOS"(?!\s*::)', '"TOTAL BULTOS"::numeric', sql)
        
        return sql.strip()

# Instancia del ejecutor
sql_executor = SQLSafeExecutor()

async def execute_sql_safe(sql: str, timeout: int = 25):
    """Ejecuta SQL validado con timeout"""
    try:
        def run_query():
            conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
            try:
                cursor = conn.cursor()
                cursor.execute(sql)
                return cursor.fetchall()
            finally:
                conn.close()
        
        loop = asyncio.get_event_loop()
        result = await asyncio.wait_for(
            loop.run_in_executor(None, run_query),
            timeout=timeout
        )
        return result
    except asyncio.TimeoutError:
        logger.error(f"SQL timeout: {sql[:100]}...")
        return None
    except Exception as e:
        logger.error(f"SQL error: {e}")
        return None

@app.get("/")
async def root():
    return {
        "service": "BASF AI-SQL API",
        "version": "4.0.0",
        "descripcion": "API que ejecuta SQL dinámico generado por IA de Microsoft Copilot Studio",
        "capacidades": [
            "Ejecución SQL segura con validación",
            "Metadata completa de la base de datos",
            "Sandbox SQL para prevenir queries peligrosas",
            "Optimización automática de consultas"
        ],
        "endpoints_principales": [
            "/esquema-bd - Información completa de la estructura de datos",
            "/ejecutar-sql - Ejecutar SQL generado por Copilot IA",
            "/validar-sql - Validar SQL antes de ejecutar"
        ]
    }

@app.get("/esquema-bd", response_model=MetadataResponse)
async def obtener_esquema_bd():
    """Proporciona toda la metadata necesaria para que Copilot IA genere SQL correcto"""
    try:
        # Obtener información de columnas
        query_columnas = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns 
        WHERE table_name = 'basf_import_data'
        ORDER BY ordinal_position
        """
        
        columnas_info = await execute_sql_safe(query_columnas, timeout=10)
        
        # Obtener ejemplos de valores únicos para cada columna clave
        query_ejemplos = """
        SELECT 
            'PRODUCTO' as columna,
            ARRAY_AGG(DISTINCT "PRODUCTO" ORDER BY "PRODUCTO" LIMIT 20) as valores_ejemplo
        FROM basf_import_data 
        WHERE "PRODUCTO" IS NOT NULL AND "PRODUCTO" != ''
        
        UNION ALL
        
        SELECT 
            'PROVEEDOR' as columna,
            ARRAY_AGG(DISTINCT "PROVEEDOR" ORDER BY "PROVEEDOR" LIMIT 15) as valores_ejemplo
        FROM basf_import_data 
        WHERE "PROVEEDOR" IS NOT NULL AND "PROVEEDOR" != ''
        
        UNION ALL
        
        SELECT 
            'PAIS ORIGEN' as columna,
            ARRAY_AGG(DISTINCT "PAIS ORIGEN" ORDER BY "PAIS ORIGEN" LIMIT 15) as valores_ejemplo
        FROM basf_import_data 
        WHERE "PAIS ORIGEN" IS NOT NULL AND "PAIS ORIGEN" != ''
        
        UNION ALL
        
        SELECT 
            'IMPORTADOR' as columna,
            ARRAY_AGG(DISTINCT "IMPORTADOR" ORDER BY "IMPORTADOR" LIMIT 10) as valores_ejemplo
        FROM basf_import_data 
        WHERE "IMPORTADOR" IS NOT NULL AND "IMPORTADOR" != ''
        
        UNION ALL
        
        SELECT 
            'VIA' as columna,
            ARRAY_AGG(DISTINCT "VIA" ORDER BY "VIA") as valores_ejemplo
        FROM basf_import_data 
        WHERE "VIA" IS NOT NULL AND "VIA" != ''
        
        UNION ALL
        
        SELECT 
            'ADUANA' as columna,
            ARRAY_AGG(DISTINCT "ADUANA" ORDER BY "ADUANA" LIMIT 10) as valores_ejemplo
        FROM basf_import_data 
        WHERE "ADUANA" IS NOT NULL AND "ADUANA" != ''
        """
        
        ejemplos_info = await execute_sql_safe(query_ejemplos, timeout=15)
        
        # Generar esquema sugerido para Copilot
        esquema_sugerido = """
        TABLA: basf_import_data
        
        COLUMNAS PRINCIPALES PARA ANÁLISIS:
        
        🏢 INFORMACIÓN DE IMPORTACIÓN:
        - "No. DECLARACION" (text): Número único de declaración de importación
        - "FECHA AAAA-MM-DD" (date): Fecha de la importación
        - "IMPORTADOR" (text): Empresa importadora
        - "REPRESENTANTE LEGAL IMPORTADOR" (text): Representante legal
        
        📦 INFORMACIÓN DEL PRODUCTO:
        - "PRODUCTO" (text): Nombre del producto químico BASF
        - "DESCRIPCION COMERCIAL DEL PRODUCTO" (text): Descripción detallada
        - "ARANCELARIO" (text): Código arancelario
        - "DESCRIPCION ARANCEL" (text): Descripción del arancel
        - "CLASIFICACION" (text): Clasificación del producto
        - "SBU" (text): Strategic Business Unit
        - "MERCADO RELEVANTE" (text): Mercado objetivo
        
        🏭 INFORMACIÓN DEL PROVEEDOR:
        - "PROVEEDOR" (text): Empresa proveedora
        - "PROVEEDOR ACTUAL" (text): Proveedor actual
        - "CLASIFICACION PROVEEDOR" (text): Tipo de proveedor
        - "PAIS ORIGEN" (text): País de origen del producto
        - "CONTINENTE ORIGEN" (text): Continente de origen
        - "PAIS COMPRA" (text): País donde se realizó la compra
        - "PAIS PROCEDENCIA" (text): País de procedencia
        - "CHEM_PAIS" (text): País químico clasificado
        
        💰 INFORMACIÓN FINANCIERA (USAR ::numeric PARA CÁLCULOS):
        - "VALOR FOB (US$) TOTAL" (numeric): Valor FOB en dólares
        - "CIF (US$)" (numeric): Valor CIF en dólares
        - "FLETE" (numeric): Costo de flete en USD
        - "SEGURO" (numeric): Costo de seguro
        - "GRAVAMEN" (numeric): Gravamen aplicado
        
        📏 INFORMACIÓN DE CANTIDAD/PESO:
        - "CANTIDAD TOTAL" (numeric): Cantidad total importada
        - "UNIDAD COMERCIAL" (text): Unidad de medida
        - "PESO NETO (KG) TOTAL" (numeric): Peso neto en kilogramos
        - "TOTAL PESO BRUTO (KG)" (numeric): Peso bruto total
        - "TOTAL BULTOS" (numeric): Número total de bultos
        
        🚚 INFORMACIÓN LOGÍSTICA:
        - "VIA" (text): Vía de transporte (Marítima, Aérea, Terrestre)
        - "EMPRESA DE TRANSPORTE" (text): Empresa transportista
        - "ADUANA" (text): Aduana de ingreso
        - "CIUDAD INGRESO" (text): Ciudad de ingreso
        
        👥 CLASIFICACIONES ADICIONALES:
        - "GRUPO IMPORTADOR" (text): Grupo al que pertenece el importador
        - "CLAFICACION IMPORTADOR" (text): Clasificación del importador
        - "COD" (text): Código adicional
        - "OD" (text): Campo OD
        
        EJEMPLOS DE CONSULTAS SQL COMPLEJAS:
        
        1. Análisis de costos logísticos por producto:
        SELECT 
            "PRODUCTO",
            AVG("FLETE"::numeric) as flete_promedio,
            AVG("SEGURO"::numeric) as seguro_promedio,
            AVG(("FLETE"::numeric + "SEGURO"::numeric)) as costo_logistico_total,
            AVG("FLETE"::numeric / NULLIF("PESO NETO (KG) TOTAL"::numeric, 0)) as flete_por_kg
        FROM basf_import_data 
        WHERE "FLETE" > 0 AND "PESO NETO (KG) TOTAL" > 0
        GROUP BY "PRODUCTO" 
        ORDER BY costo_logistico_total DESC 
        LIMIT 20
        
        2. Análisis de proveedores por SBU:
        SELECT 
            "SBU",
            "PAIS ORIGEN",
            COUNT(DISTINCT "PROVEEDOR") as num_proveedores,
            SUM("VALOR FOB (US$) TOTAL"::numeric) as valor_total_usd,
            AVG("FLETE"::numeric / NULLIF("VALOR FOB (US$) TOTAL"::numeric, 0) * 100) as porcentaje_flete
        FROM basf_import_data 
        WHERE "SBU" IS NOT NULL AND "VALOR FOB (US$) TOTAL" > 0
        GROUP BY "SBU", "PAIS ORIGEN"
        ORDER BY valor_total_usd DESC 
        LIMIT 25
        
        3. Evolución temporal por mercado relevante:
        SELECT 
            "MERCADO RELEVANTE",
            EXTRACT(YEAR FROM "FECHA AAAA-MM-DD"::date) as año,
            EXTRACT(MONTH FROM "FECHA AAAA-MM-DD"::date) as mes,
            COUNT(*) as num_importaciones,
            SUM("PESO NETO (KG) TOTAL"::numeric) as volumen_total_kg,
            AVG("CIF (US$)"::numeric / NULLIF("PESO NETO (KG) TOTAL"::numeric, 0)) as precio_promedio_kg
        FROM basf_import_data 
        WHERE "MERCADO RELEVANTE" IS NOT NULL 
        AND "FECHA AAAA-MM-DD" IS NOT NULL
        AND "PESO NETO (KG) TOTAL" > 0
        GROUP BY "MERCADO RELEVANTE", EXTRACT(YEAR FROM "FECHA AAAA-MM-DD"::date), EXTRACT(MONTH FROM "FECHA AAAA-MM-DD"::date)
        ORDER BY año DESC, mes DESC 
        LIMIT 30
        
        4. Análisis comparativo de aduanas:
        SELECT 
            "ADUANA",
            "CIUDAD INGRESO",
            "VIA",
            COUNT(*) as total_importaciones,
            AVG("FLETE"::numeric) as flete_promedio,
            SUM("VALOR FOB (US$) TOTAL"::numeric) as valor_total_procesado,
            COUNT(DISTINCT "IMPORTADOR") as num_importadores_unicos
        FROM basf_import_data 
        WHERE "ADUANA" IS NOT NULL AND "FLETE" > 0
        GROUP BY "ADUANA", "CIUDAD INGRESO", "VIA"
        ORDER BY total_importaciones DESC 
        LIMIT 15
        
        REGLAS CRÍTICAS PARA GENERAR SQL:
        ✅ Siempre usar LIMIT (máximo 500 registros)
        ✅ Usar casting ::numeric para columnas monetarias y de peso
        ✅ Filtrar valores NULL y cero cuando sea relevante
        ✅ Usar comillas dobles para TODOS los nombres de columnas
        ✅ Para fechas usar: EXTRACT(YEAR/MONTH/DAY FROM "FECHA AAAA-MM-DD"::date)
        ✅ Para porcentajes: (valor1/NULLIF(valor2,0)*100)
        ✅ Para texto usar UPPER() en comparaciones LIKE
        
        COLUMNAS MÁS CONSULTADAS POR CATEGORÍA:
        📊 Análisis financiero: "VALOR FOB (US$) TOTAL", "CIF (US$)", "FLETE", "SEGURO"
        📦 Análisis de producto: "PRODUCTO", "SBU", "CLASIFICACION", "MERCADO RELEVANTE"
        🌍 Análisis geográfico: "PAIS ORIGEN", "CONTINENTE ORIGEN", "ADUANA", "CIUDAD INGRESO"
        🏢 Análisis de negocio: "IMPORTADOR", "PROVEEDOR", "GRUPO IMPORTADOR"
        """
        
        return MetadataResponse(
            tablas=[{"nombre": "basf_import_data", "descripcion": "Datos de importaciones BASF"}],
            columnas=[dict(col) for col in columnas_info] if columnas_info else [],
            ejemplos_valores=[dict(ej) for ej in ejemplos_info] if ejemplos_info else [],
            esquema_sugerido=esquema_sugerido
        )
        
    except Exception as e:
        logger.error(f"Error obteniendo esquema: {e}")
        raise HTTPException(status_code=500, detail=f"Error obteniendo esquema: {str(e)}")

@app.post("/ejecutar-sql")
async def ejecutar_sql_desde_ia(request: SQLRequest):
    """Ejecuta SQL generado por la IA de Copilot Studio con validación completa"""
    try:
        sql_original = request.instruccion_sql.strip()
        contexto = request.contexto or "Consulta desde Copilot IA"
        
        logger.info(f"SQL recibido desde IA: {sql_original[:200]}...")
        
        # 1. Validar seguridad
        es_seguro, mensaje_validacion = sql_executor.validar_sql_seguro(sql_original)
        if not es_seguro:
            return {
                "status": "sql_invalido",
                "error": mensaje_validacion,
                "sql_original": sql_original,
                "contexto": contexto,
                "datos": []
            }
        
        # 2. Sanitizar y optimizar
        sql_sanitizado = sql_executor.sanitizar_sql(sql_original)
        
        # 3. Ejecutar
        resultados = await execute_sql_safe(sql_sanitizado, timeout=22)
        
        if resultados is None:
            return {
                "status": "timeout",
                "mensaje": "La consulta tardó demasiado en ejecutarse",
                "sugerencia": "Intenta agregar más filtros WHERE o reducir el LIMIT",
                "sql_ejecutado": sql_sanitizado,
                "datos": []
            }
        
        if not resultados:
            return {
                "status": "sin_resultados",
                "mensaje": "La consulta no devolvió resultados",
                "sql_ejecutado": sql_sanitizado,
                "datos": []
            }
        
        # 4. Procesar resultados
        datos_procesados = []
        for registro in resultados:
            datos_procesados.append({k: v for k, v in registro.items()})
        
        # 5. Generar resumen inteligente
        resumen = generar_resumen_resultados(datos_procesados, contexto)
        
        return {
            "status": "exitoso",
            "mensaje": f"Consulta ejecutada exitosamente - {len(datos_procesados)} registros",
            "contexto": contexto,
            "resumen": resumen,
            "datos": datos_procesados,
            "total_registros": len(datos_procesados),
            "sql_ejecutado": sql_sanitizado,
            "metadata": {
                "columnas": list(datos_procesados[0].keys()) if datos_procesados else [],
                "tipos_datos": detectar_tipos_datos(datos_procesados)
            }
        }
        
    except Exception as e:
        logger.error(f"Error ejecutando SQL desde IA: {e}")
        return {
            "status": "error_ejecucion",
            "error": str(e),
            "sql_original": request.instruccion_sql,
            "datos": []
        }

def generar_resumen_resultados(datos: List[Dict], contexto: str) -> Dict[str, Any]:
    """Genera un resumen inteligente de los resultados"""
    if not datos:
        return {"mensaje": "Sin datos para resumir"}
    
    resumen = {
        "total_registros": len(datos),
        "columnas_principales": list(datos[0].keys())[:5],
        "muestra_datos": datos[:3] if len(datos) > 3 else datos
    }
    
    # Detectar si hay columnas numéricas para estadísticas
    columnas_numericas = []
    for col in datos[0].keys():
        if isinstance(datos[0][col], (int, float)) and datos[0][col] is not None:
            columnas_numericas.append(col)
    
    if columnas_numericas:
        resumen["estadisticas"] = {}
        for col in columnas_numericas[:3]:  # Máximo 3 columnas numéricas
            valores = [row[col] for row in datos if row[col] is not None]
            if valores:
                resumen["estadisticas"][col] = {
                    "promedio": round(sum(valores) / len(valores), 2),
                    "maximo": max(valores),
                    "minimo": min(valores)
                }
    
    return resumen

def detectar_tipos_datos(datos: List[Dict]) -> Dict[str, str]:
    """Detecta tipos de datos de las columnas"""
    if not datos:
        return {}
    
    tipos = {}
    for col, valor in datos[0].items():
        if isinstance(valor, str):
            tipos[col] = "texto"
        elif isinstance(valor, (int, float)):
            tipos[col] = "numerico"
        elif valor is None:
            tipos[col] = "nulo"
        else:
            tipos[col] = "otro"
    
    return tipos

@app.get("/validar-sql")
async def validar_sql_preview(
    sql: str = Query(..., description="SQL a validar"),
    mostrar_explicacion: bool = Query(False, description="Mostrar explicación detallada")
):
    """Valida SQL sin ejecutar - útil para debugging"""
    try:
        es_seguro, mensaje = sql_executor.validar_sql_seguro(sql)
        sql_sanitizado = sql_executor.sanitizar_sql(sql) if es_seguro else None
        
        resultado = {
            "sql_original": sql,
            "es_valido": es_seguro,
            "mensaje_validacion": mensaje,
            "sql_sanitizado": sql_sanitizado
        }
        
        if mostrar_explicacion:
            resultado["explicacion"] = {
                "palabras_detectadas": re.findall(r'\b\w+\b', sql.lower()),
                "tabla_detectada": "basf_import_data" in sql.lower(),
                "limit_detectado": "limit" in sql.lower(),
                "columnas_mencionadas": [col for col in sql_executor.columnas_validas if col.lower().replace('"', '') in sql.lower()]
            }
        
        return resultado
        
    except Exception as e:
        return {
            "sql_original": sql,
            "es_valido": False,
            "mensaje_validacion": f"Error en validación: {str(e)}"
        }

@app.get("/test-conexion")
async def test_conexion_rapida():
    """Test rápido de conectividad"""
    try:
        result = await execute_sql_safe("SELECT 1 as test", timeout=5)
        return {"status": "ok", "conexion": "exitosa"} if result else {"status": "timeout"}
    except Exception as e:
        return {"status": "error", "error": str(e)}