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

# Se indica el URL donde se almacena la base de datos
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
        
        # Se indican columnas del dataset
        self.columnas_validas = {
            "country",
            "year",
            "iso_code",
            "population",
            "gdp",
            "biofuel_cons_change_pct",
            "biofuel_cons_change_twh",
            "biofuel_cons_per_capita",
            "biofuel_consumption",
            "biofuel_elec_per_capita",
            "biofuel_electricity",
            "biofuel_share_elec",
            "biofuel_share_energy",
            "carbon_intensity_elec",
            "coal_cons_change_pct",
            "coal_cons_change_twh",
            "coal_cons_per_capita",
            "coal_consumption",
            "coal_elec_per_capita",
            "coal_electricity",
            "coal_prod_change_pct",
            "coal_prod_change_twh",
            "coal_prod_per_capita",
            "coal_production",
            "coal_share_elec",
            "coal_share_energy",
            "electricity_demand",
            "electricity_demand_per_capita",
            "electricity_generation",
            "electricity_share_energy",
            "energy_cons_change_pct",
            "energy_cons_change_twh",
            "energy_per_capita",
            "energy_per_gdp",
            "fossil_cons_change_pct",
            "fossil_cons_change_twh",
            "fossil_elec_per_capita",
            "fossil_electricity",
            "fossil_energy_per_capita",
            "fossil_fuel_consumption",
            "fossil_share_elec",
            "fossil_share_energy",
            "gas_cons_change_pct",
            "gas_cons_change_twh",
            "gas_consumption",
            "gas_elec_per_capita",
            "gas_electricity",
            "gas_energy_per_capita",
            "gas_prod_change_pct",
            "gas_prod_change_twh",
            "gas_prod_per_capita",
            "gas_production",
            "gas_share_elec",
            "gas_share_energy",
            "greenhouse_gas_emissions",
            "hydro_cons_change_pct",
            "hydro_cons_change_twh",
            "hydro_consumption",
            "hydro_elec_per_capita",
            "hydro_electricity",
            "hydro_energy_per_capita",
            "hydro_share_elec",
            "hydro_share_energy",
            "low_carbon_cons_change_pct",
            "low_carbon_cons_change_twh",
            "low_carbon_consumption",
            "low_carbon_elec_per_capita",
            "low_carbon_electricity",
            "low_carbon_energy_per_capita",
            "low_carbon_share_elec",
            "low_carbon_share_energy",
            "net_elec_imports",
            "net_elec_imports_share_demand",
            "nuclear_cons_change_pct",
            "nuclear_cons_change_twh",
            "nuclear_consumption",
            "nuclear_elec_per_capita",
            "nuclear_electricity",
            "nuclear_energy_per_capita",
            "nuclear_share_elec",
            "nuclear_share_energy",
            "oil_cons_change_pct",
            "oil_cons_change_twh",
            "oil_consumption",
            "oil_elec_per_capita",
            "oil_electricity",
            "oil_energy_per_capita",
            "oil_prod_change_pct",
            "oil_prod_change_twh",
            "oil_prod_per_capita",
            "oil_production",
            "oil_share_elec",
            "oil_share_energy",
            "other_renewable_consumption",
            "other_renewable_electricity",
            "other_renewable_exc_biofuel_electricity",
            "other_renewables_cons_change_pct",
            "other_renewables_cons_change_twh",
            "other_renewables_elec_per_capita",
            "other_renewables_elec_per_capita_exc_biofuel",
            "other_renewables_energy_per_capita",
            "other_renewables_share_elec",
            "other_renewables_share_elec_exc_biofuel",
            "other_renewables_share_energy",
            "per_capita_electricity",
            "primary_energy_consumption",
            "renewables_cons_change_pct",
            "renewables_cons_change_twh",
            "renewables_consumption",
            "renewables_elec_per_capita",
            "renewables_electricity",
            "renewables_energy_per_capita",
            "renewables_share_elec",
            "renewables_share_energy",
            "solar_cons_change_pct",
            "solar_cons_change_twh",
            "solar_consumption",
            "solar_elec_per_capita",
            "solar_electricity",
            "solar_energy_per_capita",
            "solar_share_elec",
            "solar_share_energy",
            "wind_cons_change_pct",
            "wind_cons_change_twh",
            "wind_consumption",
            "wind_elec_per_capita",
            "wind_electricity",
            "wind_energy_per_capita",
            "wind_share_elec",
            "wind_share_energy",
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
        if 'world_energy' not in sql_lower:
            return False, "Solo se permite consultar la tabla world_energy"
        
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

        sql = re.sub(r'"year"(?!\s*::)', '"year"::numeric', sql)
        sql = re.sub(r'"population"(?!\s*::)', '"population"::numeric', sql)
        sql = re.sub(r'"gdp"(?!\s*::)', '"gdp"::numeric', sql)
        sql = re.sub(r'"biofuel_cons_change_pct"(?!\s*::)', '"biofuel_cons_change_pct"::numeric', sql)
        sql = re.sub(r'"biofuel_cons_change_twh"(?!\s*::)', '"biofuel_cons_change_twh"::numeric', sql)
        sql = re.sub(r'"biofuel_cons_per_capita"(?!\s*::)', '"biofuel_cons_per_capita"::numeric', sql)
        sql = re.sub(r'"biofuel_consumption"(?!\s*::)', '"biofuel_consumption"::numeric', sql)
        sql = re.sub(r'"biofuel_elec_per_capita"(?!\s*::)', '"biofuel_elec_per_capita"::numeric', sql)
        sql = re.sub(r'"biofuel_electricity"(?!\s*::)', '"biofuel_electricity"::numeric', sql)
        sql = re.sub(r'"biofuel_share_elec"(?!\s*::)', '"biofuel_share_elec"::numeric', sql)
        sql = re.sub(r'"biofuel_share_energy"(?!\s*::)', '"biofuel_share_energy"::numeric', sql)
        sql = re.sub(r'"carbon_intensity_elec"(?!\s*::)', '"carbon_intensity_elec"::numeric', sql)
        sql = re.sub(r'"coal_cons_change_pct"(?!\s*::)', '"coal_cons_change_pct"::numeric', sql)
        sql = re.sub(r'"coal_cons_change_twh"(?!\s*::)', '"coal_cons_change_twh"::numeric', sql)
        sql = re.sub(r'"coal_cons_per_capita"(?!\s*::)', '"coal_cons_per_capita"::numeric', sql)
        sql = re.sub(r'"coal_consumption"(?!\s*::)', '"coal_consumption"::numeric', sql)
        sql = re.sub(r'"coal_elec_per_capita"(?!\s*::)', '"coal_elec_per_capita"::numeric', sql)
        sql = re.sub(r'"coal_electricity"(?!\s*::)', '"coal_electricity"::numeric', sql)
        sql = re.sub(r'"coal_prod_change_pct"(?!\s*::)', '"coal_prod_change_pct"::numeric', sql)
        sql = re.sub(r'"coal_prod_change_twh"(?!\s*::)', '"coal_prod_change_twh"::numeric', sql)
        sql = re.sub(r'"coal_prod_per_capita"(?!\s*::)', '"coal_prod_per_capita"::numeric', sql)
        sql = re.sub(r'"coal_production"(?!\s*::)', '"coal_production"::numeric', sql)
        sql = re.sub(r'"coal_share_elec"(?!\s*::)', '"coal_share_elec"::numeric', sql)
        sql = re.sub(r'"coal_share_energy"(?!\s*::)', '"coal_share_energy"::numeric', sql)
        sql = re.sub(r'"electricity_demand"(?!\s*::)', '"electricity_demand"::numeric', sql)
        sql = re.sub(r'"electricity_demand_per_capita"(?!\s*::)', '"electricity_demand_per_capita"::numeric', sql)
        sql = re.sub(r'"electricity_generation"(?!\s*::)', '"electricity_generation"::numeric', sql)
        sql = re.sub(r'"electricity_share_energy"(?!\s*::)', '"electricity_share_energy"::numeric', sql)
        sql = re.sub(r'"energy_cons_change_pct"(?!\s*::)', '"energy_cons_change_pct"::numeric', sql)
        sql = re.sub(r'"energy_cons_change_twh"(?!\s*::)', '"energy_cons_change_twh"::numeric', sql)
        sql = re.sub(r'"energy_per_capita"(?!\s*::)', '"energy_per_capita"::numeric', sql)
        sql = re.sub(r'"energy_per_gdp"(?!\s*::)', '"energy_per_gdp"::numeric', sql)
        sql = re.sub(r'"fossil_cons_change_pct"(?!\s*::)', '"fossil_cons_change_pct"::numeric', sql)
        sql = re.sub(r'"fossil_cons_change_twh"(?!\s*::)', '"fossil_cons_change_twh"::numeric', sql)
        sql = re.sub(r'"fossil_elec_per_capita"(?!\s*::)', '"fossil_elec_per_capita"::numeric', sql)
        sql = re.sub(r'"fossil_electricity"(?!\s*::)', '"fossil_electricity"::numeric', sql)
        sql = re.sub(r'"fossil_energy_per_capita"(?!\s*::)', '"fossil_energy_per_capita"::numeric', sql)
        sql = re.sub(r'"fossil_fuel_consumption"(?!\s*::)', '"fossil_fuel_consumption"::numeric', sql)
        sql = re.sub(r'"fossil_share_elec"(?!\s*::)', '"fossil_share_elec"::numeric', sql)
        sql = re.sub(r'"fossil_share_energy"(?!\s*::)', '"fossil_share_energy"::numeric', sql)
        sql = re.sub(r'"gas_cons_change_pct"(?!\s*::)', '"gas_cons_change_pct"::numeric', sql)
        sql = re.sub(r'"gas_cons_change_twh"(?!\s*::)', '"gas_cons_change_twh"::numeric', sql)
        sql = re.sub(r'"gas_consumption"(?!\s*::)', '"gas_consumption"::numeric', sql)
        sql = re.sub(r'"gas_elec_per_capita"(?!\s*::)', '"gas_elec_per_capita"::numeric', sql)
        sql = re.sub(r'"gas_electricity"(?!\s*::)', '"gas_electricity"::numeric', sql)
        sql = re.sub(r'"gas_energy_per_capita"(?!\s*::)', '"gas_energy_per_capita"::numeric', sql)
        sql = re.sub(r'"gas_prod_change_pct"(?!\s*::)', '"gas_prod_change_pct"::numeric', sql)
        sql = re.sub(r'"gas_prod_change_twh"(?!\s*::)', '"gas_prod_change_twh"::numeric', sql)
        sql = re.sub(r'"gas_prod_per_capita"(?!\s*::)', '"gas_prod_per_capita"::numeric', sql)
        sql = re.sub(r'"gas_production"(?!\s*::)', '"gas_production"::numeric', sql)
        sql = re.sub(r'"gas_share_elec"(?!\s*::)', '"gas_share_elec"::numeric', sql)
        sql = re.sub(r'"gas_share_energy"(?!\s*::)', '"gas_share_energy"::numeric', sql)
        sql = re.sub(r'"greenhouse_gas_emissions"(?!\s*::)', '"greenhouse_gas_emissions"::numeric', sql)
        sql = re.sub(r'"hydro_cons_change_pct"(?!\s*::)', '"hydro_cons_change_pct"::numeric', sql)
        sql = re.sub(r'"hydro_cons_change_twh"(?!\s*::)', '"hydro_cons_change_twh"::numeric', sql)
        sql = re.sub(r'"hydro_consumption"(?!\s*::)', '"hydro_consumption"::numeric', sql)
        sql = re.sub(r'"hydro_elec_per_capita"(?!\s*::)', '"hydro_elec_per_capita"::numeric', sql)
        sql = re.sub(r'"hydro_electricity"(?!\s*::)', '"hydro_electricity"::numeric', sql)
        sql = re.sub(r'"hydro_energy_per_capita"(?!\s*::)', '"hydro_energy_per_capita"::numeric', sql)
        sql = re.sub(r'"hydro_share_elec"(?!\s*::)', '"hydro_share_elec"::numeric', sql)
        sql = re.sub(r'"hydro_share_energy"(?!\s*::)', '"hydro_share_energy"::numeric', sql)
        sql = re.sub(r'"low_carbon_cons_change_pct"(?!\s*::)', '"low_carbon_cons_change_pct"::numeric', sql)
        sql = re.sub(r'"low_carbon_cons_change_twh"(?!\s*::)', '"low_carbon_cons_change_twh"::numeric', sql)
        sql = re.sub(r'"low_carbon_consumption"(?!\s*::)', '"low_carbon_consumption"::numeric', sql)
        sql = re.sub(r'"low_carbon_elec_per_capita"(?!\s*::)', '"low_carbon_elec_per_capita"::numeric', sql)
        sql = re.sub(r'"low_carbon_electricity"(?!\s*::)', '"low_carbon_electricity"::numeric', sql)
        sql = re.sub(r'"low_carbon_energy_per_capita"(?!\s*::)', '"low_carbon_energy_per_capita"::numeric', sql)
        sql = re.sub(r'"low_carbon_share_elec"(?!\s*::)', '"low_carbon_share_elec"::numeric', sql)
        sql = re.sub(r'"low_carbon_share_energy"(?!\s*::)', '"low_carbon_share_energy"::numeric', sql)
        sql = re.sub(r'"net_elec_imports"(?!\s*::)', '"net_elec_imports"::numeric', sql)
        sql = re.sub(r'"net_elec_imports_share_demand"(?!\s*::)', '"net_elec_imports_share_demand"::numeric', sql)
        sql = re.sub(r'"nuclear_cons_change_pct"(?!\s*::)', '"nuclear_cons_change_pct"::numeric', sql)
        sql = re.sub(r'"nuclear_cons_change_twh"(?!\s*::)', '"nuclear_cons_change_twh"::numeric', sql)
        sql = re.sub(r'"nuclear_consumption"(?!\s*::)', '"nuclear_consumption"::numeric', sql)
        sql = re.sub(r'"nuclear_elec_per_capita"(?!\s*::)', '"nuclear_elec_per_capita"::numeric', sql)
        sql = re.sub(r'"nuclear_electricity"(?!\s*::)', '"nuclear_electricity"::numeric', sql)
        sql = re.sub(r'"nuclear_energy_per_capita"(?!\s*::)', '"nuclear_energy_per_capita"::numeric', sql)
        sql = re.sub(r'"nuclear_share_elec"(?!\s*::)', '"nuclear_share_elec"::numeric', sql)
        sql = re.sub(r'"nuclear_share_energy"(?!\s*::)', '"nuclear_share_energy"::numeric', sql)
        sql = re.sub(r'"oil_cons_change_pct"(?!\s*::)', '"oil_cons_change_pct"::numeric', sql)
        sql = re.sub(r'"oil_cons_change_twh"(?!\s*::)', '"oil_cons_change_twh"::numeric', sql)
        sql = re.sub(r'"oil_consumption"(?!\s*::)', '"oil_consumption"::numeric', sql)
        sql = re.sub(r'"oil_elec_per_capita"(?!\s*::)', '"oil_elec_per_capita"::numeric', sql)
        sql = re.sub(r'"oil_electricity"(?!\s*::)', '"oil_electricity"::numeric', sql)
        sql = re.sub(r'"oil_energy_per_capita"(?!\s*::)', '"oil_energy_per_capita"::numeric', sql)
        sql = re.sub(r'"oil_prod_change_pct"(?!\s*::)', '"oil_prod_change_pct"::numeric', sql)
        sql = re.sub(r'"oil_prod_change_twh"(?!\s*::)', '"oil_prod_change_twh"::numeric', sql)
        sql = re.sub(r'"oil_prod_per_capita"(?!\s*::)', '"oil_prod_per_capita"::numeric', sql)
        sql = re.sub(r'"oil_production"(?!\s*::)', '"oil_production"::numeric', sql)
        sql = re.sub(r'"oil_share_elec"(?!\s*::)', '"oil_share_elec"::numeric', sql)
        sql = re.sub(r'"oil_share_energy"(?!\s*::)', '"oil_share_energy"::numeric', sql)
        sql = re.sub(r'"other_renewable_consumption"(?!\s*::)', '"other_renewable_consumption"::numeric', sql)
        sql = re.sub(r'"other_renewable_electricity"(?!\s*::)', '"other_renewable_electricity"::numeric', sql)
        sql = re.sub(r'"other_renewable_exc_biofuel_electricity"(?!\s*::)', '"other_renewable_exc_biofuel_electricity"::numeric', sql)
        sql = re.sub(r'"other_renewables_cons_change_pct"(?!\s*::)', '"other_renewables_cons_change_pct"::numeric', sql)
        sql = re.sub(r'"other_renewables_cons_change_twh"(?!\s*::)', '"other_renewables_cons_change_twh"::numeric', sql)
        sql = re.sub(r'"other_renewables_elec_per_capita"(?!\s*::)', '"other_renewables_elec_per_capita"::numeric', sql)
        sql = re.sub(r'"other_renewables_elec_per_capita_exc_biofuel"(?!\s*::)', '"other_renewables_elec_per_capita_exc_biofuel"::numeric', sql)
        sql = re.sub(r'"other_renewables_energy_per_capita"(?!\s*::)', '"other_renewables_energy_per_capita"::numeric', sql)
        sql = re.sub(r'"other_renewables_share_elec"(?!\s*::)', '"other_renewables_share_elec"::numeric', sql)
        sql = re.sub(r'"other_renewables_share_elec_exc_biofuel"(?!\s*::)', '"other_renewables_share_elec_exc_biofuel"::numeric', sql)
        sql = re.sub(r'"other_renewables_share_energy"(?!\s*::)', '"other_renewables_share_energy"::numeric', sql)
        sql = re.sub(r'"per_capita_electricity"(?!\s*::)', '"per_capita_electricity"::numeric', sql)
        sql = re.sub(r'"primary_energy_consumption"(?!\s*::)', '"primary_energy_consumption"::numeric', sql)
        sql = re.sub(r'"renewables_cons_change_pct"(?!\s*::)', '"renewables_cons_change_pct"::numeric', sql)
        sql = re.sub(r'"renewables_cons_change_twh"(?!\s*::)', '"renewables_cons_change_twh"::numeric', sql)
        sql = re.sub(r'"renewables_consumption"(?!\s*::)', '"renewables_consumption"::numeric', sql)
        sql = re.sub(r'"renewables_elec_per_capita"(?!\s*::)', '"renewables_elec_per_capita"::numeric', sql)
        sql = re.sub(r'"renewables_electricity"(?!\s*::)', '"renewables_electricity"::numeric', sql)
        sql = re.sub(r'"renewables_energy_per_capita"(?!\s*::)', '"renewables_energy_per_capita"::numeric', sql)
        sql = re.sub(r'"renewables_share_elec"(?!\s*::)', '"renewables_share_elec"::numeric', sql)
        sql = re.sub(r'"renewables_share_energy"(?!\s*::)', '"renewables_share_energy"::numeric', sql)
        sql = re.sub(r'"solar_cons_change_pct"(?!\s*::)', '"solar_cons_change_pct"::numeric', sql)
        sql = re.sub(r'"solar_cons_change_twh"(?!\s*::)', '"solar_cons_change_twh"::numeric', sql)
        sql = re.sub(r'"solar_consumption"(?!\s*::)', '"solar_consumption"::numeric', sql)
        sql = re.sub(r'"solar_elec_per_capita"(?!\s*::)', '"solar_elec_per_capita"::numeric', sql)
        sql = re.sub(r'"solar_electricity"(?!\s*::)', '"solar_electricity"::numeric', sql)
        sql = re.sub(r'"solar_energy_per_capita"(?!\s*::)', '"solar_energy_per_capita"::numeric', sql)
        sql = re.sub(r'"solar_share_elec"(?!\s*::)', '"solar_share_elec"::numeric', sql)
        sql = re.sub(r'"solar_share_energy"(?!\s*::)', '"solar_share_energy"::numeric', sql)
        sql = re.sub(r'"wind_cons_change_pct"(?!\s*::)', '"wind_cons_change_pct"::numeric', sql)
        sql = re.sub(r'"wind_cons_change_twh"(?!\s*::)', '"wind_cons_change_twh"::numeric', sql)
        sql = re.sub(r'"wind_consumption"(?!\s*::)', '"wind_consumption"::numeric', sql)
        sql = re.sub(r'"wind_elec_per_capita"(?!\s*::)', '"wind_elec_per_capita"::numeric', sql)
        sql = re.sub(r'"wind_electricity"(?!\s*::)', '"wind_electricity"::numeric', sql)
        sql = re.sub(r'"wind_energy_per_capita"(?!\s*::)', '"wind_energy_per_capita"::numeric', sql)
        sql = re.sub(r'"wind_share_elec"(?!\s*::)', '"wind_share_elec"::numeric', sql)
        sql = re.sub(r'"wind_share_energy"(?!\s*::)', '"wind_share_energy"::numeric', sql)
        
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
        WHERE table_name = 'world_energy'
        ORDER BY ordinal_position
        """
        
        columnas_info = await execute_sql_safe(query_columnas, timeout=10)
        
        # Obtener ejemplos de valores únicos para cada columna clave
        query_ejemplos = """
        SELECT 
            'population' as columna,
            MIN("population"::numeric) as minimo,
            MAX("population"::numeric) as maximo,
            AVG("population"::numeric) as promedio,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "population"::numeric) as mediana
        FROM world_energy 
        WHERE "population" IS NOT NULL
        
        UNION ALL
        
        SELECT 
            'gdp' as columna,
            MIN("gdp"::numeric) as minimo,
            MAX("gdp"::numeric) as maximo,
            AVG("gdp"::numeric) as promedio,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "gdp"::numeric) as mediana
        FROM world_energy 
        WHERE "gdp" IS NOT NULL
        
        UNION ALL
        
        SELECT 
            'coal_consumption' as columna,
            MIN("coal_consumption"::numeric) as minimo,
            MAX("coal_consumption"::numeric) as maximo,
            AVG("coal_consumption"::numeric) as promedio,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "coal_consumption"::numeric) as mediana
        FROM world_energy 
        WHERE "coal_consumption" IS NOT NULL
        
        UNION ALL
        
        SELECT 
            'renewables_share_elec' as columna,
            MIN("renewables_share_elec"::numeric) as minimo,
            MAX("renewables_share_elec"::numeric) as maximo,
            AVG("renewables_share_elec"::numeric) as promedio,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "renewables_share_elec"::numeric) as mediana
        FROM world_energy 
        WHERE "renewables_share_elec" IS NOT NULL
        
        UNION ALL
        
        SELECT 
            'solar_electricity' as columna,
            MIN("solar_electricity"::numeric) as minimo,
            MAX("solar_electricity"::numeric) as maximo,
            AVG("solar_electricity"::numeric) as promedio,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "solar_electricity"::numeric) as mediana
        FROM world_energy 
        WHERE "solar_electricity" IS NOT NULL
        
        UNION ALL
        
        SELECT 
            'wind_electricity' as columna,
            MIN("wind_electricity"::numeric) as minimo,
            MAX("wind_electricity"::numeric) as maximo,
            AVG("wind_electricity"::numeric) as promedio,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "wind_electricity"::numeric) as mediana
        FROM world_energy 
        WHERE "wind_electricity" IS NOT NULL
        
        UNION ALL
        
        SELECT 
            'greenhouse_gas_emissions' as columna,
            MIN("greenhouse_gas_emissions"::numeric) as minimo,
            MAX("greenhouse_gas_emissions"::numeric) as maximo,
            AVG("greenhouse_gas_emissions"::numeric) as promedio,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "greenhouse_gas_emissions"::numeric) as mediana
        FROM world_energy 
        WHERE "greenhouse_gas_emissions" IS NOT NULL
        """
        
        ejemplos_info = await execute_sql_safe(query_ejemplos, timeout=15)
        
        # Generar esquema sugerido para Copilot
        esquema_sugerido = """
        TABLA: world_energy
        
        COLUMNAS PRINCIPALES PARA ANÁLISIS:
        
       🌍 INFORMACIÓN GEOGRÁFICA Y TEMPORAL:
        - "country" (text): País o región geográfica
        - "iso_code" (text): Código ISO 3166-1 alpha-3 de tres letras
        - "year" (numeric): Año de observación (típicamente 1965-2023)
        
        👥 INFORMACIÓN DEMOGRÁFICA Y ECONÓMICA (USAR ::numeric):
        - "population" (numeric): Población total del país
        - "gdp" (numeric): PIB en dólares internacionales ajustados (2011 prices)
        
        ⚡ CONSUMO ENERGÉTICO GENERAL (USAR ::numeric):
        - "primary_energy_consumption" (numeric): Consumo total de energía primaria en TWh
        - "energy_per_capita" (numeric): Consumo energético por persona en kWh
        - "energy_per_gdp" (numeric): Energía por unidad de PIB en kWh/$
        - "energy_cons_change_pct" (numeric): Cambio porcentual anual en consumo
        - "energy_cons_change_twh" (numeric): Cambio absoluto anual en TWh
        
        ⚫ CARBÓN (USAR ::numeric):
        - "coal_consumption" (numeric): Consumo de carbón en TWh
        - "coal_production" (numeric): Producción de carbón en TWh
        - "coal_electricity" (numeric): Electricidad generada con carbón en TWh
        - "coal_cons_per_capita" (numeric): Consumo de carbón per cápita en kWh
        - "coal_share_energy" (numeric): % de energía primaria del carbón
        - "coal_share_elec" (numeric): % de electricidad generada con carbón
        - "coal_cons_change_pct" (numeric): Cambio % anual en consumo de carbón
        - "coal_prod_change_pct" (numeric): Cambio % anual en producción
        
        💨 GAS NATURAL (USAR ::numeric):
        - "gas_consumption" (numeric): Consumo de gas en TWh
        - "gas_production" (numeric): Producción de gas en TWh
        - "gas_electricity" (numeric): Electricidad generada con gas en TWh
        - "gas_energy_per_capita" (numeric): Consumo de gas per cápita en kWh
        - "gas_share_energy" (numeric): % de energía primaria del gas
        - "gas_share_elec" (numeric): % de electricidad generada con gas
        
        🛢️ PETRÓLEO (USAR ::numeric):
        - "oil_consumption" (numeric): Consumo de petróleo en TWh
        - "oil_production" (numeric): Producción de petróleo en TWh
        - "oil_electricity" (numeric): Electricidad generada con petróleo en TWh
        - "oil_energy_per_capita" (numeric): Consumo de petróleo per cápita en kWh
        - "oil_share_energy" (numeric): % de energía primaria del petróleo
        - "oil_share_elec" (numeric): % de electricidad generada con petróleo
        
        ☀️ ENERGÍA SOLAR (USAR ::numeric):
        - "solar_consumption" (numeric): Consumo de energía solar en TWh
        - "solar_electricity" (numeric): Electricidad generada con solar en TWh
        - "solar_elec_per_capita" (numeric): Electricidad solar per cápita en kWh
        - "solar_share_energy" (numeric): % de energía primaria solar
        - "solar_share_elec" (numeric): % de electricidad generada con solar
        - "solar_cons_change_pct" (numeric): Cambio % anual en solar
        
        🌬️ ENERGÍA EÓLICA (USAR ::numeric):
        - "wind_consumption" (numeric): Consumo de energía eólica en TWh
        - "wind_electricity" (numeric): Electricidad generada con eólica en TWh
        - "wind_elec_per_capita" (numeric): Electricidad eólica per cápita en kWh
        - "wind_share_energy" (numeric): % de energía primaria eólica
        - "wind_share_elec" (numeric): % de electricidad generada con eólica
        
        💧 ENERGÍA HIDROELÉCTRICA (USAR ::numeric):
        - "hydro_consumption" (numeric): Consumo hidroeléctrico en TWh
        - "hydro_electricity" (numeric): Electricidad hidroeléctrica en TWh
        - "hydro_elec_per_capita" (numeric): Hidro per cápita en kWh
        - "hydro_share_energy" (numeric): % de energía primaria hidroeléctrica
        - "hydro_share_elec" (numeric): % de electricidad hidroeléctrica
        
        ⚛️ ENERGÍA NUCLEAR (USAR ::numeric):
        - "nuclear_consumption" (numeric): Consumo nuclear en TWh
        - "nuclear_electricity" (numeric): Electricidad nuclear en TWh
        - "nuclear_elec_per_capita" (numeric): Nuclear per cápita en kWh
        - "nuclear_share_energy" (numeric): % de energía primaria nuclear
        - "nuclear_share_elec" (numeric): % de electricidad nuclear
        
        🌱 ENERGÍAS RENOVABLES TOTALES (USAR ::numeric):
        - "renewables_consumption" (numeric): Consumo total de renovables en TWh
        - "renewables_electricity" (numeric): Electricidad de renovables en TWh
        - "renewables_energy_per_capita" (numeric): Renovables per cápita en kWh
        - "renewables_share_energy" (numeric): % de energía primaria renovable
        - "renewables_share_elec" (numeric): % de electricidad renovable
        - "renewables_cons_change_pct" (numeric): Cambio % anual en renovables
        
        🏭 COMBUSTIBLES FÓSILES TOTALES (USAR ::numeric):
        - "fossil_fuel_consumption" (numeric): Consumo total de fósiles en TWh
        - "fossil_electricity" (numeric): Electricidad de fósiles en TWh
        - "fossil_energy_per_capita" (numeric): Fósiles per cápita en kWh
        - "fossil_share_energy" (numeric): % de energía primaria fósil
        - "fossil_share_elec" (numeric): % de electricidad fósil
        
        🔋 ENERGÍAS BAJAS EN CARBONO (USAR ::numeric):
        - "low_carbon_consumption" (numeric): Consumo bajo carbono en TWh
        - "low_carbon_electricity" (numeric): Electricidad baja en carbono en TWh
        - "low_carbon_share_energy" (numeric): % energía bajo carbono
        - "low_carbon_share_elec" (numeric): % electricidad baja en carbono
        
        💡 ELECTRICIDAD GENERAL (USAR ::numeric):
        - "electricity_generation" (numeric): Generación total de electricidad en TWh
        - "electricity_demand" (numeric): Demanda eléctrica en TWh
        - "electricity_demand_per_capita" (numeric): Demanda per cápita en kWh
        - "per_capita_electricity" (numeric): Generación per cápita en kWh
        - "electricity_share_energy" (numeric): % electricidad en energía primaria
        
        🌍 EMISIONES Y CARBONO (USAR ::numeric):
        - "greenhouse_gas_emissions" (numeric): Emisiones GEI en Mt CO₂ equivalente
        - "carbon_intensity_elec" (numeric): Intensidad de carbono en gCO₂/kWh
        
        🔄 IMPORTACIONES/EXPORTACIONES (USAR ::numeric):
        - "net_elec_imports" (numeric): Importaciones netas de electricidad en TWh
        - "net_elec_imports_share_demand" (numeric): % importaciones de la demanda
        
        ⚡ BIOCOMBUSTIBLES (USAR ::numeric):
        - "biofuel_consumption" (numeric): Consumo de biocombustibles en TWh
        - "biofuel_electricity" (numeric): Electricidad de biocombustibles en TWh
        - "biofuel_share_energy" (numeric): % biocombustibles en energía
        - "biofuel_share_elec" (numeric): % biocombustibles en electricidad
        
        🔆 OTRAS RENOVABLES (USAR ::numeric):
        - "other_renewable_consumption" (numeric): Otras renovables en TWh
        - "other_renewable_electricity" (numeric): Electricidad otras renovables en TWh
        - "other_renewables_share_energy" (numeric): % otras renovables

        NOTA IMPORTANTE: 
        - Solo "country" e "iso_code" son TEXT (sin casting)
        - TODAS las demás columnas son NUMERIC y requieren casting ::numeric
        - Sufijos comunes: _per_capita (por persona), _share_energy (% energía), 
        _share_elec (% electricidad), _change_pct (cambio %), _change_twh (cambio TWh)
        
        EJEMPLOS DE CONSULTAS SQL COMPLEJAS:
        
        1. Top 10 Países con Mayor Transición a Renovables (Última Década):
        WITH datos_base AS (
            SELECT 
                "country",
                MAX(CASE WHEN "year"::numeric = 2023 THEN "renewables_share_elec"::numeric END) as renovables_2023,
                MAX(CASE WHEN "year"::numeric = 2013 THEN "renewables_share_elec"::numeric END) as renovables_2013,
                MAX(CASE WHEN "year"::numeric = 2023 THEN "population"::numeric END) as poblacion_actual,
                MAX(CASE WHEN "year"::numeric = 2023 THEN "gdp"::numeric END) as pib_actual
            FROM world_energy
            WHERE "year"::numeric IN (2013, 2023)
            AND "country" IS NOT NULL
            GROUP BY "country"
        ),
        crecimiento AS (
            SELECT 
                "country",
                renovables_2013,
                renovables_2023,
                (renovables_2023 - renovables_2013) as cambio_absoluto,
                CASE 
                    WHEN renovables_2013 > 0 THEN 
                        ((renovables_2023 - renovables_2013) / renovables_2013 * 100)
                    ELSE NULL 
                END as cambio_porcentual,
                poblacion_actual,
                pib_actual
            FROM datos_base
            WHERE renovables_2013 IS NOT NULL 
            AND renovables_2023 IS NOT NULL
            AND poblacion_actual > 1000000  -- Solo países con más de 1M habitantes
        )
        SELECT 
            "country" as pais,
            ROUND(renovables_2013, 2) as "participacion_renovables_2013_%",
            ROUND(renovables_2023, 2) as "participacion_renovables_2023_%",
            ROUND(cambio_absoluto, 2) as "cambio_puntos_porcentuales",
            ROUND(cambio_porcentual, 2) as "crecimiento_%",
            ROUND(poblacion_actual / 1000000, 2) as "poblacion_millones",
            ROUND(pib_actual / 1000000000, 2) as "pib_miles_millones_usd",
            RANK() OVER (ORDER BY cambio_absoluto DESC) as ranking_cambio_absoluto,
            RANK() OVER (ORDER BY cambio_porcentual DESC) as ranking_cambio_relativo
        FROM crecimiento
        WHERE cambio_absoluto > 0  -- Solo países con crecimiento positivo
        ORDER BY cambio_absoluto DESC
        LIMIT 10;
        
        2. Análisis de Correlación entre PIB y Mix Energético por Región:
        WITH paises_clasificados AS (
            SELECT 
                "country",
                "year"::numeric,
                "gdp"::numeric,
                "population"::numeric,
                "coal_share_energy"::numeric,
                "gas_share_energy"::numeric,
                "oil_share_energy"::numeric,
                "renewables_share_energy"::numeric,
                "nuclear_share_energy"::numeric,
                "carbon_intensity_elec"::numeric,
                -- Clasificación por región (simplificada)
                CASE 
                    WHEN "country" IN ('Colombia', 'Mexico', 'Argentina', 'Chile', 'Peru', 'Brazil', 
                                    'Venezuela', 'Ecuador', 'Uruguay') THEN 'Latinoamérica'
                    WHEN "country" IN ('Spain', 'France', 'Germany', 'Italy', 'United Kingdom',
                                    'Netherlands', 'Sweden', 'Norway') THEN 'Europa'
                    WHEN "country" IN ('United States', 'Canada') THEN 'Norteamérica'
                    WHEN "country" IN ('China', 'India', 'Japan', 'South Korea', 'Indonesia') THEN 'Asia'
                    ELSE 'Otros'
                END as region,
                -- PIB per cápita
                CASE 
                    WHEN "population"::numeric > 0 THEN "gdp"::numeric / "population"::numeric 
                    ELSE NULL 
                END as pib_per_capita
            FROM world_energy
            WHERE "year"::numeric = 2022
            AND "gdp" IS NOT NULL
            AND "population" IS NOT NULL
        ),
        estadisticas_regionales AS (
            SELECT 
                region,
                COUNT(DISTINCT "country") as num_paises,
                ROUND(AVG(pib_per_capita), 2) as pib_per_capita_promedio,
                ROUND(AVG("coal_share_energy"::numeric), 2) as participacion_carbon_promedio,
                ROUND(AVG("gas_share_energy"::numeric), 2) as participacion_gas_promedio,
                ROUND(AVG("oil_share_energy"::numeric), 2) as participacion_petroleo_promedio,
                ROUND(AVG("renewables_share_energy"::numeric), 2) as participacion_renovables_promedio,
                ROUND(AVG("nuclear_share_energy"::numeric), 2) as participacion_nuclear_promedio,
                ROUND(AVG("carbon_intensity_elec"::numeric), 2) as intensidad_carbono_promedio,
                -- Desviación estándar
                ROUND(STDDEV("renewables_share_energy"::numeric), 2) as desviacion_std_renovables,
                -- Percentiles
                ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "renewables_share_energy"::numeric), 2) as percentil_25_renovables,
                ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "renewables_share_energy"::numeric), 2) as percentil_75_renovables
            FROM paises_clasificados
            WHERE region != 'Otros'
            GROUP BY region
        )
        SELECT 
            region,
            num_paises,
            pib_per_capita_promedio,
            participacion_carbon_promedio as "carbon_%",
            participacion_gas_promedio as "gas_%",
            participacion_petroleo_promedio as "petroleo_%",
            participacion_renovables_promedio as "renovables_%",
            participacion_nuclear_promedio as "nuclear_%",
            intensidad_carbono_promedio as "gCO2_por_kWh",
            desviacion_std_renovables,
            percentil_25_renovables as "p25_renovables",
            percentil_75_renovables as "p75_renovables",
            -- Índice de diversificación (inverso del índice Herfindahl-Hirschman simplificado)
            ROUND(
                1 - (
                    POWER(participacion_carbon_promedio/100, 2) +
                    POWER(participacion_gas_promedio/100, 2) +
                    POWER(participacion_petroleo_promedio/100, 2) +
                    POWER(participacion_renovables_promedio/100, 2) +
                    POWER(participacion_nuclear_promedio/100, 2)
                ),
                3
            ) as indice_diversificacion
        FROM estadisticas_regionales
        ORDER BY pib_per_capita_promedio DESC;
        
        3. Proyección de Descarbonización con Análisis de Tendencias:
        WITH serie_temporal AS (
            SELECT 
                "country",
                "year"::numeric as año,
                "coal_share_energy"::numeric as participacion_carbon,
                "fossil_share_energy"::numeric as participacion_fosiles,
                "renewables_share_energy"::numeric as participacion_renovables,
                "carbon_intensity_elec"::numeric as intensidad_carbono,
                -- Calcular tendencia con ventana móvil de 3 años
                AVG("renewables_share_energy"::numeric) OVER (
                    PARTITION BY "country" 
                    ORDER BY "year"::numeric 
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as renovables_media_movil_3años,
                -- Cambio año a año
                "renewables_share_energy"::numeric - LAG("renewables_share_energy"::numeric) OVER (
                    PARTITION BY "country" 
                    ORDER BY "year"::numeric
                ) as cambio_renovables_anual
            FROM world_energy
            WHERE "year"::numeric >= 2010
            AND "country" IN ('Spain', 'Germany', 'United Kingdom', 'Colombia', 'Chile', 'Denmark')
            AND "renewables_share_energy" IS NOT NULL
        ),
        tendencias AS (
            SELECT 
                "country",
                -- Datos actuales (2022)
                MAX(CASE WHEN año = 2022 THEN participacion_renovables END) as renovables_actual,
                MAX(CASE WHEN año = 2022 THEN participacion_fosiles END) as fosiles_actual,
                MAX(CASE WHEN año = 2022 THEN intensidad_carbono END) as intensidad_actual,
                -- Tasa de crecimiento promedio anual (últimos 5 años)
                AVG(CASE 
                    WHEN año >= 2018 AND cambio_renovables_anual IS NOT NULL 
                    THEN cambio_renovables_anual 
                END) as tasa_crecimiento_anual_promedio,
                -- Aceleración (cambio en la tasa de crecimiento)
                (
                    AVG(CASE WHEN año >= 2020 THEN cambio_renovables_anual END) -
                    AVG(CASE WHEN año BETWEEN 2015 AND 2017 THEN cambio_renovables_anual END)
                ) as aceleracion
            FROM serie_temporal
            GROUP BY "country"
        ),
        proyecciones AS (
            SELECT 
                "country",
                renovables_actual,
                fosiles_actual,
                intensidad_actual,
                ROUND(tasa_crecimiento_anual_promedio, 2) as "crecimiento_anual_%",
                ROUND(aceleracion, 3) as "aceleracion_%",
                -- Proyección simple a 2030 (lineal)
                ROUND(
                    renovables_actual + (tasa_crecimiento_anual_promedio * 8), 
                    2
                ) as proyeccion_2030_lineal,
                -- Proyección a 2030 considerando aceleración
                ROUND(
                    renovables_actual + 
                    (tasa_crecimiento_anual_promedio * 8) + 
                    (aceleracion * 28),  -- Suma de 1+2+3+...+8 = 36, ajustado
                    2
                ) as proyeccion_2030_con_aceleracion,
                -- Años para alcanzar 80% renovables (meta ambiciosa)
                CASE 
                    WHEN tasa_crecimiento_anual_promedio > 0 THEN
                        ROUND((80 - renovables_actual) / tasa_crecimiento_anual_promedio, 1)
                    ELSE NULL
                END as años_para_80pct_renovables
            FROM tendencias
        )
        SELECT 
            "country" as pais,
            ROUND(renovables_actual, 1) as "renovables_2022_%",
            ROUND(fosiles_actual, 1) as "fosiles_2022_%",
            ROUND(intensidad_actual, 1) as "gCO2_por_kWh_2022",
            "crecimiento_anual_%",
            "aceleracion_%",
            proyeccion_2030_lineal as "proyeccion_2030_simple_%",
            proyeccion_2030_con_aceleracion as "proyeccion_2030_acelerada_%",
            años_para_80pct_renovables,
            CASE 
                WHEN años_para_80pct_renovables < 10 THEN '🚀 Muy Rápido'
                WHEN años_para_80pct_renovables < 20 THEN '⚡ Rápido'
                WHEN años_para_80pct_renovables < 30 THEN '📊 Moderado'
                ELSE '🐢 Lento'
            END as velocidad_transicion
        FROM proyecciones
        ORDER BY "crecimiento_anual_%" DESC;
        
        
        REGLAS CRÍTICAS PARA GENERAR SQL:
        ✅ Siempre usar LIMIT (máximo 500 registros)
        ✅ Usar casting ::numeric para columnas monetarias y de peso
        ✅ Filtrar valores NULL y cero cuando sea relevante
        ✅ Usar comillas dobles para TODOS los nombres de columnas
        ✅ Para fechas usar: EXTRACT(YEAR/MONTH/DAY FROM "FECHA AAAA-MM-DD"::date)
        ✅ Para porcentajes: (valor1/NULLIF(valor2,0)*100)
        ✅ Para texto usar UPPER() en comparaciones LIKE
        
        COLUMNAS MÁS CONSULTADAS POR CATEGORÍA:
        📊 Análisis demográfico y económico: "population", "gdp", "year", "country", "iso_code"
        ⚡ Análisis de consumo energético: "primary_energy_consumption", "energy_per_capita", "energy_per_gdp"
        🌱 Análisis de renovables: "renewables_consumption", "renewables_share_energy", "renewables_share_elec", "solar_electricity", "wind_electricity", "hydro_electricity"
        🏭 Análisis de fósiles: "fossil_fuel_consumption", "fossil_share_energy", "coal_consumption", "gas_consumption", "oil_consumption"
        🌍 Análisis de emisiones: "greenhouse_gas_emissions", "carbon_intensity_elec"
        ⚛️ Análisis nuclear: "nuclear_consumption", "nuclear_electricity", "nuclear_share_energy"
        📈 Análisis de cambio temporal: "energy_cons_change_pct", "renewables_cons_change_twh", "coal_cons_change_pct", "solar_cons_change_twh"
        💡 Análisis de electricidad: "electricity_generation", "electricity_demand", "electricity_demand_per_capita", "per_capita_electricity"
        🔄 Análisis de producción: "coal_production", "gas_production", "oil_production"
        🌐 Análisis de dependencia: "net_elec_imports", "net_elec_imports_share_demand"
        """
        
        return MetadataResponse(
            tablas=[{"nombre": "world_energy", "descripcion": "Datos de importaciones BASF"}],
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
                "tabla_detectada": "world_energy" in sql.lower(),
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