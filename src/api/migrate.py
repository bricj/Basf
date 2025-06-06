import pandas as pd
from sqlalchemy import create_engine
import time

# Configuración
DATABASE_URL = "postgresql://basf_user:basf_secure_password_2024@localhost:5432/basf_data"
EXCEL_FILE = "data/data.xlsx"

print("🚀 Migración BASF Data")
print("=" * 30)

# Verificar archivo
if not pd.io.common.file_exists(EXCEL_FILE):
    print(f"❌ No encontrado: {EXCEL_FILE}")
    exit(1)

# Leer Excel
print(f"📖 Leyendo {EXCEL_FILE}...")
df = pd.read_excel(EXCEL_FILE, sheet_name="CP_Colombia")
print(f"📊 {len(df)} registros leídos")

# Limpiar datos
df = df.fillna("")
df = df.replace([float('inf'), float('-inf')], "")

# Esperar PostgreSQL
print("⏳ Esperando PostgreSQL...")
for i in range(30):
    try:
        engine = create_engine(DATABASE_URL)
        engine.connect().close()
        break
    except:
        time.sleep(2)
        if i == 29:
            print("❌ PostgreSQL no disponible")
            exit(1)

# Migrar
print("📤 Migrando a PostgreSQL...")
df.to_sql("basf_import_data", engine, if_exists="replace", index=False)

print(f"✅ Completado: {len(df)} registros migrados")
print("🎉 ¡Listo para usar la API!")