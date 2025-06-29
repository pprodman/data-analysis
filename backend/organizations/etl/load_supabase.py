import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# --- CONFIGURACIÓN PRINCIPAL ---
# Cambia estas variables para adaptar el script a otra tabla/archivo.
TABLE_NAME = 'organizations'
SCHEMA_NAME = 'public'

def load_data():
    """Función principal que encapsula toda la lógica del ETL."""
    
    # Cargar variables de entorno
    load_dotenv()

    # --- 1. Leer y preparar los datos ---
    print(f"--- Iniciando proceso para la tabla '{TABLE_NAME}' ---")
    
    try:
        # Construir la ruta al archivo dinámicamente
        script_dir = Path(__file__).resolve().parent
        project_root = script_dir.parent.parent.parent
        csv_path = project_root / 'data' / 'processed' / f"{TABLE_NAME}.csv"

        print(f"Buscando archivo en: {csv_path}")
        df = pd.read_csv(csv_path, delimiter=';')
        print(f"✅ Archivo encontrado. {len(df)} registros leídos.")
    
    except FileNotFoundError:
        print(f"❌ ERROR: No se encontró el archivo '{TABLE_NAME}.csv' en la ruta esperada.")
        return # Detener la ejecución si el archivo no existe

    # Estandarizar nombres de columnas a minúsculas
    df.columns = [col.lower() for col in df.columns]
    print(f"🧹 Nombres de columna estandarizados a minúsculas.")

    # --- 2. Conectar a la base de datos ---
    try:
        DB_USER = os.getenv("DB_USER")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        DB_HOST = os.getenv("DB_HOST")
        DB_PORT = os.getenv("DB_PORT")
        DB_NAME = os.getenv("DB_NAME")
        
        connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(connection_string)

        # --- 3. Limpiar la tabla y cargar los nuevos datos ---
        with engine.connect() as conn:
            with conn.begin() as transaction:
                print(f"🔌 Conexión establecida. Preparando la tabla '{SCHEMA_NAME}.{TABLE_NAME}'...")
                
                print("🧹 Limpiando datos antiguos de la tabla...")
                conn.execute(text(f"TRUNCATE TABLE {SCHEMA_NAME}.{TABLE_NAME} RESTART IDENTITY;"))
                
                print(f"🚀 Cargando {len(df)} nuevos registros...")
                df.to_sql(
                    name=TABLE_NAME,
                    con=conn,
                    schema=SCHEMA_NAME,
                    if_exists='append',
                    index=False,
                    method='multi'
                )

        print(f"✅ ¡Éxito! {len(df)} registros cargados en la tabla 'public.{TABLE_NAME}'.")

    except Exception as e:
        print(f"❌ Ocurrió un error durante la operación de base de datos: {e}")
    
    finally:
        if 'engine' in locals() and engine:
            engine.dispose()
        print("🔌 Conexión a la base de datos cerrada.")

if __name__ == "__main__":
    load_data()