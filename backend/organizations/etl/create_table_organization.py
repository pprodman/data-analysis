from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# --- CONFIGURACIÓN PRINCIPAL ---
# Cambia estas variables para crear una tabla diferente.
# Deberían coincidir con las del script de carga de datos.
TABLE_NAME = 'organizations'
SCHEMA_NAME = 'public'

def create_table():
    """
    Función que borra y vuelve a crear una tabla en la base de datos
    con una estructura predefinida.
    """
    
    # Cargar variables de entorno
    load_dotenv()
    print(f"--- Iniciando la creación del esquema para la tabla '{TABLE_NAME}' ---")

    # --- 1. Conectar a la base de datos (Supabase) ---
    try:
        DB_USER = os.getenv("DB_USER")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        DB_HOST = os.getenv("DB_HOST")
        DB_PORT = os.getenv("DB_PORT")
        DB_NAME = os.getenv("DB_NAME")
        
        connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(connection_string)

        # --- 2. Definir los comandos SQL dinámicamente ---
        drop_table_sql = text(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.{TABLE_NAME};")
        
        # El comando CREATE TABLE ahora usa las variables de configuración
        create_table_sql = text(f"""
            CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} (
                ID VARCHAR(36) PRIMARY KEY,
                NAME TEXT,
                ADDRESS TEXT,
                CITY VARCHAR(50),
                STATE VARCHAR(10),
                ZIP INTEGER,
                LAT NUMERIC(17, 15),
                LON NUMERIC(17, 15),
                PHONE VARCHAR(30),
                REVENUE NUMERIC(15, 2),
                UTILIZATION INTEGER
            );
        """)

        # --- 3. Ejecutar la creación de la tabla ---
        with engine.connect() as conn:
            with conn.begin() as transaction:
                print("🔌 Conexión establecida. Ejecutando comandos...")
                conn.execute(drop_table_sql)
                conn.execute(create_table_sql)
        
        print(f"✅ ¡Éxito! La tabla '{SCHEMA_NAME}.{TABLE_NAME}' ha sido creada correctamente.")

    except Exception as e:
        print(f"❌ Ocurrió un error durante la creación del esquema: {e}")

    finally:
        if 'engine' in locals() and engine:
            engine.dispose()
        print("🔌 Conexión cerrada.")

# --- Punto de entrada para ejecutar el script ---
if __name__ == "__main__":
    create_table()