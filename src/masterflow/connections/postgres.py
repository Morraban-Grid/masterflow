import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

# Cargamos las variables de entorno desde el archivo .env
load_dotenv()

def _build_postgres_url(
        user: str,
        password: str,
        host: str,
        port: str,
        database: str
) -> str:
    # Construye la URL de conexión a PostgreSQL para SQLAlchemy
    # Retorna un string con el formato:
    # postgresql+psycopg2://user:password@host:port/database
    # motor://usuario:contraseña@servidor:puerto/nombre_base_datos
    return(
        f"postgresql+psycopg2://{user}:{password}"
        f"@{host}:{port}/{database}"
    )

def get_retail_engine() -> Engine:
    # Crea y retorna un SQLAlchemy Engine para la base de datos de origen (retail_raw)
    url = _build_postgres_url(
        user=os.getenv("RETAIL_DB_USER"),
        password=os.getenv("RETAIL_DB_PASSWORD"),
        host=os.getenv("RETAIL_DB_HOST"),
        port=os.getenv("RETAIL_DB_PORT"),
        database=os.getenv("RETAIL_DB_NAME")
    )

    return create_engine(url)

def get_masterflow_engine() -> Engine:
    # Crea y retorna un SQLAlchemy Engine para la base de datos de destino (masterflow_db)
    url = _build_postgres_url(
        user=os.getenv("MASTERFLOW_DB_USER"),
        password=os.getenv("MASTERFLOW_DB_PASSWORD"),
        host=os.getenv("MASTERFLOW_DB_HOST"),
        port=os.getenv("MASTERFLOW_DB_PORT"),
        database=os.getenv("MASTERFLOW_DB_NAME")
    )
    
    return create_engine(url)