import os # para manejar variables de entorno
from sqlalchemy import create_engine # para crear la conexión a la base de datos
from dotenv import load_dotenv # para cargar variables de entorno desde un archivo .env

load_dotenv()

def get_engine():
    # Conecta a postgres usando un archivo .env
    return create_engine(
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )