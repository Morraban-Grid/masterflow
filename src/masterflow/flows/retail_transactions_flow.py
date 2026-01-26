from prefect import flow, task, get_run_logger # Importa las funciones necesarias de Prefect
from prefect.tasks import task_input_hash # Importa el hash de entrada de tareas para la caché
from datetime import timedelta # Importa timedata para definir duraciones
import pandas as pd

from masterflow.extractors.retail_db_extractor import RetailDBExtractor # Importa el extractor de la base de datos retail
from masterflow.transformers.transactions_transformer import TransactionsTransformer # Importa el transformador de transacciones
from masterflow.loaders.masterflow_db_loader import MasterflowDBLoader # Importa el cargador de la base de datos masterflow

@task( # Decorador de tarea de Prefect con configuración de caché
    retries=3, # Significa que si la tarea falla, se reintentará hasta 3 veces antes de marcarse como fallida
    retry_delay_seconds=10, # Tiempo de espera entre reintentos en segundos
    cache_key_fn=task_input_hash, # Función para generar la clave de caché basada en la entrada de la tarea
    cache_expiration=timedelta(days=1) # Duración de la validez de la caché (1 día en este caso)
)

# Tarea para extraer datos de la base de datos retail_raw
def extract_task() -> pd.DataFrame:
    logger = get_run_logger() # Para obtener el logger del flujo
    # ¿Qué es logger?, es una instancia que nos permite registrar mensajes de log durante la ejecución del flujo
    logger.info("Iniciando extracción de datos desde la base de datos retail_raw.")

    extractor = RetailDBExtractor() # Craemos una instancia del extractor
    df = extractor.extract() # llamamos al método extract para obtener los datos

    logger.info(f"Extracción completada. Registros obtenidos: {len(df)}")
    return df # Devolvemos el DataFrame extraído

# Tarea para transformar los datos extraídos
def transform_task(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger() # Para obtener el logger del flujo
    logger.info("Iniciando transformación de datos de transacciones.")

    transformer = TransactionsTransformer() # Creamos una instancia del transformador
    df_transformed = transformer.transform(df) # llamamos al método transform para transformar los datos

    logger.info(f"Transformación finalizada. Registros válidos: {len(df_transformed)}")
    return df_transformed # Devolvemos el DataFrame transformado

# Tarea para cargar los datos transformados en la base de datos masterflow_db
@task
def load_task(df: pd.DataFrame) -> None:
    logger = get_run_logger()
    logger.info("Iniciando carga de datos a la base de datos masterflow.")

    loader = MasterflowDBLoader()
    loader.load(df)

    logger.info("Carga completada exitosamente.")

@flow(name="retail-transactions-etl") # Decorador de flujo de Prefect con nombre personalizado
def retail_transactions_flow():
    logger = get_run_logger() # Para obtener el logger del flujo
    # ¿Qué es logger?, es una instancia que nos permite registrar mensajes de log durante la ejecución del flujo
    logger.info("Iniciando flow de transacciones retail") # Mensaje de log indicando el inicio del flujo
    
    raw_df = extract_task() # llamada a la tarea de extracción, para luego pasar su resultado a la tarea de transformación
    clean_df = transform_task(raw_df) # llamada a la tarea de transformación
    load_task(clean_df) # llamada a la tarea de carga

    logger.info("Flow finamizado correctamente") # Mensaje de log indicando la finalización del flujo

    # Esta función define el flujo ETL completo
    # - Extrae datos de la base de datos retail_raw
    # - Tranforma los datos de transacciones
    # - Carga los datos transformados en la base de datos masterflow_db
    # Cada paso está encapsulado en tarea individuales para facilitar la gestión y el monitoreo





