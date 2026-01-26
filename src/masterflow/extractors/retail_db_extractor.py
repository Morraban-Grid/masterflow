import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from abc import abstractmethod
from masterflow.config.settings import DatabaseSettings
from masterflow.connections.postgres import get_retail_engine

from masterflow.core.extractor import Extractor # Importamos la clase Extractor
from masterflow.connections.postgres import getpostgres_engine # Importamos el método para obtener la connexión a Postgres
from masterflow.config.settings import (
    SOURCE_DB_NAME,
    SOURCE_SCHEMA,
    SOURCE_TABLE,
) # Importamos las constantes de configuración de la base de datos

# Definimos la clase RetailDBExtractor que hereda de Extractor
# Esta clase implementa una extracción implícita de datos desde la base de datos retail_raw
class RetailDBExtractor(Extractor):

    # Clase que implementa la extracción de datos desde la base de datos retail_raw

    # Inicializamos la conexión a la base de datos
    def __init__(self) -> None:
        # Cargamos la información centralizada de la base de datos
        self.settings = DatabaseSettings()

        # Creamos el motor de conexión a la base de datos Postgres
        self.engine: Engine = get_retail_engine()

    # Implementamos el método extract de la clase Extractor
    def extract(self) -> pd.DataFrame:

        # Ejecuuta la extracción de datos desde PostgreSQL
        # Y retorna un DataFrame de pandas con los datos extraídos

        query = text(
            f"""
            SELECT *
            FROM {self.settings.retail_schema}.{self.settings.retail_table}
            """
        )

        try:
            with self.engine.connect() as connection:
                df = pd.read_sql(query, connection)

        except Exception as exc:
            raise RuntimeError(
                f"Error durante la extracción de datos desde retail_raw.transactions_raw: {exc}"
            ) from exc
        
        self._validate_dataframe(df)

        return df
    
    @abstractmethod
    def _validate_dataframe(df: pd.DataFrame) -> None:

        # Validar la iintegridad mínma del DataFrame extraído

        if df.empty:
            raise ValueError(
                "La tabla de origen transaction_raw está vacía. ETL abortado."
                )
        
        expected_columns = {
            "customer_id",
            "product_id",
            "quantity",
            "price",
            "transaction_date",
            "payment_method",
            "store_location",
            "product_category",
            "discount_applied",
            "total_amount",
        }

        missing_columns = expected_columns - set(df.columns)

        if missing_columns:
            raise ValueError(
                f"Faltan columnas esperadas en la extracción: {missing_columns}"
            )












