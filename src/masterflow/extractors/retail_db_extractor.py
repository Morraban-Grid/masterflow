import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from masterflow.core.extractor import Extractor
from masterflow.connections.postgres import get_retail_engine
from masterflow.config.settings import DatabaseSettings


class RetailDBExtractor(Extractor):
    """
    Extractor concreto que lee datos desde la base de datos retail_raw.
    """

    def __init__(self) -> None:
        self.settings = DatabaseSettings()
        self.engine: Engine = get_retail_engine()

    def extract(self) -> pd.DataFrame:
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
                "Error durante la extracción desde retail_raw.transactions_raw"
            ) from exc

        self._validate_dataframe(df)
        return df

    def _validate_dataframe(self, df: pd.DataFrame) -> None:
        """
        Valida que el DataFrame extraído tenga estructura mínima esperada.
        """

        if df.empty:
            raise ValueError("La tabla de origen está vacía. ETL abortado.")

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
