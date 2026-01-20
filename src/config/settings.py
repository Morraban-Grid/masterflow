from dataclasses import dataclass

@dataclass(frozen=True)
class DatabaseSettings:
    # Configuración relacionada a la base de datos

    # Base de datos de origen (retail_raw)
    retail_db_name: str = "retail_raw"
    retail_schema: str = "public"
    retail_table: str = "transactions_raw"

    # Base de datos de destino (masterflow_db)
    masterflow_db_name: str = "masterflow_db"
    masterflow_schema: str = "public"
    masterflow_table: str = "cleaned_transactions"