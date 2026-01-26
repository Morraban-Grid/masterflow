import pandas as pd
from sqlalchemy import text # text es una función para escribir consultas SQL en SQLAlchemy
from sqlalchemy.engine import Engine # Importamos la clase Engine para tipar el parámetro de la función

from masterflow.core.loader import Loader # Importamos la clase Loader base
from masterflow.connections.postgres import get_masterflow_engine # Importamos la función para obtener la conexión a la base de datos a la cual vamos a cargar
from masterflow.config.settings import DatabaseSettings # Importamos la clase de configuración de la base de datos

class MasterflowLoader(Loader): # Se trata de una clase que hereda de Loader 
    # Clase encargada de cargar datos limpios en la base de datos de destino (masterflow_db)

    def __init__(self, mode: str = "truncate_insert") -> None:
        self.engine: Engine = get_masterflow_engine() # Inicializamos la conexión a la base de datos de destino
        self.settings = DatabaseSettings() # Inicializamos la configuración de la base de datos
        self.mode = mode # Establecemos el modo de carga (por defecto "truncate_insert")

    def load(self, df: pd.DataFrame) -> None:
        # Carga el DataFrame en la base de datos de destino según el modo de carga configurado

        if df.empty:
            raise ValueError("El DataFrame está vacío. No hay datos para cargar.")
        
        if self.mode == "replace":
            self._replace_table(df)
        
        elif self.mode == "append":
            self._append_data(df)

        elif self.mode == "truncate_insert":
            self._truncate_and_insert(df)

        else:
            raise ValueError(f"Modo de carga no soportado: {self.mode}")
        
    def _truncate_and_insert(self, df: pd.DataFrame) -> None:

        # Limpia la tabla de destino y luego inserta los datos del DataFrame
        # Este método es útil para asegurar que la tabla solo contenga los datos más recientes
        # Esto garantiza la idempotencia de la carga de datos

        table_name = f"{self.settings.masterflow_schema}.{self.settings.masterflow_table}"

        with self.engine.begin() as connection:
            connection.execute(text(f"TRUNCATE TABLE {table_name}"))

            df.to_sql(
                name=self.settings.masterflow_table,
                schema=self.settings.masterflow_schema,
                con=connection,
                if_exists="append",
                index=False,
                method="multi"
            )

    def _replace_table(self, df: pd.DataFrame) -> None:
        # Reemplaza completamente la tabla de destino con los datos del DataFrame

        df.to_sql(
            name=self.settings.masterflow_table,
            schema=self.settings.masterflow_schema,
            con=self.engine,
            if_exists="replace",
            index=False,
            method="multi"
        )

    def _append_data(self, df: pd.DataFrame) -> None:
        # Agrega los datos del DataFrame a la tabla de destino sin eliminar los datos existentes
        df.to_sql(
            name=self.settings.masterflow_table,
            schema=self.settings.masterflow_schema,
            con=self.engine,
            if_exists="append",
            index=False,
            method="multi"
        )

    


