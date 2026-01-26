import pandas as pd
import numpy as np

from masterflow.core.transformer import Transformer # Importa la clase base Transformer

class TransactionsTransformer(Transformer):
    # Tranformer encargado de limpiar, tipar y aplicar reglas de negocio sobre
    # los datos de transacciones financieras.

    # Esta clase hereda de Transformer, que es la clase base para todos los
    # transformadores en el sistema.

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Ejecuta todas las transformaciones necesarias sobre el DataFrame de transacciones.

        df = df.copy()

        df = self._normalize_column_names(df)
        df = self._convert_data_types(df)
        df = self._apply_business_rules(df)
        df = self._remove_invalid_records(df)

        return df
    
    # Normalización 
    @staticmethod
    def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
        # Normaliza los nombres de las columnas para evitar inconsistencias:
        # - minúsculas
        # - snake_case esto hace que los nombres de las columnas sean consistentes y fáciles de manejar.
        # - sin caracteres especiales

        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace(r"[^\w_]", "", regex=True)
        ) 

        return df

    @staticmethod
    def _convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
        # Convierte las columnas a los tipos de datos correctos
        if "transaction_date" in df.columns:
            df["transaction_date"] = pd.to_datetime(
                df["transaction_date"],
                errors="coerce"
            )

        numeric_columns = [
            "quantity",
            "price",
            "discount_applied",
            "total_amount",
        ]

        for column in numeric_columns:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce")

        return df
    
    @staticmethod
    def _apply_business_rules(df: pd.DataFrame) -> pd.DataFrame:
        # Aplica reglas de negocio definidas para las transacciones financieras.

        # Regla 1: El descuento debe estar entre 0 y 100
        if "discount_applied" in df.columns:
            invalid_discount_mask = (
                (df["discount_applied"]<0) |
                (df["discount_applied"]>100)
            )

            df.loc[invalid_discount_mask, "discount_applied"] = np.nan

        # Recalcular total solo si existen columnas necesarias
        # Regla 2: Recalcular el total_amount
        required_columns = {"quantity", "price", "discount_applied"}
        if required_columns.issubset(df.columns):
            df["calculated_total_amount"] = (
                df["quantity"] * df["price"]
            ) * (1 - df["discount_applied"]/100)

        return df
    
    @staticmethod
    def _remove_invalid_records(df: pd.DataFrame) -> pd.DataFrame:
        # Elimina registros que no cumplen criterios mínimos de calidad

        required_for_dropna = [
            col for col in [
                "customer_id",
                "product_id",
                "transaction_date",
                "quantity",
                "price",
                "calculated_total_amount",
            ]
            if col in df.columns
        ]

        if required_for_dropna:
            df = df.dropna(subset=required_for_dropna)

        if "quantity" in df.columns:
            df = df[df["quantity"]>0]

        if "price" in df.columns:
            df = df[df["price"]>0]

        return df 

