import pandas as pd
import pytest

from masterflow.transformers.transactions_transformer import TransactionsTransformer

def _base_valid_dataframe(overrides: dict | None = None) -> pd.DataFrame:
    # Crea un DataFrame válido base para las pruebas
    # Permite sobreescribir valores específicos mediante el parámetro overrides

    data = {
        "customer_id" : [1],
        "product_id" : ["A"],
        "quantity" : [1],
        "price" : [100.0],
        "discount_applied" : [0.0],
        "transaction_date" : ["2024-01-01 10:30:00"],
    }

    if overrides:
        data.update(overrides)

    return pd.DataFrame(data)

def test_transaction_date_is_converted_to_datetime():
    transformer = TransactionsTransformer()

    df = _base_valid_dataframe()

    result = transformer.transform(df)

    assert pd.api.types.is_datetime64_any_dtype(result["transaction_date"])

def test_total_amount_is_correctly_calculated():
    transformer = TransactionsTransformer()

    df = _base_valid_dataframe(
        {
            "quantity" : [2],
            "price" : [100.0],
            "discount_applied" : [10.0],
        }
    )

    result = transformer.transform(df)

    expected_total = 2 * 100 * (1 - 0.10)

    assert result["calculated_total_amount"].iloc[0] == expected_total

def test_invalid_discount_removes_record():
    transformer = TransactionsTransformer()

    df = _base_valid_dataframe(
        {
            "discount_applied" : [150.0], # Descuento inválido
        }
    )

    result = transformer.transform(df)

    assert result.empty

@pytest.mark.parametrize(
    "quantity, price",
    [
        (0, 100),
        (-1, 100),
        (1, 0),
        (1, -50),
    ]
)
def test_invalid_quantity_or_price_are_removed(quantity, price):
    transformer = TransactionsTransformer()

    df = _base_valid_dataframe(
        {
            "quantity" : [quantity],
            "price" : [price],
        }
    )

    result = transformer.transform(df)

    assert result.empty


















