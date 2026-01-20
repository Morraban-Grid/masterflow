from abc import ABC, abstractmethod
import pandas as pd

class Transformer(ABC):
    # Contrato base para cualquier transformación de datos
    
    @abstractmethod
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        # Recibe un DataFrame de pandas, aplica una transformación y devuelve el DataFrame transformado
        pass