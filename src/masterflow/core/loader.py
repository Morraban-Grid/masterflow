from abc import ABC, abstractmethod
import pandas as pd

class Loader(ABC):

    # Contrato base para cualquier cargador de datos

    @abstractmethod
    def load(self, data: pd.DataFrame) -> None:

        # Recibe un DataFrame de pandas y lo carga en el destino especificado
        pass