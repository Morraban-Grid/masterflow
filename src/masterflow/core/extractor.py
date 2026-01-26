from abc import ABC, abstractmethod
import pandas as pd

class Extractor(ABC):
    # Contrato base para cualquier extractor de datos

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        #Extrae datos de una fuente y los devuelve como un DataFrame de pandas
        pass