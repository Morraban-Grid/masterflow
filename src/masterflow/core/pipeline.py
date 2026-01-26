from core.extractor import Extractor
from core.transformer import Transformer
from core.loader import Loader

class Pipeline:
    # Clase que orquesta la extracción, transformación y carga de datos
    # Esta clase no sabe nada sobre las implementaciones específicas de Extractor, Transformer o Loader

    # Este método inicializa la tubería con las instancias proporcinadas de Extractor, Transformer y Loader
    def __init__(
            self,
            extractor: Extractor,
            transformer: Transformer,
            loader: Loader,
    ) -> None:
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self) -> None:

        # Ejecuta el proceso ETL completo
        raw_data = self.extractor.extract()
        transformerd_data = self.transformer.transform(raw_data)
        self.loader.load(transformerd_data)





