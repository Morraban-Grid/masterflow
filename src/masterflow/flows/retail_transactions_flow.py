from masterflow.extractors.retail_db_extractor import RetailDBExtractor
from masterflow.transformers.transactions_transformer import TransactionsTransformer
from masterflow.loaders.masterflow_db_loader import MasterflowDBLoader


def run_retail_transactions_etl() -> None:
    # 1️ Extract
    extractor = RetailDBExtractor()
    df_extracted = extractor.extract()
    print(f"[EXTRACT] Rows extracted: {len(df_extracted)}")

    # 2️ Transform
    transformer = TransactionsTransformer()
    df_transformed = transformer.transform(df_extracted)
    print(f"[TRANSFORM] Rows after transform: {len(df_transformed)}")

    # 3️ Load
    loader = MasterflowDBLoader(mode="truncate_insert")
    loader.load(df_transformed)
    print("[LOAD] Data loaded successfully into masterflow_db")


if __name__ == "__main__":
    run_retail_transactions_etl()
