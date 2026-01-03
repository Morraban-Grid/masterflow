import pandas as pd
import requests
from prefect import flow, task
from utils.db_utils import get_engine

@task(name="Extract_MercadoLibre", retries=3, retry_delay_seconds=10)
def extract():
    # Usamos el sitio de Perú (MPE) ya que estás ahí
    url = "https://api.mercadolibre.com/sites/MPE/search?q=laptop&limit=50"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
        "Referer": "https://www.mercadolibre.com.pe/",
        "Connection": "keep-alive"
    }
    
    print(f"Intentando conexión con sitio regional Perú...")
    response = requests.get(url, headers=headers, timeout=15)
    
    if response.status_code == 200:
        print("¡Conexión Exitosa!")
        return response.json().get('results', [])
    else:
        # Si falla, imprimimos el error para saber qué pasa
        print(f"Respuesta del servidor: {response.text}")
        raise Exception(f"Error en extracción: {response.status_code}")

@task(name="Transform_Laptops")
def transform(raw_data):
    """TRANSFORM - Procesa el JSON y extrae el precio de los objetos anidados."""
    if not raw_data:
        print("No se recibieron datos.")
        return pd.DataFrame()
    
    # Convertimos a DataFrame
    df = pd.DataFrame(raw_data)
    
    # --- PROCESAMIENTO DE PRECIOS (LO QUE ENCONTRASTE EN LA INSPECCIÓN) ---
    # Extraemos el 'amount' dentro de 'sale_price' para evitar valores nulos o estructuras complejas
    df['final_price'] = df['sale_price'].apply(
        lambda x: x.get('amount') if isinstance(x, dict) else None
    )
    
    # Extraemos la moneda (PEN, USD, etc.)
    df['currency'] = df['sale_price'].apply(
        lambda x: x.get('currency_id') if isinstance(x, dict) else None
    )

    # Seleccionamos las columnas que viste en el navegador
    # Nota: Si en tu inspección viste 'url', usamos 'url'. Si era 'permalink', usamos ese.
    columnas_interes = ['id', 'title', 'final_price', 'currency', 'permalink']
    
    # Filtramos y limpiamos
    df_clean = df[columnas_interes].copy()
    
    # Renombramos para que la tabla en Postgres sea clara
    df_clean.rename(columns={'final_price': 'price'}, inplace=True)
    
    print(f"Transformación completada: {len(df_clean)} productos listos.")
    return df_clean

@task(name="Load_Postgres")
def load(df):
    """LOAD - Guarda los resultados en la base de datos."""
    if df.empty:
        return "Sin datos para cargar."
        
    engine = get_engine()
    # Cargamos en la tabla 'laptops'
    df.to_sql('laptops', engine, if_exists='replace', index=False)
    return "¡Datos cargados en PostgreSQL exitosamente!"

@flow(name="Masterflow_ETL_Pipeline")
def ml_ingestion_flow():
    """Flujo Principal"""
    raw_data = extract()
    clean_data = transform(raw_data)
    resultado = load(clean_data)
    print(resultado)

if __name__ == "__main__":
    ml_ingestion_flow()