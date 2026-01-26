# MASTERFLOW — ETL Profesional con Python y PostgreSQL

MASTERFLOW es un proyecto **ETL (Extract, Transform, Load)** diseñado con buenas prácticas de **Data Engineering**, enfocado en modularidad, escalabilidad y calidad de datos.

El objetivo del proyecto es extraer datos de transacciones desde una base de datos PostgreSQL (`retail_raw`), limpiarlos y transformarlos aplicando reglas de negocio, y cargarlos en una base de datos destino (`masterflow_db`), utilizando solo herramientas **100% gratuitas y locales**.

Este proyecto está pensado como **portafolio profesional** y como base sólida para sistemas ETL en entornos reales.

---

## 🚀 Características principales

- Arquitectura ETL desacoplada (Extract / Transform / Load)
- Uso de contratos abstractos (patrón profesional)
- Transformaciones robustas y testeadas
- Orquestación con Prefect
- Logging centralizado
- Tests unitarios con Pytest
- Compatible con Python 3.12
- Sin dependencias cloud

---

## 🧱 Arquitectura del proyecto

MASTERFLOW/
│
├── src/
│ └── masterflow/
│ ├── core/ # Contratos ETL y pipeline
│ ├── extractors/ # Extracción desde PostgreSQL
│ ├── transformers/ # Limpieza y reglas de negocio
│ ├── loaders/ # Carga en base de datos destino
│ ├── flows/ # Orquestación con Prefect
│ ├── connections/ # Conexiones reutilizables
│ ├── config/ # Configuración centralizada
│ └── utils/ # Utilidades (logging, helpers)
│
├── tests/ # Tests unitarios
├── logs/ # Logs de ejecución
├── requirements.txt
├── .env
└── README.md


---

## 🔄 Flujo ETL

1. **Extract**
   - Se conecta a PostgreSQL
   - Lee la tabla `retail_raw.transactions_raw`
   - Retorna un `pandas.DataFrame`

2. **Transform**
   - Normaliza nombres de columnas
   - Convierte tipos de datos
   - Aplica reglas de negocio:
     - Validación de descuentos
     - Re-cálculo de `total_amount`
     - Eliminación de registros inválidos

3. **Load**
   - Conecta a `masterflow_db`
   - Crea o reemplaza la tabla destino
   - Garantiza idempotencia

4. **Orquestación**
   - Prefect coordina las tareas
   - Manejo de retries y logging

---

## 🛠️ Stack técnico

- **Lenguaje:** Python 3.12
- **ETL / Data:** pandas, SQLAlchemy
- **Base de datos:** PostgreSQL
- **Orquestación:** Prefect 2
- **Testing:** Pytest
- **Configuración:** python-dotenv
- **Control de versiones:** Git / GitHub

---

## ▶️ Cómo ejecutar el proyecto

### 1️⃣ Clonar el repositorio
```bash
git clone https://github.com/tu-usuario/MASTERFLOW.git
cd MASTERFLOW

2️⃣ Crear entorno virtual
python -m venv venv
source venv/bin/activate  # Linux / Mac
venv\Scripts\activate     # Windows

3️⃣ Instalar dependencias
pip install -r requirements.txt

4️⃣ Configurar variables de entorno
Crear un archivo .env con:
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=postgres

5️⃣ Ejecutar tests
pytest

6️⃣ Ejecutar el flow ETL
python -m masterflow.flows.retail_transactions_flow

🧪 Testing
El proyecto incluye tests unitarios para:
Conversión de fechas
Cálculo correcto de totales
Validación de descuentos
Eliminación de registros inválidos
Esto asegura calidad de datos y evita errores silenciosos comunes en ETL.

🎯 Decisiones de diseño
Separación estricta entre lógica ETL y orquestación
Uso de contratos abstractos para facilitar extensiones
Transformaciones tolerantes a datos parciales
Logging centralizado para observabilidad
Enfoque en claridad y mantenibilidad sobre optimización prematura

📈 Posibles mejoras futuras
Cargas incrementales
Slowly Changing Dimensions (SCD)
Validaciones con Great Expectations
Dockerización
Integración con herramientas cloud