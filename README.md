# Sistema ETL para Gestión de Transporte

## Descripción
Este proyecto implementa un sistema ETL (Extract, Transform, Load) para procesar y analizar datos relacionados con la gestión de transporte. El sistema extrae datos de diversas fuentes, los transforma y los carga en un almacén de datos dimensional para facilitar el análisis y la toma de decisiones.

## Características Principales
- Procesamiento de datos de vehículos, rutas y horarios.
- Seguimiento de incidentes de transporte.
- Gestión de operaciones diarias.
- Análisis de consumos de transporte.
- Gestión de colaboradores y asignaciones.

## Requisitos Previos
- Python 3.8 o superior.
- PostgreSQL 12 o superior.
- pip (gestor de paquetes de Python).

## Dependencias de Python
Instalar las dependencias requeridas ejecutando los siguientes comandos:

```bash
pip install pandas
pip install sqlalchemy
pip install psycopg2-binary
pip install python-dotenv
pip install faker
```

## Configuración de Base de Datos
El sistema utiliza tres bases de datos PostgreSQL para separar las capas de datos:
1. **Base OLTP:** Datos operacionales.
2. **Base Staging:** Área de preparación de datos.
3. **Base SOR:** Almacén de datos dimensional.

### Crear las Bases de Datos
Ejecuta los siguientes comandos SQL para crear las bases de datos:

```sql
CREATE DATABASE oltp;
CREATE DATABASE staging;
CREATE DATABASE sor;
```

## Configuración del Entorno

1. **Clonar el repositorio:**

```bash
git clone [url-del-repositorio]
cd [nombre-del-directorio]
```

2. **Configurar el archivo `.env`**
   - Crear un archivo `.env` en el directorio raíz con las credenciales de la base de datos:

```env
DB_HOST=localhost
DB_PORT=5432
DB_USER=admin123
DB_PASSWORD=tu_password
```

3. **Ejecutar el proceso ETL completo:**

```bash
python py_startup.py
```

## Estructura del Proyecto

```plaintext
proyecto/
├── csvs/
│   ├── incidentes_transporte.csv
│   └── rutas_historicas.csv
├── extract/
│   ├── ext_vehiculos.py  # Extracción de datos de vehículos.
│   ├── ext_rutas.py      # Extracción de datos de rutas.
│   └── ext_colaboradores.py
├── transform/
│   ├── tra_vehiculos.py  # Transformación de datos de vehículos.
│   ├── tra_rutas.py      # Transformación de datos de rutas.
│   └── tra_colaboradores.py
├── load/
│   ├── load_staging.py   # Carga en la base de datos de staging.
│   └── load_sor.py       # Carga en el almacén dimensional.
├── util/
│   └── db_connection.py  # Gestión de la conexión con PostgreSQL.
├── config.py             # Configuración del sistema.
├── populate_fake_data.py # Generar datos de prueba con Faker.
├── generate_csv_data.py  # Generar datos adicionales en CSV.
└── py_startup.py         # Script principal del ETL.
```

## Flujo del Proceso ETL

1. **Extracción (Extract):**
   - Datos obtenidos desde la base OLTP.
   - Información adicional cargada desde archivos CSV (`incidentes_transporte.csv`, `rutas_historicas.csv`).

2. **Transformación (Transform):**
   - Creación de tablas dimensionales:
     - `dim_vehiculo`
     - `dim_ruta`
     - `dim_colaborador`
     - `dim_tiempo`
   - Construcción de la tabla de hechos:
     - `fact_operaciones_transporte`

3. **Carga (Load):**
   - Los datos se cargan primero en la base de datos Staging.
   - Posteriormente, se consolidan en la base SOR con el modelo dimensional.

## Tablas Dimensionales Resultantes
- **`dim_vehiculo`**: Información de los vehículos.
- **`dim_ruta`**: Detalles de las rutas.
- **`dim_colaborador`**: Información de los colaboradores.
- **`dim_tiempo`**: Fechas y periodos de análisis.
- **`fact_operaciones_transporte`**: Registro consolidado de operaciones de transporte.

## Consideraciones Importantes
- Asegúrate de tener permisos adecuados en PostgreSQL para gestionar las bases de datos.
- Configura correctamente el locale para español: `es_ES.UTF-8`.
- Verifica que los archivos CSV estén disponibles en el directorio `csvs/` antes de ejecutar el ETL.

## Solución de Problemas

1. **Error de conexión a la base de datos:**
   - Verifica las credenciales en el archivo `.env`.
   - Asegúrate de que PostgreSQL esté en ejecución.

2. **Error de locale:**
   - Instala el locale español: `sudo locale-gen es_ES.UTF-8`.

3. **Errores de dependencias:**
   - Instala las dependencias faltantes con:

```bash
pip install -r requirements.txt
```

## Mantenimiento
- Realiza respaldos regulares de las bases de datos.
- Monitorea los logs de errores en cada ejecución del ETL.
- Actualiza las dependencias y herramientas del proyecto periódicamente para garantizar compatibilidad y seguridad.