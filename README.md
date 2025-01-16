# Sistema ETL para Gestión de Transporte

## Descripción
Este proyecto implementa un sistema ETL (Extract, Transform, Load) para procesar y analizar datos relacionados con la gestión de transporte. El sistema extrae datos de diversas fuentes, los transforma y los carga en un almacén de datos dimensional para su análisis.

## Características Principales
- Procesamiento de datos de vehículos, rutas y horarios
- Seguimiento de incidentes de transporte
- Gestión de operaciones diarias
- Análisis de consumos de transporte
- Gestión de colaboradores y asignaciones

## Requisitos Previos
- Python 3.8 o superior
- PostgreSQL 12 o superior
- pip (gestor de paquetes de Python)

## Dependencias Python\
bash
pip install pandas
pip install sqlalchemy
pip install psycopg2-binary
pip install python-dotenv
pip install faker

## Estructura de Base de Datos
El sistema requiere tres bases de datos PostgreSQL:
1. Base OLTP (datos operacionales)
2. Base Staging (área de preparación)
3. Base SOR (almacén dimensional)

### Crear las Bases de Datos
sql
CREATE DATABASE oltp;
CREATE DATABASE staging;
CREATE DATABASE sor;

## Configuración del Entorno
1. Clonar el repositorio:
bash
git clone [url-del-repositorio]
cd [nombre-del-directorio]

## Estructura del Proyecto
proyecto/
├── csvs/
│ ├── incidentes_transporte.csv
│ └── rutas_historicas.csv
├── extract/
│ ├── ext_.py (módulos de extracción)
├── transform/
│ ├── tra_.py (módulos de transformación)
├── load/
│ ├── load_.py (módulos de carga)
├── util/
│ └── db_connection.py
├── config.py
├── populate_fake_data.py
├── generate_csv_data.py
└── py_startup.py

2. Ejecutar el proceso ETL completo:
bash
python py_startup.py


## Flujo del Proceso ETL

1. **Extracción (Extract)**
   - Datos de bases OLTP
   - Archivos CSV (incidentes y rutas históricas)

2. **Transformación (Transform)**
   - Dimensiones:
     - Vehículos
     - Rutas
     - Colaboradores
     - Tiempo
   - Hechos:
     - Operaciones de transporte

3. **Carga (Load)**
   - Carga en área de staging
   - Carga final en almacén dimensional (SOR)

## Tablas Dimensionales Resultantes
- dim_vehiculo
- dim_ruta
- dim_colaborador
- dim_tiempo
- fact_operaciones_transporte

## Consideraciones Importantes
- Asegurarse de tener permisos adecuados en PostgreSQL
- Configurar correctamente el locale para español: `es_ES.UTF-8`
- Verificar la conectividad a las bases de datos antes de ejecutar
- Los archivos CSV deben estar en el directorio `csvs/`

## Solución de Problemas
1. Error de conexión a base de datos:
   - Verificar credenciales
   - Confirmar que PostgreSQL esté en ejecución

2. Error de locale:
   - Instalar locale español: `sudo locale-gen es_ES.UTF-8`

3. Errores de dependencias:
   - Ejecutar: `pip install -r requirements.txt`

## Mantenimiento
- Respaldar bases de datos regularmente
- Monitorear logs de errores
- Actualizar dependencias según sea necesario