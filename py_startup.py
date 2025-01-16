from extract.ext_vehiculos import extraer_vehiculos
from extract.per_staging import persisitir_staging
from extract.ext_rutas_horarios import extraer_rutas_horarios
from extract.ext_rutas_horarios_vehiculos import extraer_rutas_horarios_vehiculos
from extract.ext_rutas import extraer_rutas
from extract.ext_operaciones_diarias import extraer_operaciones_diarias
from extract.ext_consumos_transporte import extraer_consumos_transporte
from extract.ext_colaboradores_rutas import extraer_colaboradores_rutas
from extract.ext_colaboradores import extraer_colaboradores
from extract.ext_incidentes_transporte import extraer_incidentes_transporte
from extract.ext_rutas_historicas import extraer_rutas_historicas
import traceback
import pandas as pd
# from config import DB_CONFIG_OLTP
from populate_fake_data import poblar_tablas
from generate_csv_data import generar_rutas_historicas, generar_incidentes
from transform.tra_dimensional import ejecutar_transformacion
from load.load_dimensional import cargar_dimensiones_sor

try:
#     con_db = Db_Connection(
#         DB_CONFIG_OLTP['type'],
#         DB_CONFIG_OLTP['host'],
#         DB_CONFIG_OLTP['port'],
#         DB_CONFIG_OLTP['user'],
#         DB_CONFIG_OLTP['password'],
#         DB_CONFIG_OLTP['db']
#     )
#     ses_db = con_db.start()
#     if ses_db == -1:
#         raise Exception("El tipo de base de datos dado no es válido")
#     elif ses_db == -2:
#         raise Exception("Error tratando de conectarse a la base de datos")

#     databases = pd.read_sql('SELECT datname FROM pg_database;', ses_db)
#     print(databases)

    #print("Poblando tablas con datos de prueba...")
    #poblar_tablas()
    
    #print("Generando archivos CSV...")
    #generar_rutas_historicas()
    #generar_incidentes()
    
    
    print("Ejecutando transformaciones dimensionales...")
    transformaciones = ejecutar_transformacion()

    print("*************** TABLA VEHICULOS *******************")
    print("Extrayendo datos vehiculos desde la DB")
    vehiculos = extraer_vehiculos()
    #print("Data de tabla vehiculos:\n", vehiculos)
    print("Persistiendo en Staging datos de vehiculos")
    persisitir_staging(vehiculos, 'ext_vehiculos')
    print("Tabla Vehiculos:\n", vehiculos)
    print("***************************************************")
    
    print("*************** TABLA RUTAS HORARIOS *******************")
    print("Extrayendo datos rutas_horarios desde la DB")
    rutas_horarios = extraer_rutas_horarios()
    #print("Data de tabla rutas_horarios:\n", rutas_horarios)
    print("Persistiendo en Staging datos de rutas_horarios")
    persisitir_staging(rutas_horarios, 'ext_rutas_horarios')
    print("Tabla Rutas Horarios:\n", rutas_horarios)
    print("***************************************************")
    
    print("*************** TABLA RUTAS HORARIOS VEHICULOS *******************")
    print("Extrayendo datos de rutas_horarios_vehiculos desde la DB")
    rutas_horarios_vehiculos = extraer_rutas_horarios_vehiculos()
    #print("Data de tabla rutas_horarios_vehiculos:\n", rutas_horarios_vehiculos)
    print("Persistiendo en Staging datos de rutas_horarios_vehiculos")
    persisitir_staging(rutas_horarios_vehiculos, 'ext_rutas_horarios_vehiculos')
    print("Tabla Rutas Horarios Vehiculos:\n", rutas_horarios_vehiculos)
    print("***************************************************")
    
    print("*************** TABLA RUTAS *******************")
    print("Extrayendo datos de rutas desde la DB")
    rutas = extraer_rutas()
    #print("Data de tabla rutas:\n", rutas)
    print("Persistiendo en Staging datos de rutas")
    persisitir_staging(rutas, 'ext_rutas')
    print("Tabla Rutas:\n", rutas)
    print("***************************************************")
    
    print("*************** TABLA OPERACIONES DIARIAS *******************")
    print("Extrayendo datos de operaciones_diarias desde la DB")
    operaciones_diarias = extraer_operaciones_diarias()
    #print("Data de tabla operaciones_diarias:\n", operaciones_diarias)
    print("Persistiendo en Staging datos de operaciones_diarias")
    persisitir_staging(operaciones_diarias, 'ext_operaciones_diarias')
    print("Tabla Operaciones Diarias:\n", operaciones_diarias)
    print("***************************************************")
    
    print("*************** TABLA CONSUMOS TRANSPORTE *******************")
    print("Extrayendo datos de consumos_transporte desde la DB")
    consumos_transporte = extraer_consumos_transporte()
    #print("Data de tabla consumos_transporte:\n", consumos_transporte)
    print("Persistiendo en Staging datos de consumos_transporte")
    persisitir_staging(consumos_transporte, 'ext_consumos_transporte')
    print("Tabla Consumos Transporte:\n", consumos_transporte)
    print("***************************************************")
    
    print("*************** TABLA COLABORADORES RUTAS *******************")
    print("Extrayendo datos de colaboradores_rutas desde la DB")
    colaboradores_rutas = extraer_colaboradores_rutas()
    #print("Data de tabla colaboradores_rutas:\n", colaboradores_rutas)
    print("Persistiendo en Staging datos de colaboradores_rutas")
    persisitir_staging(colaboradores_rutas, 'ext_colaboradores_rutas')
    print("Tabla Colaboradores Rutas:\n", colaboradores_rutas)
    print("***************************************************")
    
    print("*************** TABLA COLABORADORES *******************")
    print("Extrayendo datos de colaboradores desde la DB")
    colaboradores = extraer_colaboradores()
    #print("Data de tabla colaboradores:\n", colaboradores)
    print("Persistiendo en Staging datos de colaboradores")
    persisitir_staging(colaboradores, 'ext_colaboradores')
    print("Tabla Colaboradores:\n", colaboradores)
    print("***************************************************")
    
    print("*************** TABLA INCIDENTES TRANSPORTE *******************")
    print("Extrayendo datos de incidentes_transporte desde la DB")
    incidentes_transporte = extraer_incidentes_transporte()
    #print("Data de tabla incidentes_transporte:\n", incidentes_transporte)
    print("Persistiendo en Staging datos de incidentes_transporte")
    persisitir_staging(incidentes_transporte, 'ext_incidentes_transporte')
    print("Tabla Incidentes Transport:\n", incidentes_transporte)
    print("***************************************************")
    
    print("*************** TABLA RUTAS HISTORICAS *******************")
    print("Extrayendo datos de rutas_historicas desde la DB")
    rutas_historicas = extraer_rutas_historicas()
    #print("Data de tabla rutas_historicas:\n", rutas_historicas)
    print("Persistiendo en Staging datos de rutas_historicas")
    persisitir_staging(rutas_historicas, 'ext_rutas_historicas')
    print("Tabla Rutas Historicas:\n", rutas_historicas)
    print("***************************************************")
    
    cargar_dimensiones_sor()
except:
    traceback.print_exc()
finally:
    pass
#   con_db.stop()