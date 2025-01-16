from util.db_connection import Db_Connection
from config import get_db_config
from transform.tra_dimensional import ejecutar_transformacion
from load.load_dim_vehiculo import cargar_dim_vehiculo
from load.load_dim_ruta import cargar_dim_ruta
from load.load_dim_colaborador import cargar_dim_colaborador
from load.load_dim_tiempo import cargar_dim_tiempo
from load.load_fact_operaciones import cargar_fact_operaciones
import traceback
from sqlalchemy import text

def cargar_dimensiones_sor():
    try:
        transformaciones = ejecutar_transformacion()
        config = get_db_config('sor')
        con_db = Db_Connection(
            config['type'],
            config['host'],
            config['port'],
            config['user'],
            config['password'],
            config['db']
        )
        ses_sor_db = con_db.start()
        
        # Eliminar tablas existentes en orden correcto
        print("Eliminando tablas existentes...")
        with ses_sor_db.connect() as connection:
            connection.execute(text("DROP TABLE IF EXISTS fact_operaciones_transporte CASCADE"))
            connection.execute(text("DROP TABLE IF EXISTS dim_vehiculo CASCADE"))
            connection.execute(text("DROP TABLE IF EXISTS dim_ruta CASCADE"))
            connection.execute(text("DROP TABLE IF EXISTS dim_colaborador CASCADE"))
            connection.execute(text("DROP TABLE IF EXISTS dim_tiempo CASCADE"))
            connection.commit()
        
        # Cargar dimensiones
        cargar_dim_vehiculo(ses_sor_db, transformaciones['dim_vehiculo'])
        cargar_dim_ruta(ses_sor_db, transformaciones['dim_ruta'])
        cargar_dim_colaborador(ses_sor_db, transformaciones['dim_colaborador'])
        cargar_dim_tiempo(ses_sor_db, transformaciones['dim_tiempo'])
        
        # Cargar hechos
        cargar_fact_operaciones(ses_sor_db, transformaciones['hechos_operaciones'])
        
        print("\n¡Datos cargados exitosamente en el SOR!")
        
    except Exception as e:
        print(f"Error en la carga al SOR: {str(e)}")
        traceback.print_exc()
    finally:
        if 'con_db' in locals():
            con_db.stop()

if __name__ == "__main__":
    cargar_dimensiones_sor() 