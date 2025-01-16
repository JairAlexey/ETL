from transform.tra_dim_vehiculo import transformar_dim_vehiculo
from transform.tra_dim_ruta import transformar_dim_ruta
from transform.tra_dim_colaborador import transformar_dim_colaborador
from transform.tra_dim_tiempo import transformar_dim_tiempo
from transform.tra_fact_operaciones import transformar_hechos_operaciones
from util.db_connection import Db_Connection
from config import get_db_config
import traceback

def ejecutar_transformacion():
    try:
        config = get_db_config('staging')
        con_db = Db_Connection(
            config['type'],
            config['host'],
            config['port'],
            config['user'],
            config['password'],
            config['db']
        )
        ses_db = con_db.start()
        
        # Transformar dimensiones
        dim_vehiculo = transformar_dim_vehiculo(ses_db)
        dim_ruta = transformar_dim_ruta(ses_db)
        dim_colaborador = transformar_dim_colaborador(ses_db)
        dim_tiempo = transformar_dim_tiempo(ses_db)
        
        # Transformar hechos
        hechos_operaciones = transformar_hechos_operaciones(ses_db)
        
        return {
            'dim_vehiculo': dim_vehiculo,
            'dim_ruta': dim_ruta,
            'dim_colaborador': dim_colaborador,
            'dim_tiempo': dim_tiempo,
            'hechos_operaciones': hechos_operaciones
        }
        
    except Exception as e:
        print(f"Error en la transformación: {str(e)}")
        traceback.print_exc()
        return None
    finally:
        if 'con_db' in locals():
            con_db.stop()

if __name__ == "__main__":
    ejecutar_transformacion()