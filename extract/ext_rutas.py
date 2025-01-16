import traceback
from util.db_connection import Db_Connection
from config import get_db_config
import pandas as pd

def extraer_rutas():
    try:
        config = get_db_config('oltp')
        con_db = Db_Connection(
            config['type'],
            config['host'],
            config['port'],
            config['user'],
            config['password'],
            config['db']
        )
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        stores = pd.read_sql ('SELECT * FROM rutas', ses_db)
        return stores
    except:
        traceback.print_exc()
    finally:
        pass