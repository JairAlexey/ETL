from util.db_connection import Db_Connection
import pandas as pd

def transformar_dim_ruta(ses_db):
    """Transforma datos para la dimensión ruta"""
    try:
        query = """
            SELECT 
                r.id as ruta_id,
                r.codigo,
                r.nombre,
                r.distancia,
                r.duracion,
                r.lugar_origen_id,
                r.lugar_destino_id
            FROM ext_rutas r
            WHERE r.eliminado = false
        """
        return pd.read_sql(query, ses_db)
    except Exception as e:
        print(f"Error en transformación de dim_ruta: {str(e)}")
        return pd.DataFrame()