from util.db_connection import Db_Connection
import pandas as pd

def transformar_dim_vehiculo(ses_db):
    """Transforma datos para la dimensión vehículo"""
    try:
        query = """
            SELECT 
                id as vehiculo_id,
                codigo,
                tipo_vehiculo,
                capacidad,
                anio_fabricacion,
                color
            FROM ext_vehiculos
            WHERE eliminado = false
        """
        return pd.read_sql(query, ses_db)
    except Exception as e:
        print(f"Error en transformación de dim_vehiculo: {str(e)}")
        return pd.DataFrame()