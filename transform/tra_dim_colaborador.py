from util.db_connection import Db_Connection
import pandas as pd
import traceback

def transformar_dim_colaborador(ses_db):
    """Transforma datos para la dimensión colaborador"""
    try:
        query = """
            SELECT 
                id as colaborador_id,
                CONCAT(nombres, ' ', primer_apellido, ' ', segundo_apellido) as nombre_completo,
                numero_identificacion
            FROM ext_colaboradores
        """
        return pd.read_sql(query, ses_db)
    except Exception as e:
        print(f"Error en transformación de dim_colaborador: {str(e)}")
        traceback.print_exc()
        return pd.DataFrame()