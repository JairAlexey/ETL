from util.db_connection import Db_Connection
import pandas as pd
import locale

def transformar_dim_tiempo(ses_db):
    """Genera dimensión tiempo desde operaciones_diarias"""
    try:
        # Configurar locale en español
        locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
        
        query = """
            SELECT DISTINCT fecha_inicio as fecha
            FROM ext_operaciones_diarias
            WHERE vigente = true
        """
        fechas_df = pd.read_sql(query, ses_db)
        
        tiempo_df = pd.DataFrame()
        tiempo_df['fecha'] = pd.to_datetime(fechas_df['fecha'])
        tiempo_df['tiempo_id'] = range(1, len(tiempo_df) + 1)
        tiempo_df['anio'] = tiempo_df['fecha'].dt.year
        tiempo_df['mes'] = tiempo_df['fecha'].dt.month
        tiempo_df['dia'] = tiempo_df['fecha'].dt.day
        tiempo_df['dia_semana'] = tiempo_df['fecha'].dt.strftime('%A').str.capitalize()
        tiempo_df['semana_anio'] = tiempo_df['fecha'].dt.isocalendar().week
        
        columnas = ['tiempo_id', 'fecha', 'anio', 'mes', 'dia', 'dia_semana', 'semana_anio']
        tiempo_df = tiempo_df[columnas]
        
        return tiempo_df
    except Exception as e:
        print(f"Error en transformación de dim_tiempo: {str(e)}")
        return pd.DataFrame()