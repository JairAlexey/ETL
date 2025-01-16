from util.db_connection import Db_Connection
import pandas as pd
import traceback

def transformar_hechos_operaciones(ses_db):
    """Transforma datos para la tabla de hechos operaciones_transporte"""
    try:
        query = """
            SELECT 
                od.id as operacion_id,
                od.vehiculo_id,
                r.id as ruta_id,
                cr.colaborador_id,
                od.fecha_inicio,
                od.fecha_fin,
                od.kilometraje_inicio,
                od.kilometraje_final,
                EXTRACT(EPOCH FROM (
                    od.fecha_inicio - 
                    (DATE(od.fecha_inicio) + rhv.hora_salida)
                ))/60 as retraso_salida_minutos,
                EXTRACT(EPOCH FROM (
                    od.fecha_fin - 
                    (DATE(od.fecha_fin) + rhv.hora_llegada)
                ))/60 as retraso_llegada_minutos,
                CAST(
                    CASE 
                        WHEN od.fecha_inicio <= (DATE(od.fecha_inicio) + rhv.hora_salida)
                        AND od.fecha_fin <= (DATE(od.fecha_fin) + rhv.hora_llegada)
                        THEN 1 
                        ELSE 0 
                    END as INTEGER
                ) as cumplimiento_horario
            FROM ext_operaciones_diarias od
            JOIN ext_rutas_horarios_vehiculos rhv ON od.vehiculo_id = rhv.vehiculo_id
            JOIN ext_rutas_horarios rh ON rhv.ruta_horario_id = rh.id
            JOIN ext_rutas r ON rh.ruta_id = r.id
            JOIN ext_colaboradores_rutas cr ON rhv.ruta_horario_id = cr.ruta_horario_id
            WHERE od.vigente = true
        """
        df = pd.read_sql(query, ses_db)
        
        df['cumplimiento_horario'] = df['cumplimiento_horario'].astype(bool)
        
        tiempo_df = pd.DataFrame()
        tiempo_df['fecha'] = pd.to_datetime(df['fecha_inicio'].unique())
        tiempo_df['tiempo_id'] = range(1, len(tiempo_df) + 1)
        
        df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])
        df = df.merge(tiempo_df[['fecha', 'tiempo_id']], 
                    left_on=pd.to_datetime(df['fecha_inicio']).dt.date, 
                    right_on=pd.to_datetime(tiempo_df['fecha']).dt.date, 
                    how='left')
        
        columnas_finales = [
            'operacion_id', 'vehiculo_id', 'ruta_id', 'colaborador_id', 'tiempo_id',
            'kilometraje_inicio', 'kilometraje_final', 'retraso_salida_minutos',
            'retraso_llegada_minutos', 'cumplimiento_horario'
        ]
        
        df = df.drop_duplicates(subset=['operacion_id'])
        df = df[columnas_finales]
        
        return df
    except Exception as e:
        print(f"Error en transformación de hechos_operaciones: {str(e)}")
        traceback.print_exc()
        return pd.DataFrame()