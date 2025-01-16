from util.db_connection import Db_Connection
import pandas as pd
import traceback
from sqlalchemy import text

def cargar_fact_operaciones(ses_sor_db, df_operaciones):
    try:
        print("Cargando tabla de hechos operaciones...")
        with ses_sor_db.connect() as connection:
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS fact_operaciones_transporte (
                    operacion_id INTEGER PRIMARY KEY,
                    vehiculo_id INTEGER REFERENCES dim_vehiculo(vehiculo_id),
                    ruta_id INTEGER REFERENCES dim_ruta(ruta_id),
                    colaborador_id INTEGER REFERENCES dim_colaborador(colaborador_id),
                    tiempo_id INTEGER REFERENCES dim_tiempo(tiempo_id),
                    kilometraje_inicio NUMERIC,
                    kilometraje_final NUMERIC,
                    retraso_salida_minutos NUMERIC,
                    retraso_llegada_minutos NUMERIC,
                    cumplimiento_horario BOOLEAN
                )
            """))
            connection.commit()
            
        df_operaciones.to_sql('fact_operaciones_transporte', ses_sor_db, 
                             if_exists='append', index=False)
        print("Tabla de hechos operaciones cargada exitosamente")
        
    except Exception as e:
        print(f"Error cargando fact_operaciones: {str(e)}")
        traceback.print_exc()
