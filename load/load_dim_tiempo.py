from util.db_connection import Db_Connection
import pandas as pd
import traceback
from sqlalchemy import text

def cargar_dim_tiempo(ses_sor_db, df_tiempo):
    try:
        print("Cargando dimensión tiempo...")
        with ses_sor_db.connect() as connection:
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_tiempo (
                    tiempo_id INTEGER PRIMARY KEY,
                    fecha DATE,
                    anio INTEGER,
                    mes INTEGER,
                    dia INTEGER,
                    dia_semana VARCHAR(20),
                    semana_anio INTEGER
                )
            """))
            connection.commit()
            
        df_tiempo.to_sql('dim_tiempo', ses_sor_db, 
                        if_exists='append', index=False)
        print("Dimensión tiempo cargada exitosamente")
        
    except Exception as e:
        print(f"Error cargando dim_tiempo: {str(e)}")
        traceback.print_exc()
