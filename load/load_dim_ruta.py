from util.db_connection import Db_Connection
import pandas as pd
import traceback
from sqlalchemy import text

def cargar_dim_ruta(ses_sor_db, df_ruta):
    try:
        print("Cargando dimensión ruta...")
        with ses_sor_db.connect() as connection:
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_ruta (
                    ruta_id INTEGER PRIMARY KEY,
                    codigo VARCHAR(50),
                    nombre VARCHAR(100),
                    distancia NUMERIC,
                    duracion INTEGER,
                    lugar_origen_id INTEGER,
                    lugar_destino_id INTEGER
                )
            """))
            connection.commit()
            
        df_ruta.to_sql('dim_ruta', ses_sor_db, 
                       if_exists='append', index=False)
        print("Dimensión ruta cargada exitosamente")
        
    except Exception as e:
        print(f"Error cargando dim_ruta: {str(e)}")
        traceback.print_exc()
