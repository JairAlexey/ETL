from util.db_connection import Db_Connection
import pandas as pd
import traceback
from sqlalchemy import text

def cargar_dim_colaborador(ses_sor_db, df_colaborador):
    try:
        print("Cargando dimensión colaborador...")
        with ses_sor_db.connect() as connection:
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_colaborador (
                    colaborador_id INTEGER PRIMARY KEY,
                    nombre_completo VARCHAR(200),
                    numero_identificacion VARCHAR(20)
                )
            """))
            connection.commit()
            
        df_colaborador.to_sql('dim_colaborador', ses_sor_db, 
                             if_exists='append', index=False)
        print("Dimensión colaborador cargada exitosamente")
        
    except Exception as e:
        print(f"Error cargando dim_colaborador: {str(e)}")
        traceback.print_exc()
