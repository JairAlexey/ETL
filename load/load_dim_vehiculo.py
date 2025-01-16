from util.db_connection import Db_Connection
import pandas as pd
import traceback
from sqlalchemy import text

def cargar_dim_vehiculo(ses_sor_db, df_vehiculo):
    try:
        print("Cargando dimensión vehículo...")
        with ses_sor_db.connect() as connection:
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_vehiculo (
                    vehiculo_id INTEGER PRIMARY KEY,
                    codigo VARCHAR(50),
                    tipo_vehiculo VARCHAR(50),
                    capacidad INTEGER,
                    anio_fabricacion INTEGER,
                    color VARCHAR(50)
                )
            """))
            connection.commit()
            
        df_vehiculo.to_sql('dim_vehiculo', ses_sor_db, 
                          if_exists='append', index=False)
        print("Dimensión vehículo cargada exitosamente")
        
    except Exception as e:
        print(f"Error cargando dim_vehiculo: {str(e)}")
        traceback.print_exc()
