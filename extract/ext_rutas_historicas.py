import traceback
import pandas as pd

def extraer_rutas_historicas():
    try:
        filename = './csvs/rutas_historicas.csv'
        rutas_historicas = pd.read_csv(filename)
        return rutas_historicas # return del dataframe contiene todos los datos como tal
    except:
        traceback.print_exc()
    finally:
        pass
