import traceback
import pandas as pd

def extraer_incidentes_transporte():
    try:
        filename = './csvs/incidentes_transporte.csv'
        incidentes_transporte = pd.read_csv(filename)
        return incidentes_transporte  # return del dataframe contiene todos los datos como tal
    except:
        traceback.print_exc()
    finally:
        pass
