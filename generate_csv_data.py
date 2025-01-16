from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta
from util.db_connection import Db_Connection
from config import DB_CONFIG_OLTP

fake = Faker('es_ES')

def generar_fechas_uniformes(fecha_inicio, fecha_fin, num_fechas):
    """Genera fechas distribuidas uniformemente en el rango dado"""
    delta = fecha_fin - fecha_inicio
    fechas = []
    
    for i in range(num_fechas):
        # Calculamos un punto uniforme en el rango de días
        dias_aleatorios = (delta.days * i) // num_fechas
        nueva_fecha = fecha_inicio + timedelta(days=dias_aleatorios)
        # Agregamos una hora aleatoria al día
        hora = timedelta(hours=random.randint(6, 22), 
                        minutes=random.randint(0, 59))
        fechas.append(nueva_fecha + hora)
    
    # Mezclamos las fechas para evitar que estén en orden estricto
    random.shuffle(fechas)
    return fechas

def obtener_codigos_existentes():
    try:
        con_db = Db_Connection(
            DB_CONFIG_OLTP['type'],
            DB_CONFIG_OLTP['host'],
            DB_CONFIG_OLTP['port'],
            DB_CONFIG_OLTP['user'],
            DB_CONFIG_OLTP['password'],
            DB_CONFIG_OLTP['db']
        )
        ses_db = con_db.start()
        
        # Obtener códigos de vehículos existentes
        vehiculos_df = pd.read_sql('SELECT codigo FROM vehiculos', ses_db)
        codigos_vehiculos = vehiculos_df['codigo'].tolist()
        
        # Obtener códigos de rutas existentes
        rutas_df = pd.read_sql('SELECT codigo FROM rutas', ses_db)
        codigos_rutas = rutas_df['codigo'].tolist()
        
        return codigos_vehiculos, codigos_rutas
        
    except Exception as e:
        print(f"Error al obtener códigos: {str(e)}")
        return [], []
    finally:
        if 'con_db' in locals():
            con_db.stop()

def generar_rutas_historicas(num_registros=100):
    rutas = []
    _, codigos_rutas = obtener_codigos_existentes()
    ciudades = ['Quito', 'Guayaquil', 'Cuenca', 'Manta', 'Ambato', 'Ibarra', 'Loja', 
                'Portoviejo', 'Machala', 'Riobamba', 'Esmeraldas', 'Santo Domingo']
    
    if not codigos_rutas:
        print("No se encontraron códigos de rutas en la base de datos")
        return pd.DataFrame()
    
    for _ in range(num_registros):
        origen = random.choice(ciudades)
        while True:
            destino = random.choice(ciudades)
            if destino != origen:
                break
                
        ruta = {
            'codigo_ruta': random.choice(codigos_rutas),
            'lugar_origen': origen,
            'lugar_destino': destino,
            'distancia': random.randint(50, 800),
            'tiempo_estimado': random.randint(60, 720)
        }
        rutas.append(ruta)
    
    df = pd.DataFrame(rutas)
    df.to_csv('rutas_historicas.csv', index=False)
    return df

def generar_incidentes(num_registros=150):
    incidentes = []
    codigos_vehiculos, _ = obtener_codigos_existentes()
    tipos_incidente = [
        'Fallo Mecánico', 'Tráfico Intenso', 'Accidente', 
        'Clima Adverso', 'Manifestación', 'Obras en Vía'
    ]
    
    if not codigos_vehiculos:
        print("No se encontraron códigos de vehículos en la base de datos")
        return pd.DataFrame()
    
    # Definimos el rango de fechas (2 años)
    fecha_inicio = datetime(2023, 1, 1)
    fecha_fin = datetime(2024, 12, 31)
    
    # Generamos fechas distribuidas uniformemente
    fechas = generar_fechas_uniformes(fecha_inicio, fecha_fin, num_registros)
    
    for i in range(num_registros):
        incidente = {
            'fecha': fechas[i].strftime('%Y-%m-%d %H:%M:%S'),
            'vehiculo_codigo': random.choice(codigos_vehiculos),
            'tipo_incidente': random.choice(tipos_incidente),
            'descripcion': fake.sentence(),
            'tiempo_retraso': random.randint(15, 180)
        }
        incidentes.append(incidente)
    
    df = pd.DataFrame(incidentes)
    df.to_csv('incidentes_transporte.csv', index=False)
    return df

if __name__ == "__main__":
    print("Generando archivo rutas_historicas.csv...")
    generar_rutas_historicas()
    
    print("Generando archivo incidentes_transporte.csv...")
    generar_incidentes()
    
    print("¡Archivos CSV generados exitosamente!") 