from faker import Faker
import pandas as pd
from datetime import datetime, timedelta
import random
from util.db_connection import Db_Connection
from config import DB_CONFIG_OLTP
import traceback
from sqlalchemy.sql import text

fake = Faker('es_ES')

def generar_vehiculos(num_registros=50):
    vehiculos = []
    tipos_vehiculo = ['Bus', 'Minibus', 'Van']
    colores = ['Blanco', 'Gris', 'Negro', 'Azul', 'Rojo']
    
    for _ in range(num_registros):
        vehiculo = {
            'codigo': fake.unique.random_number(digits=6),
            'tipo_vehiculo': random.choice(tipos_vehiculo),
            'capacidad': random.randint(8, 45),
            'anio_fabricacion': random.randint(2015, 2024),
            'color': random.choice(colores),
            'fecha_vencimiento_matricula': fake.date_between(start_date='-1y', end_date='+2y'),
            'eliminado': False
        }
        vehiculos.append(vehiculo)
    
    return pd.DataFrame(vehiculos)

def generar_rutas(num_registros=20):
    rutas = []
    
    for _ in range(num_registros):
        # Calculamos la duración en minutos totales
        horas = random.randint(1, 4)
        minutos = random.randint(0, 59)
        duracion_total_minutos = (horas * 60) + minutos
        
        ruta = {
            'codigo': fake.unique.random_number(digits=6),
            'nombre': f'Ruta {fake.unique.random_number(digits=3)}',
            'lugar_origen_id': random.randint(1, 10),
            'lugar_destino_id': random.randint(1, 10),
            'duracion': duracion_total_minutos,  # Ahora es un entero
            'distancia': random.randint(10, 500),
            'eliminado': False
        }
        rutas.append(ruta)
    
    return pd.DataFrame(rutas)

def generar_rutas_horarios(rutas_ids, num_registros=30):
    rutas_horarios = []
    
    for _ in range(num_registros):
        ruta_id = random.choice(rutas_ids['id'].tolist())
        ruta_horario = {
            'ruta_id': ruta_id,
            'horario': fake.time(),
            'eliminado': False
        }
        rutas_horarios.append(ruta_horario)
    
    return pd.DataFrame(rutas_horarios)

def generar_rutas_horarios_vehiculos(vehiculos_ids, rutas_horarios_ids, num_registros=100):
    rutas_horarios_vehiculos = []
    
    # Definimos estados como números
    ESTADOS = {
        1: 'Activo',
        2: 'Inactivo',
        3: 'En mantenimiento'
    }
    
    for _ in range(num_registros):
        vehiculo_id = random.choice(vehiculos_ids['id'].tolist())
        ruta_horario_id = random.choice(rutas_horarios_ids['id'].tolist())
        
        # Calculamos la duración en minutos totales
        horas = random.randint(1, 4)
        minutos = random.randint(0, 59)
        duracion_total_minutos = (horas * 60) + minutos
        
        fecha_desde = fake.date_between(start_date='-6m', end_date='today')
        fecha_hasta = fake.date_between(start_date=fecha_desde, end_date='+6m')
        
        registro = {
            'vehiculo_id': vehiculo_id,
            'fecha_desde': fecha_desde,
            'fecha_hasta': fecha_hasta,
            'hora_salida': fake.time(),
            'duracion': duracion_total_minutos,
            'hora_llegada': fake.time(),
            'estado': random.randint(1, 3),  # Ahora usamos números en lugar de strings
            'fecha_creacion': datetime.now(),
            'ruta_horario_id': ruta_horario_id,
            'eliminado': False
        }
        rutas_horarios_vehiculos.append(registro)
    
    return pd.DataFrame(rutas_horarios_vehiculos)
def generar_operaciones_diarias(vehiculos_ids, num_registros=100):
    operaciones = []
    
    for _ in range(num_registros):
        vehiculo_id = random.choice(vehiculos_ids['id'].tolist())
        fecha_inicio = fake.date_time_between(start_date='-1y', end_date='now')
        fecha_fin = fake.date_time_between(start_date=fecha_inicio, end_date='+1d')
        
        kilometraje_inicio = random.randint(0, 100000)
        kilometraje_final = kilometraje_inicio + random.randint(50, 500)
        
        operacion = {
            'vehiculo_id': vehiculo_id,
            'fecha_inicio': fecha_inicio,
            'fecha_fin': fecha_fin,
            'kilometraje_inicio': kilometraje_inicio,
            'kilometraje_final': kilometraje_final,
            'vigente': True
        }
        operaciones.append(operacion)
    
    return pd.DataFrame(operaciones)


def generar_consumos_transporte(operaciones_diarias_ids, num_registros=150):
    consumos = []
    
    for _ in range(num_registros):
        operacion_diaria_id = random.choice(operaciones_diarias_ids['id'].tolist())
        
        consumo = {
            'operacion_diaria_id': operacion_diaria_id,
            'coordenada_x_embarque': random.uniform(-90, 90),
            'coordenada_y_embarque': random.uniform(-180, 180),
            'coordenada_x_desembarque': random.uniform(-90, 90),
            'coordenada_y_desembarque': random.uniform(-180, 180),
            'vigente': True
        }
        consumos.append(consumo)
    
    return pd.DataFrame(consumos)


def generar_colaboradores_rutas(colaboradores_ids, rutas_horarios_ids, num_registros=80):
    colaboradores_rutas = []
    for _ in range(num_registros):
        colaborador_id = random.choice(colaboradores_ids['id'].tolist())
        ruta_horario_id = random.choice(rutas_horarios_ids['id'].tolist())
        
        colaborador_ruta = {
            'colaborador_id': colaborador_id,
            'ruta_horario_id': ruta_horario_id,
            'estado': random.randint(1, 3),
            'fecha_asignacion': fake.date_time_between(start_date='-6m', end_date='now')
        }
        colaboradores_rutas.append(colaborador_ruta)
    return pd.DataFrame(colaboradores_rutas)


def generar_colaboradores(num_registros=50):
    colaboradores = []
    for _ in range(num_registros):
        colaborador = {
            'numero_identificacion': fake.unique.random_number(digits=8),
            'nombres': fake.first_name(),
            'primer_apellido': fake.last_name(),
            'segundo_apellido': fake.last_name()
        }
        colaboradores.append(colaborador)
    return pd.DataFrame(colaboradores)



def poblar_tablas():
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
        
        # Crear una conexión para ejecutar los TRUNCATE
        with ses_db.connect() as connection:
            print("Actualizando estructura de tablas...")
            # Agregar columna eliminado si no existe
            connection.execute(text("""
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (
                        SELECT 1 
                        FROM information_schema.columns 
                        WHERE table_name='rutas_horarios_vehiculos' 
                        AND column_name='eliminado'
                    ) THEN 
                        ALTER TABLE rutas_horarios_vehiculos 
                        ADD COLUMN eliminado BOOLEAN DEFAULT FALSE;
                    END IF;
                END $$;
            """))
            connection.commit()
            
            print("Limpiando tablas existentes...")
            connection.execute(text("TRUNCATE TABLE colaboradores_rutas CASCADE"))
            connection.execute(text("TRUNCATE TABLE operaciones_diarias CASCADE"))
            connection.execute(text("TRUNCATE TABLE consumos_transporte CASCADE"))
            connection.execute(text("TRUNCATE TABLE rutas_horarios_vehiculos CASCADE"))
            connection.execute(text("TRUNCATE TABLE rutas_horarios CASCADE"))
            connection.execute(text("TRUNCATE TABLE rutas CASCADE"))
            connection.execute(text("TRUNCATE TABLE vehiculos CASCADE"))
            connection.execute(text("TRUNCATE TABLE colaboradores CASCADE"))
            connection.commit()
        
        print("Generando datos de colaboradores...")
        df_colaboradores = generar_colaboradores()
        df_colaboradores.to_sql('colaboradores', ses_db, if_exists='append', index=False)
        
        print("Generando datos de vehículos...")
        df_vehiculos = generar_vehiculos()
        df_vehiculos.to_sql('vehiculos', ses_db, if_exists='append', index=False)
        
        print("Generando datos de rutas...")
        df_rutas = generar_rutas()
        df_rutas.to_sql('rutas', ses_db, if_exists='append', index=False)
        
        # Obtener los IDs generados
        vehiculos_ids = pd.read_sql("SELECT id FROM vehiculos", ses_db)
        rutas_ids = pd.read_sql("SELECT id FROM rutas", ses_db)
        
        print("Generando datos de rutas y horarios...")
        df_rutas_horarios = generar_rutas_horarios(rutas_ids)
        df_rutas_horarios.to_sql('rutas_horarios', ses_db, if_exists='append', index=False)
        
        rutas_horarios_ids = pd.read_sql("SELECT id FROM rutas_horarios", ses_db)
        
        print("Generando datos de rutas_horarios_vehiculos...")
        df_rutas_horarios_vehiculos = generar_rutas_horarios_vehiculos(vehiculos_ids, rutas_horarios_ids)
        df_rutas_horarios_vehiculos.to_sql('rutas_horarios_vehiculos', ses_db, if_exists='append', index=False)
        
        print("¡Datos generados y guardados exitosamente!")
        
        print("Generando datos de operaciones diarias...")
        df_operaciones = generar_operaciones_diarias(vehiculos_ids)
        df_operaciones.to_sql('operaciones_diarias', ses_db, if_exists='append', index=False)
        
        operaciones_ids = pd.read_sql("SELECT id FROM operaciones_diarias", ses_db)
        
        print("Generando datos de consumos de transporte...")
        df_consumos = generar_consumos_transporte(operaciones_ids)
        df_consumos.to_sql('consumos_transporte', ses_db, if_exists='append', index=False)
        
        # Asumiendo que tienes una tabla de colaboradores
        colaboradores_ids = pd.read_sql("SELECT id FROM colaboradores", ses_db)
        
        print("Generando datos de colaboradores_rutas...")
        df_colaboradores_rutas = generar_colaboradores_rutas(colaboradores_ids, rutas_horarios_ids)
        df_colaboradores_rutas.to_sql('colaboradores_rutas', ses_db, if_exists='append', index=False)
        
        print("¡Datos generados y guardados exitosamente!")
        
        
    except Exception as e:
        print(f"Error al poblar las tablas: {str(e)}")
        traceback.print_exc()
    finally:
        if 'con_db' in locals():
            con_db.stop()

if __name__ == "__main__":
    poblar_tablas()