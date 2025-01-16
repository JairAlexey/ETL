def get_db_config(db_name):
    return {
        'type': 'postgresql',
        'host': '192.168.115.130',
        'port': '5432',
        'user': 'admin123',
        'password': 'admin123',
        'db': db_name
    }

DB_CONFIG_OLTP = get_db_config('oltp')
DB_CONFIG_STAGING = get_db_config('staging')
DB_CONFIG_SOR = get_db_config('sor')

