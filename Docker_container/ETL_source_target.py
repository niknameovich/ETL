from psycopg2 import connect, extensions


def postgres_connect(info):
    try:
        # declare a new PostgreSQL connection object
        conn = connect(
            dbname=info['dbname'],
            user=info['user'],
            host=info['host'],
            port=info['port'],
            password=info['password']
        )
    except Exception as err:
        print("psycopg2 connect() ERROR:", err)
        conn = None
    finally:
        return conn


def sql_execute(query, cursor, log):
    try:
        cursor.execute(query)
        cursor.connection.commit()
        return ''
    except Exception as ex:
        cursor.connection.rollback()
        return str(ex)


def get_schema_ddl(cursor):
    query = 'select table_name,string_agg(' \
            'CONCAT_WS(\' \',column_name, udt_name, ' \
            'REPLACE(REPLACE(is_nullable,\'YES\',\'NULL\'),\'NO\',\'NOT NULL\')' \
            '),' \
            '\',\' order by ordinal_position) from information_schema.columns ' \
            'where table_schema = \'public\' ' \
            'group by table_name'
    cursor.execute(query)
    for result in cursor.fetchall():
        yield result[0], f'Create table {result[0]} ({result[1]});'


def copy_table_data(table_name, source_cursor, target_cursor, ddl_query):
    fake_result = sql_execute(ddl_query, target_cursor, '')
    if not fake_result:
        with open(f'{table_name}.csv', 'w+') as file:
            source_cursor.copy_to(file, table_name, sep='|')
            file.seek(0)
            target_cursor.copy_from(file, table_name, sep='|')
    else:
        print(fake_result)


def copy_container_db(database_info):
    with postgres_connect(
            database_info['source']) as source_connection, source_connection.cursor() as source_executor:
        with postgres_connect(
                database_info['target']) as target_connection, target_connection.cursor() as target_executor:
            for table_name, ddl_query in get_schema_ddl(source_connection):
                copy_table_data(table_name, source_executor, target_executor,ddl_query)
        target_connection.close()
    source_connection.close()


database_info = {
    'source':
        {'dbname': 'source_database', 'user': 'root', 'host': '172.18.0.2', 'port': '5432', 'password': 'postgres'},
    'target': {'dbname': 'target_db', 'user': 'root', 'host': '172.19.0.1', 'port': '5433', 'password': 'postgres'}}

copy_container_db(database_info)
