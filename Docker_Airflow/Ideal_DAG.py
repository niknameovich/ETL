import os.path
import pathlib
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from psycopg2 import connect, extensions



DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 12),
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": True,
}

dag = DAG(
 dag_id="Dump_source_db",
default_args=DEFAULT_ARGS,
 schedule_interval=None,
)

database_info = {
    'source':
        {'dbname': 'source', 'user': 'root', 'host': 'db2', 'port': '5432', 'password': 'postgres'},
    'target': {'dbname': 'target', 'user': 'root', 'host': 'db', 'port': '5432', 'password': 'postgres'}}

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

def get_source_db():
    with postgres_connect(database_info['source']) as connection, connection.cursor() as cursor:
        query = "select table_name,\'Create Table \' || table_name || \' (\' || string_agg(CONCAT_WS(\' \',column_name, udt_name," \
            "REPLACE(REPLACE(is_nullable,\'YES\',\'NULL\'),\'NO\',\'NOT NULL\')),\',\' order by ordinal_position) || \');\' " \
            "from information_schema.columns where table_schema = \'public\' group by table_name"
        cursor.execute(query)
        pathlib.Path("/tmp/schema").mkdir(parents=True, exist_ok=True)
        pathlib.Path("/tmp/data").mkdir(parents=True, exist_ok=True)
        with open(f'/tmp/schema/schema.ddl', 'w+') as file:
            for result in cursor.fetchall():
                file.write(f'{result[1]}\r\n')
                with open(f'/tmp/data/{result[0]}.txt', 'w+') as data_file:
                    cursor.copy_to(data_file, result[0], sep='|')
    connection.close()

def load_schema_ddl():
    with postgres_connect(database_info['target']) as connection, connection.cursor() as cursor:
        with open(f'/tmp/schema/schema.ddl', 'r') as file:
            for ddl_query in file.readlines():
                try:
                    cursor.execute(ddl_query)
                except Exception as ex:
                    print(ex)
        connection.commit()
    connection.close()

def load_datatables():
    with postgres_connect(database_info['target']) as connection, connection.cursor() as cursor:
        files_to_copy = pathlib.Path('/tmp/data').iterdir()
        for file in files_to_copy:
            with open(file, 'r') as datafile:
                cursor.copy_from(datafile, os.path.splitext(file.name)[0], sep='|')
        connection.commit()
    connection.close()

# execute_schema = PostgresOperator(
#     task_id='execute_schema',
#     postgres_conn_id='postgres_source_1',
#     sql="COPY (select \'Create Table \' || table_name || \' (\' || string_agg(CONCAT_WS(\' \',column_name, udt_name,"
#         "REPLACE(REPLACE(is_nullable,\'YES\',\'NULL\'),\'NO\',\'NOT NULL\')),\',\' order by ordinal_position) || \');\'"
#         " from information_schema.columns where table_schema = \'public\' group by table_name) "
#         "TO \'/var/lib/postgresql/data/schema_tables.sql\'",
#     dag = dag
#     )

execute_source_db = PythonOperator(
    task_id='execute_source_db',
    python_callable=get_source_db,
    dag = dag
    )

load_schema = PythonOperator(
    task_id='load_schema',
    python_callable=load_schema_ddl,
    dag = dag
    )

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_datatables,
    dag = dag
    )
execute_source_db>>load_schema>>load_data


