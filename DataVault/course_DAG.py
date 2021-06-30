import os.path
import pathlib
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import json


config = None

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 12),
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": True,
}


def copy_schema(**kwargs):
    source_connection = 'source_postgres'
    target_connection = 'target_postgres'
    src = PostgresHook(postgres_conn_id=source_connection)
    target = PostgresHook(postgres_conn_id=target_connection)
    src_conn = src.get_conn()
    cursor = src_conn.cursor()
    trgt_con = target.get_conn()
    trgt_cursor = trgt_con.cursor()

    query = "select table_name,\'Create Table stage.\' || table_name || \' (\' || string_agg(CONCAT_WS(\' \',column_name, udt_name," \
            "REPLACE(REPLACE(is_nullable,\'YES\',\'NULL\'),\'NO\',\'NOT NULL\')),\',\' order by ordinal_position) || \');\' " \
            "from information_schema.columns where table_schema = \'public\' group by table_name"
    cursor.execute(query)
    for result in cursor.fetchall():
        trgt_cursor.execute(f'Drop table if Exists stage.{result[0]}')
        trgt_cursor.execute(result[1])
    trgt_con.commit()


def copy_constraints(**kwargs):
    source_connection = 'source_postgres'
    target_connection = 'target_postgres'
    src = PostgresHook(postgres_conn_id=source_connection)
    target = PostgresHook(postgres_conn_id=target_connection)
    src_conn = src.get_conn()
    cursor = src_conn.cursor()
    trgt_con = target.get_conn()
    trgt_cursor = trgt_con.cursor()

    constraint_query = 'Select \'ALTER TABLE \' || conrelid::regclass ||\' ADD \'|| pg_get_constraintdef(oid,true)  from pg_constraint where contype in(\'f\',\'p\') ' \
                       'and connamespace = \'public\'::regnamespace'
    cursor.execute(constraint_query)
    for constraint_ddl in cursor.fetchall():
        trgt_cursor.execute(constraint_ddl[0])
    trgt_con.commit()


def export_data_from_tables(**kwargs):
    pathlib.Path("/tmp/data").mkdir(parents=True, exist_ok=True)

    source_connection = 'source_postgres'
    src = PostgresHook(postgres_conn_id=source_connection)
    src_conn = src.get_conn()
    cursor = src_conn.cursor()
    query = 'select table_name from information_schema.tables where table_schema=\'public\''
    cursor.execute(query)
    for table in cursor.fetchall():
        with open(f'/tmp/data/{table[0]}.txt', 'w') as file:
            cursor.copy_to(file, table[0], sep='|')
    src_conn.commit()
    src_conn.close()

def import_data_from_tables(**kwargs):
    ti=kwargs['ti']
    connection = 'target_postgres'
    PHook = PostgresHook(postgres_conn_id=connection)
    conn = PHook.get_conn()
    cursor = conn.cursor()
    files_to_copy = pathlib.Path('/tmp/data').iterdir()
    for file in files_to_copy:
        with open(file, 'r') as datafile:
            cursor.copy_from(datafile, f'stage.{os.path.splitext(file.name)[0]}', sep='|')
    conn.commit()
    conn.close()


def transmit_data_to_Vault(**kwargs):
    connection = 'target_postgres'
    PHook = PostgresHook(postgres_conn_id=connection)
    conn = PHook.get_conn()

    config = Variable.get("SOURCE_CONFIG", deserialize_json=True)
    source_system= config['source_postgres']
    source_id = source_system['connection_id']
    fill_hubs(conn,source_system,source_id)
    fill_links(conn,source_system,source_id)
    conn.close()

def fill_hubs(conn, source_system,source_id):
    cursor = conn.cursor()
    hubs = source_system['hubs']
    for key, value in hubs.items():
        query = f'insert into core.h_{key}({value[1]},source_system,processed_dttm) (select distinct {value[0]},{source_id},CURRENT_TIMESTAMP from stage.{key})' \
                f' ON CONFLICT ({value[1]}) DO UPDATE SET source_system=EXCLUDED.source_system,processed_dttm = EXCLUDED.processed_dttm'
        print(query)
        cursor.execute(query)
        conn.commit()
        fill_satellite(conn,key,list(value),source_id)

def fill_links(conn, source_system,source_id):
    cursor = conn.cursor()
    links = source_system['links']
    for key, value in links.items():
        stage_table = value['stage']
        core_table = value['core']
        id_columns = value ['columnsid']
        hubs = value['core_hubs']
        query = f'insert into core.{core_table}({id_columns["hub_rk"][0]},{id_columns["hub_rk"][1]},source_system,processed_dttm) ' \
                f'(select hub1.{id_columns["hub_rk"][0]},hub2.{id_columns["hub_rk"][1]},{source_id},CURRENT_TIMESTAMP from stage.{stage_table} as slink' \
                f' join core.{hubs[0]} hub1 on hub1.{id_columns["core"][0]} = slink.{id_columns["stage"][0]}' \
                f' join core.{hubs[1]} hub2 on hub2.{id_columns["core"][1]} = slink.{id_columns["stage"][1]})' \
                f'ON CONFLICT DO NOTHING'
        print(query)
        cursor.execute(query)
        conn.commit()
        fill_satellite(conn,stage_table,dict(id_columns),source_id,hubs,core_table)


def fill_satellite(conn, table_name,idcolumns,source_id,hubs = None, core_table = None):
    cursor = conn.cursor()
    if isinstance(idcolumns, list):
        sqlcolumns = "','".join(idcolumns)
    else:
        sqlcolumns = "','".join(idcolumns['stage'])
    print (sqlcolumns)
    columnquery = f'select STRING_AGG(column_name,\',\') from information_schema.columns where table_schema = \'stage\' ' \
                      f'and column_name not in (\'{sqlcolumns}\') and table_name = \'{table_name}\' group by table_name'
    cursor.execute(columnquery)
    print (columnquery)
    columns = str(cursor.fetchone()[0]).strip('\'')
    if isinstance(idcolumns,list):
        query = f'insert into core.s_{table_name if core_table is None else core_table}' \
                f'({idcolumns[-1]},{columns},source_system,valid_from_dttm,valid_to_dttm,processed_dttm) ' \
                f'(select distinct hub1.{idcolumns[-1]},{columns},{source_id},CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP from stage.{table_name}' \
            f' join core.h_{table_name} hub1 on hub1.{idcolumns[1]} = stage.{table_name}.{idcolumns[0]})' \
                f' ON CONFLICT DO NOTHING'
        print (query)
    else:
        stage_columns = idcolumns['stage']
        core_columns = idcolumns['core']
        link_column = idcolumns['link']
        hub_columns = idcolumns['hub_rk']
        query = f'insert into core.s_{table_name if core_table is None else core_table}' \
                f'({link_column},{columns},source_system,valid_from_dttm,valid_to_dttm,processed_dttm)' \
                f' (select distinct link.{link_column},{columns},{source_id},CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP from stage.{table_name} as ssat' \
                f' join core.{hubs[0]} hub1 on hub1.{core_columns[0]} = ssat.{stage_columns[0]}' \
                f' join core.{hubs[1]} hub2 on hub2.{core_columns[1]} = ssat.{stage_columns[1]}'\
                f' join core.{core_table} link on link.{hub_columns[0]} = hub1.{hub_columns[0]} and link.{hub_columns[1]}=hub2.{hub_columns[1]})' \
                f' ON CONFLICT DO NOTHING'
        print(query)
    cursor.execute(query)
    conn.commit()


with DAG(
 dag_id="ETL_to_DataVault",
default_args=DEFAULT_ARGS,
 schedule_interval=None,
) as dag:

    load_schema = PythonOperator(
        task_id='copy_schema',
        python_callable=copy_schema,
        dag = dag
        )
    # copy_constraints = PythonOperator(
    #     task_id='copy_constraints',
    #     python_callable=copy_constraints,
    #     dag=dag
    # )
    extract_data = PythonOperator(
        task_id='export_data_from_tables',
        python_callable=export_data_from_tables,
        dag = dag
        )
    load_data = PythonOperator(
        task_id='import_data_from_tables',
        python_callable=import_data_from_tables,
        dag = dag
        )
    transform_data = PythonOperator(
        task_id='transmit_data_to_Vault',
        python_callable=transmit_data_to_Vault,
        dag=dag
    )
    load_schema >> extract_data >> load_data >> transform_data