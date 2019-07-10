from builtins import range
from datetime import datetime, timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

# Params DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 9),
    'email': ['bigdata@pedidosya.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 15,
    'retry_delay': timedelta(minutes=60)
}

# Funciones
def print_ok_sessions():
    print('Sessions OK!')

# Set variables
try:
    from_interval = int(Variable.get('execution_from_interval'))
    to_interval = int(Variable.get('execution_to_interval'))
except:
    from_interval = -1
    to_interval = -1

yesterday_date = datetime.strftime(datetime.now() - timedelta(to_interval*-0), "%Y%m%d")    

table_name_android = '`bpy---pedidosya.110190327.ga_sessions_' + yesterday_date + '`'
table_name_ios = '`bpy---pedidosya.110191523.ga_sessions_' + yesterday_date + '`'    

with DAG('PRIMER_DAG', schedule_interval='00 12 * * *', catchup=False, default_args=default_args) as dag:
    #Check
    check_ga_sessions_all_platfroms_exist = BigQueryOperator(
        task_id = 'check_ga_sessions_all_platfroms_exist',
        sql = """
        SELECT 1 FROM {{ params.table_name_android }}
        UNION ALL
        SELECT 1 FROM {{ params.table_name_ios }}
        """,
        use_legacy_sql = False,
        bigquery_conn_id='peya_bigquery',
        params={
            'table_name_android': table_name_android,
            'table_name_ios': table_name_ios
        }
    )

    print_sessions_ok = PythonOperator(
        task_id = 'print_sessions_ok',
        python_callable = print_ok_sessions
    ) 

    check_ga_sessions_all_platfroms_exist >> print_sessions_ok