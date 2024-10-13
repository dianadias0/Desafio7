from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import pandas as pd
import sqlite3

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Função para ler os dados da tabela 'Order' e exportar para CSV
def read_orders():
    conn = sqlite3.connect('data/Northwind_small.sqlite')
    query = "SELECT * FROM [Order]"
    df = pd.read_sql_query(query, conn)
    conn.close()
    df.to_csv('output_orders.csv', index=False)

# Função para ler os dados da tabela 'OrderDetail', fazer o JOIN e calcular a soma
def read_order_details_and_calculate():
    conn = sqlite3.connect('data/Northwind_small.sqlite')
    order_detail_query = "SELECT * FROM [OrderDetail]"
    order_detail_df = pd.read_sql_query(order_detail_query, conn)

    orders_df = pd.read_csv('output_orders.csv')

    print("Colunas em order_detail_df:", order_detail_df.columns.tolist())
    print("Colunas em orders_df:", orders_df.columns.tolist())

    merged_df = pd.merge(order_detail_df, orders_df, left_on='OrderId', right_on='Id')
    total_quantity = merged_df[merged_df['ShipCity'] == 'Rio de Janeiro']['Quantity'].sum()

    # Exporta o resultado para 'count.txt'
    with open('count.txt', 'w') as f:
        f.write(str(total_quantity))

    conn.close()

## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##

with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.     
    """

   # Definindo as tarefas
    read_orders_task = PythonOperator(
        task_id='read_orders',
        python_callable=read_orders
    )

    read_order_details_task = PythonOperator(
        task_id='read_order_details',
        python_callable=read_order_details_and_calculate
    )
   
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )

    # Definindo a ordem das tarefas
    read_orders_task >> read_order_details_task >> export_final_output