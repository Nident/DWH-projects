from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.generic_transfer import GenericTransfer
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label
import airflow.utils.dates
import psycopg2
import psycopg2.extras as extras
import numpy as np
import pandas as pd
import datetime


conn_pg_params = {
    'host': '10.4.49.51',
    'database': 'student69_latyshev_av',
    'user': 'airflow',
    'password': 'airflow',
}

with DAG(
        'st69_case_3',
        default_args={
            'depends_on_past': False,
            # 'email': ['developer@yandex.ru'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 0,
            'retry_delay': datetime.timedelta(minutes=5),
            # 'queue': 'bash_queue',
            # 'pool': 'backfill',
            # 'priority_weight': 10,
            # 'end_date': datetime(2016, 1, 1),
            # 'wait_for_downstream': False,
            # 'sla': timedelta(hours=2),
            # 'execution_timeout': timedelta(seconds=300),
            # 'on_failure_callback': some_function,
            # 'on_success_callback': some_other_function,
            # 'on_retry_callback': another_function,
            # 'sla_miss_callback': yet_another_function,
            # 'trigger_rule': 'all_success'
        },
        description='',
        schedule_interval='*/10 * * * *',
        start_date=datetime.datetime(2023, 5, 5),
        catchup=False,
        max_active_runs=1,
        tags=['case_3'],
) as dag_generate_data:

    task_0_dummy = DummyOperator(task_id='task_0_dummy')

    def connect(params_dic):
        # NOTE подключение к серверу
        conn = None
        try:
            conn = psycopg2.connect(**params_dic)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            exit(1)
        return conn


    def generate_new_tasks(**kwargs):
        conn_pg = connect(conn_pg_params)
        conn_pg.cursor().execute('select * from oltp_src_system.create_tasks();')
        conn_pg.commit()
        conn_pg.close()


    task_1_generate_new_tasks = PythonOperator(
        task_id='task_1_generate_new_tasks',
        python_callable=generate_new_tasks,
        op_kwargs={},
    )


    def update_exists_tasks(**kwargs):
        conn_pg = connect(conn_pg_params)
        conn_pg.cursor().execute('select * from oltp_src_system.update_existed_task();')
        conn_pg.commit()
        conn_pg.close()


    task_2_update_exists_tasks = PythonOperator(
        task_id='task_2_update_exists_tasks',
        python_callable=update_exists_tasks,
        op_kwargs={},
    )


    def delete_random_task(**kwargs):
        conn_pg = connect(conn_pg_params)
        conn_pg.cursor().execute('select * from oltp_src_system.deleted_existed_task();')
        conn_pg.commit()
        conn_pg.close()


    task_3_delete_random_task = PythonOperator(
        task_id='task_3_delete_random_task',
        python_callable=delete_random_task,
        op_kwargs={},
    )


    # @task_group()
    # def gr_load_dag_code():
    with TaskGroup(group_id="gr_load_dag") as gr_load_dag:

        # RUN PostgreSQL operator
        load_to_stage_from_oltp_cdc_src_system = PostgresOperator(
            task_id="load_to_stage_from_oltp_cdc_src_system"
            , sql="""select * from dwh_stage.load_stage_from_oltp_cdc_src_system();"""
            , postgres_conn_id='st69_t_case_3_src_conn'
        )

        load_to_cdc_from_stage = PostgresOperator(
            task_id='load_to_cdc_from_stage'
            , sql="""select * from cdc_from_stage_to_ods.load_stage_account_data_cdc();"""
            , postgres_conn_id='st69_t_case_3_src_conn'
        )

        load_to_ods_from_cdc = PostgresOperator(
            task_id="load_to_ods_from_cdc"
            , sql="""select * from dwh_ods.load_to_dwh_stage_account_data_hist();"""
            , postgres_conn_id='st69_t_case_3_src_conn'
        )

        load_to_dim_date_from_ods = PostgresOperator(
            task_id="load_to_dim_date_from_ods"
            , sql="""select * from dwh_ods.dim_date_uploading();"""
            , postgres_conn_id='st69_t_case_3_src_conn'
        )

        load_to_stage_from_oltp_cdc_src_system >> Label("CDC") >> load_to_cdc_from_stage >> Label(
            "Increment") >> load_to_ods_from_cdc >> load_to_dim_date_from_ods


    def load_to_recent_changes(**kwargs):
        conn_pg = connect(conn_pg_params)
        conn_pg.cursor().execute('select report.load_account_data_recent_changes();')
        conn_pg.commit()
        conn_pg.close()


    recent_changes = PythonOperator(
        task_id='recent_changes',
        python_callable=load_to_recent_changes,
        op_kwargs={},
    )


    def load_to_day_dependence(**kwargs):
        conn_pg = connect(conn_pg_params)
        conn_pg.cursor().execute('select report.load_day_dependence();')
        conn_pg.commit()
        conn_pg.close()


    day_dependence = PythonOperator(
        task_id='day_dependence',
        python_callable=load_to_day_dependence,
        op_kwargs={},
    )


    def load_to_dependencies(**kwargs):
        conn_pg = connect(conn_pg_params)
        conn_pg.cursor().execute('select report.load_dependencies();')
        conn_pg.commit()
        conn_pg.close()


    dependencies = PythonOperator(
        task_id='dependencies',
        python_callable=load_to_dependencies,
        op_kwargs={},
    )


    def load_to_full_opeartions_report(**kwargs):
        conn_pg = connect(conn_pg_params)
        conn_pg.cursor().execute('select report.load_full_opeartions_report();')
        conn_pg.commit()
        conn_pg.close()


    full_opeartions_report = PythonOperator(
        task_id='full_opeartions_report',
        python_callable=load_to_full_opeartions_report,
        op_kwargs={},
    )
    task_0_dummy >> [task_1_generate_new_tasks, task_2_update_exists_tasks] >> task_3_delete_random_task\
        >> gr_load_dag >> recent_changes >> day_dependence >> dependencies >> full_opeartions_report