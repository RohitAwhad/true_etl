from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator


from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


with DAG("true_data_pipeline", start_date=datetime(2022, 3 ,24), 
    schedule_interval="@daily", default_args=default_args, catchup=False) as dag:


    is_event_file_available = FileSensor(
        task_id="is_event_file_available",
        filepath="/opt/airflow/dags/files/",
        poke_interval=10,
        timeout=60
    )
    
    saving_events = BashOperator(
        task_id="saving_events",
        bash_command="""
            hdfs dfs -mkdir -p /events && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/* /events
        """
    )
    
    creating_events_table = HiveOperator(
        task_id="creating_events_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS AGGREGATED_EVENTS(
            settings MAP<STRING,STRING>
            )
            PARTITIONED BY(id BIGINT)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
			location '/user/hive_events'
        """
    )
    
    events_processing = SparkSubmitOperator(
        task_id="events_processing",
        application="/opt/airflow/dags/trueetl_2.11-0.1.jar",
        java_class="com.demo.unifiedLoader",
        name="events_processing",
        application_args=["true","aggregated_events","/events/*","\t"],
        conn_id="spark_conn",
        verbose=False
    )
    
    archive_processed_files = BashOperator(
        task_id="archive_processed_files",
        bash_command="""
            hdfs dfs -mkdir -p /archived_events && \
            hdfs dfs -mv  /events/* /archived_events && \
            rm -rf $AIRFLOW_HOME/dags/files/*
        """
    )

    is_event_file_available >> saving_events >> creating_events_table >> events_processing >> archive_processed_files