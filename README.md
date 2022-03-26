# Creating data pipeline using Airflow and docker <br>

### Problem Statement <br>

1) Daily ETL pipeline that read data , apply some transformation using spark and produce partitioned table in Hive.
2) ETL pipeline should run daily at midnight
3) ETL pipeline should have sensor to check for data availability.
4) ETL pipeline should run the scala Spark job on sensor success.

### Development Setup <br>
1) Clone or download zip 
2) Run start.sh file or run commands manually from true_etl folder to create and start docker container (Note : 8GB should be allocated to docker and it take 15-20 mins to start container for the first time)
3) Check container status using *docker ps* . All container should be in healthy state.

### Port Details for accessing docker container ui
1) Airflow :: localhost:8080 :: ui user=airflow :: ui password=airflow
2) Hue :: localhost:32762 :: user=root :: pass=root
3) Spark :: localhost:32766
4) HDFS :: localhost:32763

### Prerequisite before running data pipeline
Setup connection in airflow UI for spark and HIVE under Admin > Connections <br>
Spark :
1) Conn Id : spark_conn
2) Conn Type : spark
3) Host : spark://spark-master
4) Port : 7077

HIVE :
1) Conn Id : hive_conn
2) Conn Type : HIVE Server 2 Thrift
3) Host : hive-server
4) Port : 10000
5) Login : hive
6) Password : hive

### Data pipeline Details 
Data pipeline consists of 5 steps : <br>
1) is_event_file_available : Check if files is present in particular folder to process ie *true_etl\mnt\airflow\dags\files*
2) saving_events : If files are present move to those files to HDFS location */events*
3) creating_events_table : Create HIVE table if it does not exists .
4) events_processing : processing data present in HDFS location */events* using spark job.
5) archive_processed_files : archive processed files from */events* to *archived_events* . Also delete processed files from *true_etl\mnt\airflow\dags\files*

Order in which data pipeline runs

is_event_file_available >> saving_events >> creating_events_table >> events_processing >> archive_processed_files

### Triggering pipeline
1) Since it is scheduled to run daily at midnight, we need to manually trigger it from UI.<br>
      If pipeline runs successfully output can be verified through Hue with below steps.
      1) In browser type localhost:32762
      2) In Hive query window type *select * from AGGREGATED_EVENTS*
      3) Table partioned can also be verified from hdfs location */user/hive_events* 
2) To test each step individually follow below steps <br>
   trigger below docker command to access airflow container <br>
   *docker exec -it docker_id /bin/bash* <br>
   Note : docker_id can be obtained from *docker ps* . copy container ID corresponding to *true_etl_airflow* <br>
   Run below command one after other to test each step in pipeline
   1) airflow tasks test true_data_pipeline is_event_file_available 2022-03-23
   2) airflow tasks test true_data_pipeline saving_events 2022-03-23
   3) airflow tasks test true_data_pipeline creating_events_table 2022-03-23
   4) airflow tasks test true_data_pipeline events_processing 2022-03-23
   5) airflow tasks test true_data_pipeline archive_processed_files 2022-03-23



