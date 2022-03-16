from datetime import datetime,timedelta , date 

from airflow import models,DAG 

from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator,DataProcPySparkOperator,DataprocClusterDeleteOperator

from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from airflow.operators import BashOperator 

from airflow.models import *

from airflow.utils.trigger_rule import TriggerRule


current_date = str(date.today())

BUCKET = "gs://<bucket_name>"

PROJECT_ID = "<PROJECT_ID>"

PYSPARK_JOB = BUCKET + "/spark-job/car_purchase.py"

DEFAULT_DAG_ARGS = {
    'owner':"airflow",
    'depends_on_past' : False,
    "start_date":datetime.utcnow(),
    "email_on_failure":False,
    "email_on_retry":False,
    "retries": 1,
    "retry_delay":timedelta(minutes=5),
    "project_id": "smart-paratext-336819",
    "scheduled_interval":"30 2 * * *"
}

with DAG("car_purchase_etl",default_args=DEFAULT_DAG_ARGS) as dag : 

    create_cluster = DataprocClusterCreateOperator(

        task_id ="create_dataproc_cluster",
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        master_machine_type="n1-standard-2",
        worker_machine_type="n1-standard-2",
        num_workers=2,
        region="us-east1",
        zone ="us-east1-b"
    )

    submit_pyspark = DataProcPySparkOperator(
        task_id = "run_pyspark_etl",
        main = PYSPARK_JOB,
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        region="us-east1"
    )

    bq_load_car_purchase = GoogleCloudStorageToBigQueryOperator(

        task_id = "bq_load_car_purchase_data",
        bucket= "<bucket_name>",
        source_objects=["car_purchase_clean_data_output/"+current_date+"_car_data/part-*"],
        destination_project_dataset_table="car_purchase_data.daily_car_purchase",
        autodetect = True,
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=0,
        write_disposition="WRITE_APPEND",
        max_bad_records=0
    )


    delete_cluster = DataprocClusterDeleteOperator(

        task_id ="delete_dataproc_cluster",
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        region="us-east1",
        trigger_rule = TriggerRule.ALL_DONE
    )

    delete_tranformed_files = BashOperator(
        task_id = "delete_tranformed_files",
        bash_command = "gsutil -m rm -r " +BUCKET + "/car_purchase_clean_data_output/*"
    )

    create_cluster.dag = dag

    create_cluster.set_downstream(submit_pyspark)

    submit_pyspark.set_downstream([bq_load_car_purchase,delete_cluster])

    delete_cluster.set_downstream(delete_tranformed_files)