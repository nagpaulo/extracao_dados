import sys
sys.path.append("airflow_pipeline")

from airflow.models import DAG 
from datetime import datetime, timedelta
from operators.twitter_operator import TwitterOperator
from os.path import join
from airflow.utils.dates import days_ago

with DAG(dag_id = "TwitterDAG", start_date=days_ago(30), schedule_interval="@daily") as dag:
    query = "datascience"
    to = TwitterOperator(file_path=join("datalake/twitter_datascience",
                                        "extract_date={{ ds }}",
                                        "datascience_{{ ds_nodash }}.json"), 
                            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
                            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
                            query=query, 
                            task_id="twitter_datascience")