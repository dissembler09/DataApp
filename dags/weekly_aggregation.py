from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession

def aggregate_data(execution_date):
    spark = SparkSession.builder.appName("UserActionAggregation").getOrCreate()

    start_date = (execution_date - timedelta(days=7)).strftime('%Y-%m-%d')
    end_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')

    input_path = "/opt/airflow/data/input/"
    output_path = f"/opt/airflow/output/{execution_date.strftime('%Y-%m-%d')}.csv"


    df = spark.read.csv(f"{input_path}*.csv", header=True)

    df_filtered = df.filter((df.dt >= start_date) & (df.dt <= end_date))

    agg_df = df_filtered.groupBy("email").agg(
        {"action": "count"}
    ).withColumnRenamed("count(action)", "total_actions")

    for action in ["CREATE", "READ", "UPDATE", "DELETE"]:
        action_count = df_filtered.filter(df_filtered.action == action).groupBy("email").count()
        agg_df = agg_df.join(action_count.withColumnRenamed("count", f"{action.lower()}_count"), "email", "left")

    agg_df.write.csv(output_path, header=True)

    spark.stop()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weekly_aggregation',
    default_args=default_args,
    description='Weekly user action aggregation',
    schedule='0 7 * * *',
    catchup=False,
) as dag:

    task_aggregate_data = PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_data,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    task_aggregate_data
