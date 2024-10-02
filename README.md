## Micro-Batching: End-to-End Project (Airflow, Kafka, PySpark, Azure Storage, Docker)
This project includes four DAGs, each with specific tasks:
Generate data and push it to Kafka.
Retrieve data from Kafka and store it in PostgreSQL or Azure Storage as JSON, CSV, or Parquet.
Load data from PostgreSQL and perform cleaning using PySpark.
Conduct further data manipulation with PySpark.
Explore the DAGs, pyspark jobs and docker compose here: GitHub link.
![MicroBatching.png](images%2FMicroBatching.png)


## Financial ML
DAG for Generating Finance Data, Preprocessing, and Using ML Techniques for Prediction


This DAG performs three main steps:

Generate data and store it in an Azure Storage Account.
Detect new data, download it to PostgreSQL, clean it, and prepare it for machine learning.
Extract features, train models, and plot the model results.
During implementation, we use various operators and hooks, including: BlobServiceClient, WasbHook, WasbPrefixSensor, LocalFilesystemToWasbOperator, PostgresOperator, and PostgresHook.

Link to DAGs: https://github.com/amyard/DE/blob/master/DE_end-to-end/dags/finance_elt.py

![pipeline_finance.png](images%2Fpipeline_finance.png)

## Optimizing Sensor and Trigger Rule Usage: A Practical Guide
Check out our latest example demonstrating the effective use of SqlSensor and WasbPrefixSensor in Airflow. Dive into the details and see how to implement these sensors with trigger rules in your DAGs.

Explore the DAGs here: https://github.com/amyard/DE/blob/master/DE_end-to-end/dags/sensor_with_conditions.py

![SensorAndTriggerRules.png](images%2FSensorAndTriggerRules.png)


# DagDependency and BranchOperator
Explore the DAGs here: https://github.com/amyard/DE/blob/master/DE_end-to-end/dags/dag_dependency.py
![DagDependency1.png](images%2FDagDependency1.png)
![DagDependency2.png](images%2FDagDependency2.png)

# Using SparkSubmitOperator with Additional JAR Packages and Parameters
We're running a PySpark job enhanced with JAR packages for Azure and PostgreSQL integrations, while securely handling sensitive data.
Pro Tip: Make sure to allocate sufficient resources for the Spark master and worker in your Docker file, or you'll encounter errors related to insufficient resources for the worker.
Check out the DAG here: https://github.com/amyard/DE/blob/master/DE_end-to-end/dags/micro_batching_load_data.py
![pyspark.png](images%2Fpyspark.png)
![pyspark2.png](images%2Fpyspark2.png)


# Trigger a Task Once Per Day, Even with Frequent DAG Runs
To ensure a task runs only once per day, even if your DAG executes every 5 minutes, implement a helper function to verify if the task was successfully executed today. Add a branch task in your DAG to check the result of this function and conditionally execute the correct task based on the outcome.
Pro Tip: Be cautious with SQLAlchemy when using datetime conditions, as timezone issues can lead to exceptions.
Check out the DAG here: https://github.com/amyard/DE/blob/master/DE_end-to-end/dags/micro_batching_generate_data.py
![TriggerOnce1.png](images%2FTriggerOnce1.png)
![TriggerOnce3.png](images%2FTriggerOnce3.png)
![TriggerOnce2.png](images%2FTriggerOnce2.png)

`airflow connections export scripts/connection.json` - export from airflow to local
`airflow connections import scripts/connection.json` - import to airflow to local
