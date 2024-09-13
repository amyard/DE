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




`airflow connections export connections.json` - export from airflow to local
