## Financial ML
DAG for Generating Finance Data, Preprocessing, and Using ML Techniques for Prediction

This DAG performs three main steps:

Generate data and store it in an Azure Storage Account.
Detect new data, download it to PostgreSQL, clean it, and prepare it for machine learning.
Extract features, train models, and plot the model results.
During implementation, we use various operators and hooks, including: BlobServiceClient, WasbHook, WasbPrefixSensor, LocalFilesystemToWasbOperator, PostgresOperator, and PostgresHook.

Link to DAGs: https://github.com/amyard/DE/tree/master/financial-ml

![alt text](https://github.com/amyard/DE/blob/master/pipeline_finance.png?raw=true)