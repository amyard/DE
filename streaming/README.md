
### check if we have access to cassandra db
`docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042`

`describe spark_streams.created_users`  
`select * from spark_streams.created_users`

### airflow 
`http://localhost:8080`
### kafka
`http://localhost:9021`
### spark
`http://localhost:9090/`

`spark-submit --master spark://localhost:7077 spark_stream.py
`