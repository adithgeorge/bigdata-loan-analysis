
#!/bin/bash

# Running Spark Jobs accessing S3 File Storage

spark-submit --deploy-mode cluster --master spark://localhost:7077 --num-executors 2
--executor-cores 2 --executor-memory 1g --conf spark.yarn.submit.waitAppCompletion=false
wordcount.py s3n://lti858/Rishu/patient.txt s3n://lti858/Rishu/hive_ext/

spark-submit --deploy-mode cluster --master yarn --num-executors 2 --executor-cores 2
--executor-memory 1g --conf spark.yarn.submit.waitAppCompletion=false wordcount.py
s3n://lti858/Rishu/patient.txt s3n://lti858/Rishu/hive_ext/