# FROM apache/spark:3.5.0

# USER root

# # Upgrade pip first (important!)
# RUN python3 -m pip install --upgrade pip

# # Install ML dependencies safely
# RUN pip install --no-cache-dir \
#     --trusted-host pypi.org \
#     --trusted-host files.pythonhosted.org \
#     numpy==1.23.5 \
#     pandas==2.0.3 \
#     scikit-learn==1.2.2 \
#     joblib

FROM apache/spark:3.5.0

USER root

RUN pip install scikit-learn pandas numpy joblib

# Add Kafka connector
RUN /opt/spark/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
--version 




# FROM apache/spark:3.5.0

# USER root

# # install curl (required for kafka jar download)
# RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# # upgrade pip
# RUN python3 -m pip install --upgrade pip

# # install ML dependencies
# RUN pip install --no-cache-dir \
#     numpy==1.23.5 \
#     pandas==2.0.3 \
#     scikit-learn==1.2.2 \
#     joblib

# # install Spark Kafka connector
# RUN curl -fSL \
# https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
# -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar    