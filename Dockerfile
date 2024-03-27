FROM bitnami/spark:latest

COPY ./resource/mongo-spark-connector_2.12-3.0.1.jar /opt/bitnami/spark/resource/
COPY ./resource/mongo-java-driver-3.12.8.jar /opt/bitnami/spark/resource/