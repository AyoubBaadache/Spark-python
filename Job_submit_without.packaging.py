import subprocess


def submit_spark_job_via_spark_submit():
    # Submit the Spark job using spark-submit command
    submit_command = [
        'docker', 'exec', 'spark-master', 'bin/spark-submit',
        '--master', 'spark://spark-master:7077',
        '--jars', '/opt/bitnami/spark/resource/mongo-spark-connector_2.12-3.0.1.jar,'
                  '/opt/bitnami/spark/resource/mongo-java-driver-3.12.8.jar',
        '/opt/bitnami/spark/jobs/Mongo_Job.py'  # Specify the path to your Spark application script
    ]
    subprocess.run(submit_command, shell=True, check=True)


if __name__ == "__main__":
    # Or submit Spark job using spark-submit
    submit_spark_job_via_spark_submit()
