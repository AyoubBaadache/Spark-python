import subprocess


def submit_spark_job_via_spark_submit(master_url, jars, app_script_path):
    submit_command = [
        'docker', 'exec', 'spark-master', 'bin/spark-submit',
        '--master', master_url,
        '--jars', jars,
        app_script_path
    ]
    subprocess.run(submit_command, shell=True, check=True)


if __name__ == "__main__":
    master_url = 'spark://spark-master:7077'
    jars = ('/opt/bitnami/spark/resource/mongo-spark-connector_2.12-3.0.1.jar,'
            '/opt/bitnami/spark/resource/mongo-java-driver-3.12.8.jar')
    app_script_path = '/opt/bitnami/spark/jobs/My_Job.py'

    submit_spark_job_via_spark_submit(master_url, jars, app_script_path)
