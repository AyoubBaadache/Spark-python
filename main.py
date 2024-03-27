import subprocess


def package_and_submit():
    # Step 1: Package your application using setup.py
    package_command = ['python', 'setup.py', 'sdist']
    subprocess.run(package_command, check=True)

    # Step 2: Submit your application using spark-submit
    submit_command = [
        'docker exec -it spark-master bin/spark-submit',
        '--master', 'spark://spark-master:7077',
        '--py-files', 'dist\\My_Job-0.1.tar.gz',  # Assuming this is the generated package
    ]
    subprocess.run(submit_command, check=True)


if __name__ == "__main__":
    package_and_submit()
