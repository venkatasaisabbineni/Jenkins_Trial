pipeline {
    agent any // This specifies that the job can run on any available agent.

    environment {
        // AWS_ACCESS_KEY_ID = credentials('aws_access_key_id') // Accessing stored AWS credentials.
        // AWS_SECRET_ACCESS_KEY = credentials('aws_secret_access_key')
        SPARK_HOME="/Users/venkatasaisabbineni/Work/Spark"
        PATH="$PATH:$SPARK_HOME/bin"
        // PYTHONPATH=$PYTHONPATH:/Users/venkatasaisabbineni/Work/Jenkins_Trial
    }

    stages {
        // stage('Clone Repository') {
        //     steps {
        //         git 'https://github.com/venkatasaisabbineni/Jenkins_Trial.git' // Cloning the repository to the agent's workspace.
        //     }
        // }
        stage('Setup Python Environment') {
            steps {
                sh '''
                    python3 -m venv venv
                    . venv/bin/activate
                    pip install -r requirements.txt
                '''
            }
        }
        stage('Run Spark Job') {
            steps {
                sh '''
                    . venv/bin/activate
                    spark-submit etl/drivers.py
                    ''' // Running the Spark job on the agent.
            }
        }
        stage('Upload to S3') {
            steps {
                withAWS(credentials: 'aws_creds', region: 'us-east-1') {
                    script {
                        def output_path = '/Users/venkatasaisabbineni/Work/Jenkins_Trial/data' // Define your output path.
                        def s3_bucket = 'jenkinstrialf1data' // Define your S3 bucket name.
                        sh """
                            . venv/bin/activate
                            aws s3 cp ${output_path} s3://${s3_bucket} --recursive
                        """
                    }
                }
            }
        }
    }
    post {
        always {
            cleanWs() // Cleaning up the workspace after the job.
        }
    }
}
