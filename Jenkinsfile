pipeline {
    agent any
    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }
        stage('Restore') {
            steps {
                bat 'dotnet restore DistributedSystem/DistributedSystem.sln'
            }
        }
        stage('Build') {
            steps {
                bat 'dotnet build DistributedSystem/DistributedSystem.sln --configuration Release --no-restore'
            }
        }
        stage('Test') {
            steps {
                bat 'dotnet test DistributedSystem/Tests/Tests.csproj --configuration Release --no-restore --logger "trx"'
            }
        }
    }
}
