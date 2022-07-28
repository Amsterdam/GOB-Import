#!groovy

def tryStep(String message, Closure block, Closure tearDown = null) {
    try {
        block()
    }
    catch (Throwable t) {
        slackSend message: "${env.JOB_NAME}: ${message} failure ${env.BUILD_URL}", channel: '#ci-channel', color: 'danger'

        throw t
    }
    finally {
        if (tearDown) {
            tearDown()
        }
    }
}


node('GOBBUILD') {
    withEnv([
        "DOCKER_IMAGE_NAME=datapunt/gob_import:${env.BUILD_NUMBER}",
        "BENK_ACR_ONTW=benkweuacrofkl2hn5eivwy.azurecr.io",
        "BENK_ACR_DOCKER_IMAGE_NAME=gob_import:${env.BUILD_NUMBER}",
        ]) {

        stage("Test Connection") {
            sh "nslookup ${BENK_ACR_ONTW}; nc -w 5 -zv ${BENK_ACR_ONTW} 443"
        }

        stage("Checkout") {
            checkout scm
        }

        stage('Test') {
            tryStep "test", {

                sh "docker-compose -p gob_import_client -f src/.jenkins/test/docker-compose.yml build --no-cache && " +
                   "docker-compose -p gob_import_client -f src/.jenkins/test/docker-compose.yml run -u root --rm test"

            }, {
                sh "docker-compose -p gob_import_client -f src/.jenkins/test/docker-compose.yml down"
            }
        }

        stage("Build image") {
            tryStep "build", {
                docker.withRegistry("${DOCKER_REGISTRY_HOST}",'docker_registry_auth') {
                    def image = docker.build("${DOCKER_IMAGE_NAME}",
                        "--no-cache " +
                        "--shm-size 1G " +
                        "--build-arg BUILD_ENV=acc " +
                        "--target application " +
                        "src")
                    image.push()
                }
            }
        }

        String BRANCH = "${env.BRANCH_NAME}"
        if (BRANCH == "49006-push-to-acr") {
            stage("Push develop image to ACR") {
                tryStep "image tagging", {
                    // Create credentials for the ACR:
                    // az acr token create --registry=<registry_name> --name=jenkins-build-token --scope-map=_repositories_push
                    withCredentials([usernamePassword(credentialsId: 'BENK_ONTW_ACR_JENKINS_2', usernameVariable: 'ACR_USERNAME', passwordVariable: 'ACR_TOKEN')]) {
                        echo "Push image to ${ACR_USERNAME}@${BENK_ACR_ONTW}"
                        docker.withRegistry("https://${BENK_ACR_ONTW}", 'BENK_ONTW_ACR_JENKINS_2') {
                            def image = docker.image("${BENK_ACR_DOCKER_IMAGE_NAME}")
                            image.push("develop")
                            image.push("test")
                        }
                    }
                }
            }
        }

        if (BRANCH == "develop") {

            stage('Push develop image') {
                tryStep "image tagging", {
                    docker.withRegistry("${DOCKER_REGISTRY_HOST}",'docker_registry_auth') {
                        def image = docker.image("${DOCKER_IMAGE_NAME}")
                        image.pull()
                        image.push("develop")
                        image.push("test")
                    }
                }
            }

            stage("Deploy to TEST") {
                tryStep "deployment", {
                    build job: 'Subtask_Openstack_Playbook',
                        parameters: [
                            [$class: 'StringParameterValue', name: 'INVENTORY', value: 'test'],
                            [$class: 'StringParameterValue', name: 'PLAYBOOK', value: 'deploy.yml'],
                            [$class: 'StringParameterValue', name: 'PLAYBOOKPARAMS', value: "-e cmdb_id=app_gob-import"],
                        ]
                }
            }

        }

        if (BRANCH == "master") {

            stage('Push acceptance image') {
                tryStep "image tagging", {
                    docker.withRegistry("${DOCKER_REGISTRY_HOST}",'docker_registry_auth') {
                        def image = docker.image("${DOCKER_IMAGE_NAME}")
                        image.pull()
                        image.push("acceptance")
                    }
                }
            }

            stage("Deploy to ACC") {
                tryStep "deployment", {
                    build job: 'Subtask_Openstack_Playbook',
                        parameters: [
                            [$class: 'StringParameterValue', name: 'INVENTORY', value: 'acceptance'],
                            [$class: 'StringParameterValue', name: 'PLAYBOOK', value: 'deploy.yml'],
                            [$class: 'StringParameterValue', name: 'PLAYBOOKPARAMS', value: "-e cmdb_id=app_gob-import"],
                        ]
                }
            }
        }
    }
}
