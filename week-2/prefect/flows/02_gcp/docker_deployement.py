from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from etl_web_to_gcs import etl_web_to_gcs_parent_flow
from etl_gcs_to_bq import gcs_to_bq_parent_flow


docker_block = DockerContainer.load("docker-course-etl")

docker_dep1 = Deployment.build_from_flow(etl_web_to_gcs_parent_flow, name='flow')

docker_dep2 = Deployment.build_from_flow(gcs_to_bq_parent_flow, name='flow')

if __name__ == '__main__':
    docker_dep1.apply()
    docker_dep2.apply()