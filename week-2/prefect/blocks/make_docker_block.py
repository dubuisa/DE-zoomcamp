from prefect.infrastructure.docker import DockerContainer

docker_block = DockerContainer(
    image='dubuisa/prefect:course_etl',
    image_pull_policy="ALWAYS",
    auto_remove=True
)

docker_block.save("docker-course-etl", overwrite=True)