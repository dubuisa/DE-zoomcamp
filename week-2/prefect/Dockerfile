from prefecthq/prefect:2.7.9-python3.9


COPY docker_requirements.txt . 

RUN pip install -r docker_requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect/flows