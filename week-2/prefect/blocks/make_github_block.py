
from prefect.filesystems import GitHub

gh = GitHub(
    repository="https://github.com/dubuisa/DE-zoomcamp", reference="main"
)
gh.save("zoomcamp", overwrite=True)
