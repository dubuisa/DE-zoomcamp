
# Introduction

This repository contains instructions and scripts to create a data lake on Google Cloud Storage and a Data Warehouse using Google BigQuery using Terraform.
Prerequisites

- You should have a Google Cloud Platform account and a project created.
- You should have the gcloud command-line tool installed and configured with your project.
- You should have Terraform installed on your machine.
- You should have a service account credentials JSON file to authenticate with GCP.

# Instructions

1. Clone this repository to your local machine.
2. In the terminal, navigate to the root directory of the repository.
3. Refresh the service-account's auth-token for this session by running the command gcloud auth application-default login
4. Run the command terraform init to initialize the state file (.tfstate)
5. Check the changes to the new infrastructure plan by running the command
   
```shell
terraform plan -var="project=<your-gcp-project-id>"
```
6. Create the new infrastructure by running the command 

```shell
   terraform apply -var="project=<your-gcp-project-id>"
```
7. After you finish working with the data lake and data warehouse, make sure to delete the infrastructure to avoid costs on any running services by running the command terraform destroy

# Script Parameters

    project: The ID of the GCP project where the data lake and data warehouse will be created.

# Conclusion

With these instructions and scripts, you should be able to create a data lake on Google Cloud Storage and a Data Warehouse using Google BigQuery using Terraform. If you have any questions or issues, feel free to open a GitHub issue and I'll try my best to help you out.

# Note

- Make sure to keep your service-account credentials JSON file in a secure location.
- Make sure to add the correct permissions to your service account to interact with the GCP resources.