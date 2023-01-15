# Introduction

This repository contains a script and instructions for collecting New York taxi data using Docker and storing it in a database. The script is written in Python, build and use Docker image to run the data collection and storage process.

# Prerequisites

Docker and Docker Compose must be installed on your system.
The taxi_ingest image must be built before running the script.

#  Instructions

1. Clone this repository to your local machine.
In the terminal, navigate to the root directory of the repository.
2. Run the command `docker-compose up` to start the database.
3. Build the taxi_ingest image by running the command 
   
   `docker build -t taxi_ingest:v001`

4. Run the script by using the command
   
   ```docker run -it --network=host taxi_ingest:v001 --table_name=trips --host=127.0.0.1 --db=taxi-data --user=root --password=root```
   
   The script will start running and will collect and store the New York taxi data in the specified database.

5. Additionaly, you can log to the pgAdmin user interface with your web browser here `localhost:8080` with the user `admin@admin.com` and password `admin`.

#  Script Parameters

- `table_name`: The name of the table in the database where the data will be stored.
- `host`: The hostname or IP address of the database.
- `db`: The name of the database.
- `user`: The username to connect to the database.
- `password`: The password to connect to the database.

# Note

Make sure to stop the running container after finish using the command `docker-compose down`

# Conclusion

With this script and instructions, you should be able to collect New York taxi data using Docker and store it in a database. 