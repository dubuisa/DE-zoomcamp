locals {
  data_lake_bucket = "data_lake"
}

variable "project" {
    description = "Your GCP project ID"
}

variable "region" {
  description = "Region for GCP resources"
  default = "europe-west6"
}

variable "storage_class" {
  description = "Storage class type for bucket"
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "trips_data_all"
}