variable "credentials" {
  description = "My Credentials"
  default     = "./../credentials.json"
}

variable "project" {
  description = "Project"
  default     = "capstone-proj-464212"
}

variable "region" {
  description = "Region"
  default     = "us-west1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name_1" {
  description = "My BigQuery Staging Dataset Name"
  default     = "stg_data"
}

variable "bq_dataset_name_2" {
  description = "My BigQuery Processed Dataset Name"
  default     = "us_blue_chips_fact"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "us_blue_chips_buckets"
}