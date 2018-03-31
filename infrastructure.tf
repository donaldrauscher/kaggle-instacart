variable "project" {}

variable "region" {
  default = "us-central1"
}

variable "zone" {
  default = "us-central1-f"
}

provider "google" {
  version = "~> 1.8"
  project = "${var.project}"
  region = "${var.region}"
}

resource "google_storage_bucket" "instacart-data" {
  name = "instacart-data"
  storage_class = "REGIONAL"
  location = "${var.region}"
}

resource "google_storage_bucket" "instacart-dataproc-staging" {
  name = "instacart-dataproc-staging"
  storage_class = "REGIONAL"
  location = "${var.region}"
}

resource "google_dataproc_cluster" "instacart-dataproc" {
  name = "instacart-dataproc"
  region = "${var.region}"

  cluster_config {
    staging_bucket = "instacart-dataproc-staging"

    master_config {
      num_instances = 1
      machine_type = "n1-standard-1"
      disk_config {
        boot_disk_size_gb = 10
      }
    }

    worker_config {
      num_instances = 2
      machine_type = "n1-standard-1"
      disk_config {
        boot_disk_size_gb = 10
        num_local_ssds = 1
      }
    }

    # TODO: figure out how to take in multiple packages; ':' separator doesn't work!
    gce_cluster_config {
      zone = "${var.zone}"

			metadata {
        JUPYTER_CONDA_CHANNELS = "conda-forge"
				JUPYTER_CONDA_PACKAGES = "pandas"
        JUPYTER_PORT = 8123
			}
    }

    initialization_action {
      script = "gs://dataproc-initialization-actions/jupyter/jupyter.sh"
      timeout_sec = 600
    }

  }
}

resource "google_bigquery_dataset" "instacart" {
  dataset_id = "instacart"
}
