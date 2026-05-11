terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~>4.0"
    }
  }
}

# Configure the GCP Provider
provider "google" {
  project = "ecommerce-de-project-495511"
  region  = "asia-south1"
  zone    = "asia-south1-a"
}

# ==========================================
# 1. DATA LAKE (GCS BUCKETS)
# ==========================================
resource "google_storage_bucket" "bronze_lake" {
  name                        = "olist-bronze-prod-lake"
  location                    = "asia-south1"
  force_destroy               = true
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "silver_lake" {
  name                        = "olist-silver-prod-lake"
  location                    = "asia-south1"
  force_destroy               = true
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "gold_lake" {
  name                        = "olist-gold-prod-lake"
  location                    = "asia-south1"
  force_destroy               = true
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "quarantine_lake" {
  name          = "olist-quarantine-prod-lake"
  location      = "asia-south1"
  force_destroy = true
  uniform_bucket_level_access = true
  
  # Auto-delete quarantined data after 90 days to save costs if no one fixes it
  lifecycle_rule {
    condition {
      age = 90 
    }
    action {
      type = "Delete"
    }
  }
}

# ==========================================
# ARTIFACTS (CODE & SCRIPTS)
# ==========================================
resource "google_storage_bucket" "artifacts_bucket" {
  name          = "olist-artifacts-prod-lake" # Must be globally unique!
  location      = "asia-south1"
  force_destroy = true
  uniform_bucket_level_access = true
  
  # Automatically clean up old code versions if versioning is enabled later
  lifecycle_rule {
    condition {
      num_newer_versions = 5
    }
    action {
      type = "Delete"
    }
  }
}

# ==========================================
# 2. DATA WAREHOUSE (BIGQUERY)
# ==========================================
resource "google_bigquery_dataset" "data_warehouse" {
  dataset_id                 = "olist_warehouse_prod"
  location                   = "asia-south1"
  delete_contents_on_destroy = true
}

# ==========================================
# 3. IDENTITY & SECURITY (IAM)
# ==========================================
# Create the dedicated pipeline service account
resource "google_service_account" "pipeline_sa" {
  account_id   = "dataproc-airflow-sa-prod"
  display_name = "Olist Production Pipeline SA"
}

# Define the exact roles needed for Dataproc, GCS, and BQ
locals {
  pipeline_roles = [
    "roles/dataproc.editor",
    "roles/dataproc.worker",
    "roles/storage.objectAdmin",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/iam.serviceAccountUser"
  ]
}

# Bind the roles to the Service Account
resource "google_project_iam_member" "sa_role_bindings" {
  for_each = toset(local.pipeline_roles)

  project = "ecommerce-de-project-495511"
  role    = each.key
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# ==========================================
# 4. NETWORKING
# ==========================================
resource "google_compute_network" "airflow_network" {
  name                    = "airflow-vpc-prod"
  auto_create_subnetworks = true
}

resource "google_compute_firewall" "allow_airflow_ui" {
  name    = "allow-airflow-8080-prod"
  network = google_compute_network.airflow_network.name

  allow {
    protocol = "tcp"
    ports    = ["8080", "22"]
  }
  source_ranges = ["0.0.0.0/0"]
}

# ==========================================
# 5. ORCHESTRATOR SERVER (AIRFLOW VM)
# ==========================================
resource "google_compute_instance" "airflow_vm" {
  name         = "airflow-orchestrator-prod"
  machine_type = "e2-medium"

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = 30
    }
  }

  network_interface {
    network = google_compute_network.airflow_network.name
    access_config {
      # Grants Public IP
    }
  }

  # Attach the newly created Service Account to the VM
  service_account {
    email  = google_service_account.pipeline_sa.email
    scopes = ["cloud-platform"]
  }

  # Automatically install Docker and Docker Compose on boot
  metadata_startup_script = <<-EOF
    #!/bin/bash
    sudo apt-get update
    sudo apt-get install -y apt-transport-https ca-certificates curl software-properties-common
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
    sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
    sudo apt-get update
    sudo apt-get install -y docker-ce docker-compose
    sudo usermod -aG docker ubuntu
  EOF

  # Ensure the SA is created before the VM tries to use it
  depends_on = [google_service_account.pipeline_sa]
}

# ==========================================
# 6. OUTPUTS (Prints to your terminal)
# ==========================================
output "airflow_ui_url" {
  value       = "http://${google_compute_instance.airflow_vm.network_interface.0.access_config.0.nat_ip}:8080"
  description = "Access your Airflow Webserver here."
}

output "dataproc_service_account_email" {
  value       = google_service_account.pipeline_sa.email
  description = "COPY THIS into your Airflow DAG gce_cluster_config block."
}