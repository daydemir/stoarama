terraform {
  backend "s3" {
    bucket                      = "stoarama"
    key                         = "terraform/stoarama/youtube-relay-prod/terraform.tfstate"
    region                      = "us-east-1"
    endpoints                   = { s3 = "https://sfo3.digitaloceanspaces.com" }
    skip_credentials_validation = true
    skip_metadata_api_check     = true
    skip_region_validation      = true
    skip_requesting_account_id  = true
    force_path_style            = false
  }
}
