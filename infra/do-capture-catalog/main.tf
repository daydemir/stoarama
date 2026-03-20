locals {
  base_tags = [
    "project:stoarama",
    "role:capture-catalog-sweeper",
    "fleet:do-capture-catalog-v1",
    "env:prod",
  ]
  tags = distinct(concat(local.base_tags, var.extra_tags))
}

data "digitalocean_project" "capture_catalog" {
  name = var.project_name
}

data "digitalocean_ssh_keys" "account" {}

locals {
  existing_ssh_key_fingerprints = [
    for key in data.digitalocean_ssh_keys.account.ssh_keys : key.fingerprint
    if trimspace(key.public_key) == trimspace(var.ssh_public_key)
  ]
  existing_ssh_key_fingerprint = length(local.existing_ssh_key_fingerprints) > 0 ? local.existing_ssh_key_fingerprints[0] : ""
}

resource "digitalocean_ssh_key" "capture_catalog" {
  count      = trimspace(var.ssh_key_fingerprint) == "" && local.existing_ssh_key_fingerprint == "" ? 1 : 0
  name       = var.ssh_key_name
  public_key = var.ssh_public_key
}

resource "digitalocean_tag" "fleet" {
  for_each = toset(local.tags)
  name     = each.value
}

locals {
  effective_ssh_key_fingerprint = trimspace(var.ssh_key_fingerprint) != "" ? trimspace(var.ssh_key_fingerprint) : (
    local.existing_ssh_key_fingerprint != "" ? local.existing_ssh_key_fingerprint : try(digitalocean_ssh_key.capture_catalog[0].fingerprint, "")
  )
}

resource "digitalocean_droplet" "capture_catalog" {
  count    = var.droplet_count
  name     = format("%s-%02d", var.instance_name_prefix, count.index + 1)
  region   = var.region
  size     = var.droplet_size
  image    = var.droplet_image
  ssh_keys = [local.effective_ssh_key_fingerprint]
  tags     = local.tags
  user_data = templatefile("${path.module}/cloud-init.yaml.tftpl", {
    repo_url                               = var.repo_url
    repo_ref                               = var.repo_ref
    repo_clone_token                       = var.repo_clone_token
    backend_api_url                        = var.backend_api_url
    backend_api_token                      = var.backend_api_token
    capture_shared_capacity                = var.capture_shared_capacity
    capture_catalog_sweep_execution_classes = var.capture_catalog_sweep_execution_classes
    capture_catalog_sweep_batch_per_class   = var.capture_catalog_sweep_batch_per_class
    capture_catalog_sweep_max_streams      = var.capture_catalog_sweep_max_streams
    capture_catalog_sweep_duration         = var.capture_catalog_sweep_duration
    capture_catalog_sweep_idle_sec         = var.capture_catalog_sweep_idle_sec
    capture_catalog_sweep_poll_timeout_sec = var.capture_catalog_sweep_poll_timeout_sec
    capture_catalog_sweep_refresh_sec      = var.capture_catalog_sweep_refresh_sec
    capture_unsupported_threshold          = var.capture_unsupported_threshold
    capture_frame_queue_size               = var.capture_frame_queue_size
    capture_frame_enqueue_timeout_sec      = var.capture_frame_enqueue_timeout_sec
    capture_frame_writers                  = var.capture_frame_writers
  })

  lifecycle {
    precondition {
      condition     = trimspace(local.effective_ssh_key_fingerprint) != ""
      error_message = "failed to resolve SSH key fingerprint: set ssh_key_fingerprint or provide ssh_public_key present in DigitalOcean account"
    }
    ignore_changes = [user_data]
  }
}

resource "digitalocean_project_resources" "capture_catalog" {
  project   = data.digitalocean_project.capture_catalog.id
  resources = [for droplet in digitalocean_droplet.capture_catalog : droplet.urn]
}

resource "digitalocean_firewall" "capture_catalog" {
  name        = "${var.instance_name_prefix}-fw"
  droplet_ids = [for droplet in digitalocean_droplet.capture_catalog : droplet.id]

  inbound_rule {
    protocol         = "tcp"
    port_range       = "22"
    source_addresses = var.admin_cidrs
  }

  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "udp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}
