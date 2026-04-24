locals {
  draining_modes = [for mode in var.draining_modes : trimspace(mode) if trimspace(mode) != ""]
  draining_csv   = join(",", local.draining_modes)
  base_tags = [
    "project:stoarama",
    "role:capture",
    "fleet:do-capture-v1",
    "env:prod",
  ]
  tags = distinct(concat(local.base_tags, var.extra_tags))
}

data "digitalocean_project" "capture" {
  name = var.project_name
}

resource "digitalocean_ssh_key" "capture" {
  name       = var.ssh_key_name
  public_key = var.ssh_public_key
}

resource "digitalocean_tag" "fleet" {
  for_each = toset(local.tags)
  name     = each.value
}

resource "digitalocean_droplet" "capture" {
  count    = var.droplet_count
  name     = format("%s-%02d", var.instance_name_prefix, count.index + 1)
  region   = var.region
  size     = var.droplet_size
  image    = var.droplet_image
  ssh_keys = [digitalocean_ssh_key.capture.fingerprint]
  tags     = local.tags
  user_data = templatefile("${path.module}/cloud-init.yaml.tftpl", {
    repo_url                        = var.repo_url
    repo_ref                        = var.repo_ref
    repo_clone_token                = var.repo_clone_token
    server_id                       = format("%s-%02d", var.instance_name_prefix, count.index + 1)
    backend_api_url                 = var.backend_api_url
    backend_api_token               = var.backend_api_token
    capture_shared_capacity         = var.capture_shared_capacity
    draining_modes_csv              = local.draining_csv
    capture_tick_sec                = var.capture_tick_sec
    capture_heartbeat_sec           = var.capture_heartbeat_sec
    capture_lease_sec               = var.capture_lease_sec
    capture_unsupported_threshold   = var.capture_unsupported_threshold
    capture_frame_queue_size        = var.capture_frame_queue_size
    capture_frame_enqueue_timeout_s = var.capture_frame_enqueue_timeout_sec
    capture_frame_writers           = var.capture_frame_writers
  })

  lifecycle {
    ignore_changes = [user_data]
  }
}

resource "digitalocean_project_resources" "capture" {
  project   = data.digitalocean_project.capture.id
  resources = [for droplet in digitalocean_droplet.capture : droplet.urn]
}

resource "digitalocean_firewall" "capture" {
  name        = "${var.instance_name_prefix}-fw"
  droplet_ids = [for droplet in digitalocean_droplet.capture : droplet.id]

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
