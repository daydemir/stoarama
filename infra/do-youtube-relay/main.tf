locals {
  base_tags = [
    "project:stoarama",
    "role:youtube-relay-sink",
    "fleet:do-youtube-relay-v1",
    "env:prod",
  ]
  tags = distinct(concat(local.base_tags, var.extra_tags))
}

data "digitalocean_project" "youtube_relay" {
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

resource "digitalocean_ssh_key" "youtube_relay" {
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
    local.existing_ssh_key_fingerprint != "" ? local.existing_ssh_key_fingerprint : try(digitalocean_ssh_key.youtube_relay[0].fingerprint, "")
  )
}

resource "digitalocean_droplet" "youtube_relay" {
  count    = var.droplet_count
  name     = format("%s-%02d", var.instance_name_prefix, count.index + 1)
  region   = var.region
  size     = var.droplet_size
  image    = var.droplet_image
  ssh_keys = [local.effective_ssh_key_fingerprint]
  tags     = local.tags
  user_data = templatefile("${path.module}/cloud-init.yaml.tftpl", {
    repo_url                                = var.repo_url
    repo_ref                                = var.repo_ref
    repo_clone_token                        = var.repo_clone_token
    backend_api_url                         = var.backend_api_url
    backend_api_token                       = var.backend_api_token
    youtube_relay_source_public_base_url    = var.youtube_relay_source_public_base_url
    youtube_relay_shared_token              = var.youtube_relay_shared_token
    youtube_relay_network_transport         = var.youtube_relay_network_transport
    youtube_relay_topology_id               = var.youtube_relay_topology_id
    youtube_relay_hub_server_id             = var.youtube_relay_hub_server_id
    youtube_relay_source_server_id          = var.youtube_relay_source_server_id
    youtube_relay_wg_interface              = var.youtube_relay_wg_interface
    youtube_relay_wg_source_ip              = var.youtube_relay_wg_source_ip
    youtube_relay_wg_ip                     = cidrhost(var.youtube_relay_wg_sink_cidr, var.youtube_relay_wg_sink_offset + count.index)
    youtube_relay_sink_capacity             = var.youtube_relay_sink_capacity
    youtube_relay_heartbeat_sec             = var.youtube_relay_heartbeat_sec
    youtube_relay_lease_sec                 = var.youtube_relay_lease_sec
    youtube_relay_refresh_sec               = var.youtube_relay_refresh_sec
    youtube_relay_unsupported_threshold     = var.youtube_relay_unsupported_threshold
    youtube_relay_frame_queue_size          = var.youtube_relay_frame_queue_size
    youtube_relay_frame_enqueue_timeout_sec = var.youtube_relay_frame_enqueue_timeout_sec
    youtube_relay_frame_writers             = var.youtube_relay_frame_writers
  })

  lifecycle {
    precondition {
      condition     = trimspace(local.effective_ssh_key_fingerprint) != ""
      error_message = "failed to resolve SSH key fingerprint: set ssh_key_fingerprint or provide ssh_public_key present in DigitalOcean account"
    }
    ignore_changes = [user_data]
  }
}

resource "digitalocean_project_resources" "youtube_relay" {
  project   = data.digitalocean_project.youtube_relay.id
  resources = [for droplet in digitalocean_droplet.youtube_relay : droplet.urn]
}

resource "digitalocean_firewall" "youtube_relay" {
  name        = "${var.instance_name_prefix}-fw"
  droplet_ids = [for droplet in digitalocean_droplet.youtube_relay : droplet.id]

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
