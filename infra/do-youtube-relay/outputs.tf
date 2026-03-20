output "project_id" {
  value = data.digitalocean_project.youtube_relay.id
}

output "droplet_ids" {
  value = [for droplet in digitalocean_droplet.youtube_relay : droplet.id]
}

output "droplet_names" {
  value = [for droplet in digitalocean_droplet.youtube_relay : droplet.name]
}

output "droplet_ipv4" {
  value = [for droplet in digitalocean_droplet.youtube_relay : droplet.ipv4_address]
}

output "recording_server_ids" {
  value = [for droplet in digitalocean_droplet.youtube_relay : "do-${droplet.id}-yt-relay"]
}

output "wireguard_sink_ips" {
  value = [for idx, droplet in digitalocean_droplet.youtube_relay : cidrhost(var.youtube_relay_wg_sink_cidr, var.youtube_relay_wg_sink_offset + idx)]
}

output "wireguard_source_ip" {
  value = var.youtube_relay_wg_source_ip
}

output "youtube_relay_sink_capacity" {
  value = var.youtube_relay_sink_capacity
}

output "ssh_key_fingerprint" {
  value = local.effective_ssh_key_fingerprint
}
