output "project_id" {
  value = data.digitalocean_project.capture.id
}

output "droplet_ids" {
  value = [for droplet in digitalocean_droplet.capture : droplet.id]
}

output "droplet_names" {
  value = [for droplet in digitalocean_droplet.capture : droplet.name]
}

output "droplet_ipv4" {
  value = [for droplet in digitalocean_droplet.capture : droplet.ipv4_address]
}

output "recording_server_ids" {
  value = [for droplet in digitalocean_droplet.capture : "do-${droplet.id}"]
}

output "capture_shared_capacity" {
  value = var.capture_shared_capacity
}
