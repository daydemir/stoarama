output "project_id" {
  value = data.digitalocean_project.capture_catalog.id
}

output "droplet_ids" {
  value = [for droplet in digitalocean_droplet.capture_catalog : droplet.id]
}

output "droplet_names" {
  value = [for droplet in digitalocean_droplet.capture_catalog : droplet.name]
}

output "droplet_ipv4" {
  value = [for droplet in digitalocean_droplet.capture_catalog : droplet.ipv4_address]
}

output "capture_catalog_server_ids" {
  value = [for droplet in digitalocean_droplet.capture_catalog : droplet.name]
}

output "capture_shared_capacity" {
  value = var.capture_shared_capacity
}
