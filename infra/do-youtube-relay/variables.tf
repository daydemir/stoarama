variable "project_name" {
  description = "DigitalOcean project name."
  type        = string
  default     = "Stoarama"
}

variable "region" {
  description = "DigitalOcean region slug."
  type        = string
  default     = "nyc3"
}

variable "droplet_count" {
  description = "Number of youtube relay sink droplets."
  type        = number
  default     = 2
}

variable "droplet_size" {
  description = "Droplet size slug."
  type        = string
  default     = "s-2vcpu-4gb"
}

variable "droplet_image" {
  description = "Droplet image slug."
  type        = string
  default     = "ubuntu-24-04-x64"
}

variable "instance_name_prefix" {
  description = "Prefix for droplet names."
  type        = string
  default     = "si-yt-sink"
}

variable "ssh_key_name" {
  description = "Name for the managed SSH key record."
  type        = string
  default     = "stoarama-youtube-relay-key"
}

variable "ssh_public_key" {
  description = "SSH public key content to inject into droplets."
  type        = string
}

variable "ssh_key_fingerprint" {
  description = "Optional existing SSH key fingerprint to reuse (skips key creation when set)."
  type        = string
  default     = ""
}

variable "admin_cidrs" {
  description = "Allowed CIDRs for SSH ingress."
  type        = list(string)
  default     = ["0.0.0.0/0", "::/0"]
}

variable "repo_url" {
  description = "Repository URL containing backend/scripts/start-youtube-relay-sink.sh."
  type        = string
  default     = "https://github.com/daydemir/stoarama.git"
}

variable "repo_ref" {
  description = "Git branch or tag checked out by cloud-init."
  type        = string
  default     = "main"
}

variable "repo_clone_token" {
  description = "Optional GitHub token used for private HTTPS clone."
  type        = string
  default     = ""
  sensitive   = true
}

variable "backend_api_url" {
  description = "Render backend API URL."
  type        = string
}

variable "backend_api_token" {
  description = "Render backend API token."
  type        = string
  sensitive   = true
}

variable "youtube_relay_source_public_base_url" {
  description = "URL reachable by sinks for relay pulls, e.g. https://relay-source.example.com/ytrelay"
  type        = string
}

variable "youtube_relay_shared_token" {
  description = "Shared token required by relay endpoint."
  type        = string
  sensitive   = true
}

variable "youtube_relay_network_transport" {
  description = "Relay data-plane transport label surfaced in telemetry."
  type        = string
  default     = "wireguard"
}

variable "youtube_relay_topology_id" {
  description = "Relay topology identifier shown in server telemetry."
  type        = string
  default     = "do-youtube-relay-hub"
}

variable "youtube_relay_hub_server_id" {
  description = "Relay hub server identifier shown in server telemetry."
  type        = string
  default     = "do-youtube-relay-hub"
}

variable "youtube_relay_source_server_id" {
  description = "Source server identifier expected by sinks."
  type        = string
  default     = ""
}

variable "youtube_relay_wg_interface" {
  description = "WireGuard interface label for telemetry."
  type        = string
  default     = "wg0"
}

variable "youtube_relay_wg_source_ip" {
  description = "WireGuard source IP label for telemetry."
  type        = string
  default     = "10.77.0.2"
}

variable "youtube_relay_wg_sink_cidr" {
  description = "WireGuard sink subnet used to assign per-sink IP labels."
  type        = string
  default     = "10.77.0.0/24"
}

variable "youtube_relay_wg_sink_offset" {
  description = "Host offset in sink CIDR used for first sink IP assignment."
  type        = number
  default     = 11
}

variable "youtube_relay_sink_capacity" {
  description = "Per-server youtube_relay capacity announced by each sink."
  type        = number
  default     = 8
}

variable "youtube_relay_heartbeat_sec" {
  description = "Heartbeat interval seconds for sink mode capacity."
  type        = number
  default     = 15
}

variable "youtube_relay_lease_sec" {
  description = "Lease ttl seconds for sink mode capacity."
  type        = number
  default     = 45
}

variable "youtube_relay_refresh_sec" {
  description = "Assignment refresh interval seconds for sink manager."
  type        = number
  default     = 5
}

variable "youtube_relay_unsupported_threshold" {
  description = "Consecutive errors before unsupported marking."
  type        = number
  default     = 8
}

variable "youtube_relay_frame_queue_size" {
  description = "Per-stream frame queue size."
  type        = number
  default     = 64
}

variable "youtube_relay_frame_enqueue_timeout_sec" {
  description = "Frame queue enqueue timeout seconds."
  type        = number
  default     = 3
}

variable "youtube_relay_frame_writers" {
  description = "Per-stream frame writer workers."
  type        = number
  default     = 2
}

variable "extra_tags" {
  description = "Additional DO tags to apply to droplets."
  type        = list(string)
  default     = []
}
