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
  description = "Number of capture droplets."
  type        = number
  default     = 4
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
  default     = "si-capture"
}

variable "ssh_key_name" {
  description = "Name for the managed SSH key record."
  type        = string
  default     = "stoarama-capture-key"
}

variable "ssh_public_key" {
  description = "SSH public key content to inject into droplets."
  type        = string
}

variable "admin_cidrs" {
  description = "Allowed CIDRs for SSH ingress."
  type        = list(string)
  default     = ["0.0.0.0/0", "::/0"]
}

variable "repo_url" {
  description = "Repository URL containing backend/scripts/start-capture-server.sh."
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

variable "capture_shared_capacity" {
  description = "Shared max active stream capacity across hls_live, ffmpeg_direct, and image_poll on each DO server."
  type        = number
  default     = 6
}

variable "draining_modes" {
  description = "Modes marked draining on heartbeat."
  type        = list(string)
  default     = []
}

variable "capture_tick_sec" {
  description = "Capture manager reconcile interval seconds."
  type        = number
  default     = 5
}

variable "capture_heartbeat_sec" {
  description = "Heartbeat interval seconds."
  type        = number
  default     = 15
}

variable "capture_lease_sec" {
  description = "Heartbeat lease seconds."
  type        = number
  default     = 45
}

variable "capture_unsupported_threshold" {
  description = "Consecutive errors before unsupported marking."
  type        = number
  default     = 8
}

variable "capture_frame_queue_size" {
  description = "Per-stream frame queue size."
  type        = number
  default     = 64
}

variable "capture_frame_enqueue_timeout_sec" {
  description = "Frame queue enqueue timeout seconds."
  type        = number
  default     = 3
}

variable "capture_frame_writers" {
  description = "Per-stream frame writer workers."
  type        = number
  default     = 2
}

variable "extra_tags" {
  description = "Additional DO tags to apply to droplets."
  type        = list(string)
  default     = []
}
