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
  description = "Number of catalog sweeper droplets."
  type        = number
  default     = 1
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
  default     = "si-capture-catalog"
}

variable "ssh_key_name" {
  description = "Name for the managed SSH key record."
  type        = string
  default     = "stoarama-capture-catalog-key"
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
  description = "Repository URL containing backend/scripts/start-capture-catalog-sweeper.sh."
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
  description = "Shared capacity for each catalog sweep batch."
  type        = number
  default     = 10
}

variable "capture_catalog_sweep_execution_classes" {
  description = "Comma-separated non-recording execution classes included in each sweep cycle."
  type        = string
  default     = "video_live,image_poll"
}

variable "capture_catalog_sweep_batch_per_class" {
  description = "Per-execution-class candidate count fetched each sweep cycle."
  type        = number
  default     = 10
}

variable "capture_catalog_sweep_max_streams" {
  description = "Maximum stream IDs included in one sweep batch."
  type        = number
  default     = 30
}

variable "capture_catalog_sweep_duration" {
  description = "Run duration per sweep batch (Go duration string)."
  type        = string
  default     = "4m"
}

variable "capture_catalog_sweep_idle_sec" {
  description = "Sleep time when no batch is available or batch fails."
  type        = number
  default     = 20
}

variable "capture_catalog_sweep_poll_timeout_sec" {
  description = "HTTP timeout seconds for dashboard stream queries."
  type        = number
  default     = 20
}

variable "capture_catalog_sweep_refresh_sec" {
  description = "Capture manager reconcile interval seconds within a sweep batch."
  type        = number
  default     = 5
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
