variable "region" {
  type    = string
  default = "us-east-1"
}

variable "env" {
  type    = string
  default = "dev"
}

variable "owner" {
  type        = string
  description = "Team or person accountable for this stack (shows up in tags and alerts)."
}

variable "alert_email" {
  type = string
}

variable "monthly_budget_usd" {
  type    = string
  default = "25"
}

variable "model_id" {
  type    = string
  default = "anthropic.claude-opus-5-5"
}

variable "enrich_lambda_zip" {
  type    = string
  default = "../../dist/enrich_lambda.zip"
}
