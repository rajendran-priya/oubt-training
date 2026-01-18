terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket  = "terraform-state-bucket-oubttraining" # bucket that contains terraform state file
    key     = "day5/terraform.tfstate"
    region  = "us-east-2"
    encrypt = true
  }
}

provider "aws" {
  region = var.aws_region
}