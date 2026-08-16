packer {
  required_plugins {
    amazon = {
      version = ">= 1.2.8"
      source  = "github.com/hashicorp/amazon"
    }
  }
}

source "amazon-ebs" "ubuntu" {
  ami_name      = "winnow-benchmark-${formatdate("YYYY-MM-DD-hhmm", timestamp())}"
  instance_type = "t3.large"
  region        = "us-west-1"
  source_ami_filter {
    filters = {
      name                = "ubuntu/images/*ubuntu-jammy-22.04-amd64-server-*"
      root-device-type    = "ebs"
      virtualization-type = "hvm"
    }
    most_recent = true
    owners      = ["099720109477"]
  }
  ssh_username = "ubuntu"

  launch_block_device_mappings {
    device_name           = "/dev/sda1"
    volume_size           = 45
    volume_type           = "gp3"
    delete_on_termination = true
  }
}

build {
  name = "winnow-benchmark"
  sources = [
    "source.amazon-ebs.ubuntu"
  ]

  provisioner "file" {
    sources = [
      "load_data.sh",
      "run_test.sh",
      "job.json",
      "cluster.json",
      "winnow",
      "winnow_cli",
      "reducer.wasm",
      "reader.wasm",
      "mapper.wasm",
      "partitioner.wasm"
    ]
    destination = "/tmp/"
  }

  provisioner "shell" {
    script = "setup.sh"
  }
}
