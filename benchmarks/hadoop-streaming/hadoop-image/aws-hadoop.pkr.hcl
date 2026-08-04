packer {
  required_plugins {
    amazon = {
      version = ">= 1.2.8"
      source  = "github.com/hashicorp/amazon"
    }
  }
}

source "amazon-ebs" "ubuntu" {
  ami_name      = "hadoop-benchmark-${formatdate("YYYY-MM-DD-hhmm", timestamp())}"
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
}

build {
  name = "hadoop-benchmark"
  sources = [
    "source.amazon-ebs.ubuntu"
  ]

  provisioner "file" {
    source      = "hadoop-3.5.0.tar.gz"
    destination = "/tmp/hadoop-3.5.0.tar.gz"
  }

  provisioner "file" {
    sources = [
      "core-site.xml",
      "hdfs-site.xml",
      "yarn-site.xml",
      "mapred-site.xml",
      "load_data.sh",
      "run_test.sh",
      "verify_results.sh",
      "mapper.py",
      "reducer.py"
    ]
    destination = "/tmp/"
  }

  provisioner "shell" {
    script = "setup.sh"
  }
}
