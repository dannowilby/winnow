
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.55.0"
    }
  }

  required_version = ">=1.15"
}

provider "aws" {
  region = "us-west-1"
}

resource "aws_vpc" "main" {
  cidr_block           = "10.0.1.0/24"
  enable_dns_support   = true
  enable_dns_hostnames = true
}

resource "aws_subnet" "b-sn" {
  vpc_id     = aws_vpc.main.id
  cidr_block = "10.0.1.0/24"

  tags = {
    Name = "b-sn"
  }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
}

resource "aws_route_table" "b-rt" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "b-rt"
  }
}

resource "aws_route_table_association" "b-sn" {
  subnet_id      = aws_subnet.b-sn.id
  route_table_id = aws_route_table.b-rt.id
}

resource "aws_security_group" "benchmark-sg" {
  name        = "sgrp-benchmark"
  description = "Security group for Benchmark EC2s"
  vpc_id      = aws_vpc.main.id
}

resource "aws_security_group_rule" "benchmark-sg-ingress" {
  type                     = "ingress"
  description              = "Security Group for Benchmark EC2s"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
  source_security_group_id = aws_security_group.benchmark-sg.id
  security_group_id        = aws_security_group.benchmark-sg.id
}

resource "aws_security_group_rule" "benchmark-sg-egress" {
  type                     = "egress"
  description              = "Security Group for Benchmark EC2s"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
  source_security_group_id = aws_security_group.benchmark-sg.id
  security_group_id        = aws_security_group.benchmark-sg.id
}

data "aws_ami" "winnow" {
  most_recent = true

  filter {
    name   = "name"
    values = ["winnow-benchmark-*"]
  }

  owners = ["self"]
}

resource "aws_instance" "benchmark-worker" {
  count = 6

  ami           = data.aws_ami.winnow.id
  instance_type = "t3.large"

  subnet_id  = aws_subnet.b-sn.id
  private_ip = "10.0.1.1${count.index}"

  vpc_security_group_ids = [aws_security_group.benchmark-sg.id]

  user_data = file("start-node.sh")

  root_block_device {
    volume_size = 45
  }

  tags = {
    Name = "Benchmark worker ${count.index}"
  }
}

output "worker-ids" {
  description = "Identifiers for the Benchmark workers"
  value       = aws_instance.benchmark-worker.*.id
}

resource "aws_ec2_instance_connect_endpoint" "control-plane" {
  subnet_id          = aws_subnet.b-sn.id
  security_group_ids = [aws_security_group.eice.id]

  preserve_client_ip = false
}

output "eice-endpoint" {
  description = "eice endpoint id"
  value       = aws_ec2_instance_connect_endpoint.control-plane.id
}

resource "aws_security_group" "eice" {
  name        = "sgrp-eice"
  description = "Security group for EC2 Instance Connect Endpoint"
  vpc_id      = aws_vpc.main.id
}

resource "aws_security_group_rule" "ssh-from-eice" {
  type                     = "ingress"
  from_port                = 22
  to_port                  = 22
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.eice.id
  security_group_id        = aws_security_group.benchmark-sg.id
}

# Terraform strips the default AWS "allow all" egress rule on creation of a
# new security group, so eice needs its own explicit egress rule to reach
# the instance at all.
resource "aws_security_group_rule" "eice-egress-ssh" {
  type                     = "egress"
  from_port                = 22
  to_port                  = 22
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.benchmark-sg.id
  security_group_id        = aws_security_group.eice.id
}

resource "aws_security_group_rule" "benchmark-sg-egress-internet" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1" # all protocols
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.benchmark-sg.id
}

data "aws_iam_policy_document" "eice-connect" {
  statement {
    sid       = "OpenTunnel"
    effect    = "Allow"
    actions   = ["ec2-instance-connect:OpenTunnel"]
    resources = [aws_ec2_instance_connect_endpoint.control-plane.arn]
  }

  statement {
    sid       = "SendSSHPublicKey"
    effect    = "Allow"
    actions   = ["ec2-instance-connect:SendSSHPublicKey"]
    resources = tolist(aws_instance.benchmark-worker.*.arn)

    condition {
      test     = "StringEquals"
      variable = "ec2:osuser"
      values   = ["ubuntu"]
    }
  }

  statement {
    sid    = "DescribeForConnect"
    effect = "Allow"
    actions = [
      "ec2:DescribeInstances",
      "ec2:DescribeInstanceConnectEndpoints",
      "ec2:DescribeNetworkInterfaces",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_user_policy" "eice-connect" {
  name   = "eice-connect-control-plane"
  user   = "winnow-benchmarks"
  policy = data.aws_iam_policy_document.eice-connect.json
}
