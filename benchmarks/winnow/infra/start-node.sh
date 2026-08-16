#!/bin/bash
set -euxo pipefail
exec > /var/log/user-data.log 2>&1

su - winnow -c 'chmod +x winnow && ./winnow'
