#!/usr/bin/env python3
import sys
import re

sys.stdin.reconfigure(encoding="utf-8", errors="replace")

for line in sys.stdin:
    line = line.strip()
    words = set(re.findall(r"\b[a-zA-Z]+\b", line))
    for word in words:
        print(word)