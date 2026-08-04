"""
Uses a breadth-first search to compute the word frequencies on a single machine.
"""

import os
import re
from collections import Counter

def traverse(frequencies, path, size):
    for dir in os.listdir(path):
        next_path = path + "/" + dir
        if os.path.isfile(next_path):
            count(frequencies, next_path)
            size += os.path.getsize(next_path)
            print(size)
        else:
            size = traverse(frequencies, next_path, size)
    return size

def count(frequencies, path):
    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as fd:
            for line in fd:
                frequencies.update(set(re.findall(r"\b[a-zA-Z]+\b", line)))
    except Exception as e:
        print(path, e);


frequencies = Counter()

data_src = "./data"

traverse(frequencies, data_src, 0)

with open("./baseline.txt", "a+") as fd:
    for (entry, value) in frequencies.most_common():
        fd.write(str(entry) + ", " + str(value) + "\n")