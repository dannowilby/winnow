"""
Utility for cleaning and compacting baseline data.

This was needed to reduce the amount of job churn: aka map reduce jobs only
reading a small file (<64MB), causing an explosion in the number of them. By
compacting them all into one file, MapReduce can more easily saturate its map jobs.
"""

import os

def traverse(fd, path, size):
    for dir in os.listdir(path):
        next_path = path + "/" + dir
        if os.path.isfile(next_path):
            append(fd, next_path)
            size += os.path.getsize(next_path)
            print(size)
        else:
            size = traverse(fd, next_path, size)
    return size

def append(fd, path):
    content = open(path, "r", encoding='utf-8', errors='replace').read()
    fd.write(content)
    fd.write("\r\n")

input_dir = "../data"
output_file = "./data.txt"

with open(output_file, "a+") as fd:
    size_traversed = traverse(fd, input_dir, 0)
    print("Size traversed: ", size_traversed)