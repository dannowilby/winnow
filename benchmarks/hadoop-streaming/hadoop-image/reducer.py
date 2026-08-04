#!/usr/bin/env python3
import sys

current_word = None
count = 0

for line in sys.stdin:
    word = line.strip()

    if word != current_word:
        if current_word is not None:
            print(current_word, count)
        current_word = word
        count = 0

    count += 1

# end of stdin, print the last seen word
print(current_word, count)