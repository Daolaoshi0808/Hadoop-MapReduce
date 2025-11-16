#!/usr/bin/env python3
import sys

substrings = ['nu', 'chi', 'haw']

for line in sys.stdin:
    fields = line.strip().split()

    if len(fields) == 4:
        word = fields[0]
        year = fields[1]
        try:
            year_int = int(year)
            if not (0 < year_int <= 2022):
                continue
        except:
            continue

        for substring in substrings:
            if substring in word:
                print(f"{year},{substring}\t{fields[3]}")

    elif len(fields) == 5:
        word1 = fields[0]
        word2 = fields[1]
        year = fields[2]
        try:
            year_int = int(year)
            if not (0 < year_int <= 2022):
                continue
        except:
            continue

        for substring in substrings:
            if substring in word1:
                print(f"{year},{substring}\t{fields[4]}")
            if substring in word2:
                print(f"{year},{substring}\t{fields[4]}")


