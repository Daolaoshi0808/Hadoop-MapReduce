#!/usr/bin/env python3
import sys

current_key = None
total_sum = 0
total_count = 0

for line in sys.stdin:
    key, value = line.strip().split('\t')
    value = float(value)

    if current_key == key:
        total_sum += value
        total_count += 1
    else:
        if current_key is not None:
            average = total_sum / total_count
            print(f"{current_key},{average}")
        
        current_key = key
        total_sum = value
        total_count = 1

# Don't forget the last key
if current_key is not None:
    average = total_sum / total_count
    print(f"{current_key},{average}")

