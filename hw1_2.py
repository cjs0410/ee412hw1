import re
import sys
import math
import csv
import os
import time
import numpy as np

item_cnt = dict()
item_indices = dict()
item_indices_reverse = dict()

with open(sys.argv[1], "r") as f:
    for l in f.readlines():
        bucket = set(l.strip().split(" "))
        for item in bucket:
            if item in item_cnt:
                item_cnt[item] += 1
            else:
                item_cnt[item] = 1

index = 0
for item, cnt in item_cnt.items():
    if cnt >= 100:
        index += 1
        item_indices[item] = index
        item_indices_reverse[index] = item
    else:
        item_indices[item] = 0

triangle_matrix = np.zeros((index, index), dtype=int)

with open(sys.argv[1], "r") as f:
    for l in f.readlines():
        bucket = l.strip().split(" ")
        freq_items = []
        for item in bucket:
            if item_indices[item] == 0:
                continue
            freq_items.append(item_indices[item])
        freq_items = sorted(freq_items)
        for i in range(len(freq_items)):
            for j in range(i+1, len(freq_items)):
                triangle_matrix[freq_items[i]-1][freq_items[j]-1] += 1

freq_pairs = 0
association_rules = dict()
for i in range(len(triangle_matrix)):
    for j in range(i+1, len(triangle_matrix[i])):
        if triangle_matrix[i][j] < 100:
            continue
        freq_pairs += 1
        pair_support = triangle_matrix[i][j]
        item1 = item_indices_reverse[i+1]
        item2 = item_indices_reverse[j+1]
        item1_support = item_cnt[item1]
        item2_support = item_cnt[item2]

        if pair_support / item1_support >= 0.5:
            association_rules[item1 + " -> " + item2] = dict()
            association_rules[item1 + " -> " + item2]["support"] = pair_support
            association_rules[item1 + " -> " + item2]["confidence"] = float(pair_support / item1_support)

        if pair_support / item2_support >= 0.5:
            association_rules[item2 + " -> " + item1] = dict()
            association_rules[item2 + " -> " + item1]["support"] = pair_support
            association_rules[item2 + " -> " + item1]["confidence"] = float(pair_support / item2_support)

sorted_list = sorted(
    association_rules.items(),
    key=lambda x: (x[1]["confidence"], x[1]["support"]),
    reverse=True
)

print(freq_pairs) # number of freq pairs
print(len(association_rules)) # number of association rules
for rule, metrics in sorted_list[:10]:
    print(f"Rule: {rule}, Confidence: {metrics['confidence']}, Support: {metrics['support']}")