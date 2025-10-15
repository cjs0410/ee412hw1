import re
import sys
import math
import csv
import os
import time
import numpy as np

np.random.seed(0)
def shingling(text):
    shingles = set()
    cleaned_text = re.sub(r'[^A-Za-z ]', '', text)
    # print(cleaned_text)
    for i in range(len(cleaned_text)-2):
        shingles.add(cleaned_text[i:i+3].lower())
    return shingles

def is_prime(x):
    if x < 2:
        return False
    for i in range(2, int(x**0.5) + 1):
        if x % i == 0:
            return False
    return True

def next_prime(n):
    while True:
        if is_prime(n):
            return n
        n += 1


start = time.time()
total_shingle_set = set()
shingle_index_dict = dict()
article_shingle_map = dict()
article_index_map = dict()
article_index = 0

with open(sys.argv[1], "r") as f:
    for l in f.readlines():
        article_id, text = l.split(" ", 1)
        # print(shingling(text))
        shingle = shingling(text)
        total_shingle_set.update(shingle)
        article_shingle_map[article_id] = shingle
        article_index_map[article_index] = article_id
        article_index += 1
        # break
# print(total_shingle_set)
shingle_index = 0
total_shingle_set = sorted(list(total_shingle_set))
for s in total_shingle_set:
    shingle_index_dict[shingle_index] = s
    shingle_index += 1

# print(shingle_index_dict)
chr_matrix = np.zeros((shingle_index, len(article_shingle_map)), dtype=int)
# print(chr_matrix.shape)

# print(article_index_map)
for i in range(len(chr_matrix)):
    for j in range(len(chr_matrix[i])):
        if shingle_index_dict[i] in article_shingle_map[article_index_map[j]]:
            chr_matrix[i][j] = 1

# print(chr_matrix.shape)

num_of_hash_func = 120
n = next_prime(shingle_index)
hash_functions = []
for i in range(num_of_hash_func):
    a = np.random.randint(0, n)
    b = np.random.randint(0, n)
    hash_functions.append((a, b))
slot_matrix = np.full((num_of_hash_func, len(article_shingle_map)), np.inf)


for i in range(len(chr_matrix)):
    for j in range(len(chr_matrix[i])):
        if chr_matrix[i][j] == 1:
            for hash_index, hash_func in enumerate(hash_functions):
                a, b = hash_func
                hash_value = (a*i + b)%n
                if hash_value < slot_matrix[hash_index][j]:
                    slot_matrix[hash_index][j] = hash_value

# print(slot_matrix)

band_num = 6
row_num = 20

same_cols = []

for i in range(band_num):
    # for j in range(row_num):
        # slot_matrix[i*20:(i+1)*20]
    band = slot_matrix[i*row_num:(i+1)*row_num]
    for j in range(band.shape[1]):
        for k in range(j+1, band.shape[1]):
            if (j, k) in same_cols:
                continue
            if np.array_equal(band[:, j], band[:, k]):
                same_cols.append((j, k))

# print(same_cols)

for same_col in same_cols:
    col1, col2 = same_col
    sig1 = slot_matrix[:, col1]
    sig2 = slot_matrix[:, col2]
    intersection = np.sum((sig1 == 1) & (sig2 == 1))
    union = np.sum((sig1 == 1) | (sig2 == 1))
    jaccard = intersection / union if union != 0 else 0.0
    if jaccard >= 0.9:
        print(article_index_map[col1] + "\t" + article_index_map[col2] + "\t" + str(jaccard))
    
end = time.time()
# print(f"{end - start:.5f} sec")