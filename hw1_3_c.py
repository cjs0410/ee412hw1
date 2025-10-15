import re
import sys
import math
import csv
import os
import time
import numpy as np

np.random.seed(0)
start = time.time()

def tokenize(text):
    tokens = []
    cleaned_text = re.sub(r'[^A-Za-z ]', '', text).lower()
    # print(cleaned_text)
    tokens = cleaned_text.split()
    return tokens


total_token_set = set()
token_index_dict = dict()
article_token_map = dict()
article_index_map = dict()
article_index = 0

with open(sys.argv[1], "r") as f:
    for l in f.readlines():
        article_id, text = l.split(" ", 1)
        tokens = tokenize(text)
        # print(tokens)
        total_token_set.update(tokens)
        token_cnt = dict()
        for token in tokens:
            if token in token_cnt:
                token_cnt[token] += 1
            else:
                token_cnt[token] = 1
        article_token_map[article_id] = token_cnt
        article_index_map[article_index] = article_id
        article_index += 1
        # break

token_index = 0
total_token_set = sorted(list(total_token_set))
# print(len(total_token_set))
# print(article_token_map)
for t in total_token_set:
    token_index_dict[token_index] = t
    token_index += 1

vectors = np.zeros((len(article_token_map), token_index), dtype=float)

# for index, ar

for i in range(len(vectors)):
    article_token_cnt = article_token_map[article_index_map[i]]
    # max_freq = max(article_token_cnt.values())

    for j in range(len(vectors[i])):
        term = token_index_dict[j]
        # print(term)
        if term in article_token_cnt:
            # tf = article_token_cnt[term] / max_tf
            tf = article_token_cnt[term]
            df = 0
            for token_cnt in article_token_map.values():
                if term in token_cnt:
                    df += 1
            idf = math.log(article_index / df)
            vectors[i, j] = tf * idf
# print(vectors[vectors > 0].squeeze())

# vectors = vectors.T
candidates = None
for i in range(10):
    same_rows = set()
    hyperplane = np.random.randn(token_index)
    result = np.dot(vectors, hyperplane)
    for j in range(len(result)):
        for k in range(j+1, len(result)):
            # if (j, k) in same_rows:
            #     continue
            if result[j] * result[k] > 0:
                same_rows.add((j, k))
            if not candidates:
                candidates = same_rows
    candidates = candidates.intersection(same_rows)
# print(candidates)
# print(len(candidates))
for candidate_row in candidates:
    row1, row2 = candidate_row
    vec1 = vectors[row1, :]
    vec2 = vectors[row2, :]

    cos_dist = 1 - np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))
    cos_sim = np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))
    if cos_dist <= 0.1:
        print(article_index_map[row1] + "\t" + article_index_map[row2] + "\t" + str(cos_sim))


end = time.time()
# print(f"{end - start:.5f} sec")