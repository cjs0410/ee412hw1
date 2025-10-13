from pyspark import SparkConf, SparkContext
# import numpy
import re
import sys
import math
import csv
import os
import time

# def paring(pair):
#     key, values = pair
#     result = []
#     for i in range(len(values)):
#         for j in range(len(values)):
#             if i != j:
#                 result.append((values[i], (key, values[j])))
#     return result


# def filtering(pair):
#     key, values = pair
#     result = []
#     for i in range(len(values)):
#         for j in range(i+1, len(values)):
#             # if i != j:
            

                
    

# conf = SparkConf()
# sc = SparkContext(conf=conf)
# lines = sc.textFile(sys.argv[1])

# pairs = lines.map(lambda l: (l.split("\t")[0], l.split("\t")[1].split(",")))
# # pairs.map(lambda (user, friends): [(friends[i], (user, friends[j])) for i in range(len(friends))])
# pairs = pairs.flatMap(paring).groupByKey()
# pairs = pairs.

# # pairs = pairs.flatMap(lambda p: [(p[0], v) for v in p[1] if p[0] != v]) # remove self-loops


# pairs = pairs.collect()
# # for p in pairs:
# #     if (p[1], p[0]) not in pairs:
# #         print(p)
# print(pairs)

# sc.stop()


def paring(pair):
    key, values = pair
    result = []
    # for v1 in values:
    #     for v2 in values:
    #         if v1 != v2:
    #             if v1 < v2:
    #                 result.append(((v1, v2), key))
    #             else:
    #                 result.append(((v2, v1), key))

    for i in range(len(values)):
        for j in range(i, len(values)):
            if i != j:
                if values[i] < values[j]:
                    result.append(((values[i], values[j]), key))
                else:
                    result.append(((values[j], values[i]), key))
    return result


# def paring2(pair):
#     key, values = pair
#     result = []
#     for i in range(len(values)):
#         for j in range(len(values)):
#             if i != j:
#                 result.append(((values[i], values[j]), key))
#     return result

# def triplet(pair):
#     key, values = pair
#     result = []
#     for v in values:
#         new_key = sorted([key[0], key[1], v])
#         new_key = tuple(new_key)
#         result.append((new_key, 1))
#     return result

def triplet(pair):
    key, values = pair
    result = []
    for i in range(len(values)):
        for j in range(i+1, len(values)):
            new_key = sorted([key, values[i], values[j]])
            new_key = tuple(new_key)
            result.append((new_key, key))
    return result

# def re_paring(pair):
#     key, _ = pair
#     result = []
#     k1, k2, k3 = key
#     result.append(((k1, k2), k3))
#     result.append(((k1, k3), k2))
#     result.append(((k2, k3), k1))
#     return result

def re_paring(pair):
    triplet, mids = pair
    result = []
    for mid in mids:
        new_key = tuple([t for t in list(triplet) if t != mid])
        result.append((new_key, mid)) 
    return result

def quadruplet(pair):
    key, values = pair
    result = []
    # for i in range(len(values)):
    #     for j in range(i, len(values)):
    #         if i != j:
    #             new_key = sorted([key[0], key[1], values[i], values[j]])
    #             new_key = tuple(new_key)
    #             result.append((new_key, 1))

    for i in range(len(values)):
        for j in range(i+1, len(values)):
            if i != j:
                new_key = sorted([key[0], key[1], values[i], values[j]])
                # new_key = tuple(new_key)
                new_key = "\t".join(new_key)
                result.append((new_key, 1))
    return result

start = time.time()
conf = SparkConf()
sc = SparkContext(conf=conf)
lines = sc.textFile(sys.argv[1])
# pairs = lines.map(lambda l: (int(l.split("\t")[0]), [int(e) for e in l.split("\t")[1].split(",")]))

# pairs = lines.map(lambda l: (int(l.split("\t")[0]), l.split("\t")[1].split(",")))
# pairs = pairs.map(lambda p: (p[0], [int(e) for e in p[1] if e!='']))
pairs = lines.map(lambda l: (l.split("\t")[0], l.split("\t")[1].split(",")))

# pairs = pairs.flatMap(paring).groupByKey()
# pairs = pairs.flatMap(paring)


# triplets = pairs.flatMap(triplet).reduceByKey(lambda n1, n2: n1 + n2)
# triplets = triplets.filter(lambda c: c[1] != 3)
triplets = pairs.flatMap(triplet).aggregateByKey([], lambda acc, x: acc + [x], lambda acc1, acc2: acc1 + acc2)
triplets = triplets.filter(lambda t: len(t[1]) != 3)


# head = triplets.take(10)
# print("=== Top 10 ===")
# for x in head:
#     print(x)
# triplets = triplets.flatMap(re_paring).groupByKey()
triplets = triplets.flatMap(re_paring).aggregateByKey([], lambda acc, x: acc + [x], lambda acc1, acc2: acc1 + acc2)

# head = triplets.take(10)
# print("=== Top 10 ===")
# for x in head:
#     print(x)


quadruplets = triplets.flatMap(quadruplet).reduceByKey(lambda n1, n2: n1 + n2)
quadruplets = quadruplets.filter(lambda c: c[1] == 2)
# quadruplets = quadruplets.collect()
# print(quadruplets)

# head = quadruplets.take(10)
# print("=== Top 10 ===")
# for x in head:
#     print(x)

head = quadruplets.sortByKey().take(10)
tail = quadruplets.sortByKey(ascending=False).take(10)
for h in head:
    print(h[0])
for t in tail:
    print(t[0])

end = time.time()
print(f"{end - start:.5f} sec")
sc.stop()