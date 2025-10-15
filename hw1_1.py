from pyspark import SparkConf, SparkContext
# import numpy
import re
import sys
import math
import csv
import os
import time

def paring(pair):
    key, values = pair
    result = []

    for i in range(len(values)):
        for j in range(i, len(values)):
            if i != j:
                if values[i] < values[j]:
                    result.append(((values[i], values[j]), key))
                else:
                    result.append(((values[j], values[i]), key))
    return result

def triplet(pair):
    key, values = pair
    result = []
    for i in range(len(values)):
        for j in range(i+1, len(values)):
            new_key = sorted([key, values[i], values[j]])
            new_key = tuple(new_key)
            result.append((new_key, key))
    return result

def extract_mid_node(pair):
    triplet, mids = pair
    result = []
    for mid in mids:
        new_key = tuple([t for t in triplet if t != mid])
        result.append((new_key, mid)) 
    return result

def quadruplet(pair):
    key, values = pair
    result = []

    for i in range(len(values)):
        for j in range(i+1, len(values)):
            if i != j:
                new_key = sorted([key[0], key[1], values[i], values[j]])
                new_key = tuple(new_key)
                # new_key = "\t".join(new_key)
                result.append((new_key, 1))
    return result

start = time.time()
conf = SparkConf()
sc = SparkContext(conf=conf)
lines = sc.textFile(sys.argv[1])
# pairs = lines.map(lambda l: (int(l.split("\t")[0]), [int(e) for e in l.split("\t")[1].split(",")]))

pairs = lines.map( lambda l: (int(l.split("\t")[0]), list(set([int(e) for e in l.split("\t")[1].split(",") if (e != '' and e != l.split("\t")[0])]))) ) # remove self-loop & dupliates

# pairs = pairs.map(lambda p: (p[0], [int(e) for e in p[1] if e!='']))
# pairs = lines.map(lambda l: (l.split("\t")[0], l.split("\t")[1].split(",")))
triplets = pairs.flatMap(triplet).aggregateByKey([], lambda acc, x: acc + [x], lambda acc1, acc2: acc1 + acc2)
triplets = triplets.filter(lambda t: len(t[1]) != 3)

only_end_nodes = triplets.flatMap(extract_mid_node).aggregateByKey([], lambda acc, x: acc + [x], lambda acc1, acc2: acc1 + acc2)

quadruplets = only_end_nodes.flatMap(quadruplet).reduceByKey(lambda n1, n2: n1 + n2)
quadruplets = quadruplets.filter(lambda c: c[1] == 2)

head = quadruplets.sortByKey().take(10)
tail = quadruplets.sortByKey(ascending=False).take(10)
# print("first")
for h in head:
    # print(h[0])
    print("\t".join([str(usr) for usr in h[0]]))
# print("last")
for t in tail:
    # print(t[0])
    print("\t".join([str(usr) for usr in t[0]]))

end = time.time()
# print(f"{end - start:.5f} sec")
sc.stop()