#!/usr/bin/env python3

import sys
import random

# Generate true edges and false edges

if len(sys.argv) != 4:
    print("Usage: ./gen_test_edges.py edgelist_file num_samples output_file_prefix")
    print("Example: ./gen_test_edges.py 1000 edgelist.edgelist result")
    exit(0)

edgelist_file = sys.argv[1]
num_sample = int(sys.argv[2])
writer_file = sys.argv[3]

edges = set()
nodes = set()
with open(edgelist_file) as f:
    for line in f:
        edges.add(line.rstrip())
        src,dst = [int(x) for x in line.rstrip().split()]
        nodes.add(src)
        nodes.add(dst)

true_writer = open(writer_file+".true.txt", "w+") 
false_writer = open(writer_file+".false.txt", "w+")

for x in random.sample(edges, num_sample):
    true_writer.write(x+"\n")
true_writer.close()

count = 0
while 1:
    pair = [str(x) for x in random.sample(nodes, 2)]
    line = " ".join([str(x) for x in pair])
    if line not in edges:
        false_writer.write(line+"\n")
        print(line)
        count += 1
        if (count == num_sample):
            false_writer.close()
            exit()
