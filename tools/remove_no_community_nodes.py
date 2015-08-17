#!/usr/bin/env python3

import sys

nodes = set()

with open(sys.argv[1]) as f:
    for line in f:
        nodes.add(line.split(",")[0])

fout = open(sys.argv[3], "w+")
edges = []
with open(sys.argv[2]) as f:
    for line in f:
        s,t = line.split()
        if s in nodes and t in nodes:
            edges.append(line)

fout.writelines(edges)
