#!/usr/bin/env python3

import sys

nodes = set()
edges = set()

with open(sys.argv[1]) as f:
    for line in f:
        s,t = [int(x) for x in line.split()]
        nodes.add(s)
        nodes.add(t)
        e = [str(x) for x in sorted([s,t])]
        edges.add(".".join(e))

print(len(nodes))
print(len(edges))
