#!/usr/bin/env python3

import sys

edges = set()
with open(sys.argv[1]) as f:
    for line in f:
       t = " ".join([str(x) for x in sorted([int(x) for x in line.split()])])
       if t in edges:
           print("dup detected!")
       else:
           edges.add(t)

fout = open(sys.argv[2], "w+")

for elem in edges:
    fout.write(elem+"\n")

fout.close()
