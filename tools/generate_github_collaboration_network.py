#!/usr/bin/env python3

# Convert bipartite garph into user only graph

import sys
import itertools
import csv

if len(sys.argv) != 4:
    print("usage ./generate_github_collaboration_network.py bipartite_graph user_titlemap user_graph")
    exit(233)

collaboration_dict = dict() # Key: repo Value: Set[People]
user_repo_reader = csv.DictReader(open(sys.argv[1]), dialect="unix")

title_map = dict()
cnt = 1

for item in user_repo_reader:
    if item["repo"] not in collaboration_dict:
        collaboration_dict[item["repo"]] = set()
    collaboration_dict[item["repo"]].add(item["user"])
    if item["user"] not in title_map:
        if cnt == 12060:
            print(item)
            exit(233)
        title_map[item["user"]] = cnt
        cnt += 1


print("collaboration raw data loaded\n")

final_output = set()
for item in collaboration_dict:
    for pair in itertools.combinations(collaboration_dict[item], 2):
        final_output.add(pair)

print("finish deduplication, has "+str(len(final_output))+" unique connections")

with open(sys.argv[2], "w+") as fout:
    for item in final_output:
        fout.write(str(title_map[item[0]])+" "+str(title_map[item[1]])+"\n")

with open(sys.argv[3], "w+") as fout:
    for item in title_map:
        fout.write(str(title_map[item])+","+item+"\n")
