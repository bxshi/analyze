#!/usr/bin/env python3

# Extract community from bipartite graph

import sys
import csv

if len(sys.argv) != 3:
    print("usage ./generate_github_collaboration_community.py user_titlemap bipartite_graph output_community")
    exit(233)

user_map = dict()
repo_map = dict()
comm_map = dict()

user_map_reader = csv.reader(open(sys.argv[1]))
for line in user_map_reader:
    user_map[line[1]] = line[0]

user_repo_reader = csv.DictReader(open(sys.argv[2]))
for line in user_repo_reader:
    if line["repo"] not in repo_map:
        repo_map[line["repo"]] = set()
    repo_map[line["repo"]].add(user_map[line["user"]])

writer = open(sys.argv[3], "w")
for key in repo_map:
    writer.write(" ".join(repo_map[key])+"\n")

writer.close()
