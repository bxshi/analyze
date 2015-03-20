#!/usr/bin/env python3

# Convert community file into list
#
# Usage:
# ./community_to_list.py community_file output_path
#
# Original community format: 
#   One community perline, each line contains a set of ids that belongs to that community
# Output format:
#   One id perline, each line contains node id, the community it belongs to, and the number of nodes in that community
#   Note that if id to community is not a one to one mapping, then `id` column will not be unique

import sys

if len(sys.argv) != 3:
    print("./community_to_list.py community_file output_path")
    exit(233)

fout = open(sys.argv[2], "w+")
fout.write("id, cluster, count\n")

cnt = 1
with open(sys.argv[1]) as f:
    for line in f:
        community = line.split()
        member_count = str(len(community))
        comm_id = str(cnt)
        cnt += 1
        for item in community:
            fout.write(item +","+comm_id+","+member_count+"\n")

fout.close()
