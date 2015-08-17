library(igraph)

load_graph <- function(file_path) {
  tmp.csv <- read.csv(file_path, header=F, sep=" ")
  g <- graph.data.frame(tmp.csv, directed=F)
  #g <- delete.edges(g, which(degree(g) == 0))
  g <- simplify(g, remove.multiple = T, remove.loops = F)
  return(g)
}

get_top20 <- function(g, cands) {
  cands.mapped <- which(V(g)$name %in% cands)
  #stopifnot(length(cands.mapped) == length(cands))
  comm.aa <- similarity.invlogweighted(g, vids = cands.mapped, mode = "all")
  removed <- which(degree(g) == 0)
  comm.res <- apply(comm.aa, 1, function(x){
    tmp.res <- data.frame(score = as.vector(x), id = 1:length(x))
    tmp.res <- tmp.res[which(!tmp.res$id %in% removed),]
    top20 <- tmp.res[order(tmp.res$score, decreasing = T), ]
    print(paste("non-zero values", length(which(top20[1:20, "score"] > 0))))
    return(top20[1:20, "id"])
  })
  comm.res <- rbind(cands, comm.res)
  comm.output <- do.call("rbind", apply(comm.res, 2, function(x) {
    return(data.frame(query = x[1], id = x[2:length(x)], rank=1:20, score=0))
  }))
  return(comm.output)
}

 github.g <- load_graph("/data/bshi/dataset/github_collaboration/github.collaboration.ungraph.nodup.csv")
 github.comm.cands <- read.csv("/data/bshi/dataset/github_collaboration/query.txt", header=FALSE)$V1
# 
 github.comm.output <- get_top20(github.g, github.comm.cands)
 write.table(github.comm.output, file = "/data/bshi/dataset/github_collaboration/new.aa.community.txt", row.names=FALSE, col.names=FALSE, sep=" ")
# 
 citation.g <- load_graph("/data/bshi/dataset/dblp_citation/dblp.citation.digraph.csv")
 citation.comm.cands <- read.csv("/data/bshi/dataset/dblp_citation/commquery.txt", header=FALSE)$V1
# 
 write.table(get_top20(citation.g, citation.comm.cands), file = "/data/bshi/dataset/dblp_citation/new.aa.reduced.community.txt", row.names=FALSE, col.names=FALSE, sep=" ")
# 
collaboration.g <- load_graph("/data/bshi/dataset/my_dblp_collaboration/edgelist.dedup.txt")
collaboration.comm.cands <- read.csv("/data/bshi/dataset/my_dblp_collaboration/query.txt", header=FALSE)$V1
# 
write.table(get_top20(collaboration.g, collaboration.comm.cands), file = "/data/bshi/dataset/my_dblp_collaboration/new.aa.community.txt", row.names=FALSE, col.names=FALSE, sep=" ")

collaboration.comm.bigfigure <- c(4391596,4496798,4389178,4382417,4391778)
write.table(get_top20(collaboration.g, collaboration.comm.bigfigure), file = "/data/bshi/dataset/my_dblp_collaboration/aa.community.big.five.txt", row.names=FALSE, col.names=FALSE, sep=" ")


gg.csv <- read.csv("/data/bshi/dataset/wikipedia/graph/graph.txt", header=F, sep=" ")
g <- graph.edgelist(as.matrix(gg.csv), directed=F)
cands <- read.csv("/data/bshi/dataset/wikipedia/start_pairs", header=F, sep=" ")
cand.nodes <- cands$V1
cand.categories <- cands[1, 2:ncol(cands)]
colnames(cand.categories) <- NULL
rownames(cand.categories) <- NULL

for (filename in unique(str_replace(
  str_replace(
    str_replace(
      str_replace(
        str_replace(list.files("/data/bshi/dataset/wikipedia/output/"), "aa.txt","" ),
        ".txt", ""),
      "PR", ""),
    "Sim", ""),
  "Salsa", ""))) {
  node <- read.csv(paste("/data/bshi/dataset/wikipedia/output/",filename, "Sim.txt", sep=""), header=F, sep=" ")$V1[1]
  print(paste(filename, node))
  tmp.dt <- NULL
  for(i in 1:length(cand.categories)) {
    node.inters <- intersect(neighbors(g, c(node), mode="all"), neighbors(g, c(cand.categories[i]), mode="all"))
    node.degress <- degree(g, node.inters)
    tmp.res <- sum((1/log2(node.degress)))
    tmp.dt <- rbind(tmp.dt, data.frame(src = node,
                                       dst = cand.categories[i],
                                       score = as.numeric(tmp.res)))
  }
  write.table(tmp.dt, paste("/data/bshi/dataset/wikipedia/output/",filename, "aa.txt", sep=""), col.names=F, row.names=F, sep=" ")
}


# for(node in cand.nodes) {
#   tmp.dt <- NULL
#   for(i in 1:length(cand.categories)) {
#     node.inters <- intersect(neighbors(g, c(node), mode="all"), neighbors(g, c(cand.categories[i]), mode="all"))
#     node.degress <- degree(g, node.inters)
#     tmp.res <- sum((1/log2(node.degress)))
#     tmp.dt <- rbind(tmp.dt, data.frame(src = node,
#                                          dst = cand.categories[i],
#                                          score = as.numeric(tmp.res)))
#   }
#   res.dt[[cnt]] <- tmp.dt
#   cnt <- cnt + 1
# }
# res.dt <- rbind.fill(res.dt)
