library(igraph)
library(plyr)

fuzzy_modularity <- function (graph_file, comm_file) {
  g <- read.graph(graph_file, directed=F, format = "edgelist")
  g <- delete.vertices(g, which(degree(g) == 0))
  
  comm <- read.csv(comm_file, header=TRUE)
  comm <- comm[,1:2]
  comm <- comm[which(comm$id %in% V(g)),]
  comm.id <- split(comm$cluster, comm$id)
  comm.cluster <- split(comm$id, comm$cluster)
  #comm.cluster <- comm.cluster[lapply(comm.cluster, length) >= 2]
  modularity_res <- 0
  m <- length(E(g))
  max_cluster <- length(unique(comm[,"cluster"]))
  
  print("load okay")
  print(paste("clusters",length(comm.cluster)))
  
  modularity_res <- sum(unlist(lapply(comm.cluster, function(cands){
    if (length(cands) < 2) {
      return(0)
    }
    edge_pairs <- combn(cands, 2)
    tmp_res <- sum(apply(edge_pairs, 2, function(ids){
      comm_x <- comm.id[[ids[1]]]
      comm_y <- comm.id[[ids[2]]]
      comm_intersect <- length(intersect(comm_x, comm_y))
      return( (ifelse(are.connected(g, ids[1],ids[2]), 1,0) - prod(degree(g, v=ids))/(2*m)) * comm_intersect / max_cluster)
    })) 
    return(tmp_res)
  })))
  
  return(modularity_res/(2*m))
}