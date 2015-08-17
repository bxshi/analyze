library(igraph)

get_graph <- function(graph_file, directed=T) {
  g.csv <- read.csv(graph_file, sep=" ", header=F)
  g <- graph.data.frame(g.csv, directed=directed)
  g <- delete.vertices(g, which(degree(g, mode = "all") == 0))
  
  cands <- as.numeric(sample(unique(c(g.csv$V1, g.csv$V2)), 20000, replace = F))
  cands.edges <- list()
  cnt <- 1
  for(cand in cands) {
    cand.mapped <- which(V(g)$name == cand)
    hop1 <- neighbors(g, v = cand.mapped) # All one-hop nodes
    if (length(hop1) != 0) {
      hop2 <- unique(unlist(apply(as.matrix(hop1), 1, function(x){neighbors(g, x)}))) # All two-hop nodes
      targets <- hop2[which(!hop2 %in% hop1)]
      targets <- targets[which(targets != cand.mapped)]
      if (length(targets) >= 1) {
        dst <- sample(targets, 1)
        if (!are.connected(g, cand.mapped, dst)) { # connected 
          cands.edges[[cnt]] <- data.frame(src = cand, dst = as.numeric(V(g)[dst]$name))
          print(cands.edges[[cnt]])
          cnt <- cnt + 1
        } else if (!are.connected(g, dst, cand.mapped)) { # connected
          cands.edges[[cnt]] <- data.frame(src = as.numeric(V(g)[dst]$name), dst = cand)
          print(cands.edges[[cnt]])
          cnt <- cnt + 1
        }
      }
      if (cnt >= 100000) {
        return(rbind.fill(cands.edges))
      }
    }
  }
  return(rbind.fill(cands.edges))
}