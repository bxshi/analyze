library(parallel)

run_calculation <- function(graph_file, true_edge, false_edge, prefix) {
  library(igraph)
  
  load_graph <- function(file_path) {
    tmp.csv <- read.csv(file_path, header=F, sep=" ")
    g <- graph.data.frame(tmp.csv, directed=F)
    #g <- delete.edges(g, which(degree(g) == 0))
    g <- simplify(g, remove.multiple = T, remove.loops = F)
    return(g)
  }
  
  g <- load_graph(graph_file)
  edges.true <- read.csv(true_edge, sep=" ", header=F)
  colnames(edges.true) <- c("src","dst")
  
  edges.true <- do.call("rbind", apply(edges.true, 1, function(x) {
    src <- which(V(g)$name == x[1])
    dst <- which(V(g)$name == x[2])
    inters <- intersect(neighbors(g,src, mode = "all"), neighbors(g,dst, mode="all"))
    if (length(inters) == 0) {
      score <- 0
    } else {
      score <- sum(1/(log(degree(g,inters, mode = "all"))))
    }
    return(data.frame(src = x[1], dst = x[2], score = score))
  }))
  
  write.table(edges.true, paste(prefix, "aa.true.txt", sep=""), sep=" ", col.names = F, row.names = F)
  
  edges.false <- read.csv(false_edge, sep=" ", header=F)
  colnames(edges.true) <- c("src","dst")
  
  edges.false <- do.call("rbind", apply(edges.false, 1, function(x) {
    src <- which(V(g)$name == x[1])
    dst <- which(V(g)$name == x[2])
    inters <- intersect(neighbors(g,src, mode = "all"), neighbors(g,dst, mode="all"))
    if (length(inters) == 0) {
      score <- 0
    } else {
      score <- sum(1/(log(degree(g,inters, mode = "all"))))
    }
    return(data.frame(src = x[1], dst = x[2], score = score))
  }))
  
  write.table(edges.false, paste(prefix, "aa.false.txt", sep=""), sep=" ", col.names = F, row.names = F)
}

 cl <- makeCluster(3)
 clusterExport(cl, "run_calculation")
 parApply(cl, data.frame(graph_file=c("/data/bshi/dataset/github_collaboration/github.collaboration.ungraph.nodup.csv",
                                  "/data/bshi/dataset/dblp_citation/dblp.citation.digraph.csv",
                                  "/data/bshi/dataset/my_dblp_collaboration/edgelist.dedup.txt"),
                     true_edge=c("/data/bshi/dataset/github_collaboration/10000true.txt",
                                 "/data/bshi/dataset/dblp_citation/10000true.txt",
                                 "/data/bshi/dataset/my_dblp_collaboration/10000true.txt"),
                     false_edge=c("/data/bshi/dataset/github_collaboration/10000false.txt",
                                  "/data/bshi/dataset/dblp_citation/10000false.txt",
                                  "/data/bshi/dataset/my_dblp_collaboration/10000false.txt"),
                     prefix=c("/data/bshi/dataset/github_collaboration/link_prediction/10000/",
                              "/data/bshi/dataset/dblp_citation/link_prediction/10000/",
                              "/data/bshi/dataset/my_dblp_collaboration/link_prediction/10000/new_")), 1, function(x){
                                    run_calculation(x[1], x[2], x[3], x[4])
                                  })
 stopCluster(cl)
