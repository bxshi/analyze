library(bear)
library(plyr)
library(parallel)
library(ggplot2)

calc.dcg <- function(scores) {
  return( sum((2^scores - 1) / log2(1:length(scores) + 1)) )
}

draw.ndcg <- function(fppr_file, simrank_file, salsa_file, comm_file, titlemap_file, topk = 20) {
  
  dat.res <- NULL
  
  comm <- read.csv(comm_file)
  titlemap <- read.csv(titlemap_file, header=FALSE)
  
  colnames(titlemap) <- c("id", "title")
  
  dat.fppr <- read.csv(fppr_file)
  dat.fppr$fppr_score <- scale(dat.fppr$fppr_score)
  dat.fppr$bppr_score <- scale(dat.fppr$bppr_score)
  #dat.sim <- read.csv(simrank_file)
  #dat.salsa <- read.csv(salsa_file)
  
  cl <- makeCluster(10)
  
  dat.fppr$jaccard <- unlist(parApply(cl, dat.fppr, 1, function(x){
    target_comm <- comm[which(comm$cluster %in% comm[which(comm$id == as.numeric(x[1])), "cluster"]), "id"]
    cand_comm <- comm[which(comm$cluster %in% comm[which(comm$id == as.numeric(x[2])), "cluster"]), "id"]
    return(length(intersect(target_comm, cand_comm)) / length(union(target_comm, cand_comm)))
  }))
  
  dat.fppr.splitted <- split(dat.fppr, dat.fppr$query_id)

  clusterExport(cl=cl, varlist=c("calc.dcg"), envir = environment())
  
  dat.fppr.idcg <- as.numeric(unlist(parLapply(cl, dat.fppr.splitted, function(x){
    x <- x[order(x$jaccard, decreasing = T), "jaccard"]
    return(unlist(lapply(1:topk, function(k){return(calc.dcg(x[1:k]))})))
  })))

  dat.fppr.idcg <- as.data.frame(cbind(unlist(lapply(as.numeric(names(dat.fppr.splitted)), function(x){return(rep(x, topk))})),
                                       rep(1:topk, length(names(dat.fppr.splitted))),
                                       dat.fppr.idcg))
  
  colnames(dat.fppr.idcg) <- c("query_id", "k", "idcg")
  
  clusterExport(cl=cl, varlist=c("dat.fppr.idcg"), envir = environment())
  
  for(lam in seq(0, 1, by=0.25)) {
    tmp.res <- unlist(parLapply(cl, dat.fppr.splitted, function(x){
      qid <- x$query_id[1]
      x <- x[order(x$fppr_score * lam + x$bppr_score * (1-lam), decreasing = T), "jaccard"]
      idcg <- dat.fppr.idcg[which(dat.fppr.idcg$query_id == qid), ]
      idcg <- idcg[order(idcg$k), "idcg"]
      return(unlist(lapply(1:topk, function(k){return(calc.dcg(x[1:k]))})) / idcg)
    }))
    tmp.df <- data.frame(query_id = unlist(lapply(as.numeric(names(dat.fppr.splitted)), function(x){return(rep(x, topk))})),
                         k = rep(1:topk, length(names(dat.fppr.splitted))),
                         w = lam,
                         ndcg = tmp.res)
    dat.res <- rbind(dat.res, tmp.df)
  }
  
  stopCluster(cl)
  return(dat.res)
}
