library(plyr)
library(parallel)
library(RankAggreg)
library(Kendall)

mergeName <- function(df, startNodes, categories) {
  df <- merge(df, startNodes, by.x = "query_id", by.y="id")
  df <- merge(df, categories, by.x = "id", by.y="id")
  df <- df[df$name != "Main_topic_classifications",]
  return(df)
}

getRawData <- function(filenames) {
  startNodes <- read.csv("/data/bshi/dataset/wikipedia/graph/start_nodes.csv", header=F, sep=" ")
  categories <- read.csv("/data/bshi/dataset/wikipedia/graph/main_topic_classifications.txt", header=F,sep=" ")
  colnames(startNodes) <- c("query_name", "id")
  colnames(categories) <- c("name", "id")
  
  res <- NULL
  
  for(filename in filenames) {
    startNodes$query_name <- str_replace(startNodes$query_name, ",", "_")
    
    score.ppr <- read.csv(paste("/data/bshi/dataset/wikipedia/output/",filename,"PR.txt", sep=""), header=FALSE, sep=" ",
                          col.names=c("query_id","id", "fppr_score", "fppr_rank", "bppr_score", "bppr_rank"))
        
    score.sim <- read.csv(paste("/data/bshi/dataset/wikipedia/output/",filename,"Sim.txt", sep=""), header=FALSE, sep=" ",
                          col.names=c("query_id", "id", "sim_score"))
        
    score.salsa <- read.csv(paste("/data/bshi/dataset/wikipedia/output/",filename,"Salsa.txt", sep=""), header=FALSE, sep=" ",
                            col.names=c("query_id", "id", "query_auth_score", "query_auth_rank","query_hub_score",
                                        "query_hub_rank","id_auth_score", "id_auth_rank","id_hub_score","id_hub_rank"))
    score.aa <- read.csv(paste("/data/bshi/dataset/wikipedia/output/", filename, "aa.txt", sep=""), header=F, sep=" ",
                         col.names = c("query_id", "id", "aa_score"))
    
    tmp.res <- merge( merge(merge(score.ppr, score.sim, by = c("query_id", "id")),
                             score.salsa, by = c("query_id", "id")),
                      score.aa, by=c("query_id","id")  )  
    
    res <- rbind(res, mergeName(tmp.res, startNodes, categories))
  }
  
  return(res)
}

getResult <- function(filename) {
  
  startNodes <- read.csv("/data/bshi/dataset/wikipedia/graph/start_nodes.csv", header=F, sep=" ")
  categories <- read.csv("/data/bshi/dataset/wikipedia/graph/main_topic_classifications.txt", header=F,sep=" ")
  colnames(startNodes) <- c("query_name", "id")
  colnames(categories) <- c("name", "id")
  
  startNodes$query_name <- str_replace(startNodes$query_name, ",", "_")
  
  score.ppr <- read.csv(paste("/data/bshi/dataset/wikipedia/output/",filename,"PR.txt", sep=""), header=FALSE, sep=" ",
                        col.names=c("query_id","id", "fppr_score", "fppr_rank", "bppr_score", "bppr_rank"))
  score.ppr <- mergeName(score.ppr, startNodes, categories)
  
  score.ppr$fbppr_score <- scale(score.ppr$fppr_score) + scale(score.ppr$bppr_score) 
  
  score.sim <- read.csv(paste("/data/bshi/dataset/wikipedia/output/",filename,"Sim.txt", sep=""), header=FALSE, sep=" ",
                        col.names=c("query_id", "id", "sim_score"))
  
  score.sim <- mergeName(score.sim, startNodes, categories)
  
  score.salsa <- read.csv(paste("/data/bshi/dataset/wikipedia/output/",filename,"Salsa.txt", sep=""), header=FALSE, sep=" ",
                          col.names=c("query_id", "id", "query_auth_score", "query_auth_rank","query_hub_score",
                                      "query_hub_rank","id_auth_score", "id_auth_rank","id_hub_score","id_hub_rank"))
  
  score.salsa <- mergeName(score.salsa, startNodes, categories)
  
  score.aa <- read.csv(paste("/data/bshi/dataset/wikipedia/output/", filename, "aa.txt", sep=""), header=F, sep=" ",
                       col.names = c("query_id", "id", "aa_score"))
  
  score.aa <- mergeName(score.aa, startNodes, categories)
  
  result <- data.frame(fppr = as.character(score.ppr[order(-score.ppr$fppr_score),"name"]),
                       bppr = as.character(score.ppr[order(-score.ppr$bppr_score),"name"]),
                       fbppr = as.character(score.ppr[order(-score.ppr$fbppr_score),"name"]),
                       salsa = as.character(score.salsa[order(-score.salsa$query_auth_score), "name"]),
                       simrank = as.character(score.sim[order(-score.sim$sim_score), "name"]),
                       aa = as.character(score.sim[order(-score.aa$aa_score), "name"]),
                       rand = as.character(score.sim[sample(1:nrow(score.sim), nrow(score.sim), replace = F), "name"]),
                       id = as.character(score.ppr$query_name),
                       rank = 1:nrow(score.ppr))
  return(result)
}

getTopk <- function(df, topk=3) {
  return(data.frame(id=rep(1:topk, 7),
                    model=unlist(lapply(c("fppr","bppr","fbppr","salsa","simrank", "aa", "rand"), function(x){rep(x,topk)})),
                    category=unlist(lapply(c("fppr","bppr","fbppr","salsa","simrank", "aa", "rand"), function(x){df[1:topk,x]}))))
}

# \lambda * fppr + (1-\lambda) * bppr
getLinearTopK <- function(df, topk=3) {
  lam <- seq(-1, 1, by = 0.001)
  res.list <- list()
  df$fppr_score <- scale(df$fppr_score)
  df$bppr_score <- scale(df$bppr_score)
  tmp.df <- split(df, df$query_id)
  for (tmp.lambda in lam) {
    tmp.res <- do.call("rbind", lapply(names(tmp.df), function(x){
                  tmp.df[[x]]$tmp <- tmp.lambda * (tmp.df[[x]]$fppr_score) + (1-tmp.lambda) * (tmp.df[[x]]$bppr_score)
                  return(data.frame(id = tmp.df[[x]]$query_name[1], 
                                    rank = 1:topk,
                                    category = unlist(tmp.df[[x]][order(tmp.df[[x]]$tmp, decreasing = T),"name"][1:topk]),
                                    model = tmp.lambda))
    }))
    row.names(tmp.res) <- NULL
    res.list[[length(res.list) + 1]] <- tmp.res
    print(tmp.lambda)
  }
  return(rbind.fill(res.list))
}

# \lambda * \frac{fppr}{fppr+k_1} + (1 - \lambda) * \frac{bppr}{bppr+k_2}
getSaturationTopK <- function(df, topk=5) {
  df$fppr_score <- scale(df$fppr_score)
  df$bppr_score <- scale(df$bppr_score)
  #lam <- seq(0.5, 0.6, by = 0.001)
  #k1s <- seq(0.6, 0.8, by = 0.01)
  #k2s <- seq(0.3, 0.5, by = 0.01)
  #lam <- seq(0, 1, by = 0.01)
  #k1s <- seq(0, 1, by = 0.1)
  #k2s <- seq(0, 1, by = 0.1)
  #r0.1012k10.001k20.001
  lam <- c(0.1012)
  k1s <- c(0.001)
  k2s <- c(0.001)
  res.list <- list()
  df <- df[,c("query_id", "query_name","fppr_score","bppr_score", "name")]
  tmp.df <- split(df, df$query_id)
  
  cl <- makeCluster(30)
  print("Cluster initialized")
  clusterExport(cl=cl, varlist=c("tmp.df","topk", "rbind.fill"), envir = environment())
  print("Data transferred")
  #res.list <- lapply(lam, function(tmp.lambda) {   
  res.list <- parLapply(cl, lam, function(tmp.lambda) {
    tmp.res <- lapply(names(tmp.df), function(x) {
      tmp.tmp.res <- list()
      tmp.df.tmp <- tmp.df[[x]]
      qname <- tmp.df.tmp$query_name[1]
      for (k1 in k1s) {
        for(k2 in k2s) {
          #print(paste("calc",system.time(
            tmp.df.tmp$tmp <- tmp.lambda * (tmp.df.tmp$fppr_score)/(tmp.df.tmp$fppr_score + k1) +
                                           (1-tmp.lambda) * (tmp.df.tmp$bppr_score)/(tmp.df.tmp$bppr_score + k2)
            #)))
          #print(paste("construct",system.time(
            tmp.tmp.res[[length(tmp.tmp.res)+1]] <- data.frame(id = qname, 
                                                              rank = 1:topk,
                                                              category = tmp.df.tmp[order(tmp.df.tmp$tmp, decreasing = T),"name"][1:topk],
                                                              model = paste("r",tmp.lambda,"k1",k1,"k2",k2, sep = ""))
          #)))
        }
      }
      system.time(tmp.rtn <- rbind.fill(tmp.tmp.res))
      return(tmp.rtn)
    })
    print(tmp.lambda)
    return(rbind.fill(tmp.res))
  })
  stopCluster(cl)
  return(rbind.fill(res.list))
}

getTopkUnordered <- function(df, topk = 3) {
  tmp <- as.character(unique(unlist(df[1:3,c("fppr","bppr","fbppr","salsa","simrank")])))
  return(c(as.character(df[1,"id"]), tmp[sample(1:length(tmp), length(tmp), replace=F)]))
}

extractHumanPreference <- function(df) {
  num2Char <- c("one","two","three","four","five","six","seven","eight","nine","ten")
  df$id <- as.character(df$id)
  df <- split(df, df$id) # Split into lists by id
  print(length(df))
  res <- NULL
  for(ind in 1:length(df)) {
    tmp.res <- as.character(unlist(df[[ind]][1,num2Char[df[[ind]]$choose_category_number_from_above]]))
    tmp.res <- table(tmp.res)
    tmp.partcipated <- sum(tmp.res[which(names(tmp.res) != "--")])
    tmp.candidates <- length(which(names(tmp.res) != "--"))
    res <- rbind(res, data.frame(id = names(df)[ind],
                          choose = unlist(names(tmp.res)),
                          count = as.numeric(tmp.res),
                          candidates = tmp.candidates,
                          total = tmp.partcipated))
  }
  res <- res[which(res$choose != "--"),]
  return(res)
}

getDCGParallel <- function(predf, hdf, hidcg, topk=3) {
  predf$model <- as.character(predf$model)
  predf$id <- as.character(predf$id)
  predf <- split(predf, predf$id)
  hdf$id <- as.character(hdf$id)
  hidcg$id <- as.character(hidcg$id)
  #cl <- makeCluster(1)
  print("Cluster initialized")
  #clusterExport(cl=cl, varlist=c("predf","hdf","hidcg","topk"), envir = environment())
  print("Data transferred")
  res <- lapply(predf, function(x){
#  res <- parLapply(cl, predf, function(x) {
#  res <- parLapply(cl, names(predf), function(x){
    library(plyr)
    
    tmp.id <- x$id[1]
    tmp.hdf <- hdf[which(hdf$id == tmp.id),]
    tmp.topk <- x
    models <- unique(tmp.topk$model)
    tmp.res <- list()
    for (model in models) {
      tmp.topk.model <- as.character(tmp.topk[which(tmp.topk$model == model),"category"])
      tmp.dcg <- 0
      for (i in 1:topk) {
        tmp.dcgk <- ifelse(tmp.topk.model[i] %in% tmp.hdf$choose,
                           (2^(tmp.hdf[which(tmp.hdf$choose == tmp.topk.model[i]), "count"] / 
                                 tmp.hdf[which(tmp.hdf$choose == tmp.topk.model[i]), "total"])) / log2(i+1), 0)
        tmp.dcg <- tmp.dcg + tmp.dcgk
      }
      tmp.ndcg <- ifelse(tmp.dcg == 0, 0, tmp.dcg / hidcg[which(hidcg$id == tmp.id), "dcg"])
      tmp.df <- data.frame(id = tmp.id,
                           model = as.character(model),
                           ndcg = tmp.ndcg)
      tmp.res[[length(tmp.res)+1]] <- tmp.df
    }
    return(rbind.fill(tmp.res))
  })

  #stopCluster(cl)
  return(rbind.fill(res))
}

getDCG2 <- function(predf, hdf, hidcg, topk=3) {
  predf$model <- as.character(predf$model)
  predf <- split(predf, predf$id)
  res <- list()
  res.len <- 0
  for (ind in 1:length(predf)) {
    tmp.id <- names(predf)[ind]
    tmp.hdf <- hdf[which(hdf$id == tmp.id), ]
    tmp.topk <- predf[[ind]]
    for(model in unique(tmp.topk$model)) {
      tmp.topk.model <- as.character(predf[[ind]][which(predf[[ind]]$model == model),"category"])
      tmp.dcg <- 0
      for(i in 1:topk) {
        tmp.dcgk <- ifelse(tmp.topk.model[i] %in% tmp.hdf$choose,
                           (2^(tmp.hdf[which(tmp.hdf$choose == tmp.topk.model[i]), "count"] / 
                                 tmp.hdf[which(tmp.hdf$choose == tmp.topk.model[i]), "total"])) / log2(i+1),
                           0)
        tmp.dcg <- tmp.dcg + tmp.dcgk
      }
      tmp.ndcg <- ifelse(tmp.dcg == 0, 0, tmp.dcg / hidcg[which(hidcg$id == tmp.id), "dcg"])
      tmp.df <- data.frame(id = tmp.id,
                           model = as.character(model),
                           ndcg = tmp.ndcg)
      res[[res.len+1]] <- tmp.df
      res.len <- res.len + 1
    }
    print(paste(ind,"done"))
  }
  rtn <- rbind.fill(res)
  return(rtn)
}

getDCG <- function(predf, hdf, hidcg, topk=3) {
  predf$id <- as.character(predf$id)
  hdf$id <- as.character(hdf$id)
  hidcg$id <- as.character(hidcg$id)
  predf <- split(predf, predf$id)
  res <- NULL
  for (ind in 1:length(predf)) {
    tmp.id <- names(predf)[ind]
    tmp.hdf <- hdf[which(hdf$id == tmp.id), ]
    tmp.topk <- getTopk(predf[[ind]], topk = topk)
    for(model in unique(tmp.topk$model)) {
      tmp.topk.model <- as.character(tmp.topk[which(tmp.topk$model == model),"category"])
      tmp.dcg <- 0
      for(i in 1:topk) {
        tmp.dcgk <- ifelse(tmp.topk.model[i] %in% tmp.hdf$choose,
                           (2^(tmp.hdf[which(tmp.hdf$choose == tmp.topk.model[i]), "count"] / 
                                tmp.hdf[which(tmp.hdf$choose == tmp.topk.model[i]), "total"])) / log2(i+1),
                           0)
        tmp.dcg <- tmp.dcg + tmp.dcgk
        print(paste("i",i,"dcgk",tmp.dcgk,"total",tmp.dcg))
      }
      print("get here")
      tmp.ndcg <- ifelse(tmp.dcg == 0, 0, tmp.dcg / hidcg[which(hidcg$id == tmp.id), "dcg"])
      print(paste("ndcg", tmp.ndcg))
      res <- rbind(res, data.frame(id = tmp.id,
                                   model = as.character(model),
                                   ndcg = tmp.ndcg))
    }
  }
  return(res)
}

getIDCG <- function(df, topk = 3) {
  df$id <- as.character(df$id)
  df <- split(df, df$id)
  res <- NULL
  for(ind in 1:length(df)) {
    tmp.relval <- sort(df[[ind]][,"count"] / df[[ind]][, "total"], decreasing = T)
    print(tmp.relval)
    tmp.topk <- ifelse(length(tmp.relval) > topk, topk, length(tmp.relval))
    tmp.relval <- tmp.relval[1:tmp.topk]
    print(tmp.relval)
    tmp.dcg <- 0
    for (i in 1:tmp.topk) {
      tmp.dcg <- tmp.dcg + (2^tmp.relval[i]) / log2(i+1)
      print(paste("i",i,"dcg",tmp.dcg))
    }
    res <- rbind(res, data.frame(id = names(df)[ind],
                                 dcg = tmp.dcg))
  }
  return(res)
}

compare <- function(df, hdf) {
  hdf$choose <- as.character(hdf$choose)
  hdf$id <- as.character(hdf$id)
  df$choose <- as.character(df$choose)
  df$id <- as.character(df$id)
  hdf <- split(hdf, hdf$id)
  df <- split(df, df$id)
  res <- list()
  for(ind in 1:length(names(hdf))) {
    if (TRUE){
    # Spearman's footrule
    rank.human <- hdf[[ind]][order(hdf[[ind]]$count, decreasing = TRUE), "choose"]
    if (length(rank.human) > 3) {
      rank.human <- rank.human[1:3]
    }
    rank.algo <- df[[ind]][order(df[[ind]]$value, decreasing = TRUE), "choose"]
    
    rank.algo <- rank.algo[which(rank.algo %in% rank.human)] # Get intersections
    rank.human <- rank.human[which(rank.human %in% rank.algo)] # and keep the order
    
    stopifnot(length(rank.algo) == length(rank.human))
    
    rank.footrule <- 0
    rank.maxfootrule <- ifelse(length(rank.algo) %% 2 == 0, (length(rank.algo)^2)/2,
                               (length(rank.algo)+1)*(length(rank.algo)-1)/2)
    for(j in 1:length(rank.human)) {
      rank.footrule <- rank.footrule + abs(j - match(rank.human[j], rank.algo))
    }    
    res[[length(res) + 1]] <- 1 - ifelse(rank.maxfootrule == 0 || rank.footrule == 0, 0, rank.footrule / rank.maxfootrule)
    #print(paste("length", length(rank.human), "fr", rank.footrule, "max", rank.maxfootrule, "res", res[[length(res)]]))
    }
    
    if(FALSE){
    #Kendall Tau
    rank.human <- hdf[[ind]][order(hdf[[ind]]$count, decreasing = TRUE), "choose"]
    if (length(rank.human) > 3) {
      rank.human <- rank.human[1:3]
    }
    rank.algo <- match(rank.human, df[[ind]][order(df[[ind]]$value, decreasing = TRUE), "choose"])
    
    rank.human <- 1:length(rank.human)
    
    # Change this part
    #if (length(rank.human) < 3) {
    #  rank.human <- c(rank.human, paste("PLACE_HOLDER", c(1:3)[1:(3 - length(rank.human))]))
    #}
    print(rank.human)
    print(rank.algo)
    
    #rank.tmp <- xtfrm(c(rank.human, rank.algo))
    #rank.human <- rank.tmp[1:length(rank.human)]
    #rank.algo <- rank.tmp[(length(rank.human)+1):length(rank.tmp)]

    #print(rank.human)
    #print(rank.algo)
    stopifnot(length(which(table(rank.human) != 1)) == 0)
    stopifnot(length(which(table(rank.algo) != 1)) == 0)
    stopifnot(length(rank.human) == length(rank.algo))
    
    if (length(rank.human) > 1)
    res[[length(res)+1]] <- cor.test(rank.human, rank.algo, method="kendall", exact = T)$p.value
    
    }
  }
  #res <- sum(log10(unlist(lapply(res, function(x){x$p.value})))) * -2
  
  #return(dchisq(res, length(rank.human) * 2))
  return(res)
}