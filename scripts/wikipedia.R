library(reshape2)
library(ggplot2)

startnodes <- read.csv("/data/bshi/dataset/wikipedia/graph/start_nodes.csv", header=F, sep=" ")
categories <- read.csv("/data/bshi/dataset/wikipedia/graph/main_topic_classifications.txt", header=F,sep=" ")
colnames(startnodes) <- c("query_name", "id")
colnames(categories) <- c("name", "id")

startnodes$query_name <- str_replace(startnodes$query_name, ",", "_")

mergeName <- function(df) {
  df <- merge(df, startnodes, by.x = "query_id", by.y="id")
  df <- merge(df, categories, by.x = "id", by.y="id")
  df <- df[df$name != "Main_topic_classifications",]
  return(df)
}

getResult <- function(filename) {
  score.ppr <- read.csv(paste("/data/bshi/dataset/wikipedia/output/",filename,"PR.txt", sep=""), header=FALSE, sep=" ",
                        col.names=c("query_id","id", "fppr_score", "fppr_rank", "bppr_score", "bppr_rank"))
  score.ppr <- mergeName(score.ppr)
  
  score.ppr$fbppr_score <- scale(score.ppr$fppr_score) + scale(score.ppr$bppr_score) 
  
  score.sim <- read.csv(paste("/data/bshi/dataset/wikipedia/output/",filename,"Sim.txt", sep=""), header=FALSE, sep=" ",
                        col.names=c("query_id", "id", "sim_score"))
  
  score.sim <- mergeName(score.sim)
  
  score.salsa <- read.csv(paste("/data/bshi/dataset/wikipedia/output/",filename,"Salsa.txt", sep=""), header=FALSE, sep=" ",
                          col.names=c("query_id", "id", "query_auth_score", "query_auth_rank","query_hub_score",
                                      "query_hub_rank","id_auth_score", "id_auth_rank","id_hub_score","id_hub_rank"))
  score.salsa <- mergeName(score.salsa)
  
  result <- data.frame(fppr = as.character(score.ppr[order(-score.ppr$fppr_score),"name"]),
                       bppr = as.character(score.ppr[order(-score.ppr$bppr_score),"name"]),
                       fbppr = as.character(score.ppr[order(-score.ppr$fbppr_score),"name"]),
                       salsa = as.character(score.salsa[order(-score.salsa$query_auth_score), "name"]),
                       simrank = as.character(score.sim[order(-score.sim$sim_score), "name"]),
                       id = as.character(score.ppr$query_name))
  return(result)
}

getSaturationCombinationResult <- function(filename, w, k) {
  score.ppr <- read.csv(paste("/data/bshi/dataset/wikipedia/output/",filename,"PR.txt", sep=""), header=FALSE, sep=" ",
                        col.names=c("query_id","id", "fppr_score", "fppr_rank", "bppr_score", "bppr_rank"))
  score.ppr <- mergeName(score.ppr)
  
  score.ppr$fbppr_score <- w * score.ppr$fppr_score/(score.ppr$fppr_score + k) +
                                 (1-w)  * score.ppr$bppr_score / (score.ppr$bppr_score + k)
  score.ppr$fbppr_score[which(is.na(score.ppr$fbppr_score))] <- 0.0
  score.sim <- read.csv(paste("/data/bshi/dataset/wikipedia/output/",filename,"Sim.txt", sep=""), header=FALSE, sep=" ",
                        col.names=c("query_id", "id", "sim_score"))
  
  score.sim <- mergeName(score.sim)
  
  score.salsa <- read.csv(paste("/data/bshi/dataset/wikipedia/output/",filename,"Salsa.txt", sep=""), header=FALSE, sep=" ",
                          col.names=c("query_id", "id", "query_auth_score", "query_auth_rank","query_hub_score",
                                      "query_hub_rank","id_auth_score", "id_auth_rank","id_hub_score","id_hub_rank"))
  score.salsa <- mergeName(score.salsa)
  
  result <- data.frame(fppr = as.character(score.ppr[order(-score.ppr$fppr_score, score.ppr$name),"name"]),
                       bppr = as.character(score.ppr[order(-score.ppr$bppr_score, score.ppr$name),"name"]),
                       fbppr = as.character(score.ppr[order(-score.ppr$fbppr_score, score.ppr$name),"name"]),
                       salsa = as.character(score.salsa[order(-score.salsa$query_auth_score, score.ppr$name), "name"]),
                       simrank = as.character(score.sim[order(-score.sim$sim_score, score.ppr$name), "name"]),
                       id = as.character(score.ppr$query_name))
  #  print(sum(result[,"fppr"] != result[,"fbppr"]))
  #  print(sum(result[,"bppr"] != result[,"fbppr"]))
  return(result)  
}

getLinearCombinationResult <- function(filename, p1, p2) {
  score.ppr <- read.csv(paste("/data/bshi/dataset/wikipedia/output/",filename,"PR.txt", sep=""), header=FALSE, sep=" ",
                        col.names=c("query_id","id", "fppr_score", "fppr_rank", "bppr_score", "bppr_rank"))
  score.ppr <- mergeName(score.ppr)
  
  score.ppr$fbppr_score <- p1 * scale(score.ppr$fppr_score) + p2 * scale(score.ppr$bppr_score)
  
  score.sim <- read.csv(paste("/data/bshi/dataset/wikipedia/output/",filename,"Sim.txt", sep=""), header=FALSE, sep=" ",
                        col.names=c("query_id", "id", "sim_score"))
  
  score.sim <- mergeName(score.sim)
  
  score.salsa <- read.csv(paste("/data/bshi/dataset/wikipedia/output/",filename,"Salsa.txt", sep=""), header=FALSE, sep=" ",
                          col.names=c("query_id", "id", "query_auth_score", "query_auth_rank","query_hub_score",
                                      "query_hub_rank","id_auth_score", "id_auth_rank","id_hub_score","id_hub_rank"))
  score.salsa <- mergeName(score.salsa)
  
  result <- data.frame(fppr = as.character(score.ppr[order(-score.ppr$fppr_score),"name"]),
                       bppr = as.character(score.ppr[order(-score.ppr$bppr_score),"name"]),
                       fbppr = as.character(score.ppr[order(-score.ppr$fbppr_score),"name"]),
                       salsa = as.character(score.salsa[order(-score.salsa$query_auth_score), "name"]),
                       simrank = as.character(score.sim[order(-score.sim$sim_score), "name"]),
                       id = as.character(score.ppr$query_name))
#  print(sum(result[,"fppr"] != result[,"fbppr"]))
#  print(sum(result[,"bppr"] != result[,"fbppr"]))
  return(result)
}

getTopK <- function(df, topk = 3) {
  tmp <- as.character(unique(unlist(df[1:3,c("fppr","bppr","fbppr","salsa","simrank")])))
  return(c(as.character(df[1,"id"]), tmp[sample(1:length(tmp), length(tmp), replace=F)]))
}

getPrecision <- function(pred, tclass) {
  # pred is top-3 result, real is from crowdflower
  pred <- unlist(as.character(pred))
  tclass <- unlist(as.character(tclass))
  truePos <- sum(ifelse(tclass %in% pred, 1, 0))
  return(truePos / length(tclass))
}

getDCG <- function(pred, tclass) {
  # pred is top-3 result, real is from crowdflower
  res <- 0
  for(i in 1:length(pred)) {
    cnt <- length(which(tclass == pred[i]))
    res <- res + (2^(cnt) - 1) / log2(i+1)
  }
  return(res)
}

getAccuracy <- function(pred, tclass) {
  # pred is top-3 result, real is from crowdflower
  pred <- unlist(as.character(pred))
  tclass <- unlist(as.character(tclass))
  truePos <- length(intersect(pred, tclass))
  trueNeg <- 39 - length(union(pred, tclass))
  return((truePos + trueNeg) / 39)
}

analysisCrowdFlowerAccuracy <- function(df, map, k = 3) {
  num2Char <- c("one","two","three","four","five","six","seven","eight","nine","ten")
  df <- split(df, df$id)
  res <- NULL
  for(ind in 1:length(df)) { # Every query node
    res.tmp <- as.character(unlist(df[[ind]][1,num2Char[df[[ind]]$choose_category_number_from_above]])) # All results
    res.tmp <- res.tmp[which(res.tmp != "-")] # Remove null responses just in case
    
    res.map <- map[which(as.character(map$id) == as.character(df[[ind]]$id[1])), ]
    res.map <- res.map[1:3,]
    
#     print(as.character(df[[ind]]$id[1]))
    #     print(res.tmp)
    #     print(res.map)
    
    res <- rbind(res, data.frame(
      id = as.character(df[[ind]]$id[1]),
      fppr = getAccuracy(res.map[,"fppr"], res.tmp),
      bppr = getAccuracy(res.map[,"bppr"], res.tmp),
      fbppr = getAccuracy(res.map[,"fbppr"], res.tmp),
      simrank = getAccuracy(res.map[,"simrank"], res.tmp),
      salsa = getAccuracy(res.map[,"salsa"], res.tmp)))
  }
  return(res)
}

analysisDCG <- function(df, map, k = 3) {
  num2Char <- c("one","two","three","four","five","six","seven","eight","nine","ten")
  df <- split(df, df$id)
  res <- NULL
  for(ind in 1:length(df)) { # Every query node
    res.tmp <- as.character(unlist(df[[ind]][1,num2Char[df[[ind]]$choose_category_number_from_above]])) # All results
    res.tmp <- res.tmp[which(res.tmp != "-")] # Remove null responses just in case
    
    res.map <- map[which(as.character(map$id) == as.character(df[[ind]]$id[1])), ]
    res.map <- res.map[1:k,]
    
#    print(paste("dcg test rank fppr diff",sum(res.map[,"fppr"] != res.map[,"fbppr"])))
#    print(paste("dcg test rank bppr diff",sum(res.map[,"bppr"] != res.map[,"fbppr"])))
    
    res <- rbind(res, data.frame(
      id = as.character(df[[ind]]$id[1]),
      fppr = getDCG(res.map[,"fppr"], res.tmp),
      bppr = getDCG(res.map[,"bppr"], res.tmp),
      fbppr = getDCG(res.map[,"fbppr"], res.tmp),
      simrank = getDCG(res.map[,"simrank"], res.tmp),
      salsa = getDCG(res.map[,"salsa"], res.tmp)))
  }
#  print(paste("DCG", sum(res[,"fppr"] != res[,"fbppr"])))
#  print(paste("DCG", sum(res[,"bppr"] != res[,"fbppr"])))
  
#  print(res)
  return(res)
}

analysisCrowdFlower <- function(df, map, k = 3) {
  num2Char <- c("one","two","three","four","five","six","seven","eight","nine","ten")
  df <- split(df, df$id)
  res <- NULL
  for(ind in 1:length(df)) { # Every query node
    res.tmp <- as.character(unlist(df[[ind]][1,num2Char[df[[ind]]$choose_category_number_from_above]])) # All results
    res.tmp <- res.tmp[which(res.tmp != "-")] # Remove null responses just in case
    
    res.map <- map[which(as.character(map$id) == as.character(df[[ind]]$id[1])), ]
    res.map <- res.map[1:k,]
    
#    print(as.character(df[[ind]]$id[1]))
#     print(res.tmp)
#     print(res.map)
    
    res <- rbind(res, data.frame(
      id = as.character(df[[ind]]$id[1]),
      fppr = getPrecision(res.map[,"fppr"], res.tmp),
      bppr = getPrecision(res.map[,"bppr"], res.tmp),
      fbppr = getPrecision(res.map[,"fbppr"], res.tmp),
      simrank = getPrecision(res.map[,"simrank"], res.tmp),
      salsa = getPrecision(res.map[,"salsa"], res.tmp)))
  }
  return(res)
}

reslist <- c()
map <- NULL
max_topk = 0
for (filename in unique(str_replace(str_replace(str_replace(str_replace(list.files("/data/bshi/dataset/wikipedia/output/"), ".txt", ""), "PR", ""), "Sim", ""), "Salsa", ""))) {
#for (filename in c("xad","xai", "xbg","xbo","xcn","xdk","xdn","xec", "xer", "xfe", "xhk", "xig", "xil")) {
  if (filename != "xht") {
    res <- getResult(filename)
    tmptopk <- getTopK(res)
    max_topk <- ifelse(max_topk > length(tmptopk), max_topk, length(tmptopk))
    reslist[length(reslist)+1] <- paste(tmptopk, collapse = ",")
    map <- rbind(map, res)
  }
}

print(paste("largest one is ", max_topk))
newreslist <- c()
for (i in 1:length(reslist)) {
  splitted <- str_split(reslist[i], ",")[[1]]
  print(splitted)
  #print(reslist[i])
  #print(splitted)
  if (max_topk > length(splitted)) {
    print(paste("larger", max_topk, length(splitted)))
    smp <- sample(2:length(splitted), (max_topk - length(splitted)), replace = T)
    print(smp)
    splitted <- c(splitted, splitted[smp])
  }
  if(max_topk != length(splitted)) {
    print("fuck")
  }
  newreslist[length(newreslist)+1] <- paste(splitted, collapse = ",")
}

cf <- read.csv("/data/bshi/dataset/wikipedia/f717722.csv")

# Train linear combination
lam <- seq(0,1,by = 0.01)
linear.best.res <- -1
linear.bset.lam <- -1
for(p in lam) {
  map <- NULL
  print(p)
   for (filename in c("xad","xai", "xbg","xbo","xcn","xdk","xdn","xec", "xer", "xfe", "xhk", "xig", "xil")) {
    res <- getLinearCombinationResult(filename, p, 1-p)
    reslist[length(reslist)+1] <- paste(getTopK(res), collapse = ",")
    map <- rbind(map, res)
  }
  
  tmp <- analysisDCG(cf, map, k = 3)
  result.dcg <- melt(tmp, id=c("id"), measure.vars=c("fppr","bppr","fbppr", "simrank","salsa"))
  result.dcg.summary <- summarySE(data = result.dcg, measurevar = "value", groupvars = c("variable"))
  
  if (result.dcg.summary[result.dcg.summary$variable == "fbppr", "value"] > linear.best.res) {
    linear.best.res <- result.dcg.summary[result.dcg.summary$variable == "fbppr", "value"]
    linear.bset.lam <- p
    print(paste("find a better one with score",linear.best.res," lambda is",p))
  } else {
    print(paste("score",linear.best.res," lambda is",p))
  }
}

# Train Saturation combination
lam <- seq(0,1,by = 0.01)
kv <- seq(0,1, by = 0.01)
saturation.best.res <- -1
saturation.best.lam <- -1
saturation.best.kv <- -1
for(p in lam) {
  map <- NULL
  print(paste("w",p))
  for(kk in kv) {
    #print(paste("k",kk))
    for (filename in c("xad","xai", "xbg","xbo","xcn","xdk","xdn","xec", "xer", "xfe", "xhk", "xig", "xil")) {
      res <- getSaturationCombinationResult(filename, p, 1-p)
      reslist[length(reslist)+1] <- paste(getTopK(res), collapse = ",")
      map <- rbind(map, res)
    }
    tmp <- analysisDCG(cf, map, k = 3)
    result.dcg <- melt(tmp, id=c("id"), measure.vars=c("fppr","bppr","fbppr", "simrank","salsa"))
    result.dcg.summary <- summarySE(data = result.dcg, measurevar = "value", groupvars = c("variable"))
    
    if (result.dcg.summary[result.dcg.summary$variable == "fbppr", "value"] > saturation.best.res) {
      saturation.best.res <- result.dcg.summary[result.dcg.summary$variable == "fbppr", "value"]
      saturation.best.lam <- p
      saturation.best.kv <- kk
      print(paste("find a better one with score",saturation.best.res," lambda is",p,"k is",kk))
    }
  }
}

# Draw precision
for(i in 1:3) {
  tmp <- analysisCrowdFlower(cf, map, k = i)
  result.precision <- melt(tmp, id=c("id"), measure.vars=c("fppr","bppr","fbppr", "simrank","salsa"))
  result.precision.summary <- summarySE(data = result.precision, measurevar = "value", groupvars = c("variable"))
  
  tmp.g <- ggplot(result.precision.summary, aes(x = variable, y = value)) + geom_point() + 
    geom_errorbar(aes(min=value - ci, max = value + ci), color = "red", width = 0.15) +
    geom_errorbar(aes(min=value - se, max = value + se), color = "blue", width = 0.15) +
    ylab(paste("Precision",i,sep = "@")) +
    xlab("Models") +
    theme_classic()
  print(tmp.g)
}

# Draw DCG
for(i in 1:3) {
  tmp <- analysisDCG(cf, map, k = i)
  result.dcg <- melt(tmp, id=c("id"), measure.vars=c("fppr","bppr","fbppr", "simrank","salsa"))
  result.dcg.summary <- summarySE(data = result.precision, measurevar = "value", groupvars = c("variable"))
  
  tmp.g <- ggplot(result.dcg.summary, aes(x = variable, y = value)) + geom_point() + 
    geom_errorbar(aes(min=value - ci, max = value + ci), color = "red", width = 0.15) +
    geom_errorbar(aes(min=value - se, max = value + se), color = "blue", width = 0.15) +
    ylab(paste("DCG",i,sep = "@")) +
    xlab("Models") +
    theme_classic()
  print(tmp.g)
}


# tmp.acc <- analysisCrowdFlowerAccuracy(cf, map)
# result.accuracy <- melt(tmp.acc, id=c("id"), measure.vars=c("fppr","bppr","fbppr", "simrank","salsa"))
# result.accuracy.summary <- summarySE(data = result.accuracy, measurevar = "value", groupvars = c("variable"))
# 
# ggplot(result.accuracy.summary, aes(x = variable, y = value)) + geom_point() + 
#   geom_errorbar(aes(min=value - ci, max = value + ci), color = "red", width = 0.15) +
#   geom_errorbar(aes(min=value - se, max = value + se), color = "blue", width = 0.15) +
#   geom_errorbar(aes(min=value - sd, max = value + sd), color = "grey", width = 0.15) +
#   ylab("Accuracy") +
#   xlab("Models") +
#   theme_classic()
