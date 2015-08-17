library(reshape2)
library(bear)
library(ggplot2)
library(stringr)

raw.data <- getRawData(unique(str_replace(str_replace(str_replace(str_replace(str_replace(list.files("/data/bshi/dataset/wikipedia/output/"), "aa.txt",""), ".txt", ""), "PR", ""), "Sim", ""), "Salsa", "")))

#raw.data.splitted <- split(raw.data, raw.data$query_name)
#raw.data.validNames <- names(which(unlist(lapply(raw.data.splitted, function(x){nrow(x) - length(which(x$bppr_score == 0))})) != 0))
#raw.data <- raw.data[which(raw.data$query_name %in% raw.data.validNames),]

algorithm.dat <- NULL
for (filename in unique(str_replace(
                          str_replace(
                            str_replace(
                              str_replace(
                                str_replace(list.files("/data/bshi/dataset/wikipedia/output/"), "aa.txt","" ),
                              ".txt", ""),
                            "PR", ""),
                          "Sim", ""),
                        "Salsa", ""))) {
  algorithm.dat <- rbind(algorithm.dat, getResult(filename))
}

#algorithm.dat <- algorithm.dat[which(algorithm.dat$id %in% raw.data.validNames),]

human.dat <- read.csv("/data/bshi/dataset/wikipedia/f721534.csv")
human.dat$id <- str_replace(human.dat$id, ",", "_")
#human.dat <- human.dat[which(human.dat$id %in% raw.data.validNames),]

human.res <- extractHumanPreference(human.dat)

human.idcg <- getIDCG(human.res, topk = 5)


dat.fppr <- raw.data[,c("query_name", "name", "fppr_score")]
names(dat.fppr) <- c("id", "choose", "value")

dat.fppr.cor <- compare(dat.fppr, human.res)

dat.bppr <- raw.data[, c("query_name", "name", "bppr_score")]
names(dat.bppr) <- c("id", "choose", "value")
dat.bppr.cor <- unlist(compare(dat.bppr, human.res))

dat.sim <- raw.data[, c("query_name", "name", "sim_score")]
names(dat.sim) <- c("id", "choose", "value")
dat.sim.cor <- compare(dat.sim, human.res)

dat.salsa <- raw.data[, c("query_name", "name", "query_auth_score")]
names(dat.salsa) <- c("id", "choose", "value")
dat.salsa.cor <- unlist(compare(dat.salsa, human.res))

dat.aa <- raw.data[, c("query_name", "name", "aa_score")]
names(dat.aa) <- c("id", "choose", "value")
dat.aa.cor <- compare(dat.aa, human.res)

#best.mean <- 0
#best.w <- 0
#for(w in seq(0, 1, by = 0.001)) {
#  dat.fbppr <- raw.data[, c("query_name", "name", "fppr_score", "bppr_score")]
#  dat.fbppr$value <- w * (dat.fbppr$fppr_score) + (1 - w) * (dat.fbppr$bppr_score)
#  dat.fbppr$fppr_score <- NULL
#  dat.fbppr$bppr_score <- NULL
#  names(dat.fbppr) <- c("id", "choose", "value")
#  dat.fbppr.cor <- compare(dat.fbppr, human.res)
#  if (summary(unlist(dat.fbppr.cor))[4] > best.mean){
#    best.mean <- summary(unlist(dat.fbppr.cor))[4]
#    best.w <- w
#    print(paste("best w", w, "value", best.mean))
#  }
#  print(w)
#}


algorithm.res <- getDCG(algorithm.dat, human.res, human.idcg, topk = 5)

algorithm.res.summary <- summarySE(algorithm.res, measurevar = "ndcg", groupvars = c("model"))

#linear.res <- getLinearTopK(raw.data, topk = 5)
#linear.res.par <- getDCGParallel(linear.res, human.res, human.idcg, topk = 5)
#linear.res.par.summary <- summarySE(linear.res.par, measurevar = "ndcg", groupvars = c("model"))


saturation.res <- getSaturationTopK(raw.data, topk = 5)
saturation.ndcg <- getDCGParallel(saturation.res, human.res, human.idcg, topk = 5)
saturation.ndcg.summary <- summarySE(saturation.ndcg, measurevar = "ndcg", groupvars = c("model"))

draw.dat.all <- rbind(draw.dat.all, algorithm.res.summary[which(algorithm.res.summary$model %in% c("fppr","bppr", "salsa", "simrank", "aa")),],
                  #linear.res.par.summary[which.max(linear.res.par.summary$ndcg),],
                  saturation.ndcg.summary[which.max(saturation.ndcg.summary$ndcg),])
draw.dat.all$model <- rep(c("pPR", "BPPR","pSALSA","SimRank", "Adamic Adar", "FBpPR"), 2)
draw.dat.all$type <- c(rep("white",6),rep("grey",6))
g <- ggplot(draw.dat.all[which(draw.dat.all$model != "BPPR"),], aes(x = model, y=ndcg, fill=type)) + geom_bar(stat="identity", color="black", position=position_dodge()) +
  scale_fill_manual(values=rev(unique(draw.dat.all$type)))+
  geom_errorbar(aes(ymin=ndcg - ci, ymax=ndcg + ci), position =position_dodge(.9), width=.2, size = 0.2) +
  geom_errorbar(aes(ymin=ndcg - se, ymax=ndcg + se), colour = "red", position =position_dodge(.9), width=.2, size = 0.2) +
  theme_classic() +
  coord_cartesian(ylim=c(0, 0.75)) +
  xlab("Models") +
  ylab(expression(nDCG[5])) +
  theme(panel.background = element_rect(colour = "black", size=1),
        legend.position = "none",
        legend.title=element_blank())
ggsave("~/ndcg5_wrand.eps", g, width = 5, height = 5)
