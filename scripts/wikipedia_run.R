library(reshape2)
library(bear)
library(ggplot2)

raw.data <- getRawData(unique(str_replace(str_replace(str_replace(str_replace(list.files("/data/bshi/dataset/wikipedia/output/"), ".txt", ""), "PR", ""), "Sim", ""), "Salsa", "")))

algorithm.dat <- NULL
for (filename in unique(str_replace(str_replace(str_replace(str_replace(list.files("/data/bshi/dataset/wikipedia/output/"), ".txt", ""), "PR", ""), "Sim", ""), "Salsa", ""))) {
  algorithm.dat <- rbind(algorithm.dat, getResult(filename))
}

human.dat <- read.csv("/data/bshi/dataset/wikipedia/f721534.csv")

human.res <- extractHumanPreference(human.dat)

human.idcg <- getIDCG(human.res, topk = 3)


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

best.mean <- 0
best.w <- 0
for(w in seq(0, 1, by = 0.0001)) {
  dat.fbppr <- raw.data[, c("query_name", "name", "fppr_score", "bppr_score")]
  dat.fbppr$value <- w * (dat.fbppr$fppr_score) + (1 - w) * (dat.fbppr$bppr_score)
  dat.fbppr$fppr_score <- NULL
  dat.fbppr$bppr_score <- NULL
  names(dat.fbppr) <- c("id", "choose", "value")
  dat.fbppr.cor <- compare(dat.fbppr, human.res)
  if (summary(dat.fbppr.cor)[4] > best.mean){
    best.mean <- summary(dat.fbppr.cor)[4]
    best.w <- w
    print(paste("best w", w, "value", best.mean))
  }
}


algorithm.res <- getDCG(algorithm.dat, human.res, human.idcg)

algorithm.res.summary <- summarySE(algorithm.res, measurevar = "ndcg", groupvars = c("model"))

linear.res <- getLinearTopK(raw.data)
linear.res.par <- getDCGParallel(linear.res, human.res, human.idcg)
linear.res.par.summary <- summarySE(linear.res.par, measurevar = "ndcg", groupvars = c("model"))


saturation.res <- getSaturationTopK(raw.data)
saturation.ndcg <- getDCGParallel(saturation.res, human.res, human.idcg)
saturation.ndcg.summary <- summarySE(saturation.ndcg, measurevar = "ndcg", groupvars = c("model"))

saturation.res.small <- getSaturationTopK(raw.data)
saturation.ndcg.small <- getDCGParallel(saturation.res.small, human.res, human.idcg)
saturation.ndcg.summary.small <- summarySE(saturation.ndcg.small, measurevar = "ndcg", groupvars = c("model"))

dat.all <- rbind(saturation.ndcg[which(saturation.ndcg$model == "r0.166k10.003k20.001"),],
                 linear.res.res[which(linear.res.res$model == 0.049),],
                 algorithm.res)