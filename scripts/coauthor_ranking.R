titlemap <- read.csv("/data/bshi/dataset/my_dblp_collaboration/author.titlemap.csv", 
                     header=FALSE, col.names=c("id","name"))

rankAnalysis <- function(filepath="/home/lyang5/output_Chr.txt") {
  dat <- read.csv(filepath, header=FALSE, sep=" ",
                  col.names=c("id", "fppr_score", "fppr_rank", "bppr_score", "bppr_rank"))
  
  dat <- merge(dat, titlemap)
  dat$ratio <- 1 - dat$bppr_score / dat$fppr_score
  dat.10 <- dat[which(dat$fppr_rank < 10),]
  View(dat.10[order(dat.10$bppr_score, decreasing=TRUE),])
}