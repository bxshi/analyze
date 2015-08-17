titlemap <- read.csv("/data/bshi/dataset/dblp_citation/dblp.citation.titlemap.csv", 
                     header=FALSE, col.names=c("id","name"))

rankAnalysis <- function(filepath="~/withourpaper.txt") {
  dat <- read.csv(filepath, header=FALSE, sep=" ",
                  col.names=c("id", "dst", "fppr_score", "fppr_rank", "bppr_score", "bppr_rank", "comm_size"))
  dat <- dat[which(dat$dst != 4373457),]
  dat <- merge(dat, titlemap, by.x = c("dst"), by.y=c("id"))
  #dat$ratio <- dat$bppr_score * (1 - dat$bppr_rank / dat$comm_size) + dat$fppr_score
  dat$ratio <- 0.95 * (1 - dat$bppr_rank / dat$comm_size) + 0.05 * (1 - dat$fppr_rank / 32) 
  #dat$ratio <- dat$bppr_score + dat$fppr_score
  dat$name <- as.character(dat$name)
  return(dat)
  print(dat[order(dat$ratio, decreasing = TRUE),])
  dat.10 <- dat[which(dat$fppr_rank < 10),]
  #View(dat.10[order(dat.10$ratio, decreasing=TRUE),])
  #View(dat.10[order(dat.10$fppr_score, decreasing=TRUE),])
  #View(dat.10[order(dat.10$bppr_score, decreasing=TRUE),])  
}