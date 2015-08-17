library(igraph)
library(plyr)

g <- graph(edges = c(4,1,4,2,4,3,
                     5,4,
                     6,4,
                     7,4,7,5,7,6,7,8,
                     8,9,8,10,8,11,
                     12,8,
                     13,8,
                     14,8), directed=T)

V(g)$color <- ifelse(V(g) <= 7, "white", "grey")
V(g)$label <- c("A","B","C","D","E","F","G","H","I","J","K","L","M","N")

#setEPS()
#postscript("./imgs/bad_example.eps", fonts=c("serif", "Palatino"), width=5, height=5)
plot(g, layout=layout.kamada.kawai)
#dev.off()

g.ppr <- read.csv("/data/bshi/dataset/bad_example/ppr.13.csv", header = FALSE, sep = " ")
g.ppr$id <- V(g)$label
g.order.fppr <- g.ppr[order(-g.ppr$V3), "id"]
g.order.bppr <- g.ppr[order(-g.ppr$V5), "id"]
g.order.fbppr <- g.ppr[order(-g.ppr$V3 + -g.ppr$V5), "id"]

g.salsa <- read.csv("/data/bshi/dataset/bad_example/salsa.13.csv", header = FALSE, sep = " ")
g.salsa$id <- V(g)$label

g.sim <- read.csv("/data/bshi/dataset/bad_example/sim.13.csv", header = FALSE, sep = " ")
g.sim$id <- V(g)$label

