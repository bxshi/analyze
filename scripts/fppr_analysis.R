library(bear)

source("./functions.R")

# Analysis MAP by single community
# fbppr_file filepath of fbppr result
# community_file filepath of community file, [id,cluster] per line
# Note for this method, each id belongs to exactly one cluster
single_comm_analysis <- function(fbppr_file, community_file, lambda) {
  df <- read.csv(fbppr_file)
  comm <- read.csv(community_file)
  
  # Combine fbppr data and community results
  df <- merge(df, comm)
  
  # Split into groups based on queryId
  df.splitted <- split(df, f = df$query_id) # A list of splitted data frame
  df.splitted <- lapply(df.splitted, function(df.current){
    # Preprocessing
    query.node<- df.current[which(df.current$fppr_rank == 0),] # Extract query point
    df.current <- df.current[which(df.current$fppr_rank != 0), ] # Remove query point from data frame
    # Calculate average precision
    df.current <- cbind(fbppr.combine(df.current, query.node, .interval = lambda), query_id=query.node$id)
  })
  
  df <- ldply(df.splitted)
  df.summary <- summarySE(df, measurevar = "AP", groupvars = c("label", "rank"))
  
  ggplot(df.summary, aes(x=rank, y=AP, group=label, color=label, linetype=label)) + 
    geom_point() + geom_line() + 
    #geom_errorbar(aes(ymin=AP-se, ymax=AP+se), width=.1) +
    theme_classic()
}

# Calculate MAJ for other ranking algorithms
#' @param file csv file of ranking result, [query_id, id, rank] per line
#' @param lbl text label for the line
#' @param community community dataframe 
#' @param .ap Calculate MAJ or Jaccard, default is Jaccard
rank_analysis <- function(file, lbl, community, .ap = FALSE) {
  df <- read.csv(file, header=FALSE, sep=" ")
  colnames(df) <- c("query_id", "id", "rank", "score")
  df.splitted <- split(df, f = df$query_id)
  
  # If by any chance, there are less than 20 retrieved documents, remove them
  df.splitted <- df.splitted[unlist(lapply(df.splitted,function(x){dim(x)[1] == 20}))]
  
  # Calculae MAJ for every query node
  df.splitted <- lapply(df.splitted, function(df.current){
    # Preprocessing
    query.node <- df.current[which(df.current$rank == 0),] # Extract query point
    df.current <- df.current[which(df.current$rank != 0), ] # Remove query point from data frame
    if(nrow(query.node) == 0) query.node <- data.frame(id = df.current[which(df.current$rank != 0), "query_id"][1],
                                                       query_id = df.current[which(df.current$rank != 0), "query_id"][1])
    # Calculate Jaccard coefficient for each rank
    df.current <- cbind(rank.coeff(df.current, lbl, query.node, community, .ap = .ap), query_id=query.node$id)
  })
  df <- ldply(df.splitted)
  return(summarySE(df, measurevar = "coeff", groupvars = c("label", "rank")))
}

# Analysis MAJ with multiple communities
# Precision is defined as whether this node in the same community as query node
# Recall is defined as how many communities that query point belongs to have been discovered 
# fbppr_file filepath of fbppr result
# community_file filepath of community file, [id, ids that in the same cluster] per line
#' @param fbppr_file result of our algorithm
#' @param simrank_file simrank result, [query_id, id, rank] per line
#' @param sala_file salsa result, [query_id, id, rank] per line
#' @param lambda THIS SHOULD BE DISCARD
#' @param draw THIS SHOULD ALWAYS BE TRUE
#' @param errbar Draw error bar or not
#' @param normalized THIS SHOULD BE DISCARDED
#' @param ap do average jaccard or not
#' @param multiple DISCARD this
multi_comm_analysis <- function(fbppr_file, simrank_file, salsa_file, aa_file,
                                community_file, titlemap_file, deg_file, lambda=c(0.05,0.5,0.95,1), draw=TRUE, 
                                errbar = FALSE, normalize = FALSE, 
                                ap = T, multiply = FALSE, topK=20) {
  
  df <- NULL
  if(typeof(fbppr_file) == "character"){
    df <- read.csv(fbppr_file, header = FALSE, sep=" ")
    colnames(df) <- c("id2","id","fppr_score","fppr_rank","bppr_score","bppr_rank")
    df$query_id <- df$id2
  } else {
    df <- fbppr_file
  }
  
  comm <- read.csv(community_file)
  
  #titlemap <- NULL
  #if (typeof(titlemap_file) == "character") {
  #  titlemap <- read.csv(titlemap_file, header=FALSE)
  #} else {
  #  titlemap <- titlemap_file
  #}
  #colnames(titlemap) <- c("id", "title")
  
  # Fill up missing columns
  
  #fill_up_data(df, titlemap)
  
  df <- df[which(df$fppr_rank < topK),]
  
  # Split into groups based on queryId
  df.splitted <- split(df, f = df$query_id) # A list of splitted data frame
  
  # This should not happen, check generation codes
  #df.splitted <- df.splitted[unlist(lapply(df.splitted,function(x){dim(x)[1] == 20}))]
  
  degs <- read.csv(deg_file, sep=" ",header=F)
  colnames(degs) <- c("id", "outd", "ind")
  
  print("start calculation")
  
  df.splitted <- lapply(df.splitted, function(df.current){
    # Preprocessing
    query.node <- df.current[which.max(df.current$fppr_score),] # Extract query point
    df.current <- df.current[which(df.current$id != query.node$id), ] # Remove query point from data frame
    # Calculate Jaccard coefficient for each rank
    df.current <- cbind(fbppr.coeff(df.current, query.node, comm, degs, .interval = lambda, .normalized = normalize, .ap = ap, .multiply = multiply), query_id=query.node$id)
  })
  
  print("start calculate summary")
  
  df <- ldply(df.splitted)
  df.summary <- summarySE(df, measurevar = "coeff", groupvars = c("label", "rank"))
  df.summary <- rbind(df.summary, rank_analysis(file = simrank_file, "SimRank", community = comm, .ap = ap))
  df.summary <- rbind(df.summary, rank_analysis(file = salsa_file, "pSALSA", community = comm, .ap = ap))
  df.summary <- rbind(df.summary, rank_analysis(file = aa_file, "Adamic Adar", community = comm, .ap = ap))

  return(df.summary)
  #View(df)
  g <- ggplot(df.summary[df.summary$ra], aes(x=rank, y=coeff, group=label, color=label, linetype=label)) + 
    geom_point(position=position_dodge(0.1)) + 
    geom_line(position=position_dodge(0.1)) + 
    scale_y_continuous(name="Jaccard Coefficient") +
    theme_classic()
    if (errbar){
      g <- g + geom_errorbar(aes(ymin=coeff-se, ymax=coeff+se), width=.1,
                             position=position_dodge(0.1))
    }
  return(g)
}

github <- multi_comm_analysis(fbppr_file = "/data/bshi/dataset/github_collaboration/pr.community.50iter.txt",
                              simrank_file = "/data/bshi/dataset/github_collaboration/simrank.community.txt",
                              salsa_file = "/data/bshi/dataset/github_collaboration/salsa.community.txt",
                              aa_file = "/data/bshi/dataset/github_collaboration/aa.community.txt",
                              community_file = "/data/bshi/dataset/github_collaboration/github.collaboration.community.csv",
                              titlemap_file = "/data/bshi/dataset/github_collaboration/github.collaboration.titlemap.csv",
                              deg_file = "/home/lyang5/git.outin.txt",
                              lambda = c(0.05,0.5,0.95,1), ap = T, topK=20)

github$label <- as.character(github$label)
github$label <- ifelse(github$label == "lambda  0.05", "FBS \u03BB=0.05", github$label)
github$label <- ifelse(github$label == "lambda  0.5", "FBS \u03BB=0.5", github$label)
github$label <- ifelse(github$label == "lambda  0.95", "FBS \u03BB=0.95", github$label)
github$label <- ifelse(github$label == "lambda  1", "PPR", github$label)

github$label <- factor(github$label, levels=c("FBS \u03BB=0.05",
                                              "FBS \u03BB=0.5", 
                                              "FBS \u03BB=0.95", 
                                              "PPR", "pSALSA", "SimRank", "Adamic Adar"))


cbPalette <- c("#262626", "#ff0000", "#00ff00", "#0000ff", "#b366ff", "#66ffb3", "#ffb366")

g <- ggplot(github[which(github$rank<=10),], aes(x=rank, y=coeff, group=label, color=label, linetype=label, shape=label)) + 
  geom_point(size=4) + geom_line() + 
  scale_x_discrete(expand=c(0,0), name="Rank") +
  scale_y_continuous(expand=c(0.05,0), name="Mean Average Jaccard Coefficient") +
  scale_linetype_manual(values=c("solid", "solid", "solid", "dashed", "dotted", "dotdash", "twodash")) +
  scale_shape_manual(values=c(15,16,17,7,8,9,11))+
  ylab("True Positive Rate") +
  xlab("False Positive Rate") +
  #scale_color_manual(values=cbPalette) +  
  scale_color_brewer(palette = "Dark2") +
  theme_classic() +
  guides(fill=guide_legend(ncol=2))+
  theme(panel.background = element_rect(colour = "black", size=1),
        legend.justification=c(0,0), legend.position=c(0.65,-0.03),
        legend.title=element_blank())

setEPS()
postscript("~/github_top10_k20_pretty.eps", encoding="Greek", width=5, height=5)
g
dev.off()

coauthor <- multi_comm_analysis(fbppr_file = "/data/bshi/dataset/my_dblp_collaboration/ppr.community.new.txt",
                              simrank_file = "/data/bshi/dataset/my_dblp_collaboration/simrank.community.new.txt",
                              salsa_file = "/data/bshi/dataset/my_dblp_collaboration//salsa.community.new.txt",
                              aa_file = "/data/bshi/dataset/my_dblp_collaboration/aa.community.txt",
                              community_file = "/data/bshi/dataset/my_dblp_collaboration/communities.csv",
                              titlemap_file = "/data/bshi/dataset/my_dblp_collaboration/author.titlemap.csv",
                              deg_file = "/home/lyang5/dco.outin.txt",
                              lambda = c(0.05,0.5,0.95,1), ap = T, topK=20)

coauthor$label <- as.character(coauthor$label)
coauthor$label <- ifelse(coauthor$label == "lambda  0.05", "FBS \u03BB=0.05", coauthor$label)
coauthor$label <- ifelse(coauthor$label == "lambda  0.5", "FBS \u03BB=0.5", coauthor$label)
coauthor$label <- ifelse(coauthor$label == "lambda  0.95", "FBS \u03BB=0.95", coauthor$label)
coauthor$label <- ifelse(coauthor$label == "lambda  1", "PPR", coauthor$label)

coauthor$label <- factor(coauthor$label, levels=c("FBS \u03BB=0.05",
                                                  "FBS \u03BB=0.5", 
                                                  "FBS \u03BB=0.95", 
                                                  "PPR", "pSALSA", "SimRank", "Adamic Adar"))

g <- ggplot(coauthor[which(coauthor$rank<=10),], aes(x=rank, y=coeff, group=label, color=label, linetype=label, shape=label)) + 
  geom_point(size=4) + geom_line() + 
  scale_x_discrete(expand=c(0,0), name="Rank") +
  scale_y_continuous(expand=c(0.05,0), name="Mean Average Jaccard Coefficient") +
  scale_linetype_manual(values=c("solid", "solid", "solid", "dashed", "dotted", "dotdash", "twodash")) +
  scale_shape_manual(values=c(15,16,17,7,8,9,11))+
  ylab("True Positive Rate") +
  xlab("False Positive Rate") +
  #scale_color_manual(values=cbPalette) +  
  scale_color_brewer(palette = "Dark2") +
  theme_classic() +
  guides(fill=guide_legend(ncol=2))+
  theme(panel.background = element_rect(colour = "black", size=1),
        legend.justification=c(0,0), legend.position=c(0.6,-0.03),
        legend.title=element_blank())

setEPS()
postscript("~/coauthor_top10_k20_pretty.eps", encoding="Greek", width=5, height=5)
g
dev.off()

citation <- multi_comm_analysis(fbppr_file = "/data/bshi/dataset/dblp_citation/ppr.reduced.community.txt",
                                simrank_file = "/data/bshi/dataset/dblp_citation/simrank.reduced.community.txt",
                                salsa_file = "/data/bshi/dataset/dblp_citation/salsa.reduced.community.txt",
                                aa_file = "/data/bshi/dataset/dblp_citation//aa.reduced.community.txt",
                                community_file = "/data/bshi/dblp/paper_condensed_community.csv",
                                titlemap_file = "/data/bshi/dataset/dblp_citation/dblp.citation.digraph.withcomm.csv",
                                deg_file = "/home/lyang5/dci.outin.txt",
                                lambda = c(0.05, 0.5, 0.95, 1), ap = T, topK = 20)

citation$label <- as.character(citation$label)
citation$label <- ifelse(citation$label == "lambda  0.05", "FBS \u03BB=0.05", citation$label)
citation$label <- ifelse(citation$label == "lambda  0.5", "FBS \u03BB=0.5", citation$label)
citation$label <- ifelse(citation$label == "lambda  0.95", "FBS \u03BB=0.95", citation$label)
citation$label <- ifelse(citation$label == "lambda  1", "PPR", citation$label)

citation$label <- factor(citation$label, levels=c("FBS \u03BB=0.05",
                                                  "FBS \u03BB=0.5", 
                                                  "FBS \u03BB=0.95", 
                                                  "PPR", "pSALSA", "SimRank", "Adamic Adar"))

g <- ggplot(citation[which(citation$rank<=10),], aes(x=rank, y=coeff, group=label, color=label, linetype=label, shape=label)) + 
  geom_point(size=4) + geom_line() + 
  scale_x_discrete(expand=c(0,0), name="Rank") +
  scale_y_continuous(expand=c(0.05,0), name="Mean Average Jaccard Coefficient") +
  scale_linetype_manual(values=c("solid", "solid", "solid", "dashed", "dotted", "dotdash", "twodash")) +
  scale_shape_manual(values=c(15,16,17,7,8,9,11))+
  ylab("True Positive Rate") +
  xlab("False Positive Rate") +
  #scale_color_manual(values=cbPalette) +  
  scale_color_brewer(palette = "Dark2") +
  theme_classic() +
  guides(fill=guide_legend(ncol=2))+
  theme(panel.background = element_rect(colour = "black", size=1),
        legend.justification=c(0,0), legend.position=c(0.6,-0.02),
        legend.title=element_blank()) #34,53,24

setEPS()
postscript("~/citation_venue_top10_k20_pretty.eps", encoding="Greek", width=5, height=5)
g
dev.off()
embed_fonts("~/citation.eps", outfile="~/citation.embedded.eps")

citation <- multi_comm_analysis(fbppr_file = "/data/bshi/dataset/dblp_citation/ppr.all.community.txt",
                                simrank_file = "/data/bshi/dataset/dblp_citation/simrank.all.community.txt",
                                salsa_file = "/data/bshi/dataset/dblp_citation/salsa.all.community.txt",
                                aa_file = "/data/bshi/dataset/dblp_citation//aa.community.txt",
                                community_file = "/data/bshi/dblp/author_communities.csv",
                                titlemap_file = "/data/bshi/dataset/dblp_citation/dblp.citation.titlemap.csv",
                                lambda = c(0.05,0.5,0.95,1), ap = T)

coauthor2$label <- as.character(coauthor2$label)
coauthor2$label <- ifelse(coauthor2$label == "lambda  0.05", "CAS \u03BB=0.05", coauthor2$label)
coauthor2$label <- ifelse(coauthor2$label == "lambda  0.5", "CAS \u03BB=0.5", coauthor2$label)
coauthor2$label <- ifelse(coauthor2$label == "lambda  0.95", "CAS \u03BB=0.95", coauthor2$label)
coauthor2$label <- ifelse(coauthor2$label == "lambda  1", "PPR", coauthor2$label)



g <- ggplot(coauthor2[which(coauthor2$rank<=10),], aes(x=rank, y=coeff, group=label, color=label, linetype=label, shape=label)) + 
  geom_point(size=2.8) + geom_line(size=1.2) + 
  scale_x_discrete(expand=c(0,0), name="Rank") +
  scale_y_continuous(expand=c(0,0), name="Mean Average Jaccard Coefficient") +
  ylab("True Positive Rate") +
  xlab("False Positive Rate") +
  #scale_color_manual(values=cbPalette) +  
  scale_color_brewer(palette = "Dark2") +
  theme_classic() +
  guides(fill=guide_legend(ncol=2))+
  theme(panel.background = element_rect(colour = "black", size=1),
        legend.justification=c(0,0), legend.position=c(0.6,0),
        legend.title=element_blank())

setEPS()
postscript("~/coauthor2.eps", encoding="Greek", width=5, height=5)
g
dev.off()

