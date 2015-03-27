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
# 
rank_analysis <- function(simrank_file, lbl, community) {
  df <- read.csv(simrank_file)
  colnames(df) <- c("query_id", "id", "rank")
  df.splitted <- split(df, f = df$query_id)
  
  df.splitted <- df.splitted[unlist(lapply(df.splitted,function(x){dim(x)[1] == 20}))]
  
  df.splitted <- lapply(df.splitted, function(df.current){
    # Preprocessing
    query.node <- df.current[which(df.current$rank == 0),] # Extract query point
    df.current <- df.current[which(df.current$rank != 0), ] # Remove query point from data frame
    # Calculate Jaccard coefficient for each rank
    df.current <- cbind(simrank.coeff(df.current, lbl, query.node, community), query_id=query.node$id)
  })
  df <- ldply(df.splitted)
  return(summarySE(df, measurevar = "coeff", groupvars = c("label", "rank")))
}

# Analysis MAJ with multiple communities
# Precision is defined as whether this node in the same community as query node
# Recall is defined as how many communities that query point belongs to have been discovered 
# fbppr_file filepath of fbppr result
# community_file filepath of community file, [id, ids that in the same cluster] per line
multi_comm_analysis <- function(fbppr_file, simrank_file, salsa_file, community_file, lambda, draw=TRUE, errbar = FALSE, normalize = FALSE, ap = FALSE, multiply = FALSE) {
  
  df <- read.csv(fbppr_file)
  comm <- read.csv(community_file)
  
  # Split into groups based on queryId
  df.splitted <- split(df, f = df$query_id) # A list of splitted data frame
  
  # This should not happen, check generation codes
  df.splitted <- df.splitted[unlist(lapply(df.splitted,function(x){dim(x)[1] == 20}))]
  
  df.splitted <- lapply(df.splitted, function(df.current){
    # Preprocessing
    query.node <- df.current[which(df.current$fppr_rank == 0),] # Extract query point
    df.current <- df.current[which(df.current$fppr_rank != 0), ] # Remove query point from data frame
    # Calculate Jaccard coefficient for each rank
    df.current <- cbind(fbppr.coeff(df.current, query.node, comm, .interval = lambda, .normalized = normalize, .ap = ap, .multiply = multiply), query_id=query.node$id)
  })
  
  df <- ldply(df.splitted)
  df.summary <- summarySE(df, measurevar = "coeff", groupvars = c("label", "rank"))
  if(simrank_file != ""){
    df.summary <- rbind(df.summary,rank_analysis(simrank_file = simrank_file, "simrank", community = comm))
  }
  if(salsa_file != "") {
    df.summary <- rbind(df.summary, rank_analysis(simrank_file = salsa_file, "salsa", community = comm))
  }
  
  g <- ggplot(df.summary, aes(x=rank, y=coeff, group=label, color=label, linetype=label)) + 
    geom_point(position=position_dodge(0.1)) + 
    geom_line(position=position_dodge(0.1)) + 
    scale_y_continuous(name="Jaccard Coefficient") +
    theme_classic()
    if (errbar){
      g <- g + geom_errorbar(aes(ymin=coeff-se, ymax=coeff+se), width=.1,
                             position=position_dodge(0.1))
    }
  g
}