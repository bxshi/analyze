library(bear)
source("./functions.R")

# Analysis MAP by single community
# fbppr_file filepath of fbppr result
# community_file filepath of community file, [id,cluster] per line
# Note for this method, each id belongs to exactly one cluster
analysis <- function(fbppr_file, community_file, lambda, draw=FALSE) {
  df <- read.csv(fbppr_file)
  lbl <- read.csv(community_file)
  
  # Combine fbppr data and community results
  df <- merge(df, lbl)
  
  # Split into groups based on queryId
  df.splitted <- split(df, f = df$query_id) # A list of splitted data frame
  df.splitted <- lapply(df.splitted, function(df.current){
    # Preprocessing
    query.node<- df.current[which(df.current$fppr_rank == 0),] # Extract query point
    df.current <- df[which(df.current$fppr_rank != 0), ] # Remove query point from data frame
    # Calculate average precision
    df.current <- cbind(fbppr.combine(df.current, query.node, .interval = lambda), query_id=query.node$id)
  })
  
  df <- ldply(df.splitted)
  df.summary <- summarySE(df, measurevar = "AP", groupvars = c("label", "rank"))
  
  if(draw) {
    ggplot(df.summary, aes(x=rank, y=AP, group=label, color=label, linetype=label)) + 
      geom_point() + geom_line() + 
      #geom_errorbar(aes(ymin=AP-se, ymax=AP+se), width=.1) +
      theme_classic()
  }
}