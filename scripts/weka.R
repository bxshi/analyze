library(RWeka)

multiplot <- function(..., plotlist=NULL, file, cols=1, layout=NULL) {
  library(grid)
  
  # Make a list from the ... arguments and plotlist
  plots <- c(list(...), plotlist)
  
  numPlots = length(plots)
  
  # If layout is NULL, then use 'cols' to determine layout
  if (is.null(layout)) {
    # Make the panel
    # ncol: Number of columns of plots
    # nrow: Number of rows needed, calculated from # of cols
    layout <- matrix(seq(1, cols * ceiling(numPlots/cols)),
                     ncol = cols, nrow = ceiling(numPlots/cols))
  }
  
  if (numPlots==1) {
    print(plots[[1]])
    
  } else {
    # Set up the page
    grid.newpage()
    pushViewport(viewport(layout = grid.layout(nrow(layout), ncol(layout))))
    
    # Make each plot, in the correct location
    for (i in 1:numPlots) {
      # Get the i,j matrix positions of the regions that contain this subplot
      matchidx <- as.data.frame(which(layout == i, arr.ind = TRUE))
      
      print(plots[[i]], vp = viewport(layout.pos.row = matchidx$row,
                                      layout.pos.col = matchidx$col))
    }
  }
}

# Refresh caches
WPM("refresh-cache")

# Load data
load_data <- function() {
  fbppr.true <- read.csv("/data/bshi/dataset/dblp_citation/true_links.txt", header=FALSE, sep=" ",
                         col.names=c("query_id","id", "fppr_score", "fppr_rank", "bppr_score", "bppr_rank"))
  fbppr.false <- read.csv("/data/bshi/dataset/dblp_citation/false_links.txt", header=FALSE, sep=" ",
                          col.names=c("query_id","id", "fppr_score", "fppr_rank", "bppr_score", "bppr_rank"))
  fbppr.true$label <- "T"
  fbppr.false$label <- "F"
  
  fbppr <- rbind(fbppr.true, fbppr.false)
  
  sim.true <- read.csv("/data/bshi/dataset/dblp_citation/simrank.true.csv", header=FALSE, sep=" ",
                       col.names=c("query_id", "id", "sim_score"))
  sim.false <- read.csv("/data/bshi/dataset/dblp_citation/simrank.false.csv", header=FALSE, sep=" ",
                        col.names=c("query_id", "id", "sim_score"))
  
  sim.true$label <- "T"
  sim.false$label <- "F"
  
  sim <- rbind(sim.true, sim.false)
  
  salsa.true <- read.csv("/data/bshi/dataset/dblp_citation/salsa.true.csv", header=FALSE, sep=" ",
                         col.names=c("query_id", "id", "query_auth_score", "query_auth_rank","query_hub_score",
                                     "query_hub_rank","id_auth_score", "id_auth_rank","id_hub_score","id_hub_rank"))
  salsa.false <- read.csv("/data/bshi/dataset/dblp_citation/salsa.false.csv", header=FALSE, sep=" ",
                          col.names=c("query_id", "id", "query_auth_score", "query_auth_rank","query_hub_score",
                                      "query_hub_rank","id_auth_score", "id_auth_rank","id_hub_score","id_hub_rank"))
  salsa.true$label <- "T"
  salsa.false$label <- "F"
  
  salsa <- rbind(salsa.true, salsa.false)
  
  # Combine them together
  dat <- merge(fbppr, sim, by=c("query_id","id","label"))
  dat <- merge(dat, salsa, by=c("query_id","id","label"))
  return(dat)
}

# Do logistic regression on all different combinations of features
do_logistic <- function(dat) {
  # Convert label to factor
  dat$label <- as.factor(dat$label)
  
  # Generate all combinations
  dat.features <- colnames(dat)[! colnames(dat) %in% c("query_id", "id", "label")]
  
  dat.res <- NULL
  for(i in 1:length(dat.features)) {
    features <- combn(dat.features, i)
    for(j in 1:dim(features)[2]) {
      feature <- paste(unlist(features[,j]), collapse = "+")
      f <- paste("label",feature,sep = "~")
      model <- Logistic(f, data = dat)
      res <- evaluate_Weka_classifier(model, numFolds = 5, complexity = FALSE, 
                                      seed = 1, class = TRUE)
      dat.res <- rbind(dat.res, data.frame(name=feature, choose=i, t(colMeans(res$detailsClass))))
    }
  }
  return(dat.res)
}

extractRes <- function(df, measurevar, topk) {
  tmpres <- NULL
  for(i in unique(df$choose)) {
    tmpdf <- df[which(df$choose == i),]
    tmpdf <- tmpdf[order(tmpdf[,measurevar], decreasing = TRUE),]
    len <- ifelse(dim(tmpdf)[1] > topk, topk, dim(tmpdf)[1])
    tmpres <- rbind(tmpres, tmpdf[1:len,])
  }
  return(tmpres)
}

abbr <- function(df) {
  df$name <- sub("fppr_score", "FS", df$name)
  df$name <- sub("fppr_rank", "FR", df$name)
  df$name <- sub("bppr_score", "BS", df$name)
  df$name <- sub("bppr_rank", "BR", df$name)
  
  df$name <- sub("sim_score", "SR", df$name)

  df$name <- sub("query_auth_score", "SAS", df$name)
  df$name <- sub("query_auth_rank", "SAR", df$name)
  df$name <- sub("query_hub_score", "SHS", df$name)
  df$name <- sub("query_hub_rank", "SHR", df$name)
  df$name <- sub("id_auth_score", "DAS", df$name)
  df$name <- sub("id_auth_rank", "DAR", df$name)
  df$name <- sub("id_hub_score", "DHS", df$name)
  df$name <- sub("id_hub_rank", "DHR", df$name)
  return(df)
}

draw <- function(df, measurevar = "areaUnderROC") {
  # Add ranking
  df$ranking <- rank(-df[,measurevar])
  maxN <- max(df$choose)
  df$choose <- as.factor(df$choose)
  df$x <- 1:dim(df)[1]
  g <- ggplot(df, aes_string(x = "x", y = measurevar, fill="choose", label="ranking")) + 
    geom_bar(stat="identity") + 
    geom_text(vjust=0.5) +
    geom_hline(yintercept =max(df[, measurevar]), colour = "red", size=.1) +
#    coord_cartesian(ylim=c(0.98 * min(df[,measurevar]),1)) +
    coord_cartesian(ylim=c(0.6, 1)) +
    scale_x_discrete(breaks=df$x, labels=df$name) +
    scale_colour_brewer(palette="Set1") +
    theme_classic() + 
    theme(axis.text.x = element_text(angle = 30, hjust = 1),
          axis.title.x = element_blank())
  g
  return(g)
}

drawModel <- function(df, measurevar = "areaUnderROC") {
  df$x <- 1:dim(df)[1]
  df$choose <- as.factor(df$choose)
  ggplot(df, aes_string(x = "x", y = measurevar, label="Models", fill="choose")) + 
    geom_bar(stat="identity") + 
    geom_hline(yintercept =max(df[, measurevar]), colour = "red", size=.1) +
    coord_cartesian(ylim=c(0.6, 1)) +
    scale_colour_brewer(palette="Set1") +
    scale_x_discrete(breaks=df$x, labels=df$Models) +    
    scale_fill_discrete(name="Number of\nModels") +
    theme_classic() + 
    theme(axis.text.x = element_text(angle = 15, hjust = 1),
          axis.title.x = element_blank())
}

compareAllModels <- function(df, measurevar = "areaUnderROC") {
  # There are 4 models FPPR/FBPPR/P-SALSA/SIMRANK
  res <- NULL
  
  # Number of models
  for(i in 1:4) {
    models <- combn(c("FPPR", "FBPPR", "P-SALSA", "SIMRANK"), i)
    # Select different model combinations
    templist <- NULL
    for( j in 1:dim(models)[2]) { #dim(models)[2]
      # Get results from this combination
      tmpdf <- NULL
      tmpidx <- rep(TRUE, dim(df)[1])
      cmodels=setdiff(c("FPPR", "FBPPR", "P-SALSA", "SIMRANK"),models[,j])
      for( m in cmodels) {
        if (m == "FPPR") {
          if (length(which(cmodels=="FBPPR"))>0)
            tmpidx <- tmpidx * (! 1:dim(df)[1] %in% grep("fppr_", df$name))
        }
        if (m == "FBPPR") {
          tmpidx <- tmpidx * (! 1:dim(df)[1] %in% grep("bppr_", df$name))
        }
        if (m == "P-SALSA") {
          tmpidx <- tmpidx * (! 1:dim(df)[1] %in% grep("query_", df$name))
          tmpidx <- tmpidx * (! 1:dim(df)[1] %in% grep("id_", df$name))
        }
        if (m == "SIMRANK") {
          tmpidx <- tmpidx * (! 1:dim(df)[1] %in% grep("sim", df$name))
        }
      }
      tmpdf <- df[which(tmpidx>0),]
      templist <- rbind(templist,data.frame(Models=paste(unlist(models[,j]),collapse = "+"),
                                         noname=max(tmpdf[,measurevar]),
                                         choose = i))
    }
    colnames(templist)[2] <- measurevar
    templist <- templist[order(templist[, measurevar],decreasing=T),]
    res <- rbind(res,templist)
  }
  
  return(res)
}

# Compare different models
drawModel(compareAllModels(dat.res, "recall"), "recall")
drawModel(compareAllModels(dat.res, "precision"), "precision")
drawModel(compareAllModels(dat.res, "fMeasure"), "fMeasure")
drawModel(compareAllModels(dat.res, "areaUnderROC"), "areaUnderROC")


#--------------------
dat.selected.res <- dat.res[! 1:dim(dat.res)[1] %in% union(grep("hub", dat.res$name), grep("auth_rank", dat.res$name)),]
dat.nohub.res <- dat.res[! 1:dim(dat.res)[1] %in% grep("hub", dat.res$name),]
dat.nobppr.res <- dat.res[! 1:dim(dat.res)[1] %in% grep("bppr_", dat.res$name),]

dat.selected.resRest <- extractRes(dat.selected.res, "areaUnderROC", 5)
draw(abbr(dat.selected.resRest), "areaUnderROC")

dat.nobppr.bestRes <- extractRes(dat.nobppr.res, "areaUnderROC", 1)
draw(abbr(dat.nobppr.bestRes), "areaUnderROC")

dat.nohub.bestRes <- extractRes(dat.nohub.res, "areaUnderROC", 1)
draw(abbr(dat.nohub.bestRes), "areaUnderROC")

dat.bestRes <- extractRes(dat.res, "areaUnderROC", 1)
draw(abbr(dat.bestRes),"areaUnderROC")