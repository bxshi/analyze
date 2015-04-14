library(pROC)
library(ggplot2)

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

load_data <- function(fbppr_true, fbppr_false, sim_true, sim_false, salsa_true, salsa_false) {
  fbppr.true <- read.csv(fbppr_true, header=FALSE, sep=" ",
                         col.names=c("query_id","id", "fppr_score", "fppr_rank", "bppr_score", "bppr_rank"))
  fbppr.false <- read.csv(fbppr_false, header=FALSE, sep=" ",
                          col.names=c("query_id","id", "fppr_score", "fppr_rank", "bppr_score", "bppr_rank"))
  fbppr.true$label <- "T"
  fbppr.false$label <- "F"
  
  fbppr <- rbind(fbppr.true, fbppr.false)
  
  sim.true <- read.csv(sim_true, header=FALSE, sep=" ",
                       col.names=c("query_id", "id", "sim_score"))
  sim.false <- read.csv(sim_false, header=FALSE, sep=" ",
                        col.names=c("query_id", "id", "sim_score"))
  
  sim.true$label <- "T"
  sim.false$label <- "F"
  
  sim <- rbind(sim.true, sim.false)
  
  salsa.true <- read.csv(salsa_true, header=FALSE, sep=" ",
                         col.names=c("query_id", "id", "query_auth_score", "query_auth_rank","query_hub_score",
                                     "query_hub_rank","id_auth_score", "id_auth_rank","id_hub_score","id_hub_rank"))
  salsa.false <- read.csv(salsa_false, header=FALSE, sep=" ",
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

#' Generate formula based on models
generate_formulas <- function() {
  feature_list <- list(FPPR=c("fppr_score"),
                    FBPPR=c("fppr_score", "fppr_rank", "bppr_score", "bppr_rank"),
                    SIMRANK=c("sim_score"),
                    PSALSA=c("query_auth_score", "id_auth_score"))
#                     PSALSA=c("query_auth_score", "query_auth_rank","query_hub_score",
#                                 "query_hub_rank","id_auth_score", "id_auth_rank","id_hub_score","id_hub_rank"))
  formulas <- NULL
  # Four models in total, FPPR, FBPPR, SIMRANK, and P-SALSA
  for(i in 1:4) { # Number of models
    selected_models <- combn(c("FPPR", "FBPPR", "SIMRANK", "PSALSA"), i)
    for(j in 1:dim(selected_models)[2]) { # 4 choose i results
      selected_features <- unlist(feature_list[selected_models[,j]]) # We do not deduplicate here so each model is unique
      formulas <- rbind(formulas, data.frame(formula = paste("label", paste(selected_features, collapse= "+"), sep= "~"),
                                             model = paste(selected_models[,j], collapse="+")))
    }
  }
  formulas$formula <- as.character(formulas$formula)
  return(formulas) # 15 combinations
}

#' Return result of logistic regression
logistic <- function(df) {
  
  op <- options("warn")
  on.exit(options(op))
  options(warn=1)
  
  # Convert label to categorical value
  df$label <- as.factor(df$label)
  # Get all possible formulas
  formulas <- generate_formulas()
  
  # Generate 5 fold datasets
  set.seed(233)
  ind <- sample(1:5, nrow(df), replace = TRUE) 
  
  # Do 5 fold on each formula
  result <- NULL
  for(index in 1:nrow(formulas)) {
    fla <- formulas[index, "formula"]
    model <- formulas[index, "model"]
    tmp_result <- NULL
    for(i in 1:5) { # 5 fold
      # Get training and testing datasets
      dat.train <- df[which(ind != i),]
      dat.test <- df[which(ind == i),]
      
      # Train model
      logit <- glm(fla, data = dat.train, family = "binomial")
      dat.test$pred <- predict(logit, newdata = dat.test, type = "response")
      tmp_result <- rbind(tmp_result, dat.test) # Prediction result
    }
    tmp_result$formula <- fla
    tmp_result$model <- model
    result <- rbind(result, tmp_result)
  }
  return(result)
}

get_model_num <- function(s) {
  return(sum(ifelse(strsplit(s, split="")[[1]] == "+", 1, 0))+1)
}

#' Generate True positive / False positive and AUC using
#' logistic regression results
roc_data <- function(df) {
  res.auc <- NULL
  res.roc <- NULL
  for(model in unique(df$model)) { # Generate ROC data for each formula
    roc_result <- roc(label ~ pred, data = df[df$model == model,])
    res.auc <- rbind(res.auc, data.frame(model = model, auc=roc_result$auc,
                                         nmodel = get_model_num(model),
                                         id = 1:roc_result$auc))
    res.roc <- rbind(res.roc, data.frame(model = model, tp = roc_result$sensitivities,
                                         fp = 1 - roc_result$specificities,
                                         nmodel = get_model_num(model),
                                         id = 1 : length(roc_result$specificities)))
  }
  return(list(auc=res.auc, roc=res.roc))
}

#' Plot ROC using a set of data
plot_roc <- function(roc_res, filename = "./roc") {
  roc_res <- roc_res$roc
  
  plot_roc.draw <- function(roc_df, legend.leftpadding=0.6) {
    g <- ggplot(roc_df, aes(x=fp, y=tp, color=model,
                            linetype=model)) +
      geom_line() + geom_abline(slope=1, intercept=0) +
      scale_x_continuous(expand=c(0,0)) +
      scale_y_continuous(expand=c(0,0)) +
      ylab("True Positive Rate") +
      xlab("False Positive Rate") +
      theme_classic() +
      theme(panel.background = element_rect(colour = "black", size=1),
            legend.justification=c(0,0), legend.position=c(legend.leftpadding,0))
  }
  
  g1 <- plot_roc.draw(roc_res[roc_res$nmodel == 1, ], 0.6)
  g2 <- plot_roc.draw(roc_res[roc_res$nmodel == 2, ], 0.5)
  g3 <- plot_roc.draw(roc_res[roc_res$nmodel == 3, ], 0.4)
  g4 <- plot_roc.draw(roc_res[roc_res$nmodel == 4, ], 0.2)
  
  ggsave(g1, filename = paste(filename, "_nmodel_1.eps", sep = ""), width = 5, height = 5)
  ggsave(g2, filename = paste(filename, "_nmodel_2.eps", sep = ""), width = 5, height = 5)
  ggsave(g3, filename = paste(filename, "_nmodel_3.eps", sep = ""), width = 5, height = 5)
  ggsave(g4, filename = paste(filename, "_nmodel_4.eps", sep = ""), width = 5, height = 5)
}

#' Draw AUC with different models
plot_auc <- function(roc_res, filename = "./auc") {
  auc_res <- roc_res$auc
  
  plot_auc.draw <- function(auc_df) {
    #auc_df <- auc_df[order(auc_df$auc, decreasing = TRUE),]
    auc_df$x <- 1 : nrow(auc_df)
    auc_df$ranking <- rank(-as.numeric(auc_df$auc))
    auc_df$nmodel <- as.factor(auc_df$nmodel)
    g <- ggplot(auc_df, aes(x=x, y=auc, fill=nmodel)) +
      geom_bar(stat="identity") + 
      scale_x_discrete(breaks=auc_df$x, labels=auc_df$model) +
       coord_cartesian(ylim=c(0.5, 1)) +
       ylab("Area Under ROC") +
       xlab("Models") +
       scale_colour_brewer(palette="Set1") +
       theme_classic() +
      theme(axis.text.x = element_text(angle = 90, hjust = 1, vjust = 0.4),
            panel.background = element_rect(colour = "black", size=1),
            legend.position = "none")
  }
  
  g <- plot_auc.draw(auc_res)
  ggsave(g, filename = filename, width = 5, height = 5)
}