library(pROC)
library(ggplot2)
library(LiblineaR)

cbPalette <- c("#000000", "#ff0000", "#00ff00", "#0000ff", "#ff66ff", "#66ffff", "#ffff66")

load_data <- function(fbppr_true, fbppr_false, sim_true, sim_false, salsa_true, salsa_false, aa_true, aa_false) {
  fbppr.true <- read.csv(fbppr_true, header=FALSE, sep=" ",
                         col.names=c("query_id","id", "fppr_score", "fppr_rank", "bppr_score", "bppr_rank"))[1:4802,]
  fbppr.false <- read.csv(fbppr_false, header=FALSE, sep=" ",
                          col.names=c("query_id","id", "fppr_score", "fppr_rank", "bppr_score", "bppr_rank"))
  fbppr.true$label <- "T"
  fbppr.false$label <- "F"
  
  fbppr <- rbind(fbppr.true, fbppr.false)
  
  #degs <- read.csv(deg_file, sep=" ", header=F)
  #colnames(degs) <- c("id","outd","ind")
  #fbppr$fppr_rank <- fbppr$fppr_rank / (degs[which(degs$id == fbppr$query_id), "outd"] + 1)
  #fbppr$bppr_rank <- bppr$bppr_rank / (degs[which(degs$id == fbppr$id), "ind"] + 1)
  
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
  
  aa.true <- read.csv(aa_true, header=F, sep=" ",
                      col.names=c("query_id","id","aa_score"))
  aa.false <- read.csv(aa_false, header=F, sep=" ",
                       col.names=c("query_id", "id", "aa_score"))
  
  aa.true$label <- "T"
  aa.false$label <- "F"
  aa <- rbind(aa.true, aa.false)
  
  # Combine them together
  dat <- merge(fbppr, sim, by=c("query_id","id","label"))
  dat <- merge(dat, salsa, by=c("query_id","id","label"))
  dat <- merge(dat, aa, by=c("query_id","id","label"))
  #dat <- merge(fbppr, aa, by=c("query_id","id","label")) # COMMENT THIS LATER
  return(dat)
}

#' Generate formula based on models
generate_formulas <- function() {
  feature_list <- list(FPPR=c("fppr_score"),
                    FBPPR=c("fppr_score", "bppr_score"),
                    SIMRANK=c("sim_score"),
                    PSALSA=c("id_auth_score"),
                    AA=c("aa_score")) #c("query_auth_score", "id_auth_score"))
#                     PSALSA=c("query_auth_score", "query_auth_rank","query_hub_score",
#                                 "query_hub_rank","id_auth_score", "id_auth_rank","id_hub_score","id_hub_rank"))
  formulas <- NULL
  # Four models in total, FPPR, FBPPR, SIMRANK, and P-SALSA
  for(i in 1:5) { # Number of models
    selected_models <- combn(c("FPPR", "FBPPR", "SIMRANK", "PSALSA", "AA"), i)
#    selected_models <- combn(c("FPPR", "FBPPR", "AA"), i)   
    for(j in 1:dim(selected_models)[2]) { # 4 choose i results
      selected_features <- unique(unlist(feature_list[selected_models[,j]])) # We do not deduplicate here so each model is unique
      formulas <- rbind(formulas, data.frame(formula = paste("label", paste(selected_features, collapse= "+"), sep= "~"),
                                             model = paste(selected_models[,j], collapse="+")))
    }
  }
  formulas$formula <- as.character(formulas$formula)
  return(formulas) # 15 combinations
}

generate_columns <- function() {
  feature_list <- list(FPPR=c("fppr_score"),
                       FBPPR=c("fppr_score", "fppr_rank", "bppr_score", "bppr_rank"),
                       SIMRANK=c("sim_score"),
                       PSALSA=c("id_auth_score"),
                       AA=c("aa_score")) #c("query_auth_score", "id_auth_score"))
  #                     PSALSA=c("query_auth_score", "query_auth_rank","query_hub_score",
  #                                 "query_hub_rank","id_auth_score", "id_auth_rank","id_hub_score","id_hub_rank"))
  formulas <- c()
  # Four models in total, FPPR, FBPPR, SIMRANK, and P-SALSA
  for(i in 1:5) { # Number of models
    selected_models <- combn(c("FPPR", "FBPPR", "SIMRANK", "PSALSA", "AA"), i)
    #selected_models <- combn(c("FPPR", "FBPPR", "AA"), i)
    for(j in 1:dim(selected_models)[2]) { # 4 choose i results
      selected_features <- unique(unlist(feature_list[selected_models[,j]])) # We do not deduplicate here so each model is unique
      tmplist <- list(model = paste(selected_models[,j], collapse="+"),
                      features = c(selected_features))
      formulas <- c(formulas, as.list(tmplist))
    }
  }
  return(formulas) # 15 combinations
}

l2logistic <- function(df) {
  op <- options("warn")
  on.exit(options(op))
  options(warn=1)
  
  # Convert label to categorical value
  df$label <- as.factor(df$label)
  
  formulas <- generate_columns()
  
  # Generate 5 fold datasets
  set.seed(233)
  ind <- sample(1:5, nrow(df), replace = TRUE) 
  
  # Do 5 fold on each formula
  result <- NULL
  for(index in 1:(length(formulas)/2)) {
    selected_features <- unlist(formulas[index * 2])
    model <- formulas[2 * index - 1]$model
    print(paste("model is ",model,"index is",index))
    tmp_result <- NULL
    for(i in 1:5) { # 5 fold
      # Get training and testing datasets
      dat.train <- df[which(ind != i),]
      dat.test <- df[which(ind == i),]
      
      # Convert data frame into data matrix
      target.train <- dat.train[, "label"]
      target.test <- dat.test[, "label"]
      dat.train <- as.matrix(dat.train[, selected_features])
      dat.test <- as.matrix(dat.test[, selected_features])
      
      # Train using L2-regularized logistic regression
      logit <- LiblineaR(dat.train, target.train, type = 1)
      
      # Train model
      pred <- predict(logit, dat.test, proba = TRUE, decisionValues=TRUE)$decisionValues[,1]
      dat.test <- df[which(ind == i),]
      dat.test$pred <- pred
      tmp_result <- rbind(tmp_result, dat.test) # Prediction result
    }
    tmp_result$formula <- paste("label", paste(selected_features, collapse= "+"), sep= "~")
    tmp_result$model <- model
    print(unique(tmp_result$model))
    result <- rbind(result, tmp_result)
  }
  return(result)
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
  df.true <- df[which(df$label == "T"),]
  df.false <- df[which(df$label == "F"),]
  ind.true <- sample(1:5, nrow(df.true), replace = TRUE) 
  ind.false <- sample(1:5, nrow(df.false), replace= TRUE)
  
  # Do 5 fold on each formula
  result <- NULL
  for(index in 1:nrow(formulas)) {
    fla <- formulas[index, "formula"]
    model <- formulas[index, "model"]
    tmp_result <- NULL
    for(i in 1:5) { # 5 fold
      # Get training and testing datasets
      dat.train <- rbind(df.true[which(ind.true != i),], df.false[which(ind.false != i),])
      dat.test <- rbind(df.true[which(ind.true == i),], df.false[which(ind.false == i),])
      
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
    roc_result <- roc(label ~ pred, data = df[df$model == model,], auc=TRUE, ci=TRUE)
    res.auc <- rbind(res.auc, data.frame(model = model, auc = roc_result$auc,
                                         ci = abs(roc_result$ci[1] - roc_result$ci[2]),
                                         se = abs(roc_result$ci[1] - roc_result$ci[2]) / 1.96,
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
plot_roc <- function(roc_res, filename = "./roc", withaa=T) {
  roc_res <- roc_res$roc
  roc_res$model <- str_replace(roc_res$model, "FBPPR", "FBS")
  roc_res$model <- str_replace(roc_res$model, "FPPR", "PPR")
  roc_res$model <- str_replace(roc_res$model, "SIMRANK", "SimRank")
  roc_res$model <- str_replace(roc_res$model, "PSALSA", "pSALSA")
  roc_res$model <- str_replace(roc_res$model, "AA", "Adamic Adar")
  
  #levels(roc_res$model) <- c("pPR","FBpPR","SimRank","pSALSA", "AA", "AB","AC","AD","BC","BD","CD","ABC","ABD","ACD","BCD","ABCD")
  plot_roc.draw <- function(roc_df, legend.leftpadding=0.6) {
    if(!withaa) {
      roc_df <- roc_df[which(roc_df$model != "Adamic Adar"),]
    }
    g <- ggplot(roc_df, aes(x=fp, y=tp, color=model,
                            linetype=model, shape=model)) +
      geom_line(size=1.1) +
      scale_linetype_manual(values=c("solid", "dashed",  "dotdash", "dotted", "twodash")) +
      scale_shape_manual(values=c(0,1,2,3,4)) + 
      #scale_linetype_manual(values=c("solid","dashed","dotted","dotdash", "longdash","twodash")) +
      geom_abline(size=0.5, slope=1, intercept=0) +
      #geom_point(position="dodge") +
      #scale_shape(solid = FALSE) +
      scale_x_continuous(expand=c(0,0)) +
      scale_y_continuous(expand=c(0,0)) +
      #scale_colour_brewer(palette = "Set1") +
      #scale_colour_manual(values = cbPalette) +
      #scale_fill_manual(values = cbPalette) +
      #scale_color_brewer(palette = "Dark2") +
      ylab("True Positive Rate") +
      xlab("False Positive Rate") +
      theme_classic() +
      guides(color=guide_legend(override.aes = list(size=0.5))) +
      theme(panel.background = element_rect(colour = "black", size=1),
            legend.justification=c(0,0), legend.position=c(legend.leftpadding,0),
            legend.title=element_blank())
  }
  
  g1 <- plot_roc.draw(roc_res[roc_res$nmodel == 1, ], 0.6)
  #g2 <- plot_roc.draw(roc_res[roc_res$nmodel == 2, ], 0.5)
  #g3 <- plot_roc.draw(roc_res[roc_res$nmodel == 3, ], 0.4)
  #g4 <- plot_roc.draw(roc_res[roc_res$nmodel == 4, ], 0.2)
  
  ggsave(g1, filename = paste(filename, "_nmodel_1.eps", sep = ""), width = 5, height = 5)
  #ggsave(g2, filename = paste(filename, "_nmodel_2.eps", sep = ""), width = 5, height = 5)
  #ggsave(g3, filename = paste(filename, "_nmodel_3.eps", sep = ""), width = 5, height = 5)
  #ggsave(g4, filename = paste(filename, "_nmodel_4.eps", sep = ""), width = 5, height = 5)
}

#' Draw AUC with different models
plot_auc <- function(roc_res, filename = "./auc") {
  auc_res <- roc_res$auc
  
  plot_auc.draw <- function(auc_df) {
    #auc_df <- auc_df[order(auc_df$auc, decreasing = TRUE),]
    auc_df$x <- 1 : nrow(auc_df)
    auc_df$ranking <- rank(-as.numeric(auc_df$auc))
    auc_df$nmodel <- as.factor(auc_df$nmodel)
    levels(auc_df$model) <- c("A","B","C","D","E",
                              "AB","AC","AD","AE",
                              "BC","BD","BE",
                              "CD","CE",
                              "DE",
                              "ABC","ABD","ABE",
                              "ACD","ACE",
                              "ADE",
                              "BCD","BCE",
                              "BDE",
                              "CDE",
                              "ABCD","ABCE","ABDE","ACDE","BCDE",
                              "ABCDE")
    auc_df$color <- ifelse(grepl("B",auc_df$model), "grey","white")
    auc_df$model <- ordered(auc_df$model, levels=c("A","B","C","D","E",
                                                   "AB","AC","AD","AE",
                                                   "BC","BD","BE",
                                                   "CD","CE",
                                                   "DE",
                                                   "ABC","ABD","ABE",
                                                   "ACD","ACE",
                                                   "ADE",
                                                   "BCD","BCE",
                                                   "BDE",
                                                   "CDE",
                                                   "ABCD","ABCE","ABDE","ACDE","BCDE",
                                                   "ABCDE"))
    g <- ggplot(auc_df, aes(x=model, y=auc, fill=color)) +
      geom_bar(stat="identity", color="black") + 
      geom_errorbar(aes(ymin=auc - ci, ymax=auc + ci), position = "identity", width=.2, size = 0.2, colour = "black") +
      geom_errorbar(aes(ymin=auc - se, ymax=auc + se), colour = "red", position = "identity", width=.2, size = 0.2) +
      #scale_x_discrete(breaks=auc_df$model, labels=auc_df$model) +
      scale_fill_manual(values = rev(unique(auc_df$color))) +
      coord_cartesian(ylim=c(0.5, 1)) +
      ylab("Area Under ROC") +
      xlab("Models") +
      #scale_colour_brewer(palette = "Set1") +
      #scale_colour_manual(values = cbPalette) +
      theme_classic() +
      theme(axis.text.x = element_text(angle = 90, hjust = 1, vjust = 0.4),
            panel.background = element_rect(colour = "black", size=1),
            legend.position = "none",
            legend.title=element_blank())
  }
  
  g <- plot_auc.draw(auc_res)
  ggsave(g, filename = filename, width = 5, height = 5)
}