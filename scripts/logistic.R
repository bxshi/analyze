source("./logistic_functions.R")

# ---------------------DBLP CITATION 
dat <- load_data(fbppr_true = "/data/bshi/dataset/dblp_citation/link_prediction/fbppr.true.csv",
                 fbppr_false = "/data/bshi/dataset/dblp_citation/link_prediction/fbppr.false.csv",
                 sim_true = "/data/bshi/dataset/dblp_citation/link_prediction/simrank.true.csv",
                 sim_false = "/data/bshi/dataset/dblp_citation/link_prediction/simrank.false.csv",
                 salsa_true = "/data/bshi/dataset/dblp_citation/link_prediction/salsa.true.csv",
                 salsa_false = "/data/bshi/dataset/dblp_citation/link_prediction/salsa.false.csv")

logistic_result <- logistic(dat)

roc_result <- roc_data(logistic_result)

# If work with multiple roc_result, do summarySE by c("model", "id)

# ROC does not need errorbar
plot_roc(roc_result, filename = "./imgs/dblp_citation")

# This does need errorbar
plot_auc(roc_result, filename = "./imgs/auc_citation.eps")

# --------------------- DBLP CITATION END

# --------------------- DBLP COLLABORATION

dat <- load_data(fbppr_true = "/data/bshi/dataset/my_dblp_collaboration/link_prediction/fbppr.true.csv",
                 fbppr_false = "/data/bshi/dataset/my_dblp_collaboration/link_prediction/fbppr.false.csv",
                 sim_true = "/data/bshi/dataset/my_dblp_collaboration/link_prediction/simrank.true.csv",
                 sim_false = "/data/bshi/dataset/my_dblp_collaboration/link_prediction/simrank.false.csv",
                 salsa_true = "/data/bshi/dataset/my_dblp_collaboration/link_prediction/salsa.true.csv",
                 salsa_false = "/data/bshi/dataset/my_dblp_collaboration/link_prediction/salsa.false.csv")

logistic_result <- logistic(dat)

roc_result <- roc_data(logistic_result)

# If work with multiple roc_result, do summarySE by c("model", "id)

# ROC does not need errorbar
plot_roc(roc_result, filename = "./imgs/dblp_coauthor")

# This does need errorbar
plot_auc(roc_result, filename = "./imgs/auc_coauthor.eps")

# --------------------- DBLP COLLABORATION END

# --------------------- GITHUB COLLABORATION
dat <- load_data(fbppr_true = "/data/bshi/dataset/github_collaboration/link_prediction/fbppr.true.csv",
                 fbppr_false = "/data/bshi/dataset/github_collaboration/link_prediction/fbppr.false.csv",
                 sim_true = "/data/bshi/dataset/github_collaboration/link_prediction/simrank.true.csv",
                 sim_false = "/data/bshi/dataset/github_collaboration/link_prediction/simrank.false.csv",
                 salsa_true = "/data/bshi/dataset/github_collaboration/link_prediction/salsa.true.csv",
                 salsa_false = "/data/bshi/dataset/github_collaboration/link_prediction/salsa.false.csv")

logistic_result <- logistic(dat)

roc_result <- roc_data(logistic_result)

# If work with multiple roc_result, do summarySE by c("model", "id)

# ROC does not need errorbar
plot_roc(roc_result, filename = "./imgs/github_collaboration")

# This does need errorbar
plot_auc(roc_result, filename = "./imgs/auc_github.eps")

# --------------------- GITHUB COLLABORATION END