source("./logistic_functions.R")

dat <- load_data(fbppr_true = "/data/bshi/dataset/dblp_citation/link_prediction/fbppr.true.csv",
                 fbppr_false = "/data/bshi/dataset/dblp_citation/link_prediction/fbppr.false.csv",
                 sim_true = "/data/bshi/dataset/dblp_citation/link_prediction/simrank.true.csv",
                 sim_false = "/data/bshi/dataset/dblp_citation/link_prediction/simrank.false.csv",
                 salsa_true = "/data/bshi/dataset/dblp_citation/link_prediction/salsa.true.csv",
                 salsa_false = "/data/bshi/dataset/dblp_citation/link_prediction/salsa.false.csv")

logistic_result <- logistic(dat)

roc_result <- roc_data(logistic_result)

# If work with multiple roc_result, do summarySE by c("model", "id)

plot_roc(roc_result, filename = "./roc_test")

plot_auc(roc_result, filename = "./auc_test.eps")
