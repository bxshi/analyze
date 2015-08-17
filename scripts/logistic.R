source("./logistic_functions.R")

# ---------------------DBLP CITATION 
dat <- load_data(fbppr_true = "/data/bshi/dataset/dblp_citation/link_prediction/10000/fbppr.true.csv",
                 fbppr_false = "/data/bshi/dataset/dblp_citation/link_prediction/10000/fbppr.false.csv",
                 sim_true = "/data/bshi/dataset/dblp_citation/link_prediction/10000/simrank.true.csv",
                 sim_false = "/data/bshi/dataset/dblp_citation/link_prediction/10000/simrank.false.csv",
                 salsa_true = "/data/bshi/dataset/dblp_citation/link_prediction/10000/salsa.true.csv",
                 salsa_false = "/data/bshi/dataset/dblp_citation/link_prediction/10000/salsa.false.csv",
                 aa_true = "/data/bshi/dataset/dblp_citation/link_prediction/10000/aa.true.txt",
                 aa_false = "/data/bshi/dataset/dblp_citation/link_prediction/10000/aa.false.txt",
                 deg_file = "/home/lyang5/dci.outin.txt")

logistic_result <- logistic(dat)

cit_roc_result <- roc_data(logistic_result)

# If work with multiple roc_result, do summarySE by c("model", "id)

# ROC does not need errorbar
plot_roc(cit_roc_result, filename = "./imgs/roc_citation_w_aa_no_rank_pretty", withaa = T)
plot_roc(cit_roc_result, filename = "./imgs/roc_citation_no_aa_pretty", withaa = F)


# This does need errorbar
plot_auc(cit_roc_result, filename = "./imgs/auc_citation_w_aa.eps")

# --------------------- DBLP CITATION END

# --------------------- DBLP COLLABORATION

dat <- load_data(fbppr_true = "/data/bshi/dataset/my_dblp_collaboration/link_prediction/10000/fbppr.true.csv",
                 fbppr_false = "/data/bshi/dataset/my_dblp_collaboration/link_prediction/10000/fbppr.false.csv",
                 sim_true = "/data/bshi/dataset/my_dblp_collaboration/link_prediction/10000/simrank.true.csv",
                 sim_false = "/data/bshi/dataset/my_dblp_collaboration/link_prediction/10000/simrank.false.csv",
                 salsa_true = "/data/bshi/dataset/my_dblp_collaboration/link_prediction/10000/salsa.true.csv",
                 salsa_false = "/data/bshi/dataset/my_dblp_collaboration/link_prediction/10000/salsa.false.csv",
                 aa_true = "/data/bshi/dataset/my_dblp_collaboration/link_prediction/10000/aa.true.txt",
                 aa_false = "/data/bshi/dataset/my_dblp_collaboration/link_prediction/10000/aa.false.txt")

logistic_result <- logistic(dat)

col_roc_result <- roc_data(logistic_result)

# If work with multiple roc_result, do summarySE by c("model", "id)

# ROC does not need errorbar
plot_roc(col_roc_result, filename = "./imgs/roc_coauthor_w_aa", withaa = T)
plot_roc(col_roc_result, filename = "./imgs/roc_coauthor_no_aa_pretty", withaa = F)

# This does need errorbar
plot_auc(col_roc_result, filename = "./imgs/auc_coauthor_w_aa.eps")

# --------------------- DBLP COLLABORATION END

# --------------------- GITHUB COLLABORATION
dat <- load_data(fbppr_true = "/data/bshi/dataset/github_collaboration/link_prediction/10000/fbppr.true.csv",
                 fbppr_false = "/data/bshi/dataset/github_collaboration/link_prediction/10000/fbppr.false.csv",
                 sim_true = "/data/bshi/dataset/github_collaboration/link_prediction/10000/simrank.true.csv",
                 sim_false = "/data/bshi/dataset/github_collaboration/link_prediction/10000/simrank.false.csv",
                 salsa_true = "/data/bshi/dataset/github_collaboration/link_prediction/10000/salsa.true.csv",
                 salsa_false = "/data/bshi/dataset/github_collaboration/link_prediction/10000/salsa.false.csv",
                 aa_true = "/data/bshi/dataset/github_collaboration/link_prediction/10000/aa.true.txt",
                 aa_false = "/data/bshi/dataset/github_collaboration/link_prediction/10000/aa.false.txt")

logistic_result <- logistic(dat)

git_roc_result <- roc_data(logistic_result)

# If work with multiple roc_result, do summarySE by c("model", "id)

# ROC does not need errorbar
plot_roc(git_roc_result, filename = "./imgs/roc_github_w_aa", withaa = T)
plot_roc(git_roc_result, filename = "./imgs/roc_github_no_aa_pretty", withaa = F)


# This does need errorbar
plot_auc(git_roc_result, filename = "./imgs/auc_github_w_aa.eps")

# --------------------- GITHUB COLLABORATION END



dat <- load_data(fbppr_true = "/data/bshi/dataset/github_collaboration/link_prediction/10000/fbppr.true.csv",
                 fbppr_false = "/data/bshi/dataset/github_collaboration/link_prediction/10000/fbppr.false.2hop.csv",
                 sim_true = "/data/bshi/dataset/github_collaboration/link_prediction/10000/simrank.true.csv",
                 sim_false = "/data/bshi/dataset/github_collaboration/link_prediction/10000/simrank.false.csv",
                 salsa_true = "/data/bshi/dataset/github_collaboration/link_prediction/10000/salsa.true.csv",
                 salsa_false = "/data/bshi/dataset/github_collaboration/link_prediction/10000/salsa.false.csv",
                 aa_true = "/data/bshi/dataset/github_collaboration/link_prediction/10000/aa.true.txt",
                 aa_false = "/data/bshi/dataset/github_collaboration/link_prediction/10000/aa.false.2hop.txt")

logistic_result <- logistic(dat)

git_roc_result <- roc_data(logistic_result)

# If work with multiple roc_result, do summarySE by c("model", "id)

# ROC does not need errorbar
plot_roc(git_roc_result, filename = "./imgs/roc_github_w_aa_test", withaa = T)
plot_roc(git_roc_result, filename = "./imgs/roc_github_no_aa", withaa = F)


# This does need errorbar
plot_auc(git_roc_result, filename = "./imgs/auc_github_w_aa_test.eps")

# --------------------- GITHUB COLLABORATION END