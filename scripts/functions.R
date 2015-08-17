# Calculate average precision
fbppr.ap <- function(pred, actual) {
  len <- min(length(pred), length(actual))
  sum_score <- 0
  ap <- rep(0.0, len)
  cnt <- 0
  for(i in 1:len) {
    if (pred[i] == actual[i]) {
      cnt <- cnt + 1
      sum_score <- sum_score + cnt / i # precision = #Rel / N
    }
    if (cnt == 0) {
      ap[i] == 0
    } else {
      ap[i] <- sum_score / cnt
    }
  }
  return(as.double(ap))
}

fbppr.dcg <- function(pred, actual, .comm) {
  len <- min(length(pred), length(actual))
  ap <- rep(0.0, len)
  cnt <- 0
  sum_score <- 0
  for (i in 1:len) {
    pred.comm <- .comm[which(.comm$id == pred[i]), "cluster"]
    actual.comm <- .comm[which(.comm$id == actual[i]), "cluster"]
    jaccard.score <- length(intersect(pred.comm, actual.comm)) / length(union(pred.comm, actual.comm))
    if (jaccard.score > 0) {
      sum_score <- sum_score + jaccard.score
      cnt <- cnt + 1
    } else {
      #print(pred, actual)
    }
    if (cnt == 0) {
      ap[i] <- 0
    } else {
      ap[i] <- sum_score / cnt
    }
  }
  return(as.double(ap))
}

fbppr.multiap <- function(pred, actual, .comm) {
  len <- min(10, min(length(pred), length(actual)))
  ap <- rep(0.0, len)
  cnt <- 0
  sum_score <- 0
  jaccard.score <- 0
  for (i in 1:len) {
    pred.comm <- .comm[which(.comm$id == pred[i]), "cluster"]
    actual.comm <- .comm[which(.comm$id == actual[i]), "cluster"]
    jaccard.score <- 0
    if(length(union(pred.comm, actual.comm)) == 0) {
      print(pred[i])
      print(actual[i])
      print("wtf")
      jaccard.score <- 0
    } else {
      jaccard.score <- length(intersect(pred.comm, actual.comm)) / length(union(pred.comm, actual.comm))

    }
    if (jaccard.score > 0) {
      sum_score <- sum_score + jaccard.score
      cnt <- cnt + 1
    } else {
      #print(pred, actual)
    }
    if (cnt == 0) {
      ap[i] <- 0
    } else {
      ap[i] <- sum_score / cnt
    }
  }
  return(as.double(ap))
}

# Test different lambda
fbppr.combine <- function(fbppr.df, query.node, .by=1, .interval=c()) {
  # Set step
  interval <- seq(from = 0, to = 1, by = .by)
  if(length(.interval) != 0) {
    interval <- .interval
  }
  g.df <- NULL # Result dataframe
  for (i in interval) { # Generate AP based on step
    lbl <- paste(expression(lambda),"=", i) # Get colname
    score <- i * fbppr.df$fppr_score  + (1-i) * fbppr.df$bppr_score # Calc score
    neworder <- order(score, decreasing = TRUE) # New rank
    newtitle <- fbppr.df[neworder, "title"]
    ap <- fbppr.ap(fbppr.df[neworder, "cluster"], rep(query.node$cluster, nrow(fbppr.df))) # Get AP by that order
    tmpres <- data.frame(rep(lbl, length(ap)), 1:length(ap), ap, newtitle)
    colnames(tmpres) <- c("label", "rank", "AP", "title")
    g.df <- rbind(g.df, tmpres)
  }
  g.df
}

fill_up_data <- function(dat, titlemap) {
  if (!"title" %in% colnames(dat)) {
    dat <- merge(dat, titlemap)
    colnames(dat) <- c(colnames(dat)[1 : length(colnames(dat)) -1], "title")
  }
  if (!"fppr_rank" %in% colnames(dat)) {
    dat <- cbind(dat[order(-dat$fppr_score),], 0:dim(dat)[1]-1)
    colnames(dat) <- c(colnames(dat)[1 : length(colnames(dat)) -1], "fppr_rank")
  }
  if (!"bppr_rank" %in% colnames(dat)) {
    dat <- cbind(dat[order(-dat$bppr_score),], 0:dim(dat)[1]-1)
    colnames(dat) <- c(colnames(dat)[1 : length(colnames(dat)) -1], "bppr_rank")
  }
  return(dat)
}

rank.coeff <- function(rank.df, lbl, query.node, comm, .ap=FALSE) {
  g.df <- NULL
  query.comm <- unlist(comm[which(comm$id == query.node$id), "cluster"])
  neworder <- order(rank.df$rank, decreasing = FALSE) # Make sure the order is following the rank
  if (.ap) { # MAJ
    coeff <- fbppr.multiap(rank.df[neworder, "id"], rep(query.node$id, nrow(rank.df)), comm) # Get AP by that order
  } else { # Jaccard
    coeff <- lapply(rank.df[neworder, "id"], function(x){
      # get communities of x
      comm_x <- unlist(comm[which(comm$id==x), "cluster"])
      # Calculate jaccard coefficient
      as.double(length(intersect(comm_x, query.comm))) / as.double(length(union(comm_x, query.comm)))
    })
  }
  tmpres <- data.frame(rep(lbl, length(coeff)),
                       1:length(coeff),
                       unlist(coeff),
                       "")
  colnames(tmpres) <- c("label", "rank", "coeff", "title")
  g.df <- rbind(g.df, tmpres)
}

# Calculate jaccard coefficient
fbppr.coeff <- function(fbppr.df, query.node, comm, deg, .by=1, .interval=c(), .normalized=FALSE, .ap=FALSE, .multiply=FALSE) {
  # Set step
  interval <- seq(from = 0, to = 1, by = .by)
  if(length(.interval) != 0) {
    interval <- .interval
  }
  g.df <- NULL
  # get communities of query node
  query.comm <- unlist(comm[which(comm$id == query.node$id), "cluster"])
  if (.multiply) {
    for(lbl in c("ppr","multiply")) {
      score <- NULL
      if(lbl == "multiply") {
        score <- fbppr.df$fppr_score * fbppr.df$bppr_score
      } else {
        score <- fbppr.df$fppr_score
      }
      neworder <- order(score, decreasing = TRUE)
      newtitle <- fbppr.df[order(neworder), "title"]
      
      coeff <- NULL
      if (.ap == FALSE) {
        coeff <- lapply(fbppr.df[neworder, "id"], function(x){
          # get communities of x
          comm_x <- unlist(comm[which(comm$id==x), "cluster"])
          # Calculate jaccard coefficient
          coe <- NULL
          if (.normalized) { # Normalize result by community size
            coe <- sum(2/unlist(comm[which(comm$id %in% intersect(comm_x, query.comm)), "count"])) / length(union(comm_x, query.comm))
          } else { # Do not normalize 
            coe <- as.double(length(intersect(comm_x, query.comm))) / as.double(length(union(comm_x, query.comm)))
          }
          coe
        })
      } else {
        coeff <- fbppr.multiap(fbppr.df[neworder, "id"], rep(query.node$id, nrow(fbppr.df)), comm) # Get AP by that order
      }
      
      tmpres <- data.frame(rep(lbl, length(coeff)), 1:length(coeff), unlist(coeff), newtitle)
      colnames(tmpres) <- c("label", "rank", "coeff", "title")
      g.df <- rbind(g.df, tmpres)
      
    }
  }else {
    for(i in interval) {
      lbl <- paste(expression(lambda),"",i) # get colname
      score <- NULL
      if (abs(i - 1) < 0.001) {
        score <- fbppr.df$fppr_score
      } else {
        #score <- i *(1 - fbppr.df$fppr_rank/(deg[which(deg$id == query.node$id2),"outd"]+1))  + (1-i) *( 1 - fbppr.df$bppr_rank/(deg[which(deg$id == fbppr.df$id),"ind"]+1)) # Calc score
        score <- i * fbppr.df$fppr_score + (1-i) * fbppr.df$bppr_score
      }
      neworder <- order(score, decreasing = TRUE) # New rank
      newtitle <- ""
      #if ("title" %in% colnames(fbppr.df)) {
      #  newtitle <- fbppr.df[order(neworder), "title"]  
      #} else {
      #  newtitle <- rep("", dim(fbppr.df)[1])
      #}
      
      # Calculate jaccard coefficient
      coeff <- NULL
      if (.ap == FALSE) {
        coeff <- lapply(fbppr.df[neworder, "id"], function(x){
          # get communities of x
          comm_x <- unlist(comm[which(comm$id==x), "cluster"])
          # Calculate jaccard coefficient
          coe <- NULL
          if (.normalized) { # Normalize result by community size
            coe <- sum(2/unlist(comm[which(comm$id %in% intersect(comm_x, query.comm)), "count"])) / length(union(comm_x, query.comm))
          } else { # Do not normalize 
            coe <- as.double(length(intersect(comm_x, query.comm))) / as.double(length(union(comm_x, query.comm)))
          }
          if (coe == 0) {
            print(query.node$id)
            print(x)
          }
          coe
        })
      } else {
        coeff <- fbppr.multiap(fbppr.df[neworder, "id"], rep(query.node$id, nrow(fbppr.df)), comm) # Get AP by that order
      }
      
      tmpres <- data.frame(rep(lbl, length(coeff)), 1:length(coeff), unlist(coeff), newtitle)
      colnames(tmpres) <- c("label", "rank", "coeff", "title")
      g.df <- rbind(g.df, tmpres)
    }
  }
  g.df
}