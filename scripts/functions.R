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

fbppr.multiap <- function(pred, actual, .comm) {
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

simrank.coeff <- function(simrank.df, lbl, query.node, comm) {
  g.df <- NULL
  query.comm <- unlist(comm[which(comm$id == query.node$id), "cluster"])
  coeff <- fbppr.multiap(simrank.df[order(simrank.df$rank, decreasing = FALSE), "id"], rep(query.node$id, nrow(simrank.df)), comm) # Get AP by that order
  tmpres <- data.frame(rep(lbl, length(coeff)), 1:length(coeff), unlist(coeff), "")
  colnames(tmpres) <- c("label", "rank", "coeff", "title")
  g.df <- rbind(g.df, tmpres)
}

# Calculate jaccard coefficient
fbppr.coeff <- function(fbppr.df, query.node, comm, .by=1, .interval=c(), .normalized=FALSE, .ap=FALSE, .multiply=FALSE) {
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
      score <- i * fbppr.df$fppr_score  + (1-i) * fbppr.df$bppr_score # Calc score
      neworder <- order(i * fbppr.df$fppr_score  + (1-i) * fbppr.df$bppr_score, decreasing = TRUE) # New rank
      newtitle <- fbppr.df[order(neworder), "title"]
      
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