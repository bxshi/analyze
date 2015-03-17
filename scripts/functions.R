# Calculate average precision
fbppr.ap <- function(pred, actual) {
  len <- min(length(pred), length(actual))
  p <- rep(0.0, len)
  ap <- rep(0.0, len)
  cnt <- 0
  for(i in 1:len) {
    if (pred[i] == actual[i]) {
      cnt <- cnt + 1
      p[i] <- cnt / i # precision = #Rel / N
    } else {
      p[i] <- 0
    }
    if (cnt == 0) {
      ap[i] == 0
    } else {
      ap[i] <- sum(p[1:i]) / cnt
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
    neworder <- order(i * fbppr.df$fppr_score  + (1-i) * fbppr.df$bppr_score, decreasing = TRUE) # New rank
    newtitle <- fbppr.df[order(neworder), "title"]
    ap <- fbppr.ap(fbppr.df[neworder, "cluster"], rep(query.node$cluster, nrow(fbppr.df))) # Get AP by that order
    tmpres <- data.frame(rep(lbl, length(ap)), 1:length(ap), ap, newtitle)
    colnames(tmpres) <- c("label", "rank", "AP", "title")
    g.df <- rbind(g.df, tmpres)
  }
  g.df
}

# Calculate average precision