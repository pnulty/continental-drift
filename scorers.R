library(dplyr)
library(magrittr)
library(stringi)

get_score <- function(type=c('log-dpf'), obs, e11){
  this_score <- NA
  print(type)
  if(type=='pmi'){
    this_score <- log(obs/e11)
  }else if(type=='n-pmi'){
    this_score <- log(obs/e11)/-log(e11)
  }else if(type=='t-score'){
    this_score <- (obs-e11)/sqrt(obs)
  }
  else if(type=='pmi-sig'){
    this_score <- sqrt(min(obs, e11)) * log(obs/e11)
  }
  else if(type=='log-dpf'){
    this_score <- log(obs/ (e11^0.78) )
  }
  else if(type=='dpf'){
    this_score <- obs/ (e11^0.78)
  }
  return(this_score)
}
