library(dplyr)
library(DT)
library(ggrepel)
library(feather)
library(igraph)
library(networkD3)
library(readr)
library(shiny)
library(stringi)
library(tidyr)
library(threejs)
library(visNetwork)


get_score <- function(type = c('log-dpf'), obs, exp) {
  this_score <- NA
  print(type)
  if (type == 'pmi') {
    this_score <- log(obs / exp)
  } else if (type == 'n-pmi') {
    this_score <- log(obs / exp) / log(1 / exp)
  } else if (type == 't-score') {
    this_score <- (obs - exp) / sqrt(obs)
  }
  else if (type == 'pmi-sig') {
    this_score <- sqrt(min(obs, exp)) * log(obs / exp)
  }
  else if (type == 'pmi-s') {
    this_score <- sqrt(max(obs, exp)) * log(obs / exp)
  }
  else if (type == 'log-dpf') {
    this_score <- log(obs / (exp ^ 0.78))
  }
  else if (type == 'dpf') {
    this_score <- obs / (exp ^ 0.78)
  }
  return(this_score)
}
