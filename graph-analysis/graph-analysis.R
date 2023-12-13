library(arrow)
library(igraph)
library(tidyverse)



rels <- arrow::read_parquet("continent-all-periods-scored.parquet")
rels <- filter(rels, score > 0)
period <- filter(rels, year=="2009-11") %>% select(-year)

g <- graph_from_data_frame(period, directed = FALSE) 
g<-igraph::simplify(g, remove.loops = TRUE)

ego_terms <- "eu uk europe"
if(nchar(ego_terms) > 1){
  tmp <- ego_terms %>% strsplit("[, ]") %>% unlist
  ego_node <- tmp[tmp!=''] 
  sublist <- make_ego_graph(g, 1, ego_node, mode = "all")
  merged <- do.call(igraph::union, sublist)
}else{
  merged <- g
}
#merged <- delete.vertices(merged, V(merged)[ degree(merged) < input$prune_slide] )
bc <- betweenness(merged, cutoff = 4)
bcdf <- data.frame(bc, names(bc)) %>% arrange(desc(bc))

ec <- eigen_centrality(merged)
ecdf <- data.frame(ec, names(bc)) %>% arrange(desc(bc))

pr <- page_rank(merged)
prdf <-data.frame(pr$vector)
