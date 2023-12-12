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

shinyServer(function(input, output) {
  
  cur_file <- 'no data loaded'
  
  draw_network2D <- function(merged){
    print('draw_network2d')
    dg <- log(degree(merged, V(merged)))
    dg <- (dg/max(dg))+1
    V(merged)$size <- dg * 10
    if (input$community_method == "fast_greedy"){
      gg.com <- fastgreedy.community(merged)
    }else if(input$community_method == "leading_eigen"){
      gg.com <- leading.eigenvector.community(merged)
    }else if(input$community_method == "walktrap"){
      gg.com <- walktrap.community(merged)
    }
    
    V(merged)$color <- gg.com$membership + 1
    if(input$prune_slide > 0){
      merged <- delete.vertices(merged, 
                                V(merged)[ degree(merged) < input$prune_slide] )
    }
      td <- toVisNetworkData(merged)
      ws <- select(td$edges, starts_with("weight"))
      ws[is.na(ws)] <- 0 
      td$edges$value <- rowSums(ws)
      td$nodes$font.size = input$label_size*10
      conc_output <- "none"
      if(input$use_dict){
        conc_output <- input$prune_slide
      }
      vn <- visNetwork(nodes=td$nodes, edges=td$edges, height='100vh', width='100%')%>%  visIgraphLayout(layout = input$`2d_layout`, type='full') %>%
        visOptions(highlightNearest = list(enabled = T, hover = T), 
                   nodesIdSelection = T) %>% visExport(name=paste(input$term, input$decade,input$measure_radio, input$thresh,input$rank,input$network_steps, input$prune_slide,conc_output,sep='-'))
    
    }
  
  
  
  draw_network <- function(merged){
    dg <- log(degree(merged, V(merged)))
    dg <- dg/max(dg)
    V(merged)$size <- dg * 0.8
    if (input$community_method == "fast_greedy"){
      gg.com <- fastgreedy.community(merged)
    }else if(input$community_method == "leading_eigen"){
      gg.com <- leading.eigenvector.community(merged)
    }else if(input$community_method == "walktrap"){
      gg.com <- walktrap.community(merged)
    }
    V(merged)$color <- gg.com$membership + 1
    if(input$highlight != '' ){
      V(merged)$color <- ifelse(V(merged)$name %in% input$highlight, 'red', 'gray')
    }
    if(input$prune_slide > 0){
      merged <- delete.vertices(merged, 
                                V(merged)[ degree(merged) < input$prune_slide] )
    }
    V(merged)$label <- V(merged)$name
    V(merged)$shape <- "circle"
    bgc <- "white"
    if (input$nightime){
      bgc <- "black"
    }
    if(input$show_labels){
      graphjs(merged, use.orbitcontrols=!(input$full_rotate), bg=bgc) %>% points3d(vertices(.),pch = V(merged)$name,size = V(merged)$size)
    }else{
      graphjs(merged, vertex.shape=('circle'), use.orbitcontrols=!(input$full_rotate), bg=bgc)
    }
  }
  
  update_data <- reactive({
    print('updating the data')
    conc <- read_tsv('data/concreteness.txt') %>% filter(Bigram == 0, Conc.M < input$conc)
    output$user_feedback <- renderText({'updating main data file... reading data.'})
    if(input$dataset == 'EU reddit corpus'){
      cur_file <- file.path('data','continent-all-periods-scored.parquet')
    }
    rels <- arrow::read_parquet(cur_file) 
    print(summary(rels))

    output$loaded_file <- renderText({cur_file})
    output$user_feedback <- renderText({'updating main data file... sorting'})
      rels <- filter(rels, score > input$thresh)
      rels <- filter(rels, year == input$time_period)
      output$user_feedback <- renderText({'updating main data file... filtering'})
      rels <- group_by(rels, focal) %>% arrange(desc(score)) %>%
        mutate(rank = order(score, decreasing = TRUE))%>% ungroup
    
    rels <- filter(rels, rank < input$rank)
    print(rels)
    output$user_feedback <- renderText({'done.'})
    rels
  })
  
  output$sp_vis <- renderScatterplotThree({
    print('in shortest path')
    rels <- update_data()
    relations <- data.frame(from=rels$focal, to=rels$bound, weight=rels$score) 
    g <- graph_from_data_frame(relations, directed = FALSE) 
    g<-igraph::simplify(g, remove.loops = TRUE)
    tmp <- input$term %>% strsplit("[, ]") %>% unlist
    ft <- tmp[tmp!=''] 
    print(ft)
    print(summary(ft))
    tmp <- shortest_paths(g, ft[1], ft[2])
    print(summary(tmp))
    sublist <- make_ego_graph(g, input$network_steps, tmp$vpath[[1]], mode = "all")
    merged <- do.call(igraph::union, sublist)
    draw_network(merged)
  })
  
  #shortest path
  output$sp_vis2D <- renderVisNetwork({
    print('in shortest path network 2D view')
    rels <- update_data()
    relations <- data.frame(from=rels$focal, to=rels$bound, weight=rels$score) 

    g <- graph_from_data_frame(relations, directed = FALSE) %>% simplify
    g<-igraph::simplify(g, remove.loops = TRUE)

    print(input$term)
    tmp <- input$term %>% strsplit("[, ]") %>% unlist
    ft <- tmp[tmp!=''] 
    print(ft)
    print(summary(ft))
    if(input$sp_weights){
      tmp <- shortest_paths(g, ft[1], ft[2], weights = NA)
    }else{
      tmp <- shortest_paths(g, ft[1], ft[2], weights = NA)
    }
    sublist <- make_ego_graph(g, input$network_steps, tmp$vpath[[1]], mode = "all")
    merged <- do.call(igraph::union, sublist)
    draw_network2D(merged)
  })  
  
  
  output$centralities <- renderDataTable({
    output$user_feedback <- renderText({'computing centrality.'})
    rels <- update_data()
    relations <- data.frame(from=rels$focal, to=rels$bound, weight=rels$score)
    g <- graph_from_data_frame(relations, directed = FALSE) 
    g<-igraph::simplify(g, remove.loops = TRUE)
    if(nchar(input$term) > 1){
      tmp <- input$term %>% strsplit("[, ]") %>% unlist
      ego_node <- tmp[tmp!=''] 
      sublist <- make_ego_graph(g, input$network_steps, ego_node, mode = "all")
      merged <- do.call(igraph::union, sublist)
    }else{
      merged <- g
    }
    merged <- delete.vertices(merged, V(merged)[ degree(merged) < input$prune_slide] )
    
    bc <- estimate_betweenness(merged, cutoff = input$cent_cutoff, weights=NA)
    bdcf8 <- data.frame(bc, names(bc)) %>% arrange(desc(bc))
    bdcf8$rank <- seq(1, nrow(bdcf8))
    p1 <- ggplot(bdcf8[1:20,], aes(x = rank, y = bc, label=names.bc. )) + geom_point() + geom_label_repel()
    output$cent_declines <- renderPlot(p1)
    output$user_feedback <- renderText({'computing centrality...done.'})
    bcdf <- data.frame(bc, names(bc)) %>% arrange(desc(bc))
  })
  
  
  output$main_table <- renderDT({
    print('searching table')
    rels <- update_data()

    tmp <- input$term %>% strsplit("[, ]") %>% unlist
    tmp <- tmp[tmp!=''] 
    print(tmp)
    t1 <- filter(rels, focal %in% tmp) %>% datatable(options = list(pageLength=20)) %>% formatRound(c("score","Freq","log_dpf"), 2)
    print(t1)
    })
  
  output$networkVis <- renderScatterplotThree({
    print('in network view')
    rels <- update_data()
    relations <- data.frame(from=rels$focal, to=rels$bound, weight=rels$score) 
    g <- graph_from_data_frame(relations, directed = FALSE) 
    g<-igraph::simplify(g, remove.loops = TRUE)
    tmp <- input$term %>% strsplit("[, ]") %>% unlist
    ego_node <- tmp[tmp!=''] 
    sublist <- make_ego_graph(g, input$network_steps, ego_node, mode = "all")
    merged <- do.call(igraph::union, sublist)
    draw_network(merged)
  })  
  
  output$networkVis2D <- renderVisNetwork({
    print('in network 2D view')
    rels <- update_data()
    relations <- data.frame(from=rels$focal, to=rels$bound, weight=rels$score) 
    g <- graph_from_data_frame(relations, directed = FALSE)
    g<-igraph::simplify(g, remove.loops = TRUE)
    print(input$term)
    tmp <- input$term %>% strsplit("[, ]") %>% unlist
   # tmp = input$term
    ego_node <- tmp[tmp!=''] 
    sublist <- make_ego_graph(g, input$network_steps, ego_node, mode = "all")
    merged <- do.call(igraph::union, sublist)
    draw_network2D(merged)
  })  
  
  
  output$diffTable <- renderDataTable({
    rels <- update_data()
    t1 <-  filter(rels, (focal == input$diff1)) 
    t2 <-  filter(rels, (focal == input$diff2)) 
    tmp <- inner_join(t1, t2, by=c('bound')) %>% mutate(freq_diff=Freq.x-Freq.y, score_diff=score.x-score.y)
    d1 <- select(tmp, bound, freq_diff, score_diff)
    d1
  }, searchDelay = 800)
  
  output$cliques <- renderScatterplotThree({
    print('in network view')
    rels <- update_data()
    relations <- data.frame(from=rels$focal, to=rels$bound, weight=rels$score) 
    g <- graph_from_data_frame(relations, directed = FALSE) %>% simplify
    tmp <- input$term %>% strsplit("[, ]") %>% unlist
    ego_node <- tmp[tmp!=''] 
    sublist <- make_ego_graph(g, 1, ego_node, mode = "all")
    merged <- do.call(igraph::union, sublist)
    mc <- cliques(merged, min = input$clique_min) %>% unlist
    print(as.numeric(mc))
    merged <- induced_subgraph(merged, as.numeric(mc))
    # merged <- delete.vertices(merged,V(merged)[which(!(V(merged) %in% mc))])
    draw_network(merged)
  })
  
  output$cliques2D <- renderVisNetwork({
    print('in network 2D view')
    rels <- update_data()
    relations <- data.frame(from=rels$focal, to=rels$bound, weight=rels$score) 
    g <- graph_from_data_frame(relations, directed = FALSE) %>% simplify
    tmp <- input$term %>% strsplit("[, ]") %>% unlist
    ego_node <- tmp[tmp!=''] 
    sublist <- make_ego_graph(g, input$network_steps, ego_node, mode = "all")
    merged <- do.call(igraph::union, sublist)
    mc <- cliques(merged, min = input$clique_min) %>% unlist
    print(as.numeric(mc))
    merged <- induced_subgraph(merged, as.numeric(mc))
    draw_network2D(merged)
  })  
  
  
  output$cliquetab <- renderDataTable({
    rels <- update_data()
    print("GETTING tab1")
    relations <- data.frame(from=rels$focal, to=rels$bound, weight=rels$score) 
    g <- graph_from_data_frame(relations, directed = FALSE) %>% simplify
    tmp <- input$term %>% strsplit("[, ]") %>% unlist
    ego_node <- tmp[tmp!=''] 
    sublist <- make_ego_graph(g, 1, ego_node, mode = "all")
    merged <- do.call(igraph::union, sublist)
    mc <- cliques(merged, min = input$clique_min) 
    m <- max(sapply(mc, length))
    tmp <- sapply(mc, function(x) {
      V(merged)$name[x]
    })
    print("GETTING CLIQUES")
    data <-
      lapply(tmp, function(row)
        c(row, rep(NA, m - length(row))))
    print(data)
    data <-
      matrix(
        unlist(data),
        nrow = length(data),
        ncol = m,
        byrow = TRUE
      ) %>% data.frame
  })
  
  output$dendr <- renderPlot({
    print('in network view')
    rels <- update_data()
    relations <- data.frame(from=rels$focal, to=rels$bound, weight=rels$score) 
    g <- graph_from_data_frame(relations, directed = FALSE) %>% simplify
    tmp <- input$term %>% strsplit("[, ]") %>% unlist
    ego_node <- tmp[tmp!=''] 
    sublist <- make_ego_graph(g, input$network_steps, ego_node, mode = "all")
    merged <- do.call(igraph::union, sublist)
    if (input$community_method == "fast_greedy"){
      com <- fastgreedy.community(merged)
    }else if(input$community_method == "leading_eigen"){
      com <- leading.eigenvector.community(merged)
    }else if(input$community_method == "walktrap"){
      com <- walktrap.community(merged)
    }
    plot_dendrogram(com)
  })
  
  output$declines <- renderPlot({
    print(input$term)
    print('in decline')
    fTable <- update_data() %>% filter(focal == input$term)
    print(head(fTable))
    print('in declines')
    p1 <- ggplot(fTable, aes(x = rank, y = score, label=bound )) + geom_point() + geom_label_repel()
    print(p1)
  })
  

  
  output$downloadNetwork <- downloadHandler(
    filename = function() {
      paste('network-', Sys.Date(), '.html', sep='')
    },
    content = function(con) {
      rels <- update_data()
      relations <- data.frame(from=rels$focal, to=rels$bound, weight=rels$score) 
      g <- graph_from_data_frame(relations, directed = FALSE) %>% simplify
      tmp <- input$term %>% strsplit("[, ]") %>% unlist
      ego_node <- tmp[tmp!=''] 
      sublist <- make_ego_graph(g, input$network_steps, ego_node, mode = "all")
      merged <- do.call(igraph::union, sublist)
      
      draw_network2D(merged) %>% visSave(con)
    }
  )
  
  output$options_status <- renderText({
    paste0("<pre>score: ", input$measure_radio,"<br>",
           "score threshold: ", input$thresh,"<br>",
           "rank threshold: ", input$rank,"<br>
           </pre>"
    )})
  
  output$data_summary <- renderText({
    paste0("<pre>",
           "Co-occurrence counts loaded.","<br>",
           "Number of relations: ", nrow(update_data()),"<br>", 
           "mean score: ", format(round( mean(update_data()$score), 3), nsmall = 2),"<br>",
           "median score: ", format(round(median(update_data()$score),3),nsmall = 2),"<br>",
           "min score:  ", format(round(min(update_data()$score),3),nsmall=2),"<br>",
           "max score:  ", format(round(max(update_data()$score),3),nsmall=2),"<br>",
           "</pre>"
    )})
  
  
})