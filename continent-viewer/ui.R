library(dplyr)
library(DT)
library(ggrepel)
library(feather)
library(igraph)
library(networkD3)
library(readr)
library(shiny)
library(shinydashboard)
library(stringi)
library(tidyr)
library(threejs)
library(visNetwork)


config_panel <- tabPanel("Configuration",
                         
                         radioButtons('measure_radio', 'measure', inline=TRUE, choices = c('log-dpf','t-score','dpf', 'pmi','n-pmi','pmi-sig','pmis')),
                         selectInput('dataset', 'Dataset', choices=c('EU reddit corpus')),
                         tags$hr(),
                         sliderInput("conc", label = 'Concreteness threshold', min = 0, max = 5, value = 4.5, step = 0.1),
                         sliderInput("label_size", label = '2D node label size', min = 0, max = 8, value = 3, step = 0.1),
                         checkboxInput('use_dict', "Filter concrete words?", value=FALSE),
                         checkboxInput('show_labels', "show labels?", value=TRUE),
                         checkboxInput("nightime",'dark backgrounds',value=FALSE),
                         checkboxInput("full_rotate",'enable full rotation',value=FALSE)
)

diff_panel <-  tabPanel("Diff view",
                        sidebarLayout(
                          sidebarPanel(
                            radioButtons('measure_radio', 'measure',choices = c('log-dpf','pmi','n-pmi','pmi-sig','pmis','t-score')),
                            textInput('diff1', 'word1', value = "climate"),
                            textInput('diff2', 'word2', value = "growth")
                          ),
                          mainPanel(
                            DT::DTOutput('diffTable')
                          )
                        ))

table_panel <- tabPanel("Main table",
                        DT::DTOutput("main_table"))

centrality_panel <- tabPanel("Centrality",
                             DT::dataTableOutput("centralities"))

sp_panel <-  tabPanel("Shortest Path",
                      scatterplotThreeOutput('sp_vis', width = '100%', height = '95vh')
)
sp_panel_2d <-  tabPanel("Shortest Path 2D",
                              visNetworkOutput('sp_vis2D', height = "100vh")
)


declines_panel <-  tabPanel("Decline Plots",
                            plotOutput('declines', width = '100%', height = '95vh')
)



cent_declines_panel <-  tabPanel("Centrality Decline",
                                 plotOutput('cent_declines', width = '100%', height = '95vh')
)

network_panel <-  tabPanel("Network 3D",
                           scatterplotThreeOutput('networkVis', width = '100%', height = '95vh')#,
)

network_panel_2d <-  tabPanel("Network 2D",
                              visNetworkOutput('networkVis2D', height = "100vh")
)

cliques_panel <-  tabPanel("Cliques 3D",
                           scatterplotThreeOutput('cliques', width = '100%', height = '95vh')#,
)
cliques_panel2D <-  tabPanel("Cliques 2D",
                             visNetworkOutput('cliques2D', width = '100%', height = '95vh')#,
)

cliques_table <-  tabPanel("Cliques Table",
                           dataTableOutput("cliquetab")
)

communities_panel <- tabPanel("Communities",
                              plotOutput("dendr"))



# Header elements for the visualization
header <- dashboardHeader(title = "EU reddit Corpus", disable = FALSE)
rels <- arrow::read_parquet(file.path('data','continent-all-periods-scored.parquet'))

# Sidebar elements for the search visualizations
sidebar <- dashboardSidebar(
 # selectizeInput('term', 'Search term(s):', sort(unique(rels$focal)), selected = NULL, multiple = TRUE,
  #               options = NULL),
  textInput('term', 'Search term(s):', value = "eu europe brexit"),
  sliderInput("thresh", label = 'Score threshold ', min = -1, max = 4, value = 0.0, step = 0.1),
  sliderInput("rank", label = 'Rank threshold ', min = 0, max = 50, value = 20, step = 1),
  selectInput('time_period', 'Time Period', choices=c("2009-11","2012-14","2015-17","2018-20"), selected="2015-17"),
  conditionalPanel(condition = "input.tabs!= null && input.tabs == 'Network 3D'", 
                   textInput('highlight', 'Highlight node:', value = "") ),
  
  conditionalPanel(condition = "input.tabs!= null && input.tabs == 'Network 2D' || input.tabs == 'Network 3D' || input.tabs == 'Cliques 3D' || input.tabs == 'Cliques 2D' || input.tabs == 'sp_panel_2d' || input.tabs == 'sp_panel'  ",
                   htmlOutput("user_feedback"),
                   
                   selectInput("2d_layout", label="2D layout",c('layout_with_fr', 'layout_with_drl', 'layout_nicely','layout_in_circle','layout_with_dh','layout_with_gem', 'layout_with_kk','layout_with_graphopt', 'layout_with_mds' )),
                   selectInput("community_method", label="Community Detection Method",c('fast_greedy','leading_eigen','walktrap')),
                   sliderInput("network_steps", label = 'Steps from search nodes (radius of ego network)', min = 0, max = 4, value = 1, step = 1),
                   sliderInput("prune_slide", label = "prune nodes of degree < ", min=1, max=5, value=2,step=1),
                   sliderInput("cent_cutoff", label = "Centrality sample size", min=4, max=8, value=4,step=1)
  ),
  conditionalPanel(condition = "input.tabs!= null && input.tabs == 'Cliques 3D' || input.tabs == 'Cliques 2D'  ",
                   sliderInput("clique_min", label = "Min Clique Size", min=1, max=8, value=3,step=1)
  ),
  conditionalPanel(condition = "input.tabs!= null && input.tabs == 'sp_panel_2d' || input.tabs == 'sp_panel' ",
                   checkboxInput("sp_weights",'path weights?',value=FALSE)
  ),
  
  
  htmlOutput("data_summary"),
  htmlOutput("options_status"),
  htmlOutput("loaded_file"),
  
  downloadLink('downloadNetwork', 'Download network')
)

#Body elements for the search visualizations.
body <- dashboardBody(
  tabsetPanel(
    id = "tabs",
    config_panel,
    network_panel,
    network_panel_2d,
    diff_panel,
    sp_panel,
    sp_panel_2d,
    centrality_panel,
    cent_declines_panel,
    cliques_panel,
    cliques_panel2D,
    cliques_table,
    communities_panel,
    declines_panel,
    table_panel
  )
)

dashboardPage(header, sidebar, body, skin = "black")