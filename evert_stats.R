
library(arrow)

# you just have to come back to this. 

#library(tidyverse)

source('scorers.R')
rels <- arrow::read_feather('9196_3000.feather')# %>% filter(str_detect(bound, paste(stopwords, collapse="|"), negate=TRUE) )
window_size <- 20

# independent frequencies of focal and bound
focal_fs <- group_by(rels, focal) %>% summarize(count = sum(count)) %>% ungroup
bound_fs <- group_by(rels, bound) %>% summarize(count = sum(count)) %>% ungroup
rels <- left_join(rels, focal_fs, by = c('focal'))
rels <- left_join(rels, bound_fs, by = c('bound'))
rels <- rename(rels, bound_e=count, focal_e = count.y)
rels <- rename(rels, count=count.x)

rels <- mutate(rels, focal_e = ((focal_e%/%window_size)+1))

rels <- mutate(rels, bound_e = ((bound_e%/%window_size)+1))


rels$o11 <- rels$count
rels$o12 <- rels$focal_e - rels$count
rels$o21 <- rels$bound_e - rels$count
rels$o22 <- thisN - (rels$focal_e + rels$bound_e)
rels$e11 <- (rels$focal_e * rels$bound_e)/thisN
rels$e12 <- ((rels$o11 +rels$o22) * (rels$o12 + rels$o22))/thisN