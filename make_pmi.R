
library(arrow)

library(quanteda)

library(tidyverse)

source('scorers.R')
rels <- arrow::read_parquet("continent-coocs-2009-11.parquet")# %>% filter(str_detect(bound, paste(stopwords, collapse="|"), negate=TRUE) )
#r2 <- arrow::read_parquet("un-coocs-fr-yearcounts2002after.parquet")
#rels <- rbind(rels, r2)
#window_size <- 200
tmp <- filter(rels, focal=="haiti")

# independent frequencies of focal and bound
focal_fs <- group_by(rels, focal) %>% summarize(count = sum(count)) %>% ungroup
bound_fs <- group_by(rels, bound) %>% summarize(count = sum(count)) %>% ungroup
rels <- left_join(rels, focal_fs, by = c('focal'))
rels <- left_join(rels, bound_fs, by = c('bound'))
rels <- rename(rels, bound_e=count, focal_e = count.y)
rels <- rename(rels, count=count.x)


#thisN <- 700000000
thisN <- sum(rels$focal_e)


#rels <- mutate(rels, cl = map(count, ~poisson.test(.x)$conf.int[[1]]) )
rels <- mutate(rels, e11 = (focal_e/thisN)*(bound_e/thisN))

rels <- mutate(rels, obs = (count/thisN)) %>% filter(obs > e11)

#tmp <- filter(rels, focal=='competition')
#tmp <- mutate(tmp, cl = map2(obs, e11, ~t.test(.x,.y)$statistic) )
#tmp$test <- map2(tmp$obs, tmp$e11, ~prop.test(.x,.y)$statistic)

#rels %>% mutate(cu = poisson.test(count)$conf.int[[0]])


#rels <- mutate(rels, fel = focal_e - (1.96*sqrt(focal_e)), feu = focal_e +(1.96*sqrt(focal_e)))
#rels <- mutate(rels, bel = bound_e - (1.96*sqrt(bound_e)), beu = bound_e + (1.96*sqrt(bound_e)))
#rels <- mutate(rels, cl = count - (1.96*sqrt(count)), cu = count + (1.96*sqrt(count)))

#rels <- mutate(rels, e11l = (fel/thisN)*(bel/thisN))
#rels <- mutate(rels, obsl = (cl/thisN))

#rels <- mutate(rels, e11u = (feu/thisN)*(beu/thisN))
#rels <- mutate(rels, obsu = (cu/thisN))

rels$log_dpf <- get_score('log-dpf', rels$obs, rels$e11)
rels$tscore <- get_score('t-score', rels$obs, rels$e11)
rels$pmi <- get_score('pmi', rels$obs, rels$e11)
rels$score <- get_score('log-dpf', rels$obs, rels$e11)
#rels$scoreu <- get_score('log-dpf', rels$obsu, rels$e11u)
#rels$scorel <- get_score('log-dpf', rels$obsl, rels$e11l)

rels$pmi <- get_score('pmi', rels$obs, rels$e11)



rels <- group_by(rels, focal) %>% arrange(desc(score)) %>%
  mutate(rank = order(score, decreasing = TRUE))%>%
  ungroup %>% rename(Freq=count)

cut_off <- quantile(rels$score, .78)

rels_tq <- filter(rels, score > cut_off)  %>% mutate(across(is.numeric, round, digits=3))

rels_tq$score <- rels_tq$log_dpf 
tmp <- filter(rels_tq, focal=="ireland")
t2 <- filter(rels_tq, focal=="brexit")


arrow::write_parquet(rels_tq, 'un-coocs-fr-scored-year.parquet')

#write_feather(rels, 'latest-scored.feather')

