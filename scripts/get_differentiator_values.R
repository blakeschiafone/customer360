#' calculate differentiator proportions for each namespace


library(dplyr)
library(RPostgreSQL)


#' get data and group proportion of messages sent (conversion + lifecycle + experiences / total volume)
adoption_auto_volume <- dbGetQuery(db_connection,
                                   paste0("select table_id, 
                                    sum(case when campaign_type in ('auto', 'trigger', 'program_message') then total else 0 end) as auto_volume,
                                    sum(total) as total_volume
                                    from bq.counters
                                    where name in ('delivered', 'ia_delivered') and date >= '", Sys.Date() - 7, "' and label = 'non-control'
                                    group by table_id
                                   "))


#' get data and group proportion of messages sent (message copy personalization / total volume)
adoption_personalization_volume <- dbGetQuery(db_connection,
                                    paste0("select table_id,
                                    sum(case when message_text LIKE '%{{%' then total else 0 end) as personalization_volume,
                                    sum(total) as total_volume
                                    from
                                    (select * from bq.counters
                                    where name in ('delivered', 'ia_delivered') and date >= '", Sys.Date() - 7, "' and label = 'non-control') as counters
                                    left join
                                    (select * from bq.campaigns) as campaigns
                                    on counters.key = campaigns.key
                                    group by table_id
                                   "))


#' get data and group proportion of messages sent (optimiztation / total volume)
adoption_optimization_volume <- dbGetQuery(db_connection,
                                    paste0("select table_id,
                                    sum(case when algo_flag = 'true' then total else 0 end) as optimization_volume,
                                    sum(total) as total_volume
                                    from
                                    bq.counters
                                    where name in ('delivered', 'ia_delivered') and date >= '", Sys.Date() - 7, "' and label = 'non-control'
                                    group by table_id
                                   "))


#' calculate proportions
# adoption_auto_volume$ratio <- with(adoption_auto_volume, {
#                                 round(as.numeric(format(auto_volume / total_volume, scientific = FALSE)), 3)
#                               })
# 
# adoption_personalization_volume$ratio <- with(adoption_personalization_volume, {
#                                             round(as.numeric(format(personalization_volume / total_volume, scientific = FALSE)), 3)
#                                           })
# 
# adoption_optimization_volume$ratio <- with(adoption_optimization_volume, {
#                                         round(as.numeric(format(optimization_volume / total_volume, scientific = FALSE)), 3)
#                                       })