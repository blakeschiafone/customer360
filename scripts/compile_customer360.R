#' build dataframe of:
#' 1. last email login
#' 2. last campaign created
#' 3. last campaign run
#' 4. total message volume
#' 5. map count
#' 6. ratio of total message volume / map count

library(dplyr)
library(RPostgreSQL)
library(jsonlite)


#' call scripts to pull data
source('/home/rstudio/scripts/db_connection.R')
source('/home/rstudio/scripts/customer360/scripts/last_email_login.R')
source('/home/rstudio/scripts/customer360/scripts/last_campaign_created.R')
source('/home/rstudio/scripts/customer360/scripts/last_campaign_run.R')
source('/home/rstudio/scripts/customer360/scripts/campaign_volume_map_counts.R')
source('/home/rstudio/scripts/customer360/scripts/get_differentiator_values.R')


#' get unique namespace from compiled results
namespaces <- c(last_login_results$namespace, map_results$namespace, max_campaign_results$namespace, message_volume$table_id,
                adoption_auto_volume$table_id, adoption_personalization_volume$table_id, adoption_optimization_volume$table_id) %>%
  unique()

#' build dataframe
cs360 <- data.frame(namespaces = namespaces, 
                    login_count = NA, 
                    last_campaign_created = NA, 
                    last_campaign_run = NA, 
                    message_volume = NA, 
                    map_count = NA, 
                    message_map_ratio = NA,
                    auto_ratio = NA,
                    personalization_ratio = NA,
                    optimization_ratio = NA)

#' match values from queries with cs360 dataframe
cs360$login_count <- last_login_results$unique_email_logins[match(cs360$namespaces, last_login_results$namespace)]
cs360$last_campaign_created <- as.Date(max_campaign_results$last_campaign_created[match(cs360$namespaces, max_campaign_results$namespace)])
cs360$last_campaign_run <- as.Date(max_campaign_run$last_campaign_run[match(cs360$namespaces, max_campaign_run$table_id)])
cs360$message_volume <- message_volume$total[match(cs360$namespaces, message_volume$table_id)]
cs360$map_count <- map_results$current_total_users[match(cs360$namespaces, map_results$namespace)]
cs360$message_map_ratio <- round(cs360$message_volume / cs360$map_count, 2)
cs360$auto <- adoption_auto_volume$auto_volume[match(cs360$namespaces, adoption_auto_volume$table_id)]
cs360$personalization <- adoption_personalization_volume$personalization_volume[match(cs360$namespaces, adoption_personalization_volume$table_id)]
cs360$optimization <- adoption_optimization_volume$optimization_volume[match(cs360$namespaces, adoption_optimization_volume$table_id)]

cs360 <- cs360[!is.na(cs360$map_count) | !is.na(cs360$login_count),]
#' convert dataframe to json format
cs360_json <- toJSON(cs360, pretty = TRUE)

#' save json file
write(cs360_json, file = paste0('/home/rstudio/scripts/customer360/output/cs360_', format(Sys.Date(), '%m_%d_%Y'), '.json'))

#' move json file to google storage bucket
system(paste0('gsutil cp /home/rstudio/scripts/customer360/output/cs360_', format(Sys.Date(), '%m_%d_%Y') ,'.json gs://customer360'))

#' update json file to web accessible link
system(paste0('gsutil acl ch -u AllUsers:R gs://customer360/cs360_', format(Sys.Date(), '%m_%d_%Y'), '.json'))

gc()
