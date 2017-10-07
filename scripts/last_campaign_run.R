#' gather namespace data from database connection
max_campaign_run <- dbGetQuery(db_connection, paste0("select table_id, max(date) as last_campaign_run from bq.counters 
                                                      where name in ('delivered', 'ia_delivered') and
                                                      label = 'non-control'
                                                      group by table_id"))

#' rename baskin_robbins to baskin-robbins
max_campaign_run$table_id[which(max_campaign_run$table_id == 'baskin_robbins')] <- 'baskin-robbins'