#' gather namespace data from database connection
message_volume <- dbGetQuery(db_connection, paste0("select * from bq.counters 
                                                   where name in ('delivered', 'ia_delivered') and 
                                                   date >= '", Sys.Date() - 7, "' and
                                                   label = 'non-control'"))

#' group message_volume data by table_id
message_volume <- message_volume %>% 
  mutate(name = ifelse(name == 'ia_delivered', 'delivered', name)) %>%
  #renaming baskin_robbins to baskin-robbins as it's called that elsewhere
  mutate(table_id = ifelse(table_id == 'baskin_robbins', 'baskin-robbins', table_id)) %>%
  group_by(table_id) %>%
  summarize(total = sum(total))

#' gather map count using Sunday as cutoff
query <- paste0("SELECT namespace, 
current_total_users, 
current_mobile_users, 
current_non_mobile, 
billing_month, 
timestamp(date) as date
FROM [tap-nexus:kahuna_users.kahuna_billing]
where timestamp(date) = timestamp('", Sys.Date() - 1, "')")

map_results <- query_exec(query = query, project = 'kahuna-bq-access')