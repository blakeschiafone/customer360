query <- 'select
namespace,
max(created) last_campaign_created
from table_query([tap-nexus:kahuna_campaign_group], \'not table_id contains "kahuna" and 
not table_id contains "_qa" and 
not table_id contains "sandbox" and 
not table_id contains "_dev" and 
not table_id contains "demo"\')
group by namespace'


max_campaign_results <- query_exec(query = query, project = 'kahuna-bq-access')