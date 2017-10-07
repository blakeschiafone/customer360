library(bigrquery)
library(dplyr)
library(RForcecom)
library(jsonlite)
library(purrr)



#set variables for BQ pull
current_month <- as.Date(Sys.Date())
previous_month <- seq(from = as.Date(current_month, format = '%Y-%m-01'), by = '-1 month', length.out = 2)[2]
project <- 'kahuna-bq-access'

query <- paste0("select
namespace,
exact_count_distinct(email) as unique_email_logins
from
(select
namespace,
email,
#((timestamp(date(CURRENT_TIMESTAMP())) - timestamp(date(max(created))))/ 86400000000) as days
from [tap-nexus:kahuna_admin_logs.admin_logs_", format(current_month, format = '%Y%m'), "], [tap-nexus:kahuna_admin_logs.admin_logs_", format(previous_month, format = '%Y%m'),"]
where (created >= timestamp('", (current_month -7), "') and created <= timestamp('", current_month, "')) AND not email contains 'kahuna' AND not (namespace contains 'sandbox' or namespace contains '_qa'))
group by namespace")

last_login_results <- query_exec(query, project)



#output results as JSON
# output_results <- toJSON(results, pretty = TRUE)
#save output_results
# write(output_results, file = paste0('/home/rstudio/scripts/customer360/output/last_campaign_created_', format(Sys.Date(), '%m_%d_%Y'), '.json'))
