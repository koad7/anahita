
globalVariables(
  c(
    "account__c", "accountid",  "ann", "ann_1", "ann_2", "ann_3",
    "apttus_billing__billtoaccountid__c", "asset_end_date",
     "community__c", "contract_status", "createddate",
    "diff_4", "domain", "email_domain__c", "engagement_level__c",
    "get_nc_whitelisted_domains", "has_config", "has_onboarded__c", "id",
    "is_operational_contact", "is_tlo", "is_top_engagement", "ispersonaccount",
     "main_industry_sector__c.x", "main_industry_sector__c.y",
    "member_status__c", "membership_type__c", "n", "name", "org_id", "org_name",
    "org_pem_1", "org_pem_2", "organization__c", "orgprofile_completeness__c.x",
    "orgprofile_completeness__c.y", "password", "password_anahita", "password_dw",
    "pem_email", "pem_name", "pem_sfid", "pem_status", "pem_tech_sfid", "pem_type",
    "personemail", "primaryorganizationid__c", "quarter", "recordtypeid",
    "si_sub_final_text__c", "si_sub_type__c",
    "stagename", "status__c", "tlo_id", "tlo_name", "tlo_pem_1", "tlo_pem_2",
    "user_anahita", "user_dw", "var_sel_people", "primary_community__c", "membername__c",
    "status", "nr2_email", "nr2_secret", "Industries", "Platform 1", "category_groups_list",
    "cb_token", "con_an", "con_anahita", "con_dw", "conn", "group_by", "industry_name", 
    "legal_name", "match_domain", "match_name", "matched", "n_sfid", "n_uuid", "platform",
    "sf_legal_name", "sf_name", "sf_url_clean", "sfid", "url_clean", "uuid", "last_updated",
    "mem_types", "org_ids", "sub_types", "dms", "top_level_organization__pc", "has_an_active_position__c",
    "datememberwillleavethecommunity__c", "joiningdate__c", "community_ids", "contract_status__c", 
    "last_col", "membership_id", "toplevelorganization__c", "list_ncs_constituents", "list_ncs_dms", 
    "list_ncs_members", "list_ncs_ptms", "billingcity", "city1__c", "main_industry_sector__c",
    "org_country", "org_industry", "org_profile", "org_region", "org_website", 
    "organization_country_name__c", "organizationprofile__c", "primary_engagement_manager_name__c",
    "region__c", "tech_orgfulladdress__c", "type_contract", "website", "city", "city_events",
    "mp_country_code", "total_events", "distinct_id", "article_id", "client", "current_url", 
    "entityId", "event", "language", "n_cities", "page", "percentage_city", "secondaryEntityId", 
    "sf_id", "subpage", "time", "org_members", "org_id", "article_published_at", "article_updated_at",
    "article_slug", "article_title", "biography", "first_name", "organisation", "position",
    "topic_title", "category__c", "community_name", "network__c", "id_url",
    "insights_events", "issues_events", "month", "total_browsing_events", "week", "surname", "time_since", 
    "time_to", "aurora_user", "aurora_pass", "Date", "account_info", "email_type", "entityName", 
    "story_name", "topic_id_url", "toplink_username__c", "user_email", "webinar_id", "account_value_network_roles__c",
    "all_users all_users_logged_in", "all_users_tpk", "become_member_time", "browser_version",
    "contactstatus__c", "deploy_status", "digital_members_users", "entityType", "firstname", "host",
    "intelligence_time", "lastname", "minute_diff", "newsletter_subscription__c",
    "payment_page_time", "personhasoptedoutofemail", "plan",  "referring_domain",
    "screen_height", "screen_width", "secondaryEntityName", "sf_ids",
    "stripe_create", "stripe_create_time", "stripe_id__c", "suffix_extract",
    "unsubscribed_in_mc__c", "users utm_campaign", "utm_medium", "utm_source",
    "all_users", "all_users_logged_in", "con_dm", "utm_campaign", "mp_dm", "mp_si", "users",
    "account_sfdc_id", "billingcountry", "company_sfid", "generic",
    "industry", "organization_name", "organization_sfdc_id", "session_sfdc_id",
    "session_start_date_utc", "whitelist", "a_articleid", "a_edited_timestamps", "a_moderators",
    "a_time_analyzed", "accountsource", "api_key_neo4j_qa", "experts__c",
    "is_primary_expertise__c", "s_active", "s_active.x", "s_active.y",
    "s_auto_approved.x", "s_auto_approved.y", "s_id", "s_name.x", "s_name.y", "test",
    "topic_id", "topic_type", ".", "a_approved", "a_time", "a_topics", "approved", "df_net",
    "ignored", "name_ins", "name_iss", "name_top", "recordtype", "rejected", "s_name",
    "score", "sfid_ins", "sfid_top", "title_ins", "title_top", "type_ins", "type_top", "fullname__c",
    "extension", "grouped_domain", "splits", "stripe_status", "tlo_calculated",
    "tlo_final"
    ))

# making sure all tables with people accounts always have a common set of fields
# easy to update for all by adding variables to the below vector

var_sel_people <- c(
  "id", "name", "age__c", "firstname", "lastname", "fullname__c", "formalgreeting__c", "personemail", "toplink_username__c",
  "gender__c", "type", "toplink_account_role_formula__c", "primary_position__c", "primary_position_level__c", 
  "primaryorganizationname__c", "primaryorganizationid__c", "primary_position_category__c", "primary_organisation__c",
  "organization_primary_community_name__c", "organization_country_name__c",
  "organizationcountry__c", "top_level_organization__pc", "primary_community__c", 
  "communitynames__c", "recordtypeid", "toplink_account_status_formula__c", "toplink_last_activity_date__c",
  "si_sub_final_text__c", "si_sub_type__c", "si_sub_override__c", "si_sub_payment__c", "accountsource", "stripe_id__c",
  "createddate", "newsletter_subscription__c", "personhasoptedoutofemail", 
  "unsubscribed_in_mc__c", "contactstatus__c"
  )

  membership_tps <- "a0eb0000000CwUXAA0"
  membership_gis <- "a0e0X00000GcHiAQAV"


  weforum_domains <- c(
    "weforum.org", "www.weforum.org", 
    "jp.weforum.org", "cn.weforum.org", 
    "fr.weforum.org", "www3.weforum.org")


valid_columns <- c(
  "time", "distinct_id", "browser", "browser_version", "city", 
  "current_url", "device_id", "initial_referrer", "initial_referring_domain", 
  "insert_id", "lib_version", "os", "region", "screen_height", 
  "screen_width", "client", "language", "mp_country_code", "mp_lib", 
  "mp_processing_time_ms", "entityId", "entityName", "entityType", 
  "page", "subpage", "referrer", "referring_domain", "secondaryEntityId", 
  "secondaryEntityName", "secondaryEntityType", "user_id", "App ID", 
  "App Name", "App Version", "Operating System", "Platform", "Screen Height", 
  "Screen Width", "User Agent", "client_id", "Model", "OS Version", 
  "customer", "userId", "device", "utm_campaign", "utm_medium", 
  "utm_source", "search_engine", "entityID", "searchTerm", "topicId", 
  "entityUrl", "mp_keyword", "event", "deepLink", "app_version_string", 
  "model", "os_version", "question", "value", "app_id", "app_name", 
  "app_version", "device_name", "platform", "screen_size", "user_agent", 
  "utm_term", "nchar", "user_type")

mixpanel_columns_sry <- c(
  "time", "distinct_id", "browser", "browser_version", "city", 
  "current_url", "device_id", "initial_referrer", "initial_referring_domain", 
  "insert_id", "lib_version", "os", "region", "screen_height", 
  "screen_width", "client", "language", "mp_country_code", "mp_lib", 
  "mp_processing_time_ms", "entityId", "entityName", "entityType", 
  "page", "subpage", "referrer", "referring_domain", "secondaryEntityId", 
  "secondaryEntityName", "secondaryEntityType", "user_id", "App ID", 
  "App Name", "App Version", "Operating System", "Platform", "Screen Height", 
  "Screen Width", "User Agent", "client_id", "Model", "OS Version", 
  "customer", "userId", "device", "utm_campaign", "utm_medium", 
  "utm_source", "search_engine", "entityID", "searchTerm", "topicId", 
  "entityUrl", "mp_keyword", "event", "deepLink", "app_version_string", 
  "model", "os_version", "question", "value", "app_id", "app_name", 
  "app_version", "device_name", "platform", "screen_size", "user_agent", 
  "utm_term", "nchar", "user_type")

mixpanel_columns_tpk <- c(
  "time", "distinct_id", "browser", "browser_version", "city", 
  "current_url", "device_id", "initial_referrer", "initial_referring_domain", 
  "insert_id", "lib_version", "os", "region", "screen_height", 
  "screen_width", "mp_country_code", "mp_lib", "mp_processing_time_ms", 
  "page", "platform", "user_id", "senderId", "type", "referrer", 
  "referring_domain", "subpage", "action", "device", "event", "utm_campaign", 
  "utm_medium", "utm_source", "search_engine", "mp_keyword", "context", 
  "label", "entityId", "entityType", "nchar", "user_type")

mixpanel_columns_dms <- c(
  "distinct_id", "event", "time", "entityName", "entityType",
  "secondaryEntityType", "mp_country_code",
  "city", "page", "subpage", "entityId", "secondaryEntityId",
  "client", "os", "utm_campaign", "utm_medium", "utm_source", "utm_term",
  "secondaryEntityName", "current_url")

stripe_excl_ids <- c(
  "0010X00004fJILnQAO",
  "0010X00004lTKwPQAW",
  "00168000002WCKlAAO"
)
