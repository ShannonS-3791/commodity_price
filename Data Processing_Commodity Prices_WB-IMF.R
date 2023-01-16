#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Commodity Prices + Index - World Bank and IMF
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


# Load Packages -----------------------------------------------------
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

library(jsonlite)
library(tidyverse)
library(janitor)
library(dplyr)

# Establish Parameters ----------------------------------------------
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

date_start <- '2012'
date_end <- '2022'

final_cols <- c("source","Commodity_Name","Unit","time_period","value","pct_change","value_2016index")

#******************************************************************************#
# Calculate: % Change and Index ------------------------------------------------
# + functions to be run on each dataframe (later)
#******************************************************************************#

calc_pct_change_year <- function(df) {
  
  annual_df <- df %>%
    filter(!grepl("M|Q",time_period)) %>%
    mutate(year = as.numeric(time_period)) %>%
    group_by(across(c(-year,-time_period,-value))) %>%
    arrange(year, .by_group = TRUE) %>%
    mutate(pct_change = ifelse((year - lag(year)) > 1, NA,(value/lag(value)-1)*100))%>%
    select(-year)
  
  
  qtrly_df <- df %>%
    filter(grepl("Q",time_period)) %>%
    mutate(fmonth = (as.numeric(substr(time_period,6,6))*3)-2) %>%
    mutate(date  = as.Date(str_c(fmonth,"-01-",substr(time_period,1,4)),"%m-%d-%Y")) %>%
    select(-fmonth) %>%
    group_by(across(c(-date,-time_period,-value))) %>%
    arrange(date, .by_group = TRUE) %>%
    mutate(pct_change = ifelse((date - lag(date,4)) > 367, NA, # if 4 lags is gt 367 days, NA
                               (value/lag(value,4)-1)*100)) %>%
    select(-date)
  
  
  monthly_df <- df %>%
    filter(grepl("M",time_period)) %>%
    mutate(date  = as.Date(str_c(substr(time_period,6,7),"-01-",substr(time_period,1,4)),"%m-%d-%Y")) %>%
    group_by(across(c(-date,-time_period,-value))) %>%
    arrange(date, .by_group = TRUE) %>%
    mutate(pct_change = ifelse((date - lag(date,12)) > 367, NA, # if 12 lags is gt 367 days, NA
                               (value/lag(value,12)-1)*100))%>%
    select(-date)
  
  final_df <- rbind(annual_df,qtrly_df,monthly_df)
  
  return(final_df)
  
  
}


# Calculates an index based off of any indicator's 2016 or Dec 2016 value = 100
## NOTE: this function assumes that the data already has "% Change" calculated
calc_2016Base <- function(df) {
  
  get2016base <- df %>%
    subset(time_period == "2016" | time_period == "2016M12") %>%
    rename(value2016 = value) %>%
    select(-time_period,-pct_change)
  
  df_final <- df %>%
    left_join(get2016base) %>%
    mutate(value_2016index = (value/value2016)*100) %>%
    select(-value2016)
  
  return(df_final)
  
}


# Run IMF API functions ---------------------------------------------
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

#Paths assume that we also have imf-data-api-pull-r repository that is saved in the same parent folder as this current repo. If not, adjust path accordingly.
my_path <- dirname(getwd())
imf_path <- (sprintf("%s/imf-data-api-pull-r/", my_path))

# Run IMF API Functions folder, which will save the functions for use
source(sprintf("%sIMF API Functions.R", imf_path))

#Create Table that just has PCPS as the Database ID
DB_table <- data.frame(database_id  = "PCPS")

#Get Dataset Name from Function
imf_DB_NamesID   <- getIMF_DbInfo(DB_table)

#Get all Metadata for this Dataset
imf_metadata <- getIMF_MetaData(imf_DB_NamesID)

#Create Table with list of all available commodity indicators
imf_comm_indicators <- imf_metadata %>%
  subset(metadata_type == "COMMODITY") %>%
  distinct(db_code,attr_code) %>%
  rename(database_id = db_code,
         indicator_code = attr_code)

#Run function to get all PCPS data.
##Note: this will likely take a long time to run.
imf_data <- getIMF_Data(imf_comm_indicators)



## Remove Extra Columns, join DB and Country Names ------------------
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

imf01 <- imf_data %>%
  select(-c("rownum","UNIT_MULT","TIME_FORMAT","REF_AREA")) %>%
  left_join(imf_DB_NamesID,by = "database_id") %>%
  mutate(source = str_c("IMF: ",database_name)) %>%
  rename(value = OBS_VALUE) %>%
  mutate(year = substr(TIME_PERIOD, 1,4)) %>%
  mutate(sub_period = ifelse(FREQ == "M" & nchar(TIME_PERIOD) == 6, str_c("M0",substr(TIME_PERIOD,6,6)),
                             ifelse(FREQ == "M" & nchar(TIME_PERIOD) == 7, str_c("M", substr(TIME_PERIOD,6,7)),
                                    ifelse(FREQ == "Q", substr(TIME_PERIOD,6,7),
                                           "")))) %>%
  mutate(time_period = str_c(year,sub_period)) %>%
  mutate(value = as.numeric(value))


## Reshape Metadata -------------------------------------------------
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

#Keep only rows of information that we want
metadata_filtered <- imf_metadata %>%
  mutate(metadata_attribute = ifelse(metadata_attribute == "INDICATOR_UINT","INDICATOR_UNIT",metadata_attribute)) %>%
  filter(grepl('COMMODITY_FULL_NAME|COMMODITY_SHORT_NAME|INDICATOR_FULL_NAME|INDICATOR_SHORT_NAME|INDICATOR_UNIT|TYPE_FULL_NAME|TYPE_NAME|UNIT_MEASURE_NAME',metadata_attribute))

#Get list of Metadata Types in the Data
all_metadataTypes <- metadata_filtered %>%
  distinct(metadata_type)

#Make new, separate tables for each type
for (i in rownames(all_metadataTypes)) {
  type <- as.character(all_metadataTypes[i,"metadata_type"])
  
  
  metaD_byType <- metadata_filtered %>%
    subset(metadata_type == type) %>%
    
    pivot_wider(names_from = metadata_attribute,
                values_from = metadata_value) %>%
    subset(select = c(-metadata_type))
  
  #assign(paste("metadata", type, sep="_"), metaD_byType)
  
  imf01 <- imf01 %>%
    left_join(metaD_byType,   by = c("database_id" = "db_code", setNames("attr_code", type )))
  
}


rm(metadata_filtered,all_metadataTypes,metaD_byType)


#This step identifies Indicators that have Monthly Frequencies, as well as others
##Since we will be putting these in Tableau, that data viz can aggregate by year if necessary
##So we will ONLY keep the most frequent (i.e. Monthly, if available)
avail_FREQ <- imf01 %>%
  distinct(database_id,COMMODITY,UNIT_MEASURE,FREQ) %>%
  group_by(across(c(-FREQ))) %>%
  mutate(n = 1) %>%
  pivot_wider(names_from = FREQ, 
              values_from = n) %>%
  subset(!is.na(M)) %>% # Delete those where Month is not present
  subset(!is.na(A) & !is.na(Q)) %>% #Delete those where ONLY Month appears
  mutate(delQ = ifelse(!is.na(Q),"Yes",NA_character_)) %>%
  mutate(delA = ifelse(!is.na(A),"Yes",NA_character_)) %>%
  select(-c("Q","A","M"))

imf02 <- imf01 %>%
  left_join(avail_FREQ, by = c("database_id","COMMODITY","UNIT_MEASURE")) %>%
  mutate(del_row = ifelse(FREQ == "Q" & !is.na(delQ),1,
                          ifelse(FREQ == "A" & !is.na(delA),1,0))) %>%
  subset(del_row == 0) %>%
  rename(frequency = FREQ)



### Calculate % Change and Index -----------------------------------------------

imf03 <- calc_pct_change_year(imf02)
imf04   <- calc_2016Base(imf03)
imf_final   <- imf04 %>%
  rename(Commodity_Name = COMMODITY_SHORT_NAME,
         Unit = UNIT_MEASURE) %>%
  select(all_of(final_cols))
  
  

##Write final table as CSV  ----------------------------------------------------

write.csv(imf_final,str_c(folder_finalData, "IMF.csv"), row.names = FALSE)

rm(imf_Indicators_Compress,imf_Indicators_Full)
rm(avail_FREQ,imf01,imf02,imf03,imf04,imf05)




