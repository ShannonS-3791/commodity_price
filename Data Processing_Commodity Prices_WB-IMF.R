#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Commodity Prices + Index - World Bank and IMF
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


# Load Packages -----------------------------------------------------
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

library(jsonlite)
library(tidyverse)
library(janitor)
library(dplyr)
library(readxl)
library(googlesheets4)
library(data.table)

# Establish Parameters ----------------------------------------------
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

date_start <- '2010'
date_end <- '2022'

prelim_cols <- c("source","commodity_name","unit","time_period","value")
final_cols <- c(prelim_cols,"pct_change","value_2019index")

folder_Raw <- "Raw Data/" # For Mac
# folder_Raw <- "Raw Data\\" # For Windows

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


# Calculates an index based off of any indicator's 2019 or Dec 2019 value = 100
## NOTE: this function assumes that the data already has "% Change" calculated
calc_2019Base <- function(df) {
  
  get2019base <- df %>%
    subset(time_period == "2019" | time_period == "2019M12") %>%
    rename(value2019 = value) %>%
    select(-time_period,-pct_change)
  
  df_final <- df %>%
    left_join(get2019base) %>%
    mutate(value_2019index = (value/value2019)*100) %>%
    select(-value2019)
  
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

write.csv(imf_data,str_c(folder_Raw, "IMF_PCPS.csv"), row.names = FALSE)
write.csv(imf_comm_indicators,str_c(folder_Raw, "IMF_PCPS_Indicators.csv"), row.names = FALSE)

## Remove Extra Columns, join DB Names ------------------------------
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
  rename(frequency = FREQ) %>%
  subset(UNIT_MEASURE != "PC_CP_A_PT" & UNIT_MEASURE != "PC_PP_PT") %>%
  rename(commodity_name = COMMODITY_SHORT_NAME,
         unit = UNIT_MEASURE_NAME)

imf03 <- imf02[,prelim_cols]

### Calculate % Change and Index -----------------------------------------------

imf04 <- calc_pct_change_year(imf03)
imf05   <- calc_2019Base(imf04)
imf_final   <- imf05  %>%
  select(all_of(final_cols))
  
  

rm(DB_table,imf_DB_NamesID,imf_metadata,imf_comm_indicators)
rm(avail_FREQ,imf01,imf02,imf03,imf04,imf05)



# World Bank Data Processing ----------------------------------------
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


wb_P01 <- read_excel(sprintf("%sCMO-Historical-Data-Monthly.xlsx",folder_Raw),sheet = "Monthly Prices") 
wb_I01 <- read_excel(sprintf("%sCMO-Historical-Data-Monthly.xlsx",folder_Raw),sheet = "Monthly Indices") %>%
  rename(`...1` = 1)

wb_P02 <- wb_P01 %>% row_to_names(row_number = 4) %>%
  rename("time_period" = 1)


pink_sheet_units <- wb_P02[1,-c(1)] %>%
  pivot_longer(cols = everything(),
               names_to = "commodity",
               values_to = "unit") %>%
  mutate(unit = ifelse(unit == "(2010=100)","Index",gsub("\\(|\\)","",as.character(unit))))

wb_P03 <- wb_P02[-c(1,2),] %>%
  mutate(year = as.numeric(substr(time_period,1,4))) %>%
  subset(year >= date_start) %>%
  select(-year) %>%
  pivot_longer(cols = -time_period,
               names_to = "commodity_name",
               values_to = "value") %>%
  left_join(pink_sheet_units, by = c("commodity_name" = "commodity"))
  
  
####Headers are in different rows --> create and process table of just headers

pink_sheet_headers <- wb_I01[5:8,] %>%
  pivot_longer(cols = everything(),
               names_to = "row",
               values_to = "header") %>% 
  mutate(rowNUMBER = as.numeric(str_replace_all(string = row, pattern = "\\.", replacement = ""))) %>%
  drop_na() %>%
  arrange(rowNUMBER) %>% select(-rowNUMBER) %>%
  pivot_wider(names_from = "row", values_from = "header") %>%
  add_column(`...1` = "time_period", .before = 1)

wb_I02 <- rbind(pink_sheet_headers, wb_I01[-(1:8),]) %>% #Run this only to check names
  row_to_names(row_number = 1) %>%
  mutate(year = as.numeric(substr(time_period,1,4))) %>%
  subset(year >= date_start) %>% drop_na(time_period) %>% 
  select(-year) %>%
  pivot_longer(cols = -time_period,
               names_to = "commodity_name",
               values_to = "value") %>%
  mutate(unit = "Index")

wb01 <- rbind(wb_P03,wb_I02) %>%
  arrange(commodity_name, time_period) %>%
  mutate(value = as.numeric(value)) %>%
  mutate(source = "World Bank: Commodity Prices (Pink Sheet)")
  

wb02 <- calc_pct_change_year(wb01)
wb03   <- calc_2019Base(wb02)
wb_final   <- wb03  %>%
  select(all_of(final_cols))

rm(wb_I01,wb_I02,wb_P01,wb_P02,wb_P03,wb01,wb02,wb03)
rm(pink_sheet_headers,pink_sheet_units)

commodity_prices <- rbind(imf_final,wb_final)


## Write final table as CSV  ----------------------------------------------------

write.csv(commodity_final,str_c(folder_finalData, "Commodity Prices.csv"), row.names = FALSE)

url <- "https://docs.google.com/spreadsheets/d/1PdawDF3d92w_P36K3QWyAuq6-5YzUrDopwAw6WIhXeQ"
sheet_write(commodity_prices, ss= url, sheet = "Full Sheet")  

## Write final table as CSV  ----------------------------------------------------
#Test Gas Index: Hypothesis: each index is average of commodities.

nrg_map <- read_sheet("https://docs.google.com/spreadsheets/d/1uObAgCEkOrAwXAuyaWuI9pFNyBfXVwqrFYVHlxcmbiY/", 
           sheet = "Sheet1")
nrgComm <- c("Coal, Australian **","Coal, South African **",
             "Crude oil, average","Crude oil, Brent","Crude oil, Dubai","Crude oil, WTI",
             "Natural gas index","Natural gas, Europe **","Natural gas, US","Liquefied natural gas, Japan",
             "Energy")
nrgIndex01 <- wb_final %>%
  subset(commodity_name %in% nrgComm) %>%
  left_join(nrg_map, by = "commodity_name") %>%
  arrange(commodity_code)

index_2010 <- nrgIndex01 %>%
  mutate(year = substr(time_period,1,4)) %>%
  subset(year == "2010") %>%
  select(commodity_name, commodity_code, value) %>%
  group_by(commodity_name, commodity_code) %>%
  summarise(avg_2010 = mean(value))

nrgIndex02 <- nrgIndex01 %>%
  left_join(index_2010) %>%
  mutate (value_2010 = value/avg_2010*100) %>%
    pivot_wider(id_cols = "time_period",
                names_from = "commodity_code", values_from = "value_2010") %>%
  mutate(coal_avg = rowMeans(select(.,COAL_AUS,COAL_SAFRICA), na.rm = TRUE ),
         oil_avg  = rowMeans(select(.,CRUDE_BRENT,CRUDE_DUBAI,CRUDE_WTI), na.rm = TRUE ),
         ngas_avg = rowMeans(select(.,NGAS_EUR,NGAS_US,NGAS_JP), na.rm = TRUE ),
         nrg_calc = coal_avg*4.65741856629374/100 +
                  	CRUDE_PETRO*84.57082988045220/100 +
                    iNATGAS*10.77175155325410/100)


# Parent-Child Mapping Tables ---------------------------------------------------------
###++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

url_IndexCalcs <- "https://docs.google.com/spreadsheets/d/1TB2kSK7yAyYPFP0j-e-uIKptOea3pwrB8Gon9toruQ8"

itemName <- read_sheet(url_IndexCalcs, sheet = "Item Names")
WB_pc_mapping01 <- read_sheet(url_IndexCalcs, sheet = "WB Parent-Child Mapping") %>%
  rename(wb_item0 = "WB Item Code",
         wb_up1 = "WB Parent Code",
         wbW_up1 = "WB Weight within Parent Index",
         wbW_dn1 = "WB Parent Total Weight") %>%
  select(c(wb_item0,wb_up1,wbW_up1,wbW_dn1))


wb_minus <- WB_pc_mapping01 %>%
  rename(wb_u1 = wb_up1,
         wbW_u1 = wbW_up1) 



for (i in 1:5) {
  jLevel   = paste0("wb_u",i)
  jLevelUp = paste0("wb_u",i+1)
  wLevel   = paste0("wbW_u",i)
  wLevelUp = paste0("wbW_u",i+1)
  
  wb_minus <- wb_minus %>%
  left_join(WB_pc_mapping01[,c("wb_item0","wb_up1","wbW_up1")], by = setNames(nm = jLevel , "wb_item0")) %>%
    rename({{jLevelUp}} := wb_up1) %>%
  mutate({{wLevelUp}} := wbW_up1 * get(!!wLevel)) %>%
    select(!wbW_up1)
}

wb_minus02 <- wb_minus %>%
  select(-wbW_dn1) %>%
  pivot_longer(cols = -wb_item0,
               names_to = c(".value","level"),
               names_sep = "\\_")

chk_calcs_u <- wb_minus02 %>%
  group_by(wb,level) %>%
  summarise(portion = sum(wbW))



##Other Direction
WB_cp <- WB_pc_mapping01 %>%
  rename(wb_dn1 = wb_item0) %>%
  select(-wbW_dn1)

wb_plus <- WB_pc_mapping01 %>%
  select(c(wb_item0)) %>%
  #Combine the items in wb_item0 and wb_up1 so every item is considered.
  # mutate(pv = "all") %>%
  # pivot_longer(cols = -pv,
  #              values_to = "wb_item0") %>%
  # distinct(wb_item0) %>%
  left_join(WB_cp, by = c("wb_item0" = "wb_up1")) %>%
  rename(wb_d1 = wb_dn1,
         wbW_d1 = wbW_up1) %>%
  #Carries forward final items, which are 100% of themselves.
  mutate(wbW_d1 = ifelse(is.na(wb_d1),1,wbW_d1))%>%
  mutate(wb_d1  = ifelse(is.na(wb_d1),wb_item0,wb_d1))

for (i in 1:4) {
  jLevel   = paste0("wb_d",i)
  jLevelDn = paste0("wb_d",i+1)
  wLevel   = paste0("wbW_d",i)
  wLevelDn = paste0("wbW_d",i+1)
  
wb_plus <- wb_plus %>%
  left_join(WB_cp, by = setNames(nm = jLevel,"wb_up1"))%>%
  rename({{jLevelDn}} := wb_dn1) %>%
  #rename({{wLevelDn}} := wbW_up1) %>%
  mutate({{wLevelDn}}  := wbW_up1 * get(!!wLevel)) %>%
  select(!wbW_up1)%>%
  #Carry items without lower levels to subsequent levels to show full breakdown
  mutate({{jLevelDn}} := ifelse(is.na(get(!!jLevelDn)), get(!!jLevel), get(!!jLevelDn)))%>%
  mutate({{wLevelDn}} := ifelse(is.na(get(!!wLevelDn)), get(!!wLevel), get(!!wLevelDn)))
}

wb_plus02 <- wb_plus %>%
  pivot_longer(cols = -wb_item0,
               names_to = c(".value","level"),
               names_sep = "\\_") %>%
  distinct() %>%
  mutate(level = as.numeric(substr(level,2,2))) 

chk_calcs_d <- wb_plus02 %>%
  group_by(`Item 0`,`level`) %>%
  summarise(portion = sum(wbW))


wb_plus_names <- wb_plus %>%
  select(starts_with("wb_"))

wb_plus03 <- data.frame()

for (i in 1:5) {
  wb_plus02_section <- wb_plus02 %>%
    filter(level == i)
  
  last_varname <- str_c("wb_d",i)
  names <- wb_plus_names %>%
    select(wb_item0:last_varname) %>%
    distinct()
  

  wb_plus_section <- wb_plus02_section %>%
    left_join(names, by = c("wb_item0" = "wb_item0", setNames(nm = "wb",last_varname)))%>%
    mutate({{last_varname}}  := wb)
  
  #assign(paste("wb_plus_names", i, sep="_"), names)
  
  wb_plus03 <- bind_rows(wb_plus03,wb_plus_section)
  

}




wb_plus_final <- wb_plus03 %>%
  rename("Item 0" = wb_item0,
         "Degrees of Separation" = level,
         "Item X" = wb,
         "Weight" = wbW)



sheet_write(wb_plus_final, url_IndexCalcs, sheet = "WB Levels - Filled")




