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
if(!require(writexl))install.packages("writexl")
library(data.table)
if(!require(openxlsx))install.packages("openxlsx")

# Establish Parameters ----------------------------------------------
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

date_start <- '1992'
date_end <- '2022'

prelim_cols <- c("source","commodity_name","unit","time_period","value")
final_cols <- c(prelim_cols,"pct_change","value_2019index")

folder_Raw <- "Raw Data/" # For Mac
# folder_Raw <- "Raw Data\\" # For Windows
folder_Input <- "Input Files/"
folder_Final <- "Final Data/"


# Names and Definitions ---------------------------------------------------------------
###++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

itemName <- read.csv(sprintf('%sItem Names.csv',folder_Input)) %>%
  pivot_longer(cols = -item_name,
               names_to = "source",
               values_to = "Item_Code") %>%
  mutate(source = ifelse(grepl("wb",source),"WB","IMF")) %>%
  rename("Item Name" = item_name)

write_xlsx(itemName,sprintf('%sItem Names and Info.xlsx',folder_Final))

# Parent-Child Mapping Tables ---------------------------------------------------------
# Create full table of all relations and levels using simple table. The table lists
#### each commodity, its immediate parent, and weight within parent index.
###++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
indexSources <- c("WB","IMF")
maxLevel <- 5

# Create blank data frame
filled_index_table <- data.frame()

#Loop through list of Index Sources (World Bank, IMF, etc.)
for (i in 1:length(indexSources)) {
  current_source <- indexSources[i]
  
# Get Table of item names filtered to only this source's commodities
  itemNames_sOnly <- itemName %>%
    filter(source == current_source) %>%
    select(-source)
  
  ##Read in Mapping Sheet
  mapping01 <- read.csv(str_c(folder_Input,current_source, " Parent-Child Mapping.csv")) %>%
    rename(item0 = "item_code",
           up1 = "parent_code",
           W_up1 = "weight_within_parent_index",
           W_dn1 = "parent_total_weight") %>%
    
    # Join names table to get full names
    left_join(itemNames_sOnly, by = c("item0" = "Item_Code")) %>%
    
    rename(item0_name = `Item Name`) %>%
    select(c(item0,item0_name,up1,W_up1,W_dn1))
  
# Create a 'filled' table based on going 'down' the indices
  
  ## Create mapping table that will be iteratively joined.
  mapping02 <- mapping01 %>%
    rename(dn1      = item0,
           dn1_name = item0_name) %>%
   select(-W_dn1)
  
  plus01 <- mapping01 %>%
    select(c(item0,item0_name)) %>%
    left_join(mapping02, by = c("item0" = "up1")) %>%
    rename(item_d1   = dn1,
           weight_d1 = W_up1,
           name_d1   = dn1_name) %>%
    #Carries forward final items, which are 100% of themselves.
    mutate(weight_d1 = ifelse(is.na(item_d1),     1, weight_d1)) %>%
    mutate(name_d1   = ifelse(is.na(item_d1), item0_name,   name_d1)) %>%
    mutate(item_d1   = ifelse(is.na(item_d1), item0,   item_d1))
  
  
  
  ## Now run a loop that will join each item with the 'next' level down. Since this is a one-to-many, the number of rows will expand. The 'maxLevel' is set before function begins. May want to run tests to see how many 'levels' are actually needed.
  
  maxLevelm1 <- maxLevel - 1
  
  for (k in 1:maxLevelm1) {
    # Set names of variables that we will both look for / use in calculations and create.
    kLevel    = paste0("item_d",k)
    kLevelDn  = paste0("item_d",k+1)
    kName     = paste0("name_d",k)
    kNameDn   = paste0("name_d",k+1)
    wkLevel   = paste0("weight_d",k)
    wkLevelDn = paste0("weight_d",k+1)
    
    plus01 <- plus01 %>%
      left_join(mapping02, by = setNames(nm = kLevel,"up1"))%>%
      rename({{kLevelDn}} := dn1) %>%
      rename({{kNameDn}}  := dn1_name) %>%
      mutate({{wkLevelDn}}  := W_up1 * get(!!wkLevel)) %>%
      select(!W_up1)%>%
      #Carry items without lower levels to subsequent levels to show full breakdown
      mutate({{kNameDn}}   := ifelse(is.na(get(!!kLevelDn)), get(!!kName),   get(!!kNameDn)))%>%
      mutate({{wkLevelDn}} := ifelse(is.na(get(!!kLevelDn)), get(!!wkLevel), get(!!wkLevelDn))) %>%
      mutate({{kLevelDn}}  := ifelse(is.na(get(!!kLevelDn)), get(!!kLevel),  get(!!kLevelDn))) 
  }
  
  ## Pivot full chart. This creates list of "Item 0" with all of the respective other indexes and commodities that are X degrees of separation away. (X is created based on the number "k" assigned in previous loop.)
  plus02 <- plus01 %>%
    pivot_longer(cols = -c('item0','item0_name'),
                 names_to = c(".value","level"),
                 names_sep = "\\_") %>%
    distinct() %>%
    mutate(level = as.numeric(substr(level,2,2))) # Removes "d"; leaving only number For less than 10 levels only.
  

  # Now we will also create a 'wide' version. This is used with Tableau's 'wide' version to create dynamic groupings.
  
  plus03 <- data.frame()
  
  for (l in 1:maxLevel) {
    ## Get one level (aka 'Degrees of Separation') at a time
    plus02_section <- plus02 %>%
      filter(level == l)
    
    last_Wvarname <- str_c("weight_d",l)
    #last_namvarname <- str_c("name_d",l)
    last_varname <- str_c("item_d",l)
    
    ## Create a wide list that only has up to the current 'level' (i.e. d1, d2, etc.)
    wides <- plus01 %>%
      #select(item0:last_varname,"name_d1",last_namvarname) %>%
      select(item0:last_Wvarname) %>%
      ## Get rid of duplicates (there will be many, especially at first because there are many branches below each index)
      distinct()
    
    ## Iteratively join the wide files.
    plus_section <- plus02_section %>%
      left_join(wides, by = c("item0" = "item0",
                              "item0_name" = "item0_name",
                              setNames(nm = "item",last_varname))) %>%
      mutate({{last_varname}}  := item)
    
    ## Bind the rows (each commodity + increasing level of 'degrees of separation')
    plus03 <- bind_rows(plus03,plus_section)
    
  }

  ## Once table is created, re-calculate weights to match respective group.
  ### Spot-check: the indicated "level" should now show to be 100% of the d1 weight. 
  #         i.e., if level = 2 then weight_d2 = 1 (100%)
  for (m in 1:maxLevel) {
    m_weight <- paste0("weight_d",m)
    
    plus03 <- plus03 %>%
      mutate({{m_weight}} := ifelse(!is.na(get(!!m_weight)), weight / get(!!m_weight),  get(!!m_weight)))
  }
  
  ## Rename for Tableau
  plus_final <- plus03 %>%
    rename("Item 0" = item0,
           "Item 0 Name" = item0_name,
           "Degrees of Separation" = level,
           "Item X" = item,
           "Item X Name" = name,
           "Weight" = weight) %>%
    mutate(source = current_source)
  
      ###Save each source file under separate name for review.
        assign(paste(current_source,"plus_final", sep="_"), plus_final)
  
  ## Bind rows
  filled_index_table <- bind_rows(filled_index_table,plus_final)
  
}

# Move 'Source' Column to front
final_index_table <- filled_index_table %>%
  select(source,everything())

write_xlsx(final_index_table,sprintf('%sALL Levels - Filled.xlsx',folder_Final))


# Functions: % Change & Index ---------------------------------------------------------
# + calculations to be run on each dataframe (later)
###++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

calc_pct_change_year <- function(df) {
  
# Annual data. Expect a string number that is only YYYY.
  annual_df <- df %>%
    filter(!grepl("M|Q",time_period)) %>%
    mutate(year = as.numeric(time_period)) %>%
    group_by(across(c(-year,-time_period,-value))) %>%
    arrange(year, .by_group = TRUE) %>%
    mutate(pct_change = ifelse((year - lag(year)) > 1, NA,(value/lag(value)-1)*100))%>%
    select(-year)
  
# Quarterly Data. time_period must contain 'Q'
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
  
## Monthly data. time_period must contain 'M'
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


# Calculates an index based off of the average value of the assigned year (value = 100)
calc_YYYY_Base <- function(df, yr) {
  yIndex_name <- str_c("value_",yr,"index")
  
  getbase <- df %>%
    #Get first four digits of the time_period as the year
    mutate(yyyy = substr(time_period,1,4)) %>%
    subset(yyyy == yr) %>%
    group_by_at(vars(-time_period,-value)) %>%
    summarise(valueyyyy = mean(value)) %>%
    select (-yyyy) # Delete for proper merge
  
  
  df_final <- df %>%
    left_join(getbase) %>%
    mutate({{yIndex_name}} := (value/valueyyyy)*100) %>%
    select(-valueyyyy)
  
  return(df_final)
  
}


# Run IMF API functions ---------------------------------------------
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
runIMF_API <- FALSE

if (runIMF_API){
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
write.csv(imf_comm_indicators,str_c(folder_Raw, "IMF_PCPS_Indicators.csv"), 
          row.names = FALSE)
write.csv(imf_DB_NamesID,str_c(folder_Raw, "IMF_PCPS_DBinfo.csv"), 
          row.names = FALSE)
write.csv(imf_metadata,str_c(folder_Raw, "IMF_PCPS_Metadata.csv"), 
          row.names = FALSE)
} else{ #Otherwise, just import previously saved 'raw' data
  
  imf_data <-  read.csv(str_c(folder_Raw, "IMF_PCPS.csv"))
  imf_DB_NamesID <- read.csv(str_c(folder_Raw, "IMF_PCPS_DBinfo.csv"))
  imf_metadata <- read.csv(str_c(folder_Raw, "IMF_PCPS_Metadata.csv"))
  
}
## Remove Extra Columns, join DB Names ------------------------------
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

imf01 <- imf_data %>%
  select(-c("rownum","UNIT_MULT","TIME_FORMAT","REF_AREA")) %>%
  left_join(imf_DB_NamesID,by = "database_id") %>%
  mutate(source = "IMF",
         source_full = str_c("IMF: ",database_name)) %>%
  rename(value = OBS_VALUE) %>%
  mutate(year = substr(TIME_PERIOD, 1,4)) %>%
  mutate(sub_period = ifelse(FREQ == "M" & nchar(TIME_PERIOD) == 6, str_c("M0",substr(TIME_PERIOD,6,6)),
                             ifelse(FREQ == "M" & nchar(TIME_PERIOD) == 7, str_c("M", substr(TIME_PERIOD,6,7)),
                                    ifelse(FREQ == "Q", substr(TIME_PERIOD,6,7),
                                           "")))) %>%
  mutate(time_period = str_c(year,sub_period)) %>%
  mutate(value = ifelse(value == "0", NA, as.numeric(value)))


## Reshape Metadata -------------------------------------------------
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

#Keep only rows of information that we want
metadata_filtered <- imf_metadata %>%
  mutate(metadata_attribute = ifelse(metadata_attribute == "INDICATOR_UINT","INDICATOR_UNIT",metadata_attribute)) %>%
  filter(grepl('COMMODITY_FULL_NAME|COMMODITY_SHORT_NAME|COMMODITY_DEFINITION|INDICATOR_FULL_NAME|INDICATOR_SHORT_NAME|INDICATOR_UNIT|TYPE_FULL_NAME|TYPE_NAME|UNIT_MEASURE_NAME',metadata_attribute))

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

## We will calculate the index for ALL commodities in a later step. 
## For now, we can drop any commodity that is in USD *AND* Index
avail_unit <- imf01 %>%
  distinct(database_id, COMMODITY, UNIT_MEASURE) %>%
  group_by(across(c(-UNIT_MEASURE))) %>%
  mutate(n = 1) %>%
  pivot_wider(names_from = UNIT_MEASURE, 
              values_from = n) %>%
  subset(!is.na(USD) & !is.na(IX)) %>%
  mutate(delIX = "Yes") %>%
  select("database_id","COMMODITY","delIX")

imf02 <- imf01 %>%
  left_join(avail_FREQ, by = c("database_id","COMMODITY","UNIT_MEASURE")) %>%
  left_join(avail_unit, by = c("database_id","COMMODITY"))%>%
  mutate(del_row = ifelse(FREQ == "Q" & !is.na(delQ),1,
                   ifelse(FREQ == "A" & !is.na(delA),1,
                   ifelse(UNIT_MEASURE == "IX" & !is.na(delIX),1, 0)))) %>%
  subset(del_row == 0) %>%
  rename(frequency = FREQ) %>%
  subset(UNIT_MEASURE != "PC_CP_A_PT" & UNIT_MEASURE != "PC_PP_PT" & COMMODITY != "LMICS") %>%
  mutate(unit = ifelse(UNIT_MEASURE == "IX", "Index (2016 = 100)",UNIT_MEASURE_NAME)) %>%
  select(source, source_full, 
         commodity_code = COMMODITY, commodity_name = COMMODITY_SHORT_NAME, commodity_definition = COMMODITY_DEFINITION,
         unit, time_period, value)

# For verifying Percent Change Calculations
imf_pctA <- imf01 %>%
  subset(UNIT_MEASURE == "PC_CP_A_PT" & FREQ == "M") %>%
  select(COMMODITY,time_period,IMFpctchg = value)


### Calculate % Change and Index -----------------------------------------------
#### Each calculation is run from the same data table, then joined.
#.     This is to avoid problems with sum / mean / etc & grouping when running the calculations.
#.     Joins should work automatically since most columns are the same name.

imf03a   <- calc_pct_change_year(imf02)
imf03b   <- calc_YYYY_Base(imf02, 2010)
imf03c  <- calc_YYYY_Base(imf02, 2016)

imf_final <- imf03a %>%
  left_join(imf03b)%>%
  left_join(imf03c)

chk_pctA <- imf_final %>%
  select(commodity_code,time_period,value,pct_change) %>%
  left_join(imf_pctA, by=c("commodity_code" = "COMMODITY","time_period" = "time_period")) %>%
  mutate(pct_chg_chk = ((round(pct_change,2) - round(IMFpctchg,2))/round(pct_change,2)) < .01 ) %>%
  filter(pct_chg_chk == FALSE)
  

rm(DB_table,imf_DB_NamesID,imf_metadata,imf_comm_indicators)
rm(avail_FREQ,imf01,imf02,imf03a,imf03b,imf03c)
rm(chk_pctA,imf_pctA)



# World Bank Data Processing ----------------------------------------
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


wb_P01 <- read_excel(sprintf("%sCMO-Historical-Data-Monthly.xlsx",folder_Raw),sheet = "Monthly Prices") 
wb_I01 <- read_excel(sprintf("%sCMO-Historical-Data-Monthly.xlsx",folder_Raw),sheet = "Monthly Indices") %>%
  rename(`...1` = 1)

wb_P02 <- wb_P01 %>% row_to_names(row_number = 4) %>%
  rename("time_period" = 1)


pink_sheet_cMap <- wb_P02[1:2,-c(1)] %>%
  mutate(type = ifelse(row_number() == 1,"unit","commodity_code")) %>%
  pivot_longer(cols = -type,
               names_to = "commodity_name",
               values_to = "step1") %>%
  pivot_wider(id_cols = "commodity_name",
              names_from = "type",
              values_from = "step1") %>%
  mutate(unit = ifelse(unit == "(2010=100)","Index (2010=100)",gsub("\\(|\\)","",as.character(unit))))

wb_P03 <- wb_P02[-c(1,2),] %>%
  mutate(year = as.numeric(substr(time_period,1,4))) %>%
  subset(year >= date_start) %>%
  select(-year) %>%
  pivot_longer(cols = -time_period,
               names_to = "commodity_name",
               values_to = "value") %>%
  left_join(pink_sheet_cMap, by = "commodity_name")
  

####Headers are in different rows --> create and process table of just headers

pink_sheet_iMap1 <- wb_I01[5:8,] %>%
  pivot_longer(cols = everything(),
               names_to = "row",
               values_to = "header") %>% 
  mutate(rowNUMBER = as.numeric(str_replace_all(string = row, pattern = "\\.", replacement = ""))) %>%
  drop_na() %>%
  arrange(rowNUMBER) %>% select(-rowNUMBER) %>%
  pivot_wider(names_from = "row", values_from = "header") %>%
  add_column(`...1` = "time_period", .before = 1)

wb_I02 <- rbind(pink_sheet_iMap1, wb_I01[-(1:8),]) %>% #Run this only to check names
  row_to_names(row_number = 1) 

pink_sheet_iMap2 <- wb_I02[1,-c(1)] %>%
  pivot_longer(cols = everything(),
               names_to = "commodity_name",
               values_to = "commodity_code")
  

wb_I03 <- wb_I02 %>%
  mutate(year = as.numeric(substr(time_period,1,4))) %>%
  subset(year >= date_start) %>% drop_na(time_period) %>% 
  select(-year) %>%
  pivot_longer(cols = -time_period,
               names_to = "commodity_name",
               values_to = "value") %>%
  mutate(unit = "Index (2010 = 100)") %>%
  left_join(pink_sheet_iMap2, by = "commodity_name")

wb01 <- rbind(wb_P03,wb_I03) %>%
  arrange(commodity_name, time_period) %>%
  mutate(value = ifelse(value == "0", NA, as.numeric(value))) %>%
  mutate(source = "WB",
         source_full = "World Bank: Commodity Prices (Pink Sheet)",
         commodity_definition = "")
  

### Calculate % Change and Index -----------------------------------------------
#### Each calculation is run from the same data table, then joined.
#.     This is to avoid problems with sum / mean / etc & grouping when running the calculations.
#.     Joins should work automatically since most columns are the same name.

wb03a   <- calc_pct_change_year(wb01)
wb03b   <- calc_YYYY_Base(wb01, 2010)
wb03c  <- calc_YYYY_Base(wb01, 2016)

wb_final <- wb03a %>%
  left_join(wb03b)%>%
  left_join(wb03c)


rm(wb_I01,wb_I02,wb_I03,wb_P01,wb_P02,wb_P03,wb01,wb02,wb03a,wb03b,wb03c)
rm(pink_sheet_cMap,pink_sheet_iMap1,pink_sheet_iMap2)



# Combine IMF and World Bank ----------------------------------------
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

commodity_prices01 <- rbind(imf_final,wb_final)

 
 commodity_prices_final <- commodity_prices01 %>%
   ungroup() %>%
   select(source, commodity_code, unit, time_period, value, pct_change, value_2010index, value_2016index) 
   

 write.csv(commodity_prices_final,sprintf("%sCommodity Prices.csv",folder_Final), 
           row.names = FALSE, na="")
 
 

## Create Metadata Table  -------------------------------------------
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

 ### Commodity Metadata goes to sheet with names, indices, etc.
 wb_definitions <- read.csv(sprintf("%sWB Definitions Map.csv",folder_Input))
 
 # These are inferred commodities (not present in data)
 inferred_wb <- wb_definitions %>%
   filter(substr(commodity_code,1,8) == "INFERRED") %>%
   mutate(source_full = "World Bank: Commodity Prices (Pink Sheet)",
          unit = NA_character_) %>%
   select(-methodological_notes) %>%
   rename(commodity_definition = definition)
 
 inferred_imf <- data.frame(commodity_code = "INFERRED_OTHERFOOD",
                            commodity_name = "Other Food, Inferred",
                            source = "IMF",
                            source_full = "IMF: Primary Commodity Price System",
                            commodity_definition = "Other Food is reported to make up 30.22% of the Food Index; and consists of eight commodities, each with given weights within this index. However, there is no accompanying data calculating this index value." ,
                            unit = NA_character_)
 
 inferred_comm <- rbind(inferred_wb,inferred_imf)
 
 commodity_info <- commodity_prices01 %>%
   ungroup() %>%
   select(source, source_full, commodity_code, commodity_name, 
          commodity_definition, unit) %>%
   distinct() %>%
   left_join(wb_definitions[,c("source","commodity_code","definition")], 
             by = c("source","commodity_code")) %>%
   mutate(commodity_definition = ifelse(source== "WB",definition,commodity_definition)) %>%
   select(-definition) %>%
   rbind(inferred_comm)
 
 write.csv(commodity_info,sprintf("%sCommodity Metadata.csv",folder_Final), 
           row.names = FALSE, na="")



 
