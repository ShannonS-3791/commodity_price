#zArchive.
#formerly used script

##Create a table based on going 'up' the chain (once pivoted works both ways)
#### This one is 'simple' i.e. does not pull forward items when there is a gap
minus01 <- mapping01 %>%
  rename(item_u1 = up1,
         weight_u1 = W_up1) %>%
  select(-W_dn1)


for (j in 1:maxLevel) {
  jLevel   = paste0("item_u",j)
  jLevelUp = paste0("item_u",j+1)
  wLevel   = paste0("weight_u",j)
  wLevelUp = paste0("weight_u",j+1)
  
  minus01 <- minus01 %>%
    left_join(mapping01[,c("item0","up1","W_up1")], by = setNames(nm = jLevel , "item0")) %>%
    rename({{jLevelUp}} := up1) %>%
    mutate({{wLevelUp}} := W_up1 * get(!!wLevel)) %>%
    select(!W_up1)
}

minus_final <- minus01  %>%
  pivot_longer(cols = -item0,
               names_to = c(".value","level"),
               names_sep = "\\_") %>%
  mutate(level = as.numeric(substr(level,2,2))) %>%
  mutate(source = current_source)


assign(paste(current_source,"minus_final", sep="_"), minus_final)

sheetName_simple <- str_c(current_source, " Levels - Simple")
sheet_write(minus_final, url_final, sheet = sheetName_simple)
