# CS881 Project

# increase Java memory before calling RJDBC library
options(java.parameters = "-Xmx2g")
library(RJDBC)

# Load Hive JDBC driver
# modify the path to match your computer Rstudio libraries path (WIN Install below, might be different in other systems)
hivedrv <- JDBC("org.apache.hive.jdbc.HiveDriver",c(list.files("C:/Users/<yourwinuser>/AppData/Local/rstudio/spark/Cache/spark-2.0.1-bin-hadoop2.7/jars/",pattern="jar$",full.names=T)))

#Connect to Hive service and query data (assumes data has been loaded in Hive table "healthdata")
hivecon <- dbConnect(hivedrv, "jdbc:hive2://127.0.0.1:10000/default","maria_dev","maria_dev")
query = "select * from healthdata"
hres <- dbGetQuery(hivecon, query)

# ANALYSIS STARTS HERE ########################
# Exploratory analysis

# clean column names and make them simpler. If you used the Ambari GUI to load data the variable names 
# might be too long and need this procedure
x<-unlist(strsplit(colnames(hres), "[.]"))
x[x != "healthdata"]
library(stringr)
colnames(hres)<-str_replace_all(x[x != "healthdata"]," ","_")
colnames(hres)
# 
# [1] "health_service_area"                 "hospital_county"                     "operating_certificate_number"       
# [4] "facility_id"                         "facility_name"                       "age_group"                          
# [7] "zip_code_-_3_digits"                 "gender"                              "race"                               
# [10] "ethnicity"                           "length_of_stay"                      "type_of_admission"                  
# [13] "patient_disposition"                 "discharge_year"                      "ccs_diagnosis_code"                 
# [16] "ccs_diagnosis_description"           "ccs_procedure_code"                  "ccs_procedure_description"          
# [19] "apr_drg_code"                        "apr_drg_description"                 "apr_mdc_code"                       
# [22] "apr_mdc_description"                 "apr_severity_of_illness_code"        "apr_severity_of_illness_description"
# [25] "apr_risk_of_mortality"               "apr_medical_surgical_description"    "payment_typology_1"                 
# [28] "payment_typology_2"                  "payment_typology_3"                  "attending_provider_license_number"  
# [31] "operating_provider_license_number"   "other_provider_license_number"       "birth_weight"                       
# [34] "abortion_edit_indicator"             "emergency_department_indicator"      "total_charges"                      
# [37] "total_costs"                  

# location quick analysis
table(hres$health_service_area)  # all data is for the state of NY for 8 Service Areas
#      Capital/Adiron     Central NY   Finger Lakes  Hudson Valley    Long Island  New York City  Southern Tier     Western NY 
# 2911         167367         158331         146577         245945         339277        1092224          30201         163927 

# types of admission
table(hres$type_of_admission) # 6 categories
# Elective     Emergency       Newborn Not Available        Trauma        Urgent 
# 447280       1488031        227100          1173          6394        176782 

# distribution of data by ethnicity
table (hres$ethnicity)
# Multi-ethnic Not Span/Hispanic  Spanish/Hispanic           Unknown 
#         8693           1954468            278587            105012

# distribution by race
table(hres$race)
# Black/African American           Multi-racial             Other Race                  White 
#                 444944                  22380                 544559                1334877

# Q1: What is the distribution of admission types per Service area
mytable<-table(hres$health_service_area,hres$type_of_admission)
apply( 
  prop.table(mytable, 1)*100, # row percentages 
  2, 
  function(u) sprintf( "%.1f%%", u ) 
) 

library(gmodels)
CrossTable(hres$health_service_area,hres$type_of_admission, prop.chisq=FALSE, 
           prop.r = TRUE, prop.c = FALSE, prop.t = FALSE, format="SPSS", digits = 1,
           chisq = FALSE)

# Cell Contents
#   |-------------------------|
#   |                   Count |
#   |             Row Percent |
#   |-------------------------|
#   
#   Total Observations in Table:  2346760 
# 
#                            | hres$type_of_admission 
#   hres$health_service_area |      Elective |     Emergency |       Newborn | Not Available |        Trauma |        Urgent |     Row Total | 
#   -------------------------|---------------|---------------|---------------|---------------|---------------|---------------|---------------|
#                            |          444  |         2015  |            0  |            2  |            1  |          449  |         2911  | 
#                            |         15.3% |         69.2% |          0.0% |          0.1% |          0.0% |         15.4% |          0.1% | 
#   -------------------------|---------------|---------------|---------------|---------------|---------------|---------------|---------------|
#             Capital/Adiron |        38193  |        98170  |        13825  |           57  |         1155  |        15967  |       167367  | 
#                            |         22.8% |         58.7% |          8.3% |          0.0% |          0.7% |          9.5% |          7.1% | 
#   -------------------------|---------------|---------------|---------------|---------------|---------------|---------------|---------------|
#                 Central NY |        34662  |        93033  |        15298  |           70  |         1529  |        13739  |       158331  | 
#                            |         21.9% |         58.8% |          9.7% |          0.0% |          1.0% |          8.7% |          6.7% | 
#   -------------------------|---------------|---------------|---------------|---------------|---------------|---------------|---------------|
#               Finger Lakes |        32898  |        81139  |        13392  |           14  |            1  |        19133  |       146577  | 
#                            |         22.4% |         55.4% |          9.1% |          0.0% |          0.0% |         13.1% |          6.2% | 
#   -------------------------|---------------|---------------|---------------|---------------|---------------|---------------|---------------|
#              Hudson Valley |        54806  |       155649  |        22344  |           73  |          131  |        12942  |       245945  | 
#                            |         22.3% |         63.3% |          9.1% |          0.0% |          0.1% |          5.3% |         10.5% | 
#   -------------------------|---------------|---------------|---------------|---------------|---------------|---------------|---------------|
#                Long Island |        58003  |       228501  |        28012  |          735  |         2161  |        21865  |       339277  | 
#                            |         17.1% |         67.3% |          8.3% |          0.2% |          0.6% |          6.4% |         14.5% | 
#   -------------------------|---------------|---------------|---------------|---------------|---------------|---------------|---------------|
#              New York City |       178676  |       720299  |       115864  |          138  |         1415  |        75832  |      1092224  | 
#                            |         16.4% |         65.9% |         10.6% |          0.0% |          0.1% |          6.9% |         46.5% | 
#   -------------------------|---------------|---------------|---------------|---------------|---------------|---------------|---------------|
#              Southern Tier |         5082  |        19170  |         2760  |            0  |            0  |         3189  |        30201  | 
#                            |         16.8% |         63.5% |          9.1% |          0.0% |          0.0% |         10.6% |          1.3% | 
#   -------------------------|---------------|---------------|---------------|---------------|---------------|---------------|---------------|
#                 Western NY |        44516  |        90055  |        15605  |           84  |            1  |        13666  |       163927  | 
#                            |         27.2% |         54.9% |          9.5% |          0.1% |          0.0% |          8.3% |          7.0% | 
#   -------------------------|---------------|---------------|---------------|---------------|---------------|---------------|---------------|
#               Column Total |       447280  |      1488031  |       227100  |         1173  |         6394  |       176782  |      2346760  | 
#   -------------------------|---------------|---------------|---------------|---------------|---------------|---------------|---------------|
# 

# Chi-squared test to verify homogeneity of proportions.
# Just in case we downsample proportionally to avoid too much power in the Chi-Squared test because of the large sample size
set.seed(1)
X <- rmultinom(1, 4000, mytable)
dim(X) <- dim(mytable)

chisq.test(X) # difference in proportions is real!

#      Pearson's Chi-squared test
# 
# data:  X
# X-squared = 136.68, df = 40, p-value = 1.709e-12

# Q2: New York provides the highest proportion of NewBorn admissions while Long Island the lowest Is there a difference
# that could explain that?
NYdata<-hres[hres$health_service_area == "New York City",]
CrossTable(NYdata$race,NYdata$type_of_admission, prop.chisq=FALSE, 
           prop.r = TRUE, prop.c = FALSE, prop.t = FALSE, format="SPSS", digits = 1,
           chisq = FALSE)

LIdata<-hres[hres$health_service_area == "Long Island",]
CrossTable(LIdata$race,LIdata$type_of_admission, prop.chisq=FALSE, 
           prop.r = TRUE, prop.c = FALSE, prop.t = FALSE, format="SPSS", digits = 1,
           chisq = FALSE)

# plot differences between races and city and see if there is something there
# prepare data
library(dplyr)
a<-hres %>% select(race,health_service_area,type_of_admission) %>%
  filter(type_of_admission == "Newborn" & health_service_area %in% c("Long Island","New York City")) %>%
  group_by(health_service_area,race) %>%
  summarise (n = n()) %>%
  mutate(freq = n / sum(n))
library(ggplot2)
library(scales)

# Answer: Other races than "White" drive the increase of Newborn in New York City!!
ggplot(a, aes(race, freq)) +   
  geom_bar(aes(fill = health_service_area), position = "dodge", stat="identity") +
  scale_y_continuous(labels=percent) +
  labs(x="\nRace Category", y="Percentage of NewBorn Admissions\n", title="Newborn admissions per Race - Long Island vs New York\n") +
  theme(plot.title = element_text(size=18))+
  theme(axis.title.x = element_text(size=14))+
  theme(axis.title.y = element_text(size=14))
  
