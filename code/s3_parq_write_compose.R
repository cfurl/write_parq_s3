# Goal of the script is to transform a .csv -> .parq and store it in an s3 bucket
# Setup I'm aiming for is .grb2 -> .csv -> .parq -> stored in s3
# This needs to be lightweight, run every hour to build a .parq database/lake in s3

# following along tutorial to spin up s3 bucket
# https://www.gormanalysis.com/blog/connecting-to-aws-s3-with-r/

# created an s3 bucket called "stg4-eaa", 'us-east-2'
# passwords and keys are currently in my .Renviron file

library("aws.s3")
library("arrow")
library("dplyr")
library("lubridate")
library("tidyr")
library("readr")
library("stringr")
library("tictoc")

# make sure you can connect to your bucket and open SubTreeFileSystem
bucket <- s3_bucket("stg4-eaa")

# list everything in your bucket in a recursive manner
bucket$ls(recursive = TRUE)

# you can also identify and individual folder to examine
#bucket$ls("year=2023",recursive = TRUE)

# identify path where you will be writing the .parq files
s3_path <- bucket$path("")

#aoi_texas_buffer<-read_csv(".//data//texas_buffer_spatial_join.csv")
aoi_texas_buffer<-read_csv("texas_buffer_spatial_join.csv")

# list files that start with st4 and ends with .txt
# should list st4_conus.2025011613.01h.txt
#raw_grib2_text = list.files('.//data',pattern = "^st4_conus.*.txt$",full.names=FALSE)
raw_grib2_text = list.files(pattern = "^st4_conus.*.txt$",full.names=FALSE)
#i = "st4_1hr_2023070101_2024010100_eaa.txt"

for (h in raw_grib2_text) {

#h = "st4_conus.2025011613.01h.txt"
    
  name <- h |>
    str_replace("st4_conus.", "t") |>
    str_replace(".01h.txt","")
  
  #aa<-read_csv(paste0(".//data//",h), col_names=FALSE) %>%
  aa<-read_csv(h, col_names=FALSE) %>%
    setNames(c("x1","x2","x3","x4","center_lon","center_lat",name)) %>%
    select(-x1,-x2,-x3,-x4)   
  
  # joins by "center_lon", "center_lat"
  bb<- left_join(aoi_texas_buffer,aa,by=NULL)%>%
     pivot_longer(!1:5, names_to = "time", values_to = "rain_mm") %>%
      mutate(time = ymd_h(str_sub(time,2,11))) %>%
      mutate (year = year(time), month = month(time), day = day(time), hour = hour(time)) %>%
     relocate(rain_mm, .after = last_col()) 
    }  



#ddd <- read_csv(i) %>%
#  pivot_longer(!1:5, names_to = "time", values_to = "rain_mm") %>%
#  mutate(time = ymd_h(str_sub(time,2,11))) %>%
#  mutate (year = year(time), month = month(time), day = day(time), hour = hour(time)) %>%
#  relocate(rain_mm, .after = last_col()) 

# write your data set to .parq
#ddd |>
 bb|>
  group_by(year,month) |>
  write_dataset(path = s3_path,
                format = "parquet")

# check and see what is there
#bucket$ls("year=2023",recursive = FALSE)

