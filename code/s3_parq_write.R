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

# read .csv pivot long
# change code back to data
i = "st4_1hr_2023070101_2024010100_eaa.txt"
ddd <- read_csv(i) %>%
  pivot_longer(!1:5, names_to = "time", values_to = "rain_mm") %>%
  mutate(time = ymd_h(str_sub(time,2,11))) %>%
  mutate (year = year(time), month = month(time), day = day(time), hour = hour(time)) %>%
  relocate(rain_mm, .after = last_col()) 

# write your data set to .parq
ddd |>
  group_by(year,month) |>
  write_dataset(path = s3_path,
                format = "parquet")

# check and see what is there
#bucket$ls("year=2023",recursive = FALSE)

