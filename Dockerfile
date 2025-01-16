FROM rocker/tidyverse:latest

RUN mkdir -p /code
RUN mkdir -p /data

WORKDIR /code

COPY .Renviron .
COPY /code/s3_parq_write_compose.R .
COPY /code/install_packages.R .
COPY /data/st4_conus.2025011613.01h.txt /code/st4_conus.2025011613.01h.txt
COPY /data/texas_buffer_spatial_join.csv /code/texas_buffer_spatial_join.csv

RUN Rscript install_packages.R

CMD Rscript s3_parq_write_compose.R