FROM rocker/tidyverse:latest

RUN mkdir -p /code
RUN mkdir -p /data

WORKDIR /code

COPY .Renviron .
COPY /code/s3_parq_write.R .
COPY /code/install_packages.R .
COPY /data/st4_1hr_2023070101_2024010100_eaa.txt /code/st4_1hr_2023070101_2024010100_eaa.txt

RUN Rscript install_packages.R

CMD Rscript s3_parq_write.R