# PI-ML-HENRY

Individual project related to Machine Learning

# Introduction

This repository contains the ETL work required for HENRY's individual project, which consisted of cleaning up a streaming services' datasets, and making queries via FastAPI to said database.

## Contents

The ETL folder contains both an ipynb notebook detailing the ETL work performed on the raw provided datasets, as well as a Python script version of said notebook used for automation in the setup script provided within this repo.

## Setup script

You may think of it as an installer of sorts, works on Linux and Windows (Git Bash required, WSL works too), it will download the necessary csv files needed to perform the required FastAPI queries, will also set up a provisional venv inside of this repo folder and install required Python libraries.

### Usage

The setup script must first be given execution permissions, via `chmod u+x setup.sh`, and make sure you are inside of this repos' root directory first after cloning `cd pi-ML-HENRY`.
You must provide at least one argument to the script, either one of `raw` or `pre`, if you wish to download the raw datasets used in this project, to which you may optionally pass a second argument of `etl` if you wish for the script to automatically setup a venv and install required libraries, as well as run the etl python script inside the ETL directory for you.
You may also pass a first argument of either `processed` or `post`, in case you wish to download the already treated datasets, ready to receive your queries.

## Uvicorn

This project uses Uvicorn to easilly and quickly make the required queries through the FastAPI library. To start, make sure the data directory contains the file "movies_db.csv", which you can download via the link provided below, or use the setup script to download it by passing the corresponding arguments to it.
After that, make sure you have fastapi and uvicorn installed, which you can easily do by entering:
`pip3 install -U -r requirements.txt`
in your shell.

After that, type:

`uvicorn app.main:app`

in your shell or terminal, and enter the following address in your web browser: "https://localhost:8000/docs".
From this interface, you may do the required queries.

#### Link to data files folder: [Drive](https://drive.google.com/drive/folders/1PftPdEGvb-byhq2BYr1cB5KJQcmJhZeW?usp=sharing)

Inside the data folder you'll find 2 gzipped tar files: raw_data.tgz and processed_data.tgz. The first, "raw", contains a data folder with the datasets provided by HENRY for the project before any transformations were applied to them, while the second, "processed", is the result of all the ETL operations contained in ETL/etl.ipynb applied to the raw csv files, resulting in 2 simple csv files: movies_db.csv and processed_ratings.csv, both of which were used throughout the rest of the project.

The setup.sh script provided in this repo should allow you to download either or both of them at your discretion.
