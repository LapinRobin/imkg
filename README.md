# Data Engineering Project: Memes

Welcome to the Memes Data Engineering Project! This repository is dedicated to the pipeline that ingests, cleanses, transforms, and enhances raw meme data from Know Your Meme. Additionally, it provides queries to analyze the meme dataset.

## Table of Contents

1. [Ingesting](#ingesting)
2. [Cleansing](#cleansing)
3. [Transforming](#transforming)
4. [Enhancing](#enhancing)
5. [Queries](#queries)
6. [Getting Started](#getting-started)

## Ingesting

In the initial phase of our data engineering project, we set up a pipeline to bring raw meme data into our landing zone, which is a MongoDB database. The raw data is sourced from a file named `kym_raw_data`, which is extracted from the Know Your Meme website.

## Cleansing

In this stage, we're currently working on the cleansing process to ensure the data is of high quality. Check back here soon for more updates!

## Transforming

Stay tuned! We're actively developing the data transformation process to prepare the meme data for further analysis. 

## Enhancing

We're also planning enhancements for the dataset, making it more robust and insightful. Updates will be provided soon.

## Queries

We offer the following queries for analyzing the meme dataset:

1. **Top 3 movies which generated the most memes**: Find out which movies have produced the most memes in our dataset.

2. **Is there more animals than humans used in memes?**: Discover whether animals are more prevalent than humans in memes.

## Getting Started

If you'd like to run the code for this project, follow these steps:

1. Make sure you have Docker installed.

2. Run the following commands:

   ```bash
   mkdir -p ./dags ./logs ./plugins ./config
   echo -e "AIRFLOW_UID=$(id -u)" > .env
   docker compose up airflow-init
   docker compose -f docker-compose.yaml up
   ```

   