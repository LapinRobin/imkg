# Data Engineering Project: Memes

Welcome to the Memes Data Engineering Project! This repository is dedicated to the pipeline that ingests, cleanses, transforms, and enhances raw meme data from Know Your Meme, Imgflip and Wikidata.

## Table of Contents

1. [Introduction](#introduction)
2. [Ingesting](#ingesting)
3. [Cleansing](#cleansing)
4. [Transforming](#transforming)
5. [Enhancing](#enhancing)
6. [Getting Started](#getting-started)

## Introduction

Questions formulated for analyzing the meme dataset:

1. **Top 3 movies which generated the most memes**: Find out which movies have produced the most memes in our dataset.

2. **Is there more animals than humans used in memes?**: Discover whether animals are more prevalent than humans in memes.

## Pipeline

![Pipeline overview](Pipeline.png)

## Ingesting

In the initial phase of our data engineering project, we set up a pipeline to bring raw meme data into our landing zone, which is a MongoDB database. To do this, we created two scraper to collect data from KnowYourMeme and Imgflip.

The `KYM_Scraper` and `Imgflip_Scraper` are `scrapy` projects that scrapes the Know Your Meme and Imgflip websites for memes and their associated data.
The scrapper use a `Redis` database to store the URLs to be scraped and store the scraped data in a `MongoDB` database.
Relationships between memes (e.g. parent-child) are stored in a `PostgreSQL` database before updating the `MongoDB` documents.

Here is the link to the tools : 

https://github.com/meme-schievous/kym-scrapper
https://github.com/meme-schievous/imgflip-scraper

## Cleansing

For wrangling the data, we use different operators in airflow :

| OPERATORS         | URL DOCUMENTATION                                                                                        |
| ----------------- | -------------------------------------------------------------------------------------------------------- |
| PythonOperator    | https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html                         |
| PapermillOperator | https://airflow.apache.org/docs/apache-airflow/1.10.10/howto/operator/papermill.html                     |
| DockerOperator    | https://airflow.apache.org/docs/apache-airflow/1.10.10/_api/airflow/operators/docker_operator/index.html |
| BashOperator      | https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/bash.html                           |

Then, for cleaning the data, we use the file `notebook/Cleaning.ipynb` and the operator `PapermillOperator` to run the file.
It  cleans both KYM data, by removing non-meme data type, removing duplicated fields and grouping content text, and Imgflip data, by changing URL structure.

## Transforming

For transforming the data, we use multiple files, namely `notebook/Extract_Children.ipynb`, `notebook/Extract_Parent.ipynb`, `notebook/Extract_Sibling.ipynb`, `notebook/Extract_Taxonomy.ipynb` and `notebook/Extract_Seed.ipynb` (THis one is only used to get the seed memes from Wikidata). With those files, we actually extract all the children, parents, siblings and taxonomy of memes to then use them later for the graph.

## Enhancing

For enhancing the data, we use the files `notebook/Enrich_Tags.ipynb.ipynb` and `notebook/Enrich_Text.ipynb.ipynb`, to enrich the data with Dbpedia Spotlight data to improve our dataset.
We have not yet implemented the enriching of the vision of our dataset with the GoogleApi since we did not have the means to pay for the service.

## Production

Now that we have the dataset, we can use the tool Virtuoso to query it and try and answer the introduction questions we formulated in the introduction (link to the tool on our github : https://github.com/meme-schievous/virtuoso).

We also used Neo4j to have a visual representation of the different graphs and query the graph :

![Graph overview](KymGraph.png)

## Futur improvements

We're also planning enhancements for the dataset, making it more robust and insightful. Updates will be provided soon.

## Getting Started

If you'd like to run the code for this project, follow these steps:

1. Make sure you have Docker installed.

2. Run the following commands:

   ```bash
   mkdir -p ./dags ./logs ./plugins ./config
   echo -e "AIRFLOW_UID=$(id -u)" > .env
   docker compose build
   docker compose up airflow-init
   docker compose -f docker-compose.yaml up
   ```

