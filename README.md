
# Backgroud
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
# Objective
Build a ETL pipeline to generate tables that facilitate user behaviour analytics
# Steps
* Design Data Warehouse schema
* Extract raw json data from S3 
* Process data on Amason Elastic MapReduce (EMR) cluster with Spark
* Write the final tables in S3
# Database Schema
![alt text](https://github.com/limengunique/Postgres-ETL/blob/master/Untitled%20drawing.png?raw=true)
# How to run it
* Downloads data and store it in your S3 bucket
* Explore data and develop the demo Spark ETL pipeline with demo.ipynb and an example dataset. Change input and output path to your own S3 path
* Transform demo.ipynb to final etl.py
* Launch EMR cluster and submit the script and AWS credential file dl.cfg with your own AWS access key
* Check results in S3
