# Web-Crawler
Web crawler made using Spark RDDs and Dataframes.

## Highlights
- Built a web crawler using **Spark** which starts with a URL, each URL having a new dataset of URLs to crawl  
- Used **RDD**s and transformations and outputted tuples of the form (url, indegree)

## Problem Statement
Problem Statement can be found [here](ps.pdf).  

## Other details
2 other programs using spark are also uploaded:  
- [stockreturns](stockreturns.py)  
- [wordcount](wordcount.py)  

Data is uploaded in the csv files.  

Command to see output in one merged file:  
$ cat webcrawler/part-*|sort > out_q3.txt  
