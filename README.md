# Web-Crawler
Web crawler made using Spark RDDs and Dataframes.

Problem Statement can be found [here](ps.pdf).  

2 other programs using spark are also uploaded:  
- [stockreturns](stockreturns.py)  
- [wordcount](wordcount.py)  

Data is uploaded in the csv files.  

Command to see output in one merged file:  
$ cat webcrawler/part-*|sort > out_q3.txt  
